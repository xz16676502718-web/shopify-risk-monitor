#!/usr/bin/env python3
"""
多店铺 Shopify 风控中台

功能：
1. 多店铺并发巡检
2. 检查最近 48 小时有更新的订单
3. 监控风险等级
4. 监控地址校验状态
5. 记录订单历史状态
6. 状态发生变化时同步 Google Sheets
7. 新订单自动新增
8. 已存在订单由 Google Sheets 接收端更新原行
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ===========================================================================
# 配置
# ===========================================================================

BASE_DIR = Path(__file__).resolve().parent

STORES_FILE = BASE_DIR / "stores.json"
DB_FILE = BASE_DIR / "orders.db"

API_VERSION = "2026-07"

# 检查最近多少小时发生过更新的订单
HOURS_BACK = 48

# 同时检查多少个店铺
MAX_WORKERS = 10

# Google Apps Script Webhook
GAS_WEBHOOK_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbwZmUawxHNCYP4w3IDchs-ape2pfTf4Ua9XHhqCv94vh8ur4HyGasvUUx-Rhp3qeyxM"
    "/exec"
)


# ===========================================================================
# 中文映射
# ===========================================================================

RISK_PRIORITY = {
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
    "NONE": 0,
    "PENDING": 0,
}


RISK_MAP_CN = {
    "HIGH": "高风险",
    "MEDIUM": "中风险",
    "LOW": "低风险",
    "NONE": "无风险",
    "PENDING": "待评估",
}


ADDR_MAP_CN = {
    "NO_ISSUES": "无异常",
    "VALIDATED": "无异常",
    "WARNING": "地址警告",
    "ERROR": "地址错误",
    "INCOMPLETE": "地址不完整",
    "UNVERIFIED": "未验证地址",
    "CORRECTED": "地址已修正",
}


# ===========================================================================
# Shopify GraphQL
# ===========================================================================

ORDERS_GQL = """
query RecentOrders($query: String!, $cursor: String) {
  orders(
    first: 50
    query: $query
    after: $cursor
    sortKey: UPDATED_AT
    reverse: true
  ) {
    pageInfo {
      hasNextPage
      endCursor
    }

    edges {
      node {
        id
        name
        createdAt
        updatedAt
        riskLevel
        tags

        risk {
          assessments {
            riskLevel
          }
        }

        shippingAddress {
          validationResultSummary
        }
      }
    }
  }
}
"""


# ===========================================================================
# 日志
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    logger.info(
        "[%s] %s",
        datetime.now(timezone.utc).strftime("%H:%M:%S"),
        msg,
    )


# ===========================================================================
# 时间
# ===========================================================================

def build_shopify_query(hours: int = HOURS_BACK) -> str:
    """
    查询最近 N 小时有更新的订单。
    """

    time_threshold = (
        datetime.now(timezone.utc)
        - timedelta(hours=hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    return f"updated_at:>{time_threshold}"


def format_utc_time(iso_str: str) -> str:
    """
    格式化 Shopify UTC 时间。
    """

    if not iso_str:
        return ""

    return (
        iso_str
        .replace("T", " ")
        .replace("Z", "")
        .split(".")[0]
    )


# ===========================================================================
# SQLite 数据库
# ===========================================================================

def init_db(db_path: Path = DB_FILE) -> sqlite3.Connection:
    """
    初始化订单状态数据库。
    """

    conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_orders (
            shop_domain    TEXT NOT NULL,
            order_name     TEXT NOT NULL,
            risk_level     TEXT NOT NULL DEFAULT '',
            address_status TEXT NOT NULL DEFAULT '',
            created_at     TEXT NOT NULL DEFAULT '',
            updated_at     REAL NOT NULL,
            PRIMARY KEY (shop_domain, order_name)
        )
        """
    )

    conn.commit()

    return conn


def get_previous_order_state(
    conn: sqlite3.Connection,
    shop_domain: str,
    order_name: str,
) -> dict[str, str] | None:
    """
    获取订单上一次保存的状态。
    """

    cursor = conn.execute(
        """
        SELECT
            risk_level,
            address_status,
            created_at
        FROM risk_orders
        WHERE shop_domain = ?
          AND order_name = ?
        """,
        (
            shop_domain,
            order_name,
        ),
    )

    row = cursor.fetchone()

    if not row:
        return None

    return {
        "risk_level": str(row[0] or ""),
        "address_status": str(row[1] or ""),
        "created_at": str(row[2] or ""),
    }


def upsert_risk_order(
    conn: sqlite3.Connection,
    shop_domain: str,
    order_name: str,
    risk_level: str,
    address_status: str,
    created_at: str,
    *,
    commit: bool = True,
) -> None:
    """
    保存订单当前状态。
    """

    now_ts = time.time()

    conn.execute(
        """
        INSERT INTO risk_orders
            (
                shop_domain,
                order_name,
                risk_level,
                address_status,
                created_at,
                updated_at
            )
        VALUES (?, ?, ?, ?, ?, ?)

        ON CONFLICT(shop_domain, order_name)
        DO UPDATE SET
            risk_level     = excluded.risk_level,
            address_status = excluded.address_status,
            created_at     = excluded.created_at,
            updated_at     = excluded.updated_at
        """,
        (
            shop_domain,
            order_name,
            risk_level,
            address_status,
            created_at,
            now_ts,
        ),
    )

    if commit:
        conn.commit()


# ===========================================================================
# Shopify Token
# ===========================================================================

_token_cache: dict[str, dict[str, float | str]] = {}

_token_lock = threading.Lock()


def load_stores(
    stores_path: Path = STORES_FILE,
) -> list[dict[str, str]]:
    """
    读取 stores.json。
    """

    if not stores_path.exists():
        raise FileNotFoundError(
            f"找不到店铺配置文件: {stores_path}"
        )

    with stores_path.open(
        encoding="utf-8"
    ) as f:
        data = json.load(f)

    stores: list[dict[str, str]] = []

    for idx, item in enumerate(
        data,
        start=1,
    ):

        name = (
            str(item.get("name", ""))
            .strip()
            or f"店铺{idx}"
        )

        domain = (
            str(item.get("domain", ""))
            .strip()
        )

        client_id = (
            str(item.get("client_id", ""))
            .strip()
        )

        client_secret = (
            str(item.get("client_secret", ""))
            .strip()
        )

        stores.append(
            {
                "name": name,
                "domain": domain,
                "client_id": client_id,
                "client_secret": client_secret,
            }
        )

    return stores


def fetch_access_token(
    domain: str,
    client_id: str,
    client_secret: str,
) -> str:

    url = (
        f"https://{domain}"
        "/admin/oauth/access_token"
    )

    for attempt in range(1, 4):

        try:

            response = requests.post(
                url,
                headers={
                    "Content-Type":
                        "application/x-www-form-urlencoded"
                },
                data={
                    "grant_type":
                        "client_credentials",
                    "client_id":
                        client_id,
                    "client_secret":
                        client_secret,
                },
                timeout=30,
                verify=False,
            )

            response.raise_for_status()

            body = response.json()

            access_token = body.get(
                "access_token"
            )

            if not access_token:
                raise RuntimeError(
                    "Shopify 没有返回 access_token"
                )

            expires_in = int(
                body.get("expires_in")
                or 86399
            )

            with _token_lock:

                _token_cache[domain] = {
                    "token": access_token,
                    "expires_at":
                        time.time()
                        + expires_in,
                }

            return access_token

        except requests.RequestException as exc:

            if attempt < 3:
                time.sleep(2)
            else:
                raise exc

    raise RuntimeError(
        f"无法取得 Shopify Access Token: {domain}"
    )


def get_access_token(
    store: dict[str, str],
) -> str:

    domain = store["domain"]

    with _token_lock:

        cached = _token_cache.get(
            domain
        )

        if (
            cached
            and time.time()
            < float(
                cached["expires_at"]
            ) - 60
        ):
            return str(
                cached["token"]
            )

    return fetch_access_token(
        domain,
        store["client_id"],
        store["client_secret"],
    )


# ===========================================================================
# Shopify GraphQL 请求
# ===========================================================================

def shopify_graphql(
    domain: str,
    token: str,
    query: str,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any]:

    url = (
        f"https://{domain}"
        f"/admin/api/{API_VERSION}"
        "/graphql.json"
    )

    headers = {
        "Content-Type":
            "application/json",
        "X-Shopify-Access-Token":
            token,
    }

    payload: dict[str, Any] = {
        "query": query
    }

    if variables:
        payload["variables"] = variables

    for attempt in range(1, 4):

        try:

            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=30,
                verify=False,
            )

            response.raise_for_status()

            body = response.json()

            if body.get("errors"):
                raise RuntimeError(
                    f"GraphQL 错误: {body['errors']}"
                )

            return body.get("data") or {}

        except requests.RequestException as exc:

            if attempt < 3:
                time.sleep(2)
            else:
                raise exc

    return {}


# ===========================================================================
# 风控判断
# ===========================================================================

def highest_risk_level(
    node: dict[str, Any],
) -> str:

    direct_level = str(
        node.get("riskLevel")
        or "NONE"
    ).upper()

    risk = node.get("risk") or {}

    assessments = (
        risk.get("assessments")
        or []
    )

    best = direct_level

    best_score = RISK_PRIORITY.get(
        best,
        0,
    )

    for item in assessments:

        level = str(
            item.get("riskLevel")
            or "NONE"
        ).upper()

        score = RISK_PRIORITY.get(
            level,
            0,
        )

        if score > best_score:

            best = level
            best_score = score

    return best


def evaluate_order(
    node: dict[str, Any],
) -> tuple[bool, str, str]:

    risk_level = highest_risk_level(
        node
    )

    shipping = (
        node.get("shippingAddress")
        or {}
    )

    address_status = str(
        shipping.get(
            "validationResultSummary"
        )
        or ""
    ).upper()

    hit_risk = (
        risk_level == "HIGH"
    )

    hit_address = (
        address_status
        in {
            "ERROR",
            "WARNING",
            "INCOMPLETE",
            "UNVERIFIED",
        }
    )

    if hit_risk or hit_address:

        return (
            True,
            risk_level,
            address_status,
        )

    return (
        False,
        risk_level,
        address_status,
    )


def build_reason(
    risk_level: str,
    address_status: str,
) -> str:

    reasons: list[str] = []

    if risk_level == "HIGH":

        reasons.append(
            "高风险"
        )

    if address_status in {
        "ERROR",
        "WARNING",
        "INCOMPLETE",
        "UNVERIFIED",
    }:

        reasons.append(
            ADDR_MAP_CN.get(
                address_status,
                address_status,
            )
        )

    return ", ".join(reasons)


# ===========================================================================
# 获取最近更新订单
# ===========================================================================

def fetch_recent_orders(
    domain: str,
    token: str,
) -> list[dict[str, Any]]:

    orders: list[
        dict[str, Any]
    ] = []

    cursor: str | None = None

    query_str = build_shopify_query(
        HOURS_BACK
    )

    while True:

        variables = {
            "query":
                query_str,
            "cursor":
                cursor,
        }

        data = shopify_graphql(
            domain,
            token,
            ORDERS_GQL,
            variables,
        )

        orders_conn = (
            data.get("orders")
            or {}
        )

        edges = (
            orders_conn.get("edges")
            or []
        )

        for edge in edges:

            node = edge.get(
                "node"
            )

            if node:

                orders.append(
                    node
                )

        page_info = (
            orders_conn.get(
                "pageInfo"
            )
            or {}
        )

        if page_info.get(
            "hasNextPage"
        ):

            cursor = (
                page_info.get(
                    "endCursor"
                )
            )

        else:

            break

    return orders


# ===========================================================================
# 单店铺检查
# ===========================================================================

def check_store_worker(
    store: dict[str, str],
) -> dict[str, Any]:

    name = store["name"]

    domain = store["domain"]

    log(
        f"开始检查店铺 "
        f"{name} ({domain})..."
    )

    try:

        token = get_access_token(
            store
        )

        raw_orders = fetch_recent_orders(
            domain,
            token,
        )

    except Exception as exc:

        log(
            f"店铺 {name} "
            f"处理失败: {exc}"
        )

        return {
            "store":
                store,
            "orders":
                [],
        }


    # ----------------------------------------------------
    # 每个店铺自己的数据库连接
    # ----------------------------------------------------

    conn = init_db()

    flagged_orders: list[
        dict[str, Any]
    ] = []

    try:

        for order in raw_orders:

            order_name = str(
                order.get("name")
                or ""
            )

            if not order_name:
                continue


            should_save, risk_level, address_status = (
                evaluate_order(order)
            )


            # ------------------------------------------------
            # 获取数据库中的历史状态
            # ------------------------------------------------

            previous_state = (
                get_previous_order_state(
                    conn,
                    domain,
                    order_name,
                )
            )


            created_at = str(
                order.get("createdAt")
                or ""
            )

            updated_at = str(
                order.get("updatedAt")
                or ""
            )


            # ------------------------------------------------
            # 判断状态有没有变化
            # ------------------------------------------------

            state_changed = False

            if previous_state is None:

                # 第一次看到这个订单
                state_changed = True

            else:

                if (
                    previous_state["risk_level"]
                    != risk_level
                    or
                    previous_state["address_status"]
                    != address_status
                ):
                    state_changed = True


            # ------------------------------------------------
            # 第一次看到的订单：
            #
            # 只记录异常订单
            # ------------------------------------------------

            if previous_state is None:

                if not should_save:

                    # 正常订单也写入数据库
                    # 作为以后状态变化的基准

                    upsert_risk_order(
                        conn,
                        domain,
                        order_name,
                        risk_level,
                        address_status,
                        created_at,
                        commit=False,
                    )

                    continue


            # ------------------------------------------------
            # 后续状态没有变化
            #
            # 不需要重复发送 Google
            # ------------------------------------------------

            elif not state_changed:

                continue


            # ------------------------------------------------
            # 状态发生变化
            #
            # 包括：
            #
            # 高风险 → 无风险
            # 无风险 → 高风险
            # 地址正常 → 地址错误
            # 地址错误 → 地址正常
            # ------------------------------------------------

            reason = build_reason(
                risk_level,
                address_status,
            )


            flagged_orders.append(
                {
                    "shop_name":
                        name,

                    "shop_domain":
                        domain,

                    "order_name":
                        order_name,

                    "risk_level":
                        risk_level,

                    "address_status":
                        address_status,

                    "created_at":
                        created_at,

                    "updated_at":
                        updated_at,

                    "reason":
                        reason,

                    "tags":
                        order.get(
                            "tags"
                        )
                        or [],
                }
            )


            # ------------------------------------------------
            # 保存最新状态
            # ------------------------------------------------

            upsert_risk_order(
                conn,
                domain,
                order_name,
                risk_level,
                address_status,
                created_at,
                commit=False,
            )


        conn.commit()

    finally:

        conn.close()


    return {
        "store":
            store,

        "orders":
            flagged_orders,
    }


# ===========================================================================
# Google Sheets 同步
# ===========================================================================

def sync_to_google_sheets(
    all_orders_list: list[
        dict[str, Any]
    ],
) -> None:

    if not GAS_WEBHOOK_URL:
        return


    if not all_orders_list:

        log(
            "本轮没有需要同步到 Google Sheets 的状态变化"
        )

        return


    synced_at_utc = (
        datetime.now(
            timezone.utc
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


    rows = []


    for item in all_orders_list:

        created_at_utc = (
            format_utc_time(
                item["created_at"]
            )
        )

        tags_str = ", ".join(
            item.get("tags")
            or []
        )


        rows.append(
            [
                item["shop_name"],
                item["shop_domain"],
                item["order_name"],
                RISK_MAP_CN.get(
                    item["risk_level"],
                    item["risk_level"],
                ),
                ADDR_MAP_CN.get(
                    item["address_status"],
                    item["address_status"],
                ),
                created_at_utc,
                item["reason"],
                synced_at_utc,
                tags_str,
            ]
        )


    payload = {
        "rows": rows,
    }


    # ----------------------------------------------------
    # 发送到 Google Apps Script
    # ----------------------------------------------------

    for attempt in range(1, 4):

        try:

            resp = requests.post(
                GAS_WEBHOOK_URL,
                json=payload,
                timeout=30,
                verify=False,
            )

            resp.raise_for_status()

            log(
                f"Google Sheets 同步成功，"
                f"发送 {len(rows)} 条订单状态变化"
            )

            break

        except requests.RequestException as exc:

            if attempt < 3:

                log(
                    f"Google Sheets 同步失败，"
                    f"第 {attempt} 次，"
                    f"2 秒后重试：{exc}"
                )

                time.sleep(2)

            else:

                log(
                    f"Google Sheets 批量同步失败，"
                    f"已重试 3 次：{exc}"
                )


# ===========================================================================
# 主程序
# ===========================================================================

def run_check() -> None:

    log(
        "===== 开始本轮风控巡检 ====="
    )


    stores = load_stores()


    worker_results: list[
        dict[str, Any]
    ] = []


    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = [
            executor.submit(
                check_store_worker,
                store,
            )
            for store in stores
        ]


        for future in as_completed(
            futures
        ):

            worker_results.append(
                future.result()
            )


    all_changed_orders: list[
        dict[str, Any]
    ] = []


    for result in worker_results:

        for item in result[
            "orders"
        ]:

            all_changed_orders.append(
                item
            )


    sync_to_google_sheets(
        all_changed_orders
    )


    log(
        "===== 本轮巡检结束，"
        f"共发现 {len(all_changed_orders)} "
        "条状态变化 ====="
    )


# ===========================================================================
# 程序入口
# ===========================================================================

if __name__ == "__main__":
    run_check()
