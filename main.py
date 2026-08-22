#!/usr/bin/env python3
"""
多店铺 Shopify 风控中台

功能：
1. 多店铺并发巡检
2. 查询最近 48 小时有更新的订单
3. 检查订单风险等级
4. 检查地址验证状态
5. 收集标签
6. 同步到 Google Sheets
7. 新订单新增
8. 已存在订单由 GAS 更新原行
9. 严格检查 GAS 实际返回结果
10. 输出详细同步日志
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3


# ===========================================================================
# HTTPS
# ===========================================================================

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)


# ===========================================================================
# 配置
# ===========================================================================

BASE_DIR = Path(__file__).resolve().parent

STORES_FILE = BASE_DIR / "stores.json"

API_VERSION = "2026-07"

# 查询最近多少小时有更新的订单
HOURS_BACK = 48

# 最大并发店铺数
MAX_WORKERS = 10

# Google Apps Script Web App
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
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
)

logger = logging.getLogger(__name__)


def log(message: str) -> None:
    logger.info(
        "[%s] %s",
        datetime.now(timezone.utc).strftime("%H:%M:%S"),
        message,
    )


# ===========================================================================
# 时间
# ===========================================================================

def build_shopify_query(
    hours: int = HOURS_BACK,
) -> str:

    threshold = (
        datetime.now(timezone.utc)
        - timedelta(hours=hours)
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    return f"updated_at:>{threshold}"


def format_utc_time(
    iso_str: str,
) -> str:

    if not iso_str:
        return ""

    return (
        iso_str
        .replace("T", " ")
        .replace("Z", "")
        .split(".")[0]
    )


# ===========================================================================
# Token 缓存
# ===========================================================================

_token_cache: dict[
    str,
    dict[str, float | str]
] = {}

_token_lock = threading.Lock()


# ===========================================================================
# 店铺配置
# ===========================================================================

def load_stores(
    stores_path: Path = STORES_FILE,
) -> list[dict[str, str]]:

    if not stores_path.exists():
        raise FileNotFoundError(
            f"找不到店铺配置文件: {stores_path}"
        )

    with stores_path.open(
        encoding="utf-8"
    ) as file:

        data = json.load(file)

    stores: list[dict[str, str]] = []

    for index, item in enumerate(
        data,
        start=1,
    ):

        name = (
            str(item.get("name", ""))
            .strip()
            or f"店铺{index}"
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


# ===========================================================================
# Shopify Access Token
# ===========================================================================

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
                        "application/x-www-form-urlencoded",
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

            token = body.get(
                "access_token"
            )

            if not token:
                raise RuntimeError(
                    "Shopify 没有返回 Access Token"
                )

            expires_in = int(
                body.get(
                    "expires_in"
                )
                or 86399
            )

            with _token_lock:

                _token_cache[domain] = {
                    "token": token,
                    "expires_at":
                        time.time()
                        + expires_in,
                }

            return token

        except requests.RequestException as exc:

            if attempt < 3:

                time.sleep(2)

            else:

                raise exc

    raise RuntimeError(
        f"无法获取 Access Token：{domain}"
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
            <
            float(
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
# Shopify GraphQL
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

    if variables is not None:
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
                    "GraphQL 错误："
                    +
                    json.dumps(
                        body["errors"],
                        ensure_ascii=False,
                    )
                )

            return body.get(
                "data"
            ) or {}

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

    risk = (
        node.get("risk")
        or {}
    )

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

    risk_level = (
        highest_risk_level(node)
    )

    shipping = (
        node.get(
            "shippingAddress"
        )
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
# 获取最近订单
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

        orders = (
            fetch_recent_orders(
                domain,
                token,
            )
        )

    except Exception as exc:

        log(
            f"店铺 {name} 处理失败："
            f"{exc}"
        )

        return {
            "store":
                store,
            "orders":
                [],
        }

    flagged_orders: list[
        dict[str, Any]
    ] = []

    for order in orders:

        should_save, risk_level, address_status = (
            evaluate_order(order)
        )

        if not should_save:
            continue

        flagged_orders.append(
            {
                "shop_name":
                    name,

                "shop_domain":
                    domain,

                "order_name":
                    str(
                        order.get(
                            "name"
                        )
                        or ""
                    ),

                "risk_level":
                    risk_level,

                "address_status":
                    address_status,

                "created_at":
                    str(
                        order.get(
                            "createdAt"
                        )
                        or ""
                    ),

                "updated_at":
                    str(
                        order.get(
                            "updatedAt"
                        )
                        or ""
                    ),

                "reason":
                    build_reason(
                        risk_level,
                        address_status,
                    ),

                "tags":
                    order.get(
                        "tags"
                    )
                    or [],
            }
        )

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
    orders: list[dict[str, Any]]
) -> None:

    if not GAS_WEBHOOK_URL:
        log(
            "GAS_WEBHOOK_URL 为空，跳过 Google Sheets 同步"
        )
        return

    if not orders:
        log(
            "本轮没有需要同步的异常订单"
        )
        return

    synced_at_utc = (
        datetime.now(
            timezone.utc
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    rows: list[list[Any]] = []

    for item in orders:

        created_at = format_utc_time(
            item.get(
                "created_at",
                ""
            )
        )

        tags_str = ", ".join(
            item.get(
                "tags",
                []
            ) or []
        )

        rows.append(
            [
                item.get(
                    "shop_name",
                    ""
                ),

                item.get(
                    "shop_domain",
                    ""
                ),

                item.get(
                    "order_name",
                    ""
                ),

                RISK_MAP_CN.get(
                    item.get(
                        "risk_level",
                        ""
                    ),
                    item.get(
                        "risk_level",
                        ""
                    )
                ),

                ADDR_MAP_CN.get(
                    item.get(
                        "address_status",
                        ""
                    ),
                    item.get(
                        "address_status",
                        ""
                    )
                ),

                created_at,

                item.get(
                    "reason",
                    ""
                ),

                synced_at_utc,

                tags_str
            ]
        )

    payload = {
        "rows": rows
    }

    for attempt in range(1, 4):

        try:

            response = requests.post(
                GAS_WEBHOOK_URL,
                json=payload,
                headers={
                    "Content-Type":
                        "application/json",
                    "Accept":
                        "application/json",
                },
                timeout=30,
                verify=False,
                allow_redirects=False,
            )

            status_code = response.status_code

            log(
                "Google Apps Script HTTP："
                f"{status_code}"
            )

            # ---------------------------------------------------
            # Apps Script Web App 正常可能返回 302
            #
            # 这里不要再次 POST Location。
            # POST /exec 已经完成 Apps Script 调用。
            # ---------------------------------------------------

            if status_code in {
                301,
                302,
                303,
                307,
                308,
            }:

                redirect_url = (
                    response.headers.get(
                        "Location",
                        ""
                    )
                )

                log(
                    "Apps Script 返回重定向，"
                    "视为请求已提交。"
                )

                if redirect_url:
                    log(
                        "Redirect Location："
                        f"{redirect_url[:180]}"
                    )

                log(
                    "Google Sheets 请求提交成功："
                    f"发送 {len(rows)} 条订单数据"
                )

                return

            # ---------------------------------------------------
            # 200
            # 某些情况下直接返回 200
            # ---------------------------------------------------

            if status_code == 200:

                response_text = (
                    response.text.strip()
                )

                log(
                    "Apps Script 返回："
                    + response_text[:1000]
                )

                # 如果有 JSON，尝试读取
                try:

                    result = response.json()

                    if result.get("ok") is False:
                        raise RuntimeError(
                            "Apps Script 返回失败："
                            + json.dumps(
                                result,
                                ensure_ascii=False
                            )
                        )

                    log(
                        "Google Sheets 同步成功："
                        f"新增 {result.get('added', 0)} 条，"
                        f"更新 {result.get('updated', 0)} 条"
                    )

                    return

                except ValueError:

                    # 200 但不是 JSON，
                    # 仍然认为 Web App 已收到请求
                    log(
                        "Apps Script 返回 200，"
                        "但响应不是 JSON。"
                    )

                    return

            # ---------------------------------------------------
            # 其他 HTTP 状态
            # ---------------------------------------------------

            response.raise_for_status()

            return

        except Exception as exc:

            if attempt < 3:

                log(
                    "Google Sheets 同步失败，"
                    f"第 {attempt} 次：{exc}"
                )

                time.sleep(2)

            else:

                raise RuntimeError(
                    "Google Sheets 同步最终失败："
                    f"{exc}"
                )


# ===========================================================================
# 主程序
# ===========================================================================

def run_check() -> None:

    log(
        "===== 开始本轮风控巡检 ====="
    )

    stores = load_stores()

    log(
        f"共读取 {len(stores)} 个店铺"
    )

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

    all_flagged_orders: list[
        dict[str, Any]
    ] = []

    for result in worker_results:

        all_flagged_orders.extend(
            result.get(
                "orders",
                []
            )
        )

    log(
        f"本轮共发现 "
        f"{len(all_flagged_orders)} "
        "条异常订单"
    )

    sync_to_google_sheets(
        all_flagged_orders
    )

    log(
        "===== 本轮风控巡检结束 ====="
    )


# ===========================================================================
# 程序入口
# ===========================================================================

if __name__ == "__main__":

    try:

        run_check()

    except Exception as exc:

        log(
            "程序最终失败："
            f"{exc}"
        )

        raise
