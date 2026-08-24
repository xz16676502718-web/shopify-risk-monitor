#!/usr/bin/env python3
"""
多店铺 Shopify 风控中台 — 定时拉取高风险 / 地址异常订单并同步至 Google Sheets。
支持环境变量 (STORES_JSON / GAS_WEBHOOK_URL)、多线程并发巡检、SQLite 本地缓存及网络容错重试。
"""

from __future__ import annotations

import json
import logging
import os
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

# 禁用不安全 HTTPS 请求警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# 全局配置与字典映射
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
STORES_FILE = BASE_DIR / "stores.json"
DB_FILE = BASE_DIR / "orders.db"
API_VERSION = "2026-07"
HOURS_BACK = 48
MAX_WORKERS = 10

# 优先读取环境变量中的 GAS_WEBHOOK_URL，若未设置则使用默认 Webhook 地址
GAS_WEBHOOK_URL = os.getenv(
    "GAS_WEBHOOK_URL",
    "https://script.google.com/macros/s/AKfycbwZmUawxHNCYP4w3IDchs-ape2pfTf4Ua9XHhqCv94vh8ur4HyGasvUUx-Rhp3qeyxM/exec"
)

RISK_PRIORITY = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0, "PENDING": 0}

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

ORDERS_GQL = """
query RecentOrders($query: String!, $cursor: String) {
  orders(first: 50, query: $query, after: $cursor, sortKey: UPDATED_AT, reverse: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
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

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    logger.info("[%s] %s", datetime.now(timezone.utc).strftime("%H:%M:%S"), msg)


def build_shopify_query(hours: int = HOURS_BACK) -> str:
    """构建 Shopify GraphQL 巡检时间范围查询"""
    time_threshold = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"updated_at:>{time_threshold}"


def format_utc_time(iso_str: str) -> str:
    """格式化原始 UTC 时间字符串为干净的 YYYY-MM-DD HH:MM:SS"""
    if not iso_str:
        return ""
    return iso_str.replace("T", " ").replace("Z", "").split(".")[0]


# ---------------------------------------------------------------------------
# SQLite 本地持久化与去重引擎
# ---------------------------------------------------------------------------


def init_db(db_path: Path = DB_FILE) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
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
    now_ts = time.time()
    conn.execute(
        """
        INSERT INTO risk_orders
            (shop_domain, order_name, risk_level, address_status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(shop_domain, order_name) DO UPDATE SET
            risk_level     = excluded.risk_level,
            address_status = excluded.address_status,
            created_at     = excluded.created_at,
            updated_at     = excluded.updated_at
        """,
        (shop_domain, order_name, risk_level, address_status, created_at, now_ts),
    )
    if commit:
        conn.commit()


# ---------------------------------------------------------------------------
# Shopify API Token 动态管理与 GraphQL 请求
# ---------------------------------------------------------------------------

_token_cache: dict[str, dict[str, float | str]] = {}
_token_lock = threading.Lock()


def load_stores(stores_path: Path = STORES_FILE) -> list[dict[str, str]]:
    # 优先从环境变量读取 STORES_JSON（适合 GitHub Actions），没有再读本地 stores.json
    env_stores = os.getenv("STORES_JSON")
    if env_stores and env_stores.strip():
        data = json.loads(env_stores)
    elif stores_path.exists():
        with stores_path.open(encoding="utf-8") as f:
            data = json.load(f)
    else:
        raise FileNotFoundError(f"未找到店铺配置！请配置 GitHub Secret 'STORES_JSON' 或在根目录提供 '{stores_path.name}' 文件。")

    stores: list[dict[str, str]] = []
    for idx, item in enumerate(data, start=1):
        name = str(item.get("name", "")).strip() or f"店铺{idx}"
        domain = str(item.get("domain", "")).strip()
        client_id = str(item.get("client_id", "")).strip()
        client_secret = str(item.get("client_secret", "")).strip()
        stores.append({
            "name": name,
            "domain": domain,
            "client_id": client_id,
            "client_secret": client_secret
        })
    return stores


def fetch_access_token(domain: str, client_id: str, client_secret: str) -> str:
    url = f"https://{domain}/admin/oauth/access_token"
    for attempt in range(1, 4):
        try:
            response = requests.post(
                url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                timeout=30,
                verify=False,
            )
            response.raise_for_status()
            body = response.json()
            access_token = str(body.get("access_token"))
            expires_in = int(body.get("expires_in") or 86399)
            with _token_lock:
                _token_cache[domain] = {
                    "token": access_token,
                    "expires_at": time.time() + expires_in,
                }
            return access_token
        except requests.RequestException as exc:
            if attempt < 3:
                time.sleep(2)
            else:
                raise exc


def get_access_token(store: dict[str, str]) -> str:
    domain = store["domain"]
    with _token_lock:
        cached = _token_cache.get(domain)
        if cached and time.time() < float(cached["expires_at"]) - 60:
            return str(cached["token"])
    return fetch_access_token(domain, store["client_id"], store["client_secret"])


def shopify_graphql(domain: str, token: str, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": token}
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    for attempt in range(1, 4):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30, verify=False)
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise RuntimeError(f"GraphQL 错误: {body['errors']}")
            return body.get("data") or {}
        except requests.RequestException as exc:
            if attempt < 3:
                time.sleep(2)
            else:
                raise exc


def highest_risk_level(node: dict[str, Any]) -> str:
    direct_level = str(node.get("riskLevel") or "NONE").upper()
    risk = node.get("risk") or {}
    assessments = risk.get("assessments") or []

    best = direct_level
    best_score = RISK_PRIORITY.get(best, 0)

    for item in assessments:
        level = str(item.get("riskLevel") or "NONE").upper()
        score = RISK_PRIORITY.get(level, 0)
        if score > best_score:
            best = level
            best_score = score
    return best


def evaluate_order(node: dict[str, Any]) -> tuple[bool, str, str]:
    risk_level = highest_risk_level(node)
    shipping = node.get("shippingAddress") or {}
    address_status = str(shipping.get("validationResultSummary") or "").upper()

    hit_risk = risk_level == "HIGH"
    hit_address = address_status in {"ERROR", "WARNING", "INCOMPLETE", "UNVERIFIED"}

    if hit_risk or hit_address:
        return True, risk_level, address_status
    return False, risk_level, address_status


def fetch_recent_orders(domain: str, token: str) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    cursor: str | None = None
    query_str = build_shopify_query(HOURS_BACK)

    while True:
        variables = {"query": query_str, "cursor": cursor}
        data = shopify_graphql(domain, token, ORDERS_GQL, variables)

        orders_conn = data.get("orders") or {}
        edges = orders_conn.get("edges") or []
        for edge in edges:
            node = edge.get("node")
            if node:
                orders.append(node)

        page_info = orders_conn.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break

    return orders


def build_reason(risk_level: str, address_status: str) -> str:
    reasons: list[str] = []
    if risk_level == "HIGH":
        reasons.append("高风险")
    if address_status in {"ERROR", "WARNING", "INCOMPLETE", "UNVERIFIED"}:
        reasons.append(ADDR_MAP_CN.get(address_status, address_status))
    return ", ".join(reasons)


# ---------------------------------------------------------------------------
# Google Sheets 批量同步 (含秘钥动态推送与网络重试机制)
# ---------------------------------------------------------------------------


def sync_to_google_sheets(all_orders_list: list[dict[str, Any]]) -> None:
    if not GAS_WEBHOOK_URL:
        log("未配置 GAS_WEBHOOK_URL，跳过 Google Sheets 同步。")
        return

    synced_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for item in all_orders_list:
        created_at_utc = format_utc_time(item["created_at"])
        tags_str = ", ".join(item.get("tags") or [])

        rows.append([
            item["shop_name"],
            item["shop_domain"],
            item["order_name"],
            RISK_MAP_CN.get(item["risk_level"], item["risk_level"]),
            ADDR_MAP_CN.get(item["address_status"], item["address_status"]),
            created_at_utc,
            item["reason"],
            synced_at_utc,
            tags_str,
        ])

    stores = load_stores()
    stores_config = {
        s["domain"]: {
            "client_id": s["client_id"],
            "client_secret": s["client_secret"],
        }
        for s in stores
    }

    payload = {
        "rows": rows,
        "stores_config": stores_config,
    }

    for attempt in range(1, 4):
        try:
            resp = requests.post(
                GAS_WEBHOOK_URL,
                json=payload,
                timeout=45,
                allow_redirects=True,
                verify=False,
            )
            resp.raise_for_status()
            res_json = resp.json()
            log(f"Google Sheets 同步成功！推送数据 {len(rows)} 条，GAS 响应: {res_json}")
            break
        except Exception as exc:
            if attempt == 3:
                log(f"Google Sheets 批量同步失败 (已重试 3 次): {exc}")
            else:
                log(f"第 {attempt} 次同步尝试失败，等待重试: {exc}")
                time.sleep(2)


# ---------------------------------------------------------------------------
# 巡检多线程并发引擎
# ---------------------------------------------------------------------------


def check_store_worker(store: dict[str, str]) -> dict[str, Any]:
    name = store["name"]
    domain = store["domain"]
    log(f"开始巡检店铺: {name} ({domain})...")

    try:
        token = get_access_token(store)
        raw_orders = fetch_recent_orders(domain, token)
    except Exception as exc:
        log(f"店铺 {name} 处理失败: {exc}")
        return {"store": store, "orders": []}

    flagged_orders: list[dict[str, Any]] = []
    for order in raw_orders:
        should_save, risk_level, address_status = evaluate_order(order)
        if not should_save:
            continue

        flagged_orders.append({
            "shop_name": name,
            "shop_domain": domain,
            "order_name": str(order.get("name") or ""),
            "risk_level": risk_level,
            "address_status": address_status,
            "created_at": str(order.get("createdAt") or ""),
            "reason": build_reason(risk_level, address_status),
            "tags": order.get("tags") or [],
        })

    return {"store": store, "orders": flagged_orders}


def run_check() -> None:
    log("===== 开始本轮 Shopify 风控巡检 =====")
    stores = load_stores()

    worker_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(check_store_worker, store) for store in stores]
        for future in as_completed(futures):
            worker_results.append(future.result())

    conn = init_db()
    all_flagged_orders: list[dict[str, Any]] = []

    try:
        for result in worker_results:
            for item in result["orders"]:
                all_flagged_orders.append(item)
                upsert_risk_order(
                    conn,
                    shop_domain=item["shop_domain"],
                    order_name=item["order_name"],
                    risk_level=item["risk_level"],
                    address_status=item["address_status"],
                    created_at=item["created_at"],
                    commit=False,
                )
        conn.commit()
    finally:
        conn.close()

    sync_to_google_sheets(all_flagged_orders)
    log(f"===== 本轮巡检结束，共检索到 {len(all_flagged_orders)} 条高风险/地址异常订单 =====")


if __name__ == "__main__":
    run_check()
