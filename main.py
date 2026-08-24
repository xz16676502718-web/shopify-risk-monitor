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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent
STORES_FILE = BASE_DIR / "stores.json"
DB_FILE = BASE_DIR / "orders.db"
API_VERSION = "2026-07"
MAX_WORKERS = 10        # 店铺并发数
ORDER_EVAL_WORKERS = 8  # 单店铺内订单并发评估数

GAS_WEBHOOK_URL = os.getenv(
    "GAS_WEBHOOK_URL",
    "https://script.google.com/macros/s/AKfycbwZmUawxHNCYP4w3IDchs-ape2pfTf4Ua9XHhqCv94vh8ur4HyGasvUUx-Rhp3qeyxM/exec",
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
    "WARNING": "地址不完整",
    "ERROR": "地址错误",
    "INCOMPLETE": "地址不完整",
    "UNVERIFIED": "未验证地址",
    "CORRECTED": "地址已修正",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    logger.info("[%s] %s", datetime.now(timezone.utc).strftime("%H:%M:%S"), msg)


def format_utc_time(iso_str: str) -> str:
    if not iso_str:
        return ""
    return iso_str.replace("T", " ").replace("Z", "").split(".")[0]


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


_token_cache: dict[str, dict[str, float | str]] = {}
_token_lock = threading.Lock()


def load_stores(stores_path: Path = STORES_FILE) -> list[dict[str, str]]:
    env_stores = os.getenv("STORES_JSON")
    if env_stores and env_stores.strip():
        data = json.loads(env_stores)
    elif stores_path.exists():
        with stores_path.open(encoding="utf-8") as f:
            data = json.load(f)
    else:
        raise FileNotFoundError("未找到店铺配置 stores.json！")

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
            "client_secret": client_secret,
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


def get_all_orders_from_shopify(
    shop_domain: str, access_token: str
) -> list[dict]:
    all_orders = []
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    updated_at_min = thirty_days_ago.isoformat()

    url = (
        f"https://{shop_domain}/admin/api/{API_VERSION}/orders.json"
        f"?status=any&limit=250&updated_at_min={updated_at_min}"
    )
    headers = {"X-Shopify-Access-Token": access_token}

    while url:
        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", 2.0))
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()
            orders = data.get("orders", [])
            all_orders.extend(orders)

            link_header = response.headers.get("Link")
            url = None
            if link_header:
                for match in link_header.split(","):
                    if 'rel="next"' in match:
                        url = match.split(";")[0].strip("<> ")
                        break
            time.sleep(0.3)
        except Exception as e:
            log(f"❌ 获取店铺 {shop_domain} 订单失败: {e}")
            break

    return all_orders


def fetch_address_validations_graphql(
    shop_domain: str, access_token: str, order_ids: list[int]
) -> dict[int, str]:
    """通过 Shopify GraphQL API 批量获取订单橙色标 (validationResultSummary)"""
    if not order_ids:
        return {}

    url = f"https://{shop_domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    results: dict[int, str] = {}
    chunk_size = 50

    for i in range(0, len(order_ids), chunk_size):
        chunk = order_ids[i : i + chunk_size]
        gids = [f"gid://shopify/Order/{oid}" for oid in chunk]

        query = """
        query getAddressValidation($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Order {
              legacyResourceId
              shippingAddress {
                validationResultSummary
              }
            }
          }
        }
        """
        payload = {"query": query, "variables": {"ids": gids}}

        try:
            resp = requests.post(
                url, headers=headers, json=payload, timeout=15, verify=False
            )
            if resp.status_code == 200:
                data = resp.json()
                nodes = data.get("data", {}).get("nodes") or []
                for node in nodes:
                    if node and "legacyResourceId" in node:
                        oid = int(node["legacyResourceId"])
                        shipping = node.get("shippingAddress") or {}
                        summary = str(
                            shipping.get("validationResultSummary") or ""
                        ).upper()
                        if summary:
                            results[oid] = summary
        except Exception as e:
            log(f"⚠️ GraphQL 查询异常 ({shop_domain}): {e}")

    return results


def fetch_order_risks(
    shop_domain: str, access_token: str, order_id: int
) -> list[dict[str, Any]]:
    url = f"https://{shop_domain}/admin/api/{API_VERSION}/orders/{order_id}/risks.json"
    headers = {"X-Shopify-Access-Token": access_token}
    for attempt in range(2):
        try:
            resp = requests.get(url, headers=headers, timeout=5, verify=False)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 1.0))
                time.sleep(retry_after)
                continue
            if resp.status_code == 200:
                return resp.json().get("risks", [])
        except Exception:
            pass
        break
    return []


def get_order_risk_and_address_status(
    order: dict[str, Any],
    shop_domain: str,
    access_token: str,
    gql_status: str = "",
) -> tuple[bool, str, str]:
    order_id = order.get("id")
    tags_raw = order.get("tags") or ""
    tags_str = (
        ", ".join(tags_raw).lower()
        if isinstance(tags_raw, list)
        else str(tags_raw).lower()
    )

    # 1. 地址评估（精准对应后台橙色标）
    address_status = "NO_ISSUES"
    if gql_status in {"WARNING", "ERROR"}:
        address_status = gql_status
    elif any(
        kw in tags_str
        for kw in ["address warning", "地址警告", "address_warning"]
    ):
        address_status = "WARNING"
    elif any(
        kw in tags_str
        for kw in ["address error", "地址错误", "invalid address"]
    ):
        address_status = "ERROR"

    # 2. 风险评估
    risk_level = "NONE"
    if order.get("cancel_reason") == "fraud" or any(
        kw in tags_str for kw in ["high risk", "高风险", "fraud", "欺诈"]
    ):
        risk_level = "HIGH"

    if order_id and risk_level != "HIGH":
        risks = fetch_order_risks(shop_domain, access_token, order_id)
        best_score = RISK_PRIORITY.get(risk_level, 0)
        for r in risks:
            rec = str(r.get("recommendation") or "").lower()
            try:
                score_val = float(r.get("score") or 0.0)
            except (ValueError, TypeError):
                score_val = 0.0

            mapped_level = "NONE"
            if (
                rec == "cancel"
                or score_val >= 0.75
                or r.get("cause_cancel") is True
            ):
                mapped_level = "HIGH"

            if RISK_PRIORITY.get(mapped_level, 0) > best_score:
                risk_level = mapped_level
                best_score = RISK_PRIORITY.get(mapped_level, 0)

    hit_risk = risk_level == "HIGH"
    hit_address = address_status in {"ERROR", "WARNING"}
    should_save = hit_risk or hit_address

    return should_save, risk_level, address_status


def build_reason(risk_level: str, address_status: str) -> str:
    reasons: list[str] = []
    if risk_level == "HIGH":
        reasons.append("高风险")
    if address_status in {"ERROR", "WARNING", "INCOMPLETE", "UNVERIFIED"}:
        reasons.append(ADDR_MAP_CN.get(address_status, address_status))
    return ", ".join(reasons)


def sync_to_google_sheets(all_orders_list: list[dict[str, Any]]) -> None:
    if not GAS_WEBHOOK_URL:
        log("❌ 错误: 未配置环境变量 GAS_WEBHOOK_URL！")
        return

    synced_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for item in all_orders_list:
        created_at_utc = format_utc_time(item["created_at"])
        tags_raw = item.get("tags") or ""
        tags_str = (
            ", ".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw)
        )

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
                log(f"Google Sheets 批量同步失败: {exc}")
            else:
                time.sleep(2)


def check_store_worker(store: dict[str, str]) -> dict[str, Any]:
    name = store["name"]
    domain = store["domain"]
    log(f"开始巡检店铺: {name} ({domain})...")

    try:
        token = get_access_token(store)
        raw_orders = get_all_orders_from_shopify(domain, token)
    except Exception as exc:
        log(f"店铺 {name} 处理失败: {exc}")
        return {"store": store, "orders": []}

    log(f"店铺 {name} 共拉取 {len(raw_orders)} 条订单，开始风险评估...")

    order_ids = [o["id"] for o in raw_orders if o.get("id")]
    gql_val_map = fetch_address_validations_graphql(domain, token, order_ids)

    flagged_orders: list[dict[str, Any]] = []

    def eval_single_order(order: dict[str, Any]) -> dict[str, Any] | None:
        oid = order.get("id")
        gql_status = gql_val_map.get(oid, "")

        should_save, risk_level, address_status = (
            get_order_risk_and_address_status(
                order, domain, token, gql_status=gql_status
            )
        )
        if not should_save:
            return None

        return {
            "shop_name": name,
            "shop_domain": domain,
            "order_name": str(order.get("name") or ""),
            "risk_level": risk_level,
            "address_status": address_status,
            "created_at": str(order.get("created_at") or ""),
            "reason": build_reason(risk_level, address_status),
            "tags": order.get("tags") or "",
        }

    with ThreadPoolExecutor(max_workers=ORDER_EVAL_WORKERS) as eval_executor:
        futures = [
            eval_executor.submit(eval_single_order, order)
            for order in raw_orders
        ]
        for future in as_completed(futures):
            res = future.result()
            if res:
                flagged_orders.append(res)

    log(f"店铺 {name} 命中高风险/地址异常订单 {len(flagged_orders)} 条")
    return {"store": store, "orders": flagged_orders}


def run_check() -> None:
    log("===== 开始本轮 Shopify 风控巡检 =====")
    stores = load_stores()

    worker_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(check_store_worker, store) for store in stores
        ]
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
