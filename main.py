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

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    logger.info("[%s] %s", datetime.now(timezone.utc).strftime("%H:%M:%S"), msg)


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
# Shopify API Token 动态管理
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
        raise FileNotFoundError(
            f"未找到店铺配置！请配置 GitHub Secret 'STORES_JSON' 或在根目录提供 '{stores_path.name}' 文件。"
        )

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


# ---------------------------------------------------------------------------
# REST API 全量订单拉取（替换原 GraphQL fetch_recent_orders）
# ---------------------------------------------------------------------------


def get_all_orders_from_shopify(
    shop_domain: str, access_token: str
) -> list[dict]:
    """全量获取店铺近 30 天内的所有订单（不限状态、自动分页、防漏单）"""
    all_orders = []

    # 1. 计算 UTC 时间 30 天前的起始时间点
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    updated_at_min = thirty_days_ago.isoformat()

    # 2. 构建 API URL：包含 status=any, limit=250, updated_at_min
    url = (
        f"https://{shop_domain}/admin/api/{API_VERSION}/orders.json"
        f"?status=any&limit=250&updated_at_min={updated_at_min}"
    )
    headers = {"X-Shopify-Access-Token": access_token}

    while url:
        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)

            # 遇到 Shopify API 限流（429）时自动挂起重试
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", 2.0))
                log(f"⚠️  触发限流，{retry_after}s 后重试 ({shop_domain})")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()
            orders = data.get("orders", [])
            all_orders.extend(orders)

            # 解析 Link 响应头，获取下一页游标
            link_header = response.headers.get("Link")
            url = None
            if link_header:
                for match in link_header.split(","):
                    if 'rel="next"' in match:
                        url = match.split(";")[0].strip("<> ")
                        break

            time.sleep(0.3)  # 平抑并发频率，保护 API 额度

        except Exception as e:
            log(f"❌ 获取店铺 {shop_domain} 订单失败: {e}")
            break

    return all_orders


# ---------------------------------------------------------------------------
# 风险评估逻辑（适配 REST API 响应字段）
# ---------------------------------------------------------------------------


def highest_risk_level(order: dict[str, Any]) -> str:
    """
    REST API 订单风险字段：
      - order["risks"] 若已通过 fields 参数拉取，则为列表；
        常规 orders.json 不返回 risks，需单独请求 /orders/{id}/risks.json。
      - 此处优先读取 order 顶层 "risk_level"（部分店铺计划可见），
        兜底为 "NONE"。
    """
    # 部分 Shopify 计划在订单对象中直接暴露 risk_level
    direct_level = str(order.get("risk_level") or "NONE").upper()
    best_score = RISK_PRIORITY.get(direct_level, 0)
    best = direct_level

    # 若已随订单返回 risks 列表（需开启对应字段权限）
    risks = order.get("risks") or []
    for risk in risks:
        level = str(risk.get("recommendation") or "NONE").upper()
        # REST risks.recommendation: "cancel" → HIGH, "investigate" → MEDIUM, "accept" → LOW
        level_mapped = {
            "CANCEL": "HIGH",
            "INVESTIGATE": "MEDIUM",
            "ACCEPT": "LOW",
        }.get(level, level)
        score = RISK_PRIORITY.get(level_mapped, 0)
        if score > best_score:
            best = level_mapped
            best_score = score

    return best


def evaluate_order(order: dict[str, Any]) -> tuple[bool, str, str]:
    """
    REST API 地址字段：
      order["shipping_address"]["validation_result_summary"]
    （需店铺开启地址验证功能，否则字段可能为 None）
    """
    risk_level = highest_risk_level(order)

    shipping = order.get("shipping_address") or {}
    address_status = str(shipping.get("validation_result_summary") or "").upper()

    hit_risk = risk_level == "HIGH"
    hit_address = address_status in {"ERROR", "WARNING", "INCOMPLETE", "UNVERIFIED"}

    if hit_risk or hit_address:
        return True, risk_level, address_status
    return False, risk_level, address_status


def build_reason(risk_level: str, address_status: str) -> str:
    reasons: list[str] = []
    if risk_level == "HIGH":
        reasons.append("高风险")
    if address_status in {"ERROR", "WARNING", "INCOMPLETE", "UNVERIFIED"}:
        reasons.append(ADDR_MAP_CN.get(address_status, address_status))
    return ", ".join(reasons)


# ---------------------------------------------------------------------------
# Google Sheets 批量同步（含密钥动态推送与网络重试机制）
# ---------------------------------------------------------------------------


def sync_to_google_sheets(all_orders_list: list[dict[str, Any]]) -> None:
    if not GAS_WEBHOOK_URL:
        log("❌ 错误: 未配置环境变量 GAS_WEBHOOK_URL，请前往 GitHub Secrets 添加入口地址！")
        return

    synced_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for item in all_orders_list:
        created_at_utc = format_utc_time(item["created_at"])
        tags_raw = item.get("tags") or ""
        # REST API tags 字段为逗号分隔字符串；GraphQL 为列表，兼容两种格式
        if isinstance(tags_raw, list):
            tags_str = ", ".join(tags_raw)
        else:
            tags_str = str(tags_raw)

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
        # 使用 REST API 全量拉取替代原 GraphQL 方法
        raw_orders = get_all_orders_from_shopify(domain, token)
    except Exception as exc:
        log(f"店铺 {name} 处理失败: {exc}")
        return {"store": store, "orders": []}

    log(f"店铺 {name} 共拉取 {len(raw_orders)} 条订单，开始风险评估...")

    flagged_orders: list[dict[str, Any]] = []
    for order in raw_orders:
        should_save, risk_level, address_status = evaluate_order(order)
        if not should_save:
            continue

        # REST API 使用 snake_case 字段名（created_at / name）
        flagged_orders.append({
            "shop_name": name,
            "shop_domain": domain,
            "order_name": str(order.get("name") or ""),
            "risk_level": risk_level,
            "address_status": address_status,
            "created_at": str(order.get("created_at") or ""),   # REST: created_at
            "reason": build_reason(risk_level, address_status),
            "tags": order.get("tags") or "",                    # REST: 逗号字符串
        })

    log(f"店铺 {name} 命中高风险/地址异常订单 {len(flagged_orders)} 条")
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
