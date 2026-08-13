#!/usr/bin/env python3
"""多店铺 Shopify 风控中台 — 定时拉取高风险 / 地址异常订单并入库。"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import schedule
import urllib3

# ---------------------------------------------------------------------------
# 禁用 SSL 警告（避免 verify=False 时控制台打印大量警告信息）
# ---------------------------------------------------------------------------
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# 配置与中文映射字典
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
STORES_FILE = BASE_DIR / "stores.json"
DB_FILE = BASE_DIR / "orders.db"
API_VERSION = "2026-07"
ORDER_QUERY = "updated_at:>-10m"
POLL_INTERVAL_MINUTES = 5
MAX_WORKERS = 10
DINGTALK_WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")
GAS_WEBHOOK_URL = (
    "https://script.google.com/macros/s/"
    "AKfycbyqSmqYTDhHRb5G8M8eoww1gP74NEmL27QDf842yhO0lX93hkXsWCR2OmywzUw6V1LE/exec"
)

RISK_PRIORITY = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0, "PENDING": 0}

# 风险等级中文映射
RISK_MAP_CN = {
    "HIGH": "高风险",
    "MEDIUM": "中风险",
    "LOW": "低风险",
    "NONE": "无风险",
    "PENDING": "待评估",
}

# 地址校验结果中文映射
ADDR_MAP_CN = {
    "NO_ISSUES": "无异常",
    "WARNING": "地址警告",
    "ERROR": "地址错误",
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

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def log(msg: str) -> None:
    """带时间戳的日志输出，例如 [10:00:00] 开始检查店铺A..."""
    ts = datetime.now().strftime("%H:%M:%S")
    logger.info("[%s] %s", ts, msg)


# ---------------------------------------------------------------------------
# 数据库
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


def order_exists(conn: sqlite3.Connection, shop_domain: str, order_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM risk_orders WHERE shop_domain = ? AND order_name = ? LIMIT 1",
        (shop_domain, order_name),
    ).fetchone()
    return row is not None


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
# 店铺配置 & OAuth Token 缓存（线程安全）
# ---------------------------------------------------------------------------

_token_cache: dict[str, dict[str, float | str]] = {}
_token_lock = threading.Lock()


def load_stores(stores_path: Path = STORES_FILE) -> list[dict[str, str]]:
    if not stores_path.exists():
        raise FileNotFoundError(f"找不到店铺配置文件: {stores_path}")

    with stores_path.open(encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list) or not data:
        raise ValueError("stores.json 必须是非空数组")

    stores: list[dict[str, str]] = []
    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"stores.json 第 {idx} 项格式错误，应为对象")
        name = str(item.get("name", "")).strip() or f"店铺{idx}"
        domain = str(item.get("domain", "")).strip()
        client_id = str(item.get("client_id", "")).strip()
        client_secret = str(item.get("client_secret", "")).strip()
        if not domain or not client_id or not client_secret:
            raise ValueError(
                f"stores.json 第 {idx} 项缺少 domain / client_id / client_secret"
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


def fetch_access_token(domain: str, client_id: str, client_secret: str) -> str:
    """使用 Client Credentials 换取 Access Token（自带 3 次自动重试与 SSL 忽略）。"""
    url = f"https://{domain}/admin/oauth/access_token"
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
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
                verify=False,  # 👈 防止代理或 SSL 拦截导致的报错
            )
            response.raise_for_status()
            body = response.json()
            access_token = body.get("access_token")
            if not access_token:
                raise RuntimeError(f"Token 响应缺少 access_token: {body}")

            expires_in = int(body.get("expires_in") or 86399)
            with _token_lock:
                _token_cache[domain] = {
                    "token": access_token,
                    "expires_at": time.time() + expires_in,
                }
            return access_token
        except requests.RequestException as exc:
            if attempt < max_retries:
                time.sleep(2)
            else:
                raise exc


def get_access_token(store: dict[str, str]) -> str:
    """获取有效 Access Token，过期前 60 秒自动刷新。"""
    domain = store["domain"]
    with _token_lock:
        cached = _token_cache.get(domain)
        if cached and time.time() < float(cached["expires_at"]) - 60:
            return str(cached["token"])

    return fetch_access_token(domain, store["client_id"], store["client_secret"])


# ---------------------------------------------------------------------------
# Shopify GraphQL
# ---------------------------------------------------------------------------


def shopify_graphql(
    domain: str,
    token: str,
    query: str,
    variables: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """Shopify GraphQL 请求（自带 3 次自动重试与 SSL 忽略）。"""
    url = f"https://{domain}/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token,
    }
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                url, headers=headers, json=payload, timeout=timeout, verify=False
            )
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise RuntimeError(f"GraphQL 错误: {body['errors']}")
            return body.get("data") or {}
        except requests.RequestException as exc:
            if attempt < max_retries:
                time.sleep(2)  # 等待 2 秒后重试
            else:
                raise exc


def highest_risk_level(assessments: list[dict[str, Any]] | None) -> str:
    if not assessments:
        return "NONE"
    best = "NONE"
    best_score = -1
    for item in assessments:
        level = str(item.get("riskLevel") or "NONE").upper()
        score = RISK_PRIORITY.get(level, 0)
        if score > best_score:
            best = level
            best_score = score
    return best


def evaluate_order(node: dict[str, Any]) -> tuple[bool, str, str]:
    """
    判断订单是否需要入库。
    返回: (是否命中, risk_level, address_status)
    """
    risk = node.get("risk") or {}
    risk_level = highest_risk_level(risk.get("assessments"))

    shipping = node.get("shippingAddress") or {}
    address_status = str(shipping.get("validationResultSummary") or "").upper()

    hit_risk = risk_level == "HIGH"
    hit_address = address_status in {"ERROR", "WARNING"}

    if hit_risk or hit_address:
        return True, risk_level, address_status
    return False, risk_level, address_status


def fetch_recent_orders(domain: str, token: str) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    cursor: str | None = None

    while True:
        variables = {"query": ORDER_QUERY, "cursor": cursor}
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
    """生成中文命中原因"""
    reasons: list[str] = []
    if risk_level == "HIGH":
        reasons.append("高风险")
    if address_status in {"ERROR", "WARNING"}:
        reasons.append(ADDR_MAP_CN.get(address_status, address_status))
    return ", ".join(reasons)


# ---------------------------------------------------------------------------
# 钉钉告警（预留）
# ---------------------------------------------------------------------------


def send_alert(shop: str, order_name: str, reason: str) -> None:
    """
    发送钉钉 Webhook 告警。
    设置环境变量 DINGTALK_WEBHOOK 后才会真正发送；未配置时仅打印日志。
    """
    text = f"[Shopify风控] 店铺: {shop} | 订单: {order_name} | 原因: {reason}"

    if not DINGTALK_WEBHOOK:
        log(f"[告警预留] {text}")
        return

    payload = {
        "msgtype": "text",
        "text": {"content": text},
    }
    try:
        resp = requests.post(DINGTALK_WEBHOOK, json=payload, timeout=10, verify=False)
        resp.raise_for_status()
        log(f"钉钉告警已发送: {order_name}")
    except requests.RequestException as exc:
        log(f"钉钉告警发送失败 ({order_name}): {exc}")


# ---------------------------------------------------------------------------
# Google Sheets 批量同步（自动转换为中文 + 格式化时间 + 重试）
# ---------------------------------------------------------------------------


def sync_to_google_sheets(new_orders_list: list[dict[str, str]]) -> None:
    """
    将本轮所有新发现的异常订单，通过 1 次 HTTP POST 批量写入 Google Sheets。
    """
    if not new_orders_list:
        return

    if not GAS_WEBHOOK_URL:
        log(f"[Sheets 预留] 本轮有 {len(new_orders_list)} 条新订单待同步（未配置 GAS_WEBHOOK_URL）")
        return

    synced_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for item in new_orders_list:
        clean_created_at = (
            item["created_at"].replace("T", " ").replace("Z", "").split(".")[0]
        )

        rows.append([
            item["shop_name"],
            item["shop_domain"],
            item["order_name"],
            RISK_MAP_CN.get(item["risk_level"], item["risk_level"]),
            ADDR_MAP_CN.get(item["address_status"], item["address_status"]),
            clean_created_at,
            item["reason"],
            synced_at,
        ])

    payload = {"rows": rows}
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                GAS_WEBHOOK_URL, 
                json=payload, 
                timeout=30, 
                verify=False
            )
            resp.raise_for_status()
            body = resp.json() if resp.content else {}
            appended = body.get("appended", len(rows))
            log(f"Google Sheets 批量同步成功，写入 {appended} 行")
            break
        except requests.RequestException as exc:
            if attempt < max_retries:
                log(f"Google Sheets 同步网络波动 (第 {attempt} 次重试中...): {exc}")
                time.sleep(2)
            else:
                log(f"Google Sheets 批量同步失败 (已重试 {max_retries} 次): {exc}")
        except json.JSONDecodeError:
            log(f"Google Sheets 批量同步完成（响应非 JSON）: {resp.text[:200]}")
            break


# ---------------------------------------------------------------------------
# 核心业务（并发查询，主线程入库）
# ---------------------------------------------------------------------------


def check_store_worker(store: dict[str, str]) -> dict[str, Any]:
    """
    单店铺并发 worker：只做 Token 获取 + GraphQL 查询，不写数据库。
    返回 flagged 订单列表；出错时 error 字段有值。
    """
    name = store["name"]
    domain = store["domain"]
    log(f"开始检查店铺 {name} ({domain})...")

    try:
        token = get_access_token(store)
    except requests.Timeout:
        log(f"店铺 {name} 获取 Token 超时，已跳过")
        return {"store": store, "orders": [], "error": "token_timeout"}
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response else "未知"
        detail = ""
        if exc.response is not None:
            try:
                detail = exc.response.json().get("error_description") or exc.response.text[:200]
            except Exception:  # noqa: BLE001
                detail = exc.response.text[:200]
        log(f"店铺 {name} 获取 Token 失败 (HTTP {status}): {detail}，已跳过")
        return {"store": store, "orders": [], "error": f"token_http_{status}"}
    except requests.RequestException as exc:
        log(f"店铺 {name} 获取 Token 网络错误: {exc}，已跳过")
        return {"store": store, "orders": [], "error": "token_network"}
    except RuntimeError as exc:
        log(f"店铺 {name} 获取 Token 失败: {exc}，已跳过")
        return {"store": store, "orders": [], "error": "token_runtime"}

    try:
        raw_orders = fetch_recent_orders(domain, token)
    except requests.Timeout:
        log(f"店铺 {name} API 超时，已跳过")
        return {"store": store, "orders": [], "error": "api_timeout"}
    except requests.HTTPError as exc:
        log(f"店铺 {name} HTTP 错误 ({exc.response.status_code if exc.response else '未知'})，已跳过")
        return {"store": store, "orders": [], "error": "api_http"}
    except requests.RequestException as exc:
        log(f"店铺 {name} 网络错误: {exc}，已跳过")
        return {"store": store, "orders": [], "error": "api_network"}
    except RuntimeError as exc:
        log(f"店铺 {name} GraphQL 返回错误: {exc}，已跳过")
        return {"store": store, "orders": [], "error": "api_graphql"}
    except Exception as exc:  # noqa: BLE001
        log(f"店铺 {name} 未知异常: {exc}，已跳过")
        return {"store": store, "orders": [], "error": "unknown"}

    flagged_orders: list[dict[str, str]] = []
    for order in raw_orders:
        should_save, risk_level, address_status = evaluate_order(order)
        if not should_save:
            continue

        order_name = str(order.get("name") or "")
        created_at = str(order.get("createdAt") or "")
        reason = build_reason(risk_level, address_status)

        flagged_orders.append(
            {
                "shop_name": name,
                "shop_domain": domain,
                "order_name": order_name,
                "risk_level": risk_level,
                "address_status": address_status,
                "created_at": created_at,
                "reason": reason,
            }
        )

    if flagged_orders:
        log(f"店铺 {name} 检查完成，发现 {len(flagged_orders)} 条高风险/地址异常订单")
    else:
        log(f"店铺 {name} 检查完成，未发现需入库订单")

    return {"store": store, "orders": flagged_orders, "error": None}


def run_check() -> None:
    log("===== 开始本轮风控巡检 =====")
    try:
        stores = load_stores()
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        log(f"读取店铺配置失败: {exc}")
        return

    log(f"共 {len(stores)} 家店铺，并发线程数 {MAX_WORKERS}")

    worker_results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(check_store_worker, store) for store in stores]
        for future in as_completed(futures):
            try:
                worker_results.append(future.result())
            except Exception as exc:  # noqa: BLE001
                log(f"并发任务异常: {exc}")

    conn = init_db()
    new_orders_list: list[dict[str, str]] = []
    total_flagged = 0

    try:
        for result in worker_results:
            for item in result["orders"]:
                total_flagged += 1
                is_new = not order_exists(conn, item["shop_domain"], item["order_name"])

                upsert_risk_order(
                    conn,
                    shop_domain=item["shop_domain"],
                    order_name=item["order_name"],
                    risk_level=item["risk_level"],
                    address_status=item["address_status"],
                    created_at=item["created_at"],
                    commit=False,
                )

                if is_new:
                    new_orders_list.append(item)
                    send_alert(item["shop_domain"], item["order_name"], item["reason"])

        conn.commit()
    finally:
        conn.close()

    sync_to_google_sheets(new_orders_list)

    log(
        f"===== 本轮巡检结束，共处理 {total_flagged} 条风险订单，"
        f"其中新发现 {len(new_orders_list)} 条 ====="
    )


def main() -> None:
    log(f"Shopify 风控中台已启动，每 {POLL_INTERVAL_MINUTES} 分钟巡检一次")
    log(f"数据库: {DB_FILE}")
    log(f"API 版本: {API_VERSION} | 查询条件: {ORDER_QUERY}")
    log(f"并发线程: {MAX_WORKERS} | GAS 批量同步: {'已配置' if GAS_WEBHOOK_URL else '未配置'}")

    run_check()

    schedule.every(POLL_INTERVAL_MINUTES).minutes.do(run_check)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("收到退出信号，程序已停止")