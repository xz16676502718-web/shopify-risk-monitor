/**
 * Shopify 风控中台 — Google Sheets 批量接收端
 *
 * 部署步骤：
 * 1. 打开 Google 表格，扩展程序 → Apps Script，粘贴本文件
 * 2. 修改下方 SHEET_NAME（工作表名称）
 * 3. 首次运行 setupSheet() 创建表头（只需执行一次）
 * 4. 部署 → 新建部署 → 类型选「网页应用」
 *    - 执行身份：我
 *    - 访问权限：任何人（Python 脚本从外部 POST 需要）
 * 5. 复制部署 URL，设为 Python 环境变量 GAS_WEBHOOK_URL
 *
 * Python POST 格式：
 * {
 *   "rows": [
 *     ["店铺名", "domain", "#1001", "HIGH", "ERROR", "2026-...", "原因", "同步时间"]
 *   ]
 * }
 */

const SHEET_NAME = 'risk_orders';
const API_TOKEN = ''; // 可选：填入密钥后，Python 需在 Header 携带 X-Api-Token

/**
 * 初始化表头（手动在 Apps Script 编辑器里运行一次）
 */
function setupSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
  }

  const headers = [
    '店铺名',
    '店铺域名',
    '订单号',
    '风险等级',
    '地址校验',
    '订单创建时间',
    '命中原因',
    '同步时间',
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, headers.length);
}

/**
 * 接收 Python 批量 POST
 */
function doPost(e) {
  try {
    if (API_TOKEN) {
      const token = (e.parameter && e.parameter.token) ||
        (e.headers && (e.headers['X-Api-Token'] || e.headers['x-api-token']));
      if (token !== API_TOKEN) {
        return jsonResponse({ ok: false, error: 'Unauthorized' }, 401);
      }
    }

    if (!e || !e.postData || !e.postData.contents) {
      return jsonResponse({ ok: false, error: 'Empty request body' }, 400);
    }

    const payload = JSON.parse(e.postData.contents);
    const rows = payload.rows;

    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      return jsonResponse({ ok: true, appended: 0, message: 'No rows to append' });
    }

    if (!Array.isArray(rows[0])) {
      return jsonResponse({ ok: false, error: 'rows must be a 2D array' }, 400);
    }

    const sheet = getOrCreateSheet_();
    const numRows = rows.length;
    const numCols = rows[0].length;

    // 校验每行列数一致
    for (let i = 0; i < numRows; i++) {
      if (!Array.isArray(rows[i]) || rows[i].length !== numCols) {
        return jsonResponse({
          ok: false,
          error: `Row ${i + 1} has inconsistent column count`,
        }, 400);
      }
    }

    const startRow = sheet.getLastRow() + 1;
    sheet.getRange(startRow, 1, numRows, numCols).setValues(rows);

    return jsonResponse({
      ok: true,
      appended: numRows,
      startRow: startRow,
      endRow: startRow + numRows - 1,
    });
  } catch (err) {
    return jsonResponse({ ok: false, error: String(err) }, 500);
  }
}

/**
 * 浏览器 GET 测试用
 */
function doGet() {
  return jsonResponse({
    ok: true,
    service: 'shopify-risk-monitor',
    sheet: SHEET_NAME,
    usage: 'POST JSON {"rows": [["col1", "col2", ...], ...]}',
  });
}

function getOrCreateSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    setupSheet();
    sheet = ss.getSheetByName(SHEET_NAME);
  }
  return sheet;
}

function jsonResponse(obj, statusCode) {
  const output = ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);

  // Apps Script 网页应用不支持自定义 HTTP 状态码，仅在 body 中标记
  if (statusCode && statusCode >= 400) {
    return ContentService
      .createTextOutput(JSON.stringify({ ...obj, httpStatus: statusCode }))
      .setMimeType(ContentService.MimeType.JSON);
  }
  return output;
}
