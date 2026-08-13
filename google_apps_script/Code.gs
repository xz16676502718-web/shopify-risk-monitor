/**
 * Shopify 风控中台 — Google Sheets 批量接收端（带自动查重防重功能）
 *
 * 部署步骤：
 * 1. 打开 Google 表格，扩展程序 → Apps Script，粘贴本文件
 * 2. 修改下方 SHEET_NAME（工作表名称）
 * 3. 首次运行 setupSheet() 创建表头（只需执行一次）
 * 4. 部署 → 管理部署 → 部署新版本（或新建部署）
 *    - 执行身份：我
 *    - 访问权限：任何人
 * 5. 复制部署 URL，设为 Python 环境变量 GAS_WEBHOOK_URL
 */

const SHEET_NAME = '高风险+地址验证问题';
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
 * 接收 Python 批量 POST（已完美集成查重逻辑，防止网络重试导致的数据重复）
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

    // ----------------------------------------------------
    // 🔍 查重逻辑：读取表格现有数据，建立「店铺域名_订单号」防重索引
    // ----------------------------------------------------
    const existingData = sheet.getDataRange().getValues();
    const existingKeys = {};
    for (let i = 1; i < existingData.length; i++) {
      const domain = String(existingData[i][1] || '').trim();  // 第2列：店铺域名
      const orderNo = String(existingData[i][2] || '').trim(); // 第3列：订单号
      if (domain && orderNo) {
        existingKeys[domain + '_' + orderNo] = true;
      }
    }

    // 过滤出真正全新的订单
    const uniqueRows = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const domain = String(row[1] || '').trim();
      const orderNo = String(row[2] || '').trim();
      const key = domain + '_' + orderNo;

      if (!existingKeys[key]) {
        uniqueRows.push(row);
        existingKeys[key] = true; // 标记防止单次推送批次内有重复
      }
    }

    // 如果全部都是已存在的重复订单，直接跳过，防止多写入
    if (uniqueRows.length === 0) {
      return jsonResponse({
        ok: true,
        appended: 0,
        message: 'All orders in this payload already exist in the sheet.',
      });
    }

    const numRows = uniqueRows.length;
    const numCols = uniqueRows[0].length;

    // 校验每行列数一致
    for (let i = 0; i < numRows; i++) {
      if (!Array.isArray(uniqueRows[i]) || uniqueRows[i].length !== numCols) {
        return jsonResponse({
          ok: false,
          error: `Row ${i + 1} has inconsistent column count`,
        }, 400);
      }
    }

    const startRow = sheet.getLastRow() + 1;
    sheet.getRange(startRow, 1, numRows, numCols).setValues(uniqueRows);

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