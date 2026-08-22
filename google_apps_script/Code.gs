/**
 * Shopify 风控中台 — Google Sheets 接收端
 *
 * 功能：
 * 1. 新订单不存在 → 新增
 * 2. 店铺域名 + 订单号已存在 → 更新原来的行
 * 3. 不会因为 GitHub Actions 重试而产生重复订单
 */

const SHEET_NAME = '高风险+地址验证问题';
const API_TOKEN = ''; // 暂时不用，保持为空


/**
 * 初始化表头
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
    '标签',
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, headers.length);
}


/**
 * 接收 Python 批量 POST
 *
 * 核心逻辑：
 *
 * 已存在：
 *   店铺域名 + 订单号
 *        ↓
 *   更新原来的行
 *
 * 不存在：
 *   新增到表格最后
 */
function doPost(e) {
  try {

    // ----------------------------------------------------
    // 1. API Token 验证
    // ----------------------------------------------------

    if (API_TOKEN) {
      const token =
        (e.parameter && e.parameter.token) ||
        (e.headers &&
          (e.headers['X-Api-Token'] ||
           e.headers['x-api-token']));

      if (token !== API_TOKEN) {
        return jsonResponse({
          ok: false,
          error: 'Unauthorized'
        }, 401);
      }
    }


    // ----------------------------------------------------
    // 2. 检查请求
    // ----------------------------------------------------

    if (!e || !e.postData || !e.postData.contents) {
      return jsonResponse({
        ok: false,
        error: 'Empty request body'
      }, 400);
    }


    // ----------------------------------------------------
    // 3. 解析 JSON
    // ----------------------------------------------------

    const payload = JSON.parse(e.postData.contents);
    const rows = payload.rows;

    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      return jsonResponse({
        ok: true,
        added: 0,
        updated: 0,
        message: 'No rows to process'
      });
    }

    if (!Array.isArray(rows[0])) {
      return jsonResponse({
        ok: false,
        error: 'rows must be a 2D array'
      }, 400);
    }


    // ----------------------------------------------------
    // 4. 获取工作表
    // ----------------------------------------------------

    const sheet = getOrCreateSheet_();


    // ----------------------------------------------------
    // 5. 读取现有数据
    // ----------------------------------------------------

    const lastRow = sheet.getLastRow();
    const lastColumn = sheet.getLastColumn();

    let existingData = [];

    if (lastRow >= 2 && lastColumn >= 3) {
      existingData = sheet
        .getRange(2, 1, lastRow - 1, lastColumn)
        .getValues();
    }


    // ----------------------------------------------------
    // 6. 建立索引
    //
    // key = 店铺域名 + "_" + 订单号
    //
    // value = 表格中的真实行号
    // ----------------------------------------------------

    const existingKeys = {};

    for (let i = 0; i < existingData.length; i++) {

      const domain =
        String(existingData[i][1] || '').trim();

      const orderNo =
        String(existingData[i][2] || '').trim();

      if (domain && orderNo) {

        const key = domain + '_' + orderNo;

        existingKeys[key] = i + 2;
      }
    }


    // ----------------------------------------------------
    // 7. 分离：
    //
    // 新订单
    // 已存在订单
    // ----------------------------------------------------

    const newRows = [];
    const updateRows = [];

    for (let i = 0; i < rows.length; i++) {

      const row = rows[i];

      if (!Array.isArray(row)) {
        return jsonResponse({
          ok: false,
          error: `Row ${i + 1} is not an array`
        }, 400);
      }

      if (row.length !== 9) {
        return jsonResponse({
          ok: false,
          error:
            `Row ${i + 1} has ${row.length} columns, expected 9`
        }, 400);
      }


      const domain =
        String(row[1] || '').trim();

      const orderNo =
        String(row[2] || '').trim();


      if (!domain || !orderNo) {
        continue;
      }


      const key = domain + '_' + orderNo;


      // ------------------------------------------------
      // 已存在 → 更新
      // ------------------------------------------------

      if (existingKeys[key]) {

        updateRows.push({
          rowNumber: existingKeys[key],
          values: row
        });

      }

      // ------------------------------------------------
      // 不存在 → 新增
      // ------------------------------------------------

      else {

        newRows.push(row);

        // 非常重要：
        // 防止同一次 POST 中出现两个完全相同的订单
        existingKeys[key] = -1;
      }
    }


    // ----------------------------------------------------
    // 8. 执行更新
    // ----------------------------------------------------

    for (let i = 0; i < updateRows.length; i++) {

      const item = updateRows[i];

      sheet
        .getRange(
          item.rowNumber,
          1,
          1,
          item.values.length
        )
        .setValues([item.values]);
    }


    // ----------------------------------------------------
    // 9. 执行新增
    // ----------------------------------------------------

    let startRow = null;
    let endRow = null;

    if (newRows.length > 0) {

      startRow = sheet.getLastRow() + 1;
      endRow = startRow + newRows.length - 1;

      sheet
        .getRange(
          startRow,
          1,
          newRows.length,
          newRows[0].length
        )
        .setValues(newRows);
    }


    // ----------------------------------------------------
    // 10. 返回处理结果
    // ----------------------------------------------------

    return jsonResponse({

      ok: true,

      added: newRows.length,

      updated: updateRows.length,

      startRow: startRow,

      endRow: endRow,

      message:
        `处理完成：新增 ${newRows.length} 条，更新 ${updateRows.length} 条`

    });


  } catch (err) {

    return jsonResponse({
      ok: false,
      error: String(err)
    }, 500);
  }
}


/**
 * GET 测试
 */
function doGet() {

  return jsonResponse({

    ok: true,

    service: 'shopify-risk-monitor',

    sheet: SHEET_NAME,

    usage:
      'POST JSON {"rows": [["col1", "col2", ...]]}'

  });
}


/**
 * 获取或创建工作表
 */
function getOrCreateSheet_() {

  const ss =
    SpreadsheetApp.getActiveSpreadsheet();

  let sheet =
    ss.getSheetByName(SHEET_NAME);

  if (!sheet) {

    setupSheet();

    sheet =
      ss.getSheetByName(SHEET_NAME);
  }

  return sheet;
}


/**
 * JSON 返回
 */
function jsonResponse(obj, statusCode) {

  const output =
    ContentService
      .createTextOutput(
        JSON.stringify(
          statusCode && statusCode >= 400
            ? {
                ...obj,
                httpStatus: statusCode
              }
            : obj
        )
      )
      .setMimeType(
        ContentService.MimeType.JSON
      );

  return output;
}
