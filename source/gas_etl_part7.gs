/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part7
 * Step6: 결과물 xlsx 생성 → Drive 저장 (날짜 + 버전 접미사)
 *
 * 파일명 규칙:
 *   base = 2026_로우데이터_결과물_MMDD
 *   첫 실행    → 2026_로우데이터_결과물_MMDD.xlsx
 *   2번째 실행 → 2026_로우데이터_결과물_MMDD_v2.xlsx
 *   3번째 실행 → 2026_로우데이터_결과물_MMDD_v3.xlsx
 *   ...
 *   ※ 기존 파일은 삭제하지 않고 보존
 * ========================================================================
 */

function Step6_exportXLSX() {
  var t0 = Date.now();
  logProgress_('Step6', 'START');

  try {
    var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
    var sheet = ss.getSheetByName(ETL_SHEET_TEMP_RESULT);
    if (!sheet) throw new Error('_tempResult 시트 없음');
    var gid = sheet.getSheetId();
    var lastRow = sheet.getLastRow();
    var lastCol = sheet.getLastColumn();
    logProgress_('Step6', '_tempResult 크기: ' + lastRow + '행 × ' + lastCol + '열');

    var todayMMDD = Utilities.formatDate(new Date(), ETL_TZ, 'MMdd');
    var base = '2026_로우데이터_결과물_' + todayMMDD;

    // xlsx export URL
    var url = 'https://docs.google.com/spreadsheets/d/' + ETL_SPREADSHEET_ID +
              '/export?format=xlsx&gid=' + gid;
    var res = UrlFetchApp.fetch(url, {
      headers: {Authorization: 'Bearer ' + ScriptApp.getOAuthToken()},
      muteHttpExceptions: true
    });
    if (res.getResponseCode() !== 200) {
      throw new Error('xlsx export 실패 HTTP ' + res.getResponseCode());
    }

    // Drive 폴더: 버전 접미사 자동 증분
    var folder = DriveApp.getFolderById(OUTPUT_FOLDER_ID);
    var finalName = base + '.xlsx';
    var ver = 1;
    while (folder.getFilesByName(finalName).hasNext()) {
      ver++;
      finalName = base + '_v' + ver + '.xlsx';
    }

    var blob = res.getBlob().setName(finalName);
    var file = folder.createFile(blob);
    logProgress_('Step6', '저장 완료: ' + finalName +
      ' (' + (blob.getBytes().length / 1024 / 1024).toFixed(2) + 'MB)' +
      ' URL=' + file.getUrl());

    // 상태 저장
    setEtlState_('Step7', {
      'ETL_OUTPUT_FILE': finalName,
      'ETL_OUTPUT_FILE_ID': file.getId(),
      'ETL_OUTPUT_URL': file.getUrl()
    });

    logProgress_('Step6', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
    // Step8 HUB 배포 → Step7 cleanup (Step8 내부에서 Step7 체이닝)
    scheduleNextStep_('Step8_deployHub', 1000);

  } catch (e) {
    logProgress_('Step6', 'FATAL: ' + e.message + '\n' + (e.stack || ''));
    throw e;
  }
}

// ============================================================
// 보조: Step5 결과 확인
// ============================================================
function checkLastOutput() {
  var fileId = getEtlState_('ETL_OUTPUT_FILE_ID');
  var name = getEtlState_('ETL_OUTPUT_FILE');
  var url = getEtlState_('ETL_OUTPUT_URL');
  console.log('마지막 결과물:');
  console.log('  파일명: ' + name);
  console.log('  파일ID: ' + fileId);
  console.log('  URL: ' + url);
  return {name: name, id: fileId, url: url};
}
