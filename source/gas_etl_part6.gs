/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part6
 * 파이프라인 오케스트레이터
 *
 * 실행 순서:
 *   runFullETL()
 *     │
 *     ├─ Step1_loadMasters               [~2분]   (Part2)
 *     ├─ Step2_readRAW                   [~5분/batch]
 *     ├─ Step3_enrichAndDecompose        [~5분/batch]  (Part3)
 *     ├─ Step4_postprocess               [~3분]  (SET 균등·NO·서식)
 *     ├─ Step5_applySellicValidation     [~3분]  (← 셀릭 검증/교정 + 재enrich)
 *     ├─ Step6_exportXLSX                [~2분]  (Part7)
 *     └─ Step7_cleanup                   [~1분]
 *
 * 셀릭 위치 근거:
 *   - 셀릭은 채널 RAW 파싱 오류 검증/교정 레이어
 *   - 원본 채널 기준 가공(SET분해·거래액균등)이 전부 끝난 뒤 마지막에 pb/cc/sz만 교정
 * ========================================================================
 */

// ============================================================
// 1. 엔트리포인트
// ============================================================
function runFullETL() {
  logProgress_('runFullETL', 'START - 전체 ETL 파이프라인 시작');

  // 기존 ETL 트리거 전부 제거 (재시작 안전장치)
  clearEtlTriggers_();

  // 상태 초기화
  PropertiesService.getScriptProperties().setProperties({
    'ETL_STATE': 'Step1',
    'ETL_BATCH_IDX': '0',
    'ETL_STARTED_AT': String(Date.now()),
    'ETL_MASTERS_READY': 'false',
    'ETL_SELLIC_READY': 'false',
    'ETL_PROC_CURSOR': '0'
  });

  // Step1 체이닝 시작
  scheduleNextStep_('Step1_loadMasters', 1000);
  logProgress_('runFullETL', 'Step1 트리거 예약 완료 — 이후 자동 진행');
}

// ============================================================
// 2. Step2: 채널별 RAW 수집 + 셀릭 오버레이 구축
//    RAW_FOLDER_ID 내 모든 xlsx 순회 → detectChannel_ → CHANNEL_READERS[] 호출
//    결과 → _tempRaw 시트 (주문 dedup 키 부여)
// ============================================================
var STEP2_MAX_FILES_PER_BATCH = 8;  // 한 번 실행당 처리 파일 수

function Step2_readRAW(batchIdx) {
  var t0 = Date.now();
  batchIdx = parseInt(batchIdx || getEtlState_('ETL_BATCH_IDX') || '0', 10);
  logProgress_('Step2', 'START batch=' + batchIdx);

  try {
    var masters = loadMastersFromSheets_();

    // 파일 목록 (캐시)
    var fileListJson = getEtlState_('ETL_S2_FILES');
    var fileList;
    if (!fileListJson) {
      fileList = listXlsxInFolder_(RAW_FOLDER_ID, true);  // 하위폴더 포함
      // 2025 이전 파일 스킵 (파일명에 단서)
      fileList = fileList.filter(function(f) {
        return !/2024|2025/.test(f.name) && !/2024|2025/.test(f.subfolder || '');
      });
      PropertiesService.getScriptProperties().setProperty('ETL_S2_FILES', JSON.stringify(fileList));
      logProgress_('Step2', '파일 리스트 캐시: ' + fileList.length + '개');
    } else {
      fileList = JSON.parse(fileListJson);
    }

    var start = batchIdx * STEP2_MAX_FILES_PER_BATCH;
    var end = Math.min(start + STEP2_MAX_FILES_PER_BATCH, fileList.length);
    if (start >= fileList.length) {
      // 모든 파일 처리 완료 → dedup 후 Step3으로
      logProgress_('Step2', '모든 파일 처리 완료, dedup 실행');
      _dedupTempRaw_();
      setEtlState_('Step3', {'ETL_BATCH_IDX': '0'});
      scheduleNextStep_('Step3_enrichAndDecompose', 1000);
      return;
    }

    // 배치 내 파일 순회
    var collected = [];
    for (var i = start; i < end; i++) {
      var f = fileList[i];
      var ch = detectChannel_(f.name);
      if (!ch) {
        logProgress_('Step2', 'SKIP(unknown): ' + f.name);
        continue;
      }
      var reader = CHANNEL_READERS[ch];
      if (!reader) continue;
      var brand = detectBrandFromFilename_(f.name);
      try {
        var rows = readXlsxAsRows_(f.id);
        var parsed = reader(rows, brand);
        parsed.forEach(function(r) {
          r._fileId = f.id; r._fileName = f.name; r._channelKey = ch;
        });
        collected = collected.concat(parsed);
        logProgress_('Step2', '읽기 ' + f.name + ' [' + ch + '] → ' + parsed.length + '행');
      } catch (e) {
        logProgress_('Step2', 'ERROR ' + f.name + ': ' + e.message);
      }
    }

    // _tempRaw 시트에 append
    _appendTempRaw_(collected);

    // 다음 배치 예약
    setEtlState_('Step2', {'ETL_BATCH_IDX': String(batchIdx + 1)});
    logProgress_('Step2', 'END batch=' + batchIdx +
      ' (' + ((Date.now() - t0) / 1000).toFixed(1) + 's) append=' + collected.length);
    scheduleNextStep_('Step2_readRAW', 1000);

  } catch (e) {
    logProgress_('Step2', 'FATAL: ' + e.message + '\n' + e.stack);
    throw e;
  }
}

function _appendTempRaw_(rows) {
  if (!rows.length) return;
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RAW);
  var keys = ['brand','channel','isOffline','erpCode','pb','cc','sz',
              'productName','colorRaw','sizeRaw','optionRaw','qty',
              'trans','transCoupon','transSelf','transPay',
              'orderDate','orderNo','isSet','_fileId','_fileName','_channelKey'];
  if (!sh) {
    sh = ss.insertSheet(ETL_SHEET_TEMP_RAW);
    sh.getRange(1, 1, 1, keys.length).setValues([keys]);
  }
  var data = rows.map(function(r) {
    return keys.map(function(k) { return r[k] != null ? r[k] : ''; });
  });
  var lastRow = sh.getLastRow();
  sh.getRange(lastRow + 1, 1, data.length, keys.length).setValues(data);
}

// ============================================================
// 3. Cross-file dedup (brand|orderNo 기준, 최신 파일 유지)
// ============================================================
function _dedupTempRaw_() {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RAW);
  if (!sh || sh.getLastRow() < 3) return;

  var data = sh.getDataRange().getValues();
  var H = data[0];
  var iBrand = H.indexOf('brand');
  var iOno = H.indexOf('orderNo');
  var iFile = H.indexOf('_fileId');
  var iIsOffline = H.indexOf('isOffline');

  // 각 brand|orderNo의 최신 fileId 결정 (후순위가 최신)
  var latestFile = {};
  for (var i = 1; i < data.length; i++) {
    if (data[i][iIsOffline] === true || data[i][iIsOffline] === 'true') continue;  // 오프라인 제외
    var ono = cleanStr_(data[i][iOno]);
    if (!ono) continue;
    var key = data[i][iBrand] + '|' + ono;
    latestFile[key] = data[i][iFile];
  }

  // 오래된 파일의 row 표시 → 삭제
  var keep = [H];
  var removed = 0;
  for (var j = 1; j < data.length; j++) {
    if (data[j][iIsOffline] === true || data[j][iIsOffline] === 'true') {
      keep.push(data[j]); continue;
    }
    var ono2 = cleanStr_(data[j][iOno]);
    if (!ono2) { keep.push(data[j]); continue; }
    var key2 = data[j][iBrand] + '|' + ono2;
    if (latestFile[key2] === data[j][iFile]) keep.push(data[j]);
    else removed++;
  }

  sh.clear();
  sh.getRange(1, 1, keep.length, H.length).setValues(keep);
  logProgress_('Step2', 'dedup 완료: 유지=' + (keep.length - 1) + ', 제거=' + removed);
}

// ============================================================
// 4. Step4: 후처리 (SET 균등배분 + NO 번호 부여 + 숫자 변환)
// ============================================================
function Step4_postprocess() {
  var t0 = Date.now();
  logProgress_('Step4', 'START');

  try {
    var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
    var sh = ss.getSheetByName(ETL_SHEET_TEMP_RESULT);
    if (!sh || sh.getLastRow() < 2) {
      logProgress_('Step4', '_tempResult 비어있음 - SKIP');
      setEtlState_('Step5', {});
      scheduleNextStep_('Step5_exportXLSX', 1000);
      return;
    }

    // 1) SET 거래액 균등배분
    //    결과 시트 행을 row 객체로 변환 → rebalance → 다시 쓰기
    var data = sh.getDataRange().getValues();
    var H = data[0];
    // 25컬럼 기준 인덱스
    var iOno = 22, iName = 8, iTrans = 15, iTC = 16, iTS = 17, iTP = 18;

    // 주문번호별 SET 그룹핑
    var groups = {};
    for (var i = 1; i < data.length; i++) {
      var ono = cleanStr_(data[i][iOno]);
      var name = cleanStr_(data[i][iName]);
      if (!ono || !detectSet_(name)) continue;
      if (!groups[ono]) groups[ono] = [];
      groups[ono].push(i);
    }

    var balanced = 0;
    Object.keys(groups).forEach(function(ono) {
      var idxs = groups[ono];
      if (idxs.length < 2) return;
      var n = idxs.length;
      [iTrans, iTC, iTS, iTP].forEach(function(col) {
        var total = idxs.reduce(function(s, k) { return s + toNum_(data[k][col]); }, 0);
        var per = Math.round(total / n);
        idxs.forEach(function(k) { data[k][col] = per; });
      });
      balanced += n;
    });
    logProgress_('Step4', 'SET 거래액 균등배분: ' + balanced + '행');

    // 2) NO 번호 부여 (1부터)
    for (var j = 1; j < data.length; j++) {
      data[j][0] = j;
    }

    // 3) 저장
    sh.getRange(1, 1, data.length, data[0].length).setValues(data);

    // 4) Red/Yellow 셀 서식 적용 (25번째 컬럼 = error_flag)
    _applyErrorFormatting_(sh, data);

    // 통계 로그
    var stats = _computeFinalStats_(data);
    logProgress_('Step4', '통계 — 총=' + stats.total + ', Red=' + stats.red +
      ' (' + (stats.red/stats.total*100).toFixed(2) + '%), Yellow=' + stats.yellow +
      ' (' + (stats.yellow/stats.total*100).toFixed(2) + '%)');

    setEtlState_('Step5', {});
    logProgress_('Step4', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
    scheduleNextStep_('Step5_applySellicValidation', 1000);

  } catch (e) {
    logProgress_('Step4', 'FATAL: ' + e.message + '\n' + e.stack);
    throw e;
  }
}

function _applyErrorFormatting_(sh, data) {
  // 대량 setBackground는 느림 → Red/Yellow 행 번호 수집 후 RangeList로 일괄 적용
  var redRows = [], yellowRows = [];
  for (var i = 1; i < data.length; i++) {
    var flag = cleanStr_(data[i][24]);
    if (flag === 'red') redRows.push(i + 1);
    else if (flag === 'yellow') yellowRows.push(i + 1);
  }
  var lastCol = sh.getLastColumn();
  if (redRows.length) {
    var rangesR = redRows.map(function(r) { return r + ':' + r; });
    // 대량이면 100행씩 청크
    _bulkSetBg_(sh, redRows, lastCol, '#FFC7CE');
  }
  if (yellowRows.length) {
    _bulkSetBg_(sh, yellowRows, lastCol, '#FFF2CC');
  }
  logProgress_('Step4', '서식 적용: Red=' + redRows.length + ', Yellow=' + yellowRows.length);
}

function _bulkSetBg_(sh, rowNums, lastCol, color) {
  var CHUNK = 200;
  for (var i = 0; i < rowNums.length; i += CHUNK) {
    var chunk = rowNums.slice(i, i + CHUNK);
    var ranges = chunk.map(function(r) { return sh.getRange(r, 1, 1, lastCol); });
    ranges.forEach(function(rng) { rng.setBackground(color); });
  }
}

function _computeFinalStats_(data) {
  var red = 0, yellow = 0;
  for (var i = 1; i < data.length; i++) {
    var f = cleanStr_(data[i][24]);
    if (f === 'red') red++;
    else if (f === 'yellow') yellow++;
  }
  return {total: data.length - 1, red: red, yellow: yellow};
}

// ============================================================
// 5. Step5: 셀릭 검증/교정 (postprocess 후, export 전)
//    ① 셀릭 오버레이 인덱스 구축
//    ② _tempResult 행에 적용 (주문번호 단위 최적배정, 옵션바코드 분리)
//    ③ pb/cc/sz 변경된 행만 재enrich → 상품명/원가/택가 갱신
//    ④ Red/Yellow 서식 재적용
// ============================================================
function Step5_applySellicValidation() {
  var t0 = Date.now();
  logProgress_('Step5', 'START (셀릭 검증/교정)');

  try {
    // 1. 셀릭 오버레이 구축
    var overlay = buildSellicOverlay_();
    saveSellicOverlay_(overlay);
    var orderCount = Object.keys(overlay).length;
    logProgress_('Step5', '셀릭 오버레이: ' + orderCount + '주문');

    if (!orderCount) {
      logProgress_('Step5', '셀릭 데이터 없음 - SKIP');
      setEtlState_('Step6', {});
      scheduleNextStep_('Step6_exportXLSX', 1000);
      return;
    }

    // 2. _tempResult 읽기
    var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
    var sh = ss.getSheetByName(ETL_SHEET_TEMP_RESULT);
    if (!sh || sh.getLastRow() < 2) {
      logProgress_('Step5', '_tempResult 비어있음 - SKIP');
      setEtlState_('Step6', {});
      scheduleNextStep_('Step6_exportXLSX', 1000);
      return;
    }
    var data = sh.getDataRange().getValues();
    var H = data[0];

    // 25컬럼 인덱스 (Part1 COL 상수와 일치)
    var iPb = 7, iName = 8, iColorName = 9, iCc = 10, iSz = 11,
        iCost = 12, iTag = 13, iQty = 14, iOno = 22, iFlag = 24;

    // 3. 출력 행 → 표준 row 객체로 변환
    var rowObjs = [];
    for (var i = 1; i < data.length; i++) {
      rowObjs.push({
        _idx: i,
        pb: cleanStr_(data[i][iPb]),
        cc: cleanStr_(data[i][iCc]),
        sz: cleanStr_(data[i][iSz]),
        productName: cleanStr_(data[i][iName]),
        colorRaw: cleanStr_(data[i][iColorName]),
        qty: toNum_(data[i][iQty]),
        orderNo: cleanStr_(data[i][iOno])
      });
    }

    // 4. 셀릭 오버레이 적용 (N:N 최적배정)
    var result = applySellicOverlayToAll_(rowObjs, overlay);
    logProgress_('Step5', '오버라이드 적용: ' + result.overridden + '/' + result.total);

    // 5. 변경 행 재enrich → name/cost/tag/colorName 갱신
    var masters = loadMastersFromSheets_();
    var reenriched = 0, stillRed = 0;
    rowObjs.forEach(function(r) {
      if (!r._sellicOverride) return;  // 변경 없으면 skip
      var info = enrichRow_(r.pb, r.cc, r.sz, masters.inv, masters.invPb, masters.invColors);
      var i = r._idx;
      data[i][iPb] = r.pb;
      data[i][iCc] = r.cc;
      data[i][iSz] = r.sz;
      if (info.valid) {
        data[i][iName] = info.name;
        data[i][iColorName] = info.colorName;
        data[i][iCost] = info.cost * toNum_(data[i][iQty]);
        data[i][iTag] = info.tag * toNum_(data[i][iQty]);
        data[i][iFlag] = info.yellow ? 'yellow' : '';
        reenriched++;
      } else {
        data[i][iFlag] = _hasRedExcludeKeyword_(data[i][iName]) ? '' : 'red';
        stillRed++;
      }
      // 시즌/카테고리 갱신
      var sc = extractSeasonCategory_(r.pb);
      data[i][5] = sc.season;
      data[i][6] = sc.category;
    });
    logProgress_('Step5', '재enrich: ' + reenriched + '건 성공, ' + stillRed + '건 Red');

    // 6. 저장 + 서식 재적용
    sh.getRange(1, 1, data.length, data[0].length).setValues(data);
    _applyErrorFormatting_(sh, data);

    // 통계
    var stats = _computeFinalStats_(data);
    logProgress_('Step5', '셀릭 검증 후 통계 — 총=' + stats.total +
      ', Red=' + stats.red + ' (' + (stats.red/stats.total*100).toFixed(2) + '%)' +
      ', Yellow=' + stats.yellow + ' (' + (stats.yellow/stats.total*100).toFixed(2) + '%)');

    setEtlState_('Step6', {});
    logProgress_('Step5', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
    scheduleNextStep_('Step6_exportXLSX', 1000);

  } catch (e) {
    logProgress_('Step5', 'FATAL: ' + e.message + '\n' + (e.stack || ''));
    throw e;
  }
}

// ============================================================
// 6. Step7: 정리 (임시 시트 삭제, 상태 초기화)
// ============================================================
function Step7_cleanup() {
  var t0 = Date.now();
  logProgress_('Step6', 'START');
  try {
    var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
    [ETL_SHEET_INV, ETL_SHEET_INVPB, ETL_SHEET_INVCOLORS, ETL_SHEET_NAMETOPB,
     ETL_SHEET_STORE, ETL_SHEET_ALIAS, ETL_SHEET_ERPCH, ETL_SHEET_SELLMATE,
     ETL_SHEET_SELLIC, ETL_SHEET_TEMP_RAW, ETL_SHEET_TEMP_RESULT].forEach(function(name) {
      var sh = ss.getSheetByName(name);
      if (sh) ss.deleteSheet(sh);
    });

    PropertiesService.getScriptProperties().deleteProperty('ETL_S2_FILES');
    setEtlState_('DONE', {
      'ETL_FINISHED_AT': String(Date.now())
    });
    clearEtlTriggers_();

    logProgress_('Step7', 'END — ETL 전체 완료 (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
  } catch (e) {
    logProgress_('Step7', 'ERROR: ' + e.message);
  }
}

// ============================================================
// 6. 상태 조회 & 재시작 유틸
// ============================================================
function getEtlStatus() {
  var p = PropertiesService.getScriptProperties().getProperties();
  var keys = ['ETL_STATE','ETL_BATCH_IDX','ETL_PROC_CURSOR',
              'ETL_MASTERS_READY','ETL_SELLIC_READY',
              'ETL_STARTED_AT','ETL_FINISHED_AT',
              'ETL_INV_SOURCE','ETL_STORE_SOURCE','ETL_SELLIC_ORDER_COUNT'];
  var out = {};
  keys.forEach(function(k) {
    var v = p[k];
    if (k.endsWith('_AT') && v) v = new Date(parseInt(v, 10)).toString();
    out[k] = v || '';
  });
  console.log(JSON.stringify(out, null, 2));
  return out;
}

function stopEtl() {
  clearEtlTriggers_();
  setEtlState_('STOPPED', {});
  logProgress_('ETL', 'STOPPED — 모든 트리거 제거');
}

function resumeEtl() {
  var state = getEtlState_('ETL_STATE');
  if (!state || state === 'DONE' || state === 'STOPPED') {
    logProgress_('ETL', 'resume 불가 — state=' + state);
    return;
  }
  var fnMap = {
    'Step1': 'Step1_loadMasters',
    'Step2': 'Step2_readRAW',
    'Step3': 'Step3_enrichAndDecompose',
    'Step4': 'Step4_postprocess',
    'Step5': 'Step5_applySellicValidation',
    'Step6': 'Step6_exportXLSX',
    'Step8': 'Step8_deployHub',
    'Step7': 'Step7_cleanup'
  };
  var fn = fnMap[state];
  if (!fn) return;
  scheduleNextStep_(fn, 1000);
  logProgress_('ETL', 'resume: ' + fn + ' 재예약');
}
