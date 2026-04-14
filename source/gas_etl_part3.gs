/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part3
 * Enrich 7단계 캐스케이드 + PB_OVERRIDE + NAME_OVERRIDE + Cross-brand Split
 *
 * 실행 위치: Step3_enrichAndDecompose
 *   1) applySellicOverlayToAll_(rows, overlay)   ← 셀릭 우선 덮어쓰기
 *   2) 행별 PB_OVERRIDE / PB_OVERRIDE_2026 적용
 *   3) enrichRow_ 7단계
 *   4) 실패 시 NAME_OVERRIDE → enrichRow_ 재시도 → SELLMATE_ENRICH_HARDCODE
 *   5) Cross-brand split (pb 기반 재분류)
 *   6) resolveStore_ 호출 → 재무/ERP/구분/채널 확정
 *   7) Red/Yellow 판정 (개인결제/세금계산서/고객님 예외)
 *
 * 요구: Part1 (유틸) + Part2 (마스터) + Sellic 모듈
 * ========================================================================
 */

// ============================================================
// 1. PB_OVERRIDE (영구 규칙)
// ============================================================
var PB_OVERRIDE = {
  'CTG1JK02': 'CTG3JK81'
};

// ============================================================
// 2. PB_OVERRIDE_2026 (2026-01-01 이후만 적용)
// ============================================================
var PB_OVERRIDE_2026 = {
  'CTA0WS01': 'CTA0WS81',   // 링클 프리 베이직 셔츠
  'CTE1WS01': 'CTA0WS81',   // 동일 상품 시즌코드
  'ATA0DP01': 'ATA0DP81'    // 셀비 포켓 데님 팬츠
};

// ============================================================
// 3. Red 제외 키워드 (정상 주문이지만 pb 없음)
// ============================================================
var RED_EXCLUDE_KEYWORDS = ['개인결제', '세금계산서', '고객님'];

function _hasRedExcludeKeyword_(name) {
  if (!name) return false;
  for (var i = 0; i < RED_EXCLUDE_KEYWORDS.length; i++) {
    if (String(name).indexOf(RED_EXCLUDE_KEYWORDS[i]) >= 0) return true;
  }
  return false;
}

// ============================================================
// 4. enrich 7단계 캐스케이드
//
//   반환: {valid, yellow, cost, tag, name, colorName, cc, sz, stage}
//     - stage: 1=direct, 2=sz0F, 3=wrongCC, 4=krColor, 5=singleOpt, 6=ccOnly, 7=anySibling
//     - valid=true && yellow=false → Green (완전 매칭)
//     - valid=true && yellow=true  → Yellow (fallback 매칭)
//     - valid=false → Red 후보
// ============================================================
function _emptyEnrich_(cc, sz) {
  return {valid: false, yellow: false, cost: 0, tag: 0,
          name: '', colorName: '', cc: cc || '', sz: sz || '', stage: 0};
}

function enrichRow_(pb, cc, sz, inv, invPb, invColors) {
  if (!pb) return _emptyEnrich_(cc, sz);

  // 1. Direct match
  var d = inv[pb + '|' + cc + '|' + sz];
  if (d) return {valid: true, yellow: false, cost: d.cost, tag: d.tag,
                 name: d.name, colorName: d.colorName, cc: cc, sz: sz, stage: 1};

  // 2. sz=0F fallback
  if (cc) {
    d = inv[pb + '|' + cc + '|0F'];
    if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                   name: d.name, colorName: d.colorName, cc: cc, sz: '0F', stage: 2};
  }

  // 3. WRONG_COLOR_MAP remap
  if (cc && WRONG_COLOR_MAP[cc]) {
    var newCc = WRONG_COLOR_MAP[cc];
    var szCandidates = [sz, '0F'];
    for (var s = 0; s < szCandidates.length; s++) {
      d = inv[pb + '|' + newCc + '|' + szCandidates[s]];
      if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                     name: d.name, colorName: d.colorName,
                     cc: newCc, sz: szCandidates[s], stage: 3};
    }
  }

  // 4. Korean color fuzzy match (cc 재파싱 후 invColors[pb] 순회)
  if (cc && invColors[pb]) {
    var krCc = parseCc_(cc);
    var mapKeys = Object.keys(invColors[pb]);
    for (var k = 0; k < mapKeys.length; k++) {
      var mapCc = mapKeys[k];
      if (krCc === mapCc) {
        var szCandidates2 = [sz, '0F'];
        for (var s2 = 0; s2 < szCandidates2.length; s2++) {
          d = inv[pb + '|' + mapCc + '|' + szCandidates2[s2]];
          if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                         name: d.name, colorName: d.colorName,
                         cc: mapCc, sz: szCandidates2[s2], stage: 4};
        }
      }
    }
  }

  // 5~7. invPb 기반 fallback
  if (invPb[pb]) {
    var avail = invPb[pb];

    // 5. 옵션 1개뿐
    if (avail.length === 1) {
      var only = avail[0];
      d = inv[pb + '|' + only[0] + '|' + only[1]];
      if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                     name: d.name, colorName: d.colorName,
                     cc: only[0], sz: only[1], stage: 5};
    }

    // 6. CC만 일치
    if (cc) {
      var matching = avail.filter(function(x) { return x[0] === cc; });
      if (matching.length) {
        var m = matching[0];
        d = inv[pb + '|' + m[0] + '|' + m[1]];
        if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                       name: d.name, colorName: d.colorName,
                       cc: m[0], sz: m[1], stage: 6};
      }
    }

    // 7. 아무 형제 옵션
    var first = avail[0];
    d = inv[pb + '|' + first[0] + '|' + first[1]];
    if (d) return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
                   name: d.name, colorName: d.colorName,
                   cc: cc || first[0], sz: sz || first[1], stage: 7};
  }

  return _emptyEnrich_(cc, sz);
}

// ============================================================
// 5. PB_OVERRIDE 적용
// ============================================================
function applyPbOverride_(row) {
  if (!row.pb) return row;
  // 영구 override
  if (PB_OVERRIDE[row.pb]) {
    row._origPbBeforeOverride = row.pb;
    row.pb = PB_OVERRIDE[row.pb];
  }
  // 2026-01-01 이후
  if (row.orderDate && row.orderDate >= REMAP_CUTOFF && PB_OVERRIDE_2026[row.pb]) {
    row._origPbBeforeOverride2026 = row.pb;
    row.pb = PB_OVERRIDE_2026[row.pb];
  }
  return row;
}

// ============================================================
// 6. NAME_OVERRIDE 재시도 (enrich 실패 시)
// ============================================================
function applyNameOverride_(row, masters) {
  if (!row.productName) return null;

  // 원본 그대로 조회
  var pbFromName = NAME_OVERRIDE[row.productName] || null;

  // 정규화 키 조회 (브랜드별 nameToPb)
  if (!pbFromName && masters.nameToPb) {
    var p2Guess = detectBrandFromPb_(row.pb) === '아티드' ? 'AT' : 'CT';
    var nk = normalizeNameKey_(row.productName);
    var branded = masters.nameToPb[p2Guess];
    if (branded && branded[nk]) pbFromName = branded[nk];
    // 브랜드 미확정 시 양쪽 조회
    if (!pbFromName) {
      ['CT', 'AT', 'CU', 'CA', 'CM', 'AX'].forEach(function(prefix) {
        if (pbFromName) return;
        var dict = masters.nameToPb[prefix];
        if (dict && dict[nk]) pbFromName = dict[nk];
      });
    }
  }

  if (!pbFromName) return null;

  // 새 pb로 enrich 재시도
  var info = enrichRow_(pbFromName, row.cc, row.sz, masters.inv, masters.invPb, masters.invColors);
  if (info.valid) {
    row._nameOverrideHit = true;
    row.pb = pbFromName;
    return info;
  }

  // SELLMATE_ENRICH_HARDCODE fallback
  if (SELLMATE_ENRICH_HARDCODE[pbFromName]) {
    var d = SELLMATE_ENRICH_HARDCODE[pbFromName];
    row._sellmateHit = true;
    row.pb = pbFromName;
    return {valid: true, yellow: true, cost: d.cost, tag: d.tag,
            name: d.name, colorName: row.colorName || '',
            cc: row.cc, sz: row.sz, stage: 99};
  }

  return null;
}

// ============================================================
// 7. Cross-brand split
//    pb prefix로 실제 브랜드 판별 → row.brand 재지정
// ============================================================
function applyCrossBrandSplit_(row) {
  if (!row.pb) return row;
  var pbBrand = detectBrandFromPb_(row.pb);
  if (pbBrand && pbBrand !== row.brand) {
    row._origBrand = row.brand;
    row.brand = pbBrand;
  }
  return row;
}

// ============================================================
// 8. 시즌 / 카테고리 추출 (품번 파싱)
// ============================================================
function extractSeasonCategory_(pb) {
  if (!pb || pb.length < 6) return {season: '', category: ''};
  // 예: CTH3KT93 → 시즌=H3 (3~4), 카테고리=KT (5~6)
  return {
    season: pb.substring(2, 4),
    category: pb.substring(4, 6)
  };
}

// ============================================================
// 9. 행별 Enrich 파이프라인
//    입력: row (표준 row 객체)
//    출력: row 갱신 + enrichInfo 주입
// ============================================================
function enrichSingleRow_(row, masters) {
  // 셀릭 오버라이드 행은 enrich 건너뛰기 (pb/cc/sz가 이미 셀릭 바코드 기준)
  // 단, 상품명/원가/택가는 종합재고에서 채워야 하므로 enrich는 돌리되 stage만 별도 기록
  applyPbOverride_(row);

  // 7단계 캐스케이드
  var info = enrichRow_(row.pb, row.cc, row.sz, masters.inv, masters.invPb, masters.invColors);

  // 실패 → NAME_OVERRIDE 재시도
  if (!info.valid) {
    var retry = applyNameOverride_(row, masters);
    if (retry) info = retry;
  }

  // 결과 주입
  row.cost = info.valid ? info.cost : 0;
  row.tag = info.valid ? info.tag : 0;
  row.productName = info.valid ? info.name : row.productName;  // 종합재고값 우선
  row.colorName = info.valid ? info.colorName : (row.colorName || '');
  row.cc = info.valid ? info.cc : row.cc;
  row.sz = info.valid ? info.sz : row.sz;

  // error_flag 결정
  if (info.valid) {
    row._errorFlag = info.yellow ? 'yellow' : '';
    row._enrichStage = info.stage;
  } else {
    row._enrichStage = 0;
    if (_hasRedExcludeKeyword_(row.productName)) {
      row._errorFlag = '';  // 정상 건 (개인결제 등)
    } else {
      row._errorFlag = 'red';
    }
  }

  // 시즌/카테고리
  var sc = extractSeasonCategory_(row.pb);
  row.season = sc.season;
  row.category = sc.category;

  // Cross-brand split (pb 확정 후)
  applyCrossBrandSplit_(row);

  // 매장 정보 (resolveStore_)
  var store = resolveStore_(
    row.brand, row.channel, !!row.isOffline,
    masters.storeLookup, masters.aliasMap, masters.erpToChannel
  );
  row.fc = store.fc;
  row.erp = store.erp;
  row.div = store.div;
  row.channelResolved = store.ch;

  return row;
}

// ============================================================
// 10. Step3 엔트리포인트 (배치 처리 + 체이닝)
// ============================================================
var STEP3_BATCH_SIZE = 5000;   // 한 번 실행당 처리할 row 수
var ETL_SHEET_TEMP_RAW = '_tempRaw';
var ETL_SHEET_TEMP_RESULT = '_tempResult';

function Step3_enrichAndDecompose(batchIdx) {
  var t0 = Date.now();
  batchIdx = parseInt(batchIdx || getEtlState_('ETL_BATCH_IDX') || '0', 10);
  logProgress_('Step3', 'START batch=' + batchIdx);

  try {
    // 마스터 로드
    var masters = loadMastersFromSheets_();

    // ※ 셀릭 오버레이는 Step3이 아닌 Step5에서 적용 (postprocess 후 검증 패스)

    // 배치 범위
    var start = batchIdx * STEP3_BATCH_SIZE;
    var end = start + STEP3_BATCH_SIZE;
    var rawBatch = _readTempRawRange_(start, end);
    if (!rawBatch.length) {
      logProgress_('Step3', 'DONE — 배치 없음, Step4로 전환');
      setEtlState_('Step4', {'ETL_BATCH_IDX': '0'});
      scheduleNextStep_('Step4_postprocess', 1000);
      return;
    }

    // 행별 enrich
    var resultRows = [];
    var startProc = Date.now();
    var yieldCount = 0;
    for (var i = 0; i < rawBatch.length; i++) {
      enrichSingleRow_(rawBatch[i], masters);
      resultRows.push(_toOutputRow_(rawBatch[i]));
      if ((i % 500 === 0) && shouldYield_(startProc, STEP_BUDGET_MS - 30000)) {
        // 시간 초과 임박 → 현재까지 처리 후 다음 트리거
        logProgress_('Step3', '시간 예산 근접, i=' + i + '/' + rawBatch.length);
        yieldCount = i + 1;
        break;
      }
    }
    if (!yieldCount) yieldCount = rawBatch.length;

    // 결과 시트에 append
    _appendResultRows_(resultRows.slice(0, yieldCount));

    // 다음 배치 상태 업데이트
    var nextBatchIdx = yieldCount < rawBatch.length ? batchIdx : batchIdx + 1;
    // 부분 처리 시 TEMP_RAW에서 처리된 부분만 제거하는 방식은 복잡 → 진행 커서 사용
    var procCursor = parseInt(getEtlState_('ETL_PROC_CURSOR') || '0', 10);
    procCursor = start + yieldCount;
    setEtlState_('Step3', {
      'ETL_BATCH_IDX': String(nextBatchIdx),
      'ETL_PROC_CURSOR': String(procCursor)
    });

    logProgress_('Step3',
      'END batch=' + batchIdx + ' 처리=' + yieldCount + '/' + rawBatch.length +
      ' (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');

    // 다음 배치 트리거
    scheduleNextStep_('Step3_enrichAndDecompose', 1000);

  } catch (e) {
    logProgress_('Step3', 'ERROR: ' + e.message + '\n' + e.stack);
    throw e;
  }
}

// ============================================================
// 11. TEMP_RAW 시트 I/O 헬퍼
// ============================================================
function _readTempRawAll_() {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RAW);
  if (!sh) return [];
  var data = sh.getDataRange().getValues();
  if (data.length < 2) return [];
  var H = data[0];
  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var obj = {};
    for (var j = 0; j < H.length; j++) obj[H[j]] = data[i][j];
    rows.push(obj);
  }
  return rows;
}

function _readTempRawRange_(start, end) {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RAW);
  if (!sh) return [];
  var lastRow = sh.getLastRow();
  if (lastRow < 2) return [];
  var headerRow = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
  var rowStart = 2 + start;
  if (rowStart > lastRow) return [];
  var rowEnd = Math.min(2 + end - 1, lastRow);
  var n = rowEnd - rowStart + 1;
  var data = sh.getRange(rowStart, 1, n, sh.getLastColumn()).getValues();
  var rows = [];
  for (var i = 0; i < data.length; i++) {
    var obj = {};
    for (var j = 0; j < headerRow.length; j++) obj[headerRow[j]] = data[i][j];
    rows.push(obj);
  }
  return rows;
}

function _writeTempRawAll_(rows) {
  if (!rows.length) return;
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RAW);
  if (!sh) sh = ss.insertSheet(ETL_SHEET_TEMP_RAW);
  sh.clear();
  var keys = Object.keys(rows[0]);
  var out = [keys];
  rows.forEach(function(r) {
    var row = keys.map(function(k) { return r[k]; });
    out.push(row);
  });
  sh.getRange(1, 1, out.length, keys.length).setValues(out);
}

function _appendResultRows_(outRows) {
  if (!outRows.length) return;
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_TEMP_RESULT);
  if (!sh) {
    sh = ss.insertSheet(ETL_SHEET_TEMP_RESULT);
    // 25컬럼 헤더
    sh.getRange(1, 1, 1, 25).setValues([[
      'NO','재무코드','ERP코드','구분','채널명','시즌','카테고리',
      '품번','상품명','컬러명','컬러코드','사이즈',
      '원가(VAT+)','정상가(TAG가)','수량',
      '거래액','거래액(쿠폰포함)','거래액(자체쿠폰포함)','거래액(결제금액)',
      '주문일','주문월','화요주차','주문번호','','오류/오류점검'
    ]]);
  }
  var lastRow = sh.getLastRow();
  sh.getRange(lastRow + 1, 1, outRows.length, 25).setValues(outRows);
}

// ============================================================
// 12. 표준 row → 25컬럼 출력 배열
// ============================================================
function _toOutputRow_(row) {
  return [
    '',                              // 0 NO (Step4에서 채움)
    row.fc || '',                    // 1 재무코드
    row.erp || '',                   // 2 ERP코드
    row.div || '',                   // 3 구분
    row.channelResolved || row.channel || '', // 4 채널명
    row.season || '',                // 5 시즌
    row.category || '',              // 6 카테고리
    row.pb || '',                    // 7 품번
    row.productName || '',           // 8 상품명
    row.colorName || '',             // 9 컬러명
    row.cc || '',                    // 10 컬러코드
    row.sz || '',                    // 11 사이즈
    toNum_(row.cost) * toNum_(row.qty),   // 12 원가(VAT+) × 수량
    toNum_(row.tag) * toNum_(row.qty),    // 13 정상가 × 수량
    toNum_(row.qty),                 // 14 수량
    toNum_(row.trans),               // 15 거래액
    toNum_(row.transCoupon),         // 16 거래액(쿠폰포함)
    toNum_(row.transSelf),           // 17 거래액(자체쿠폰포함)
    toNum_(row.transPay),            // 18 거래액(결제금액)
    row.orderDate || '',             // 19 주문일
    (row.orderDate || '').substring(0, 7),  // 20 주문월
    calcTuesdayWeek_(row.orderDate), // 21 화요주차
    row.orderNo || '',               // 22 주문번호
    '',                              // 23 공란
    row._errorFlag || ''             // 24 오류/오류점검
  ];
}

// ============================================================
// 13. 자가 검증
// ============================================================
function test_Part3_() {
  var masters = loadMastersFromSheets_();
  if (!Object.keys(masters.inv).length) {
    console.log('마스터 미준비 → Step1_loadMasters() 선행 필요');
    return;
  }

  // 샘플 1: enrich 7단계 테스트
  var samplePbs = Object.keys(masters.invPb).slice(0, 3);
  samplePbs.forEach(function(pb) {
    var opts = masters.invPb[pb][0];
    var info = enrichRow_(pb, opts[0], opts[1], masters.inv, masters.invPb, masters.invColors);
    console.log('enrich(' + pb + '|' + opts[0] + '|' + opts[1] + ') = valid:' +
      info.valid + ' stage:' + info.stage + ' name:' + (info.name || '').substring(0, 30));
  });

  // 샘플 2: PB_OVERRIDE 테스트
  var row1 = {pb: 'CTG1JK02', cc: 'BK', sz: '0M', orderDate: '2026-03-15'};
  applyPbOverride_(row1);
  console.log('PB_OVERRIDE: CTG1JK02 → ' + row1.pb + ' (expect CTG3JK81)');

  var row2 = {pb: 'CTA0WS01', cc: 'WH', sz: '0F', orderDate: '2026-03-15'};
  applyPbOverride_(row2);
  console.log('PB_OVERRIDE_2026: CTA0WS01 → ' + row2.pb + ' (expect CTA0WS81)');

  var row3 = {pb: 'CTA0WS01', cc: 'WH', sz: '0F', orderDate: '2025-12-31'};
  applyPbOverride_(row3);
  console.log('PB_OVERRIDE_2026 이전일: CTA0WS01 → ' + row3.pb + ' (expect CTA0WS01 — 미적용)');

  // 샘플 3: Red 제외 키워드
  console.log('Red 제외(개인결제): ' + _hasRedExcludeKeyword_('개인결제 주문'));
  console.log('Red 제외(일반): ' + _hasRedExcludeKeyword_('블랙 티셔츠'));

  // 샘플 4: enrichSingleRow_ 종합 테스트
  var sampleRow = {
    brand: '시티브리즈', channel: '29CM', isOffline: false,
    pb: samplePbs[0], cc: masters.invPb[samplePbs[0]][0][0], sz: masters.invPb[samplePbs[0]][0][1],
    productName: 'RAW 상품명', qty: 1,
    trans: 100000, transCoupon: 100000, transSelf: 100000, transPay: 100000,
    orderDate: '2026-03-15', orderNo: 'T123'
  };
  enrichSingleRow_(sampleRow, masters);
  console.log('enrichSingleRow_: name=' + (sampleRow.productName || '').substring(0, 30) +
    ' fc=' + sampleRow.fc + ' flag=' + sampleRow._errorFlag);
}
