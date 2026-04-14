/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — 셀릭 (SELLIC) 오버레이 모듈
 *
 * 역할 (정정):
 *  - 셀릭은 '채널' 아님, '17번째 리더' 아님
 *  - **기존 16채널 로우데이터의 pb/cc/sz를 '셀릭 옵션바코드' 기준으로 덮어쓰는 오버레이**
 *  - 동작 규칙:
 *      ① 로우데이터(16채널 결과)를 먼저 생성
 *      ② 셀릭 RAW를 orderNo 인덱스로 적재
 *      ③ 각 row.orderNo가 셀릭에 존재하면 → 옵션바코드(12자)로 pb/cc/sz 덮어쓰기
 *      ④ 셀릭에 없으면 → 채널 리더 결과 그대로 유지
 *
 * 실행 순서:
 *    Step2_readRAW (16채널)
 *      ↓
 *    Step2b_buildSellicOverlay  ← 신규 (이 모듈)
 *      ↓
 *    Step3_enrichAndDecompose
 *
 * 요구: Part1 (유틸)
 * ========================================================================
 */

// ============================================================
// 1. 셀릭 주문상태 필터 (제외 11종)
// ============================================================
var SELLIC_EXCL_STATUS = {
  '취소완료': 1, '반품완료': 1, '환불완료': 1, '교환완료': 1,
  '취소요청': 1, '반품요청': 1, '환불요청': 1, '교환요청': 1,
  '취소접수': 1, '반품접수': 1
};

// ============================================================
// 2. 컬럼 헤더 감지 헬퍼
// ============================================================
function _sellicFindCol_(header, patterns) {
  for (var p = 0; p < patterns.length; p++) {
    var re = patterns[p];
    for (var i = 0; i < header.length; i++) {
      var h = String(header[i] || '').trim();
      if (re.test(h)) return i;
    }
  }
  return -1;
}

// ============================================================
// 3. 셀릭 RAW 1파일 → orderNo 인덱스 축적
//    (overlayIndex 에 누적)
// ============================================================
function readSellicIntoOverlay_(rows, overlayIndex, stats) {
  if (!rows || rows.length < 2) return;
  var H = rows[0];

  var cBarcode   = _sellicFindCol_(H, [/옵션바코드/]);
  // 주문번호 우선순위: 단독 '주문번호' (Q열) > '주문번호(쇼핑몰)'
  var cOrderNo   = _sellicFindCol_(H, [/^주문번호$/]);
  if (cOrderNo < 0) cOrderNo = _sellicFindCol_(H, [/주문번호\(쇼핑몰\)/]);
  var cOrderNoShop = _sellicFindCol_(H, [/주문번호\(쇼핑몰\)/]);
  var cStatus    = _sellicFindCol_(H, [/주문상태/]);
  var cPname     = _sellicFindCol_(H, [/^상품명$/]);
  var cOption    = _sellicFindCol_(H, [/^옵션명$/]);
  var cQty       = _sellicFindCol_(H, [/주문수량/]);
  var cOrderDate = _sellicFindCol_(H, [/주문일자/]);

  if (cBarcode < 0 || cOrderNo < 0) {
    stats.missingCol++;
    return;
  }

  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    stats.total++;

    // 상태 필터
    if (cStatus >= 0) {
      var st = cleanStr_(r[cStatus]);
      if (SELLIC_EXCL_STATUS[st]) { stats.skipStatus++; continue; }
    }

    // 옵션바코드 분해
    var barcode = cleanStr_(r[cBarcode]).toUpperCase().replace(/\s+/g, '');
    if (!barcode || barcode.length < 8) { stats.skipBarcode++; continue; }
    var pb = barcode.substring(0, 8);
    var cc = barcode.length >= 10 ? barcode.substring(8, 10) : '';
    var sz = barcode.length >= 12 ? barcode.substring(10, 12) : '0F';
    if (!PB_RE.test(pb)) { stats.skipBarcode++; continue; }

    var orderNo = cleanStr_(r[cOrderNo]);
    if (!orderNo) { stats.skipOrderNo++; continue; }
    var orderNoShop = cOrderNoShop >= 0 ? cleanStr_(r[cOrderNoShop]) : '';

    // 2026 컷오프
    if (cOrderDate >= 0) {
      var od = normalizeDate_(r[cOrderDate]);
      if (od && od < ETL_CUTOFF_DATE) { stats.skipOld++; continue; }
    }

    var entry = {
      pb: pb,
      cc: cc,
      sz: sz,
      barcode: barcode,
      productName: cPname >= 0 ? cleanStr_(r[cPname]) : '',
      optionRaw: cOption >= 0 ? cleanStr_(r[cOption]) : '',
      qty: cQty >= 0 ? toNum_(r[cQty]) : 1
    };

    // orderNo 기준 push (동일 주문 내 여러 품목 대응)
    if (!overlayIndex[orderNo]) overlayIndex[orderNo] = [];
    overlayIndex[orderNo].push(entry);

    // 쇼핑몰 주문번호도 별도 인덱스 (채널 RAW가 다른 번호 포맷 쓰는 경우 대비)
    if (orderNoShop && orderNoShop !== orderNo) {
      if (!overlayIndex[orderNoShop]) overlayIndex[orderNoShop] = [];
      overlayIndex[orderNoShop].push(entry);
    }

    stats.indexed++;
  }
}

// ============================================================
// 4. 셀릭 폴더 전체 → overlayIndex 구축 (Step2b)
// ============================================================
function buildSellicOverlay_() {
  var files = listXlsxInFolder_(SELLIC_FOLDER_ID, false);
  var overlay = {};
  var stats = {files: 0, total: 0, indexed: 0, skipStatus: 0, skipBarcode: 0,
               skipOrderNo: 0, skipOld: 0, missingCol: 0};

  files.forEach(function(f) {
    if (/2025|2024/.test(f.name)) {
      logProgress_('Sellic', 'SKIP(과거): ' + f.name);
      return;
    }
    stats.files++;
    logProgress_('Sellic', '적재: ' + f.name);
    try {
      var rows = readXlsxAsRows_(f.id);
      readSellicIntoOverlay_(rows, overlay, stats);
    } catch (e) {
      logProgress_('Sellic', 'ERROR ' + f.name + ': ' + e.message);
    }
  });

  logProgress_('Sellic',
    '오버레이 구축 완료: files=' + stats.files +
    ', total=' + stats.total + ', indexed=' + stats.indexed +
    ', 상태제외=' + stats.skipStatus + ', 바코드불량=' + stats.skipBarcode +
    ', 주문번호없음=' + stats.skipOrderNo + ', 과거=' + stats.skipOld +
    ', 고유주문수=' + Object.keys(overlay).length);

  return overlay;
}

// ============================================================
// 5. 오버레이 저장/로드 (Step 분할 대응)
// ============================================================
var ETL_SHEET_SELLIC = '_etlSellicOverlay';

function saveSellicOverlay_(overlay) {
  var sh = getOrCreateSheet_(ETL_SHEET_SELLIC);
  var rows = [];
  Object.keys(overlay).forEach(function(ono) {
    rows.push([ono, JSON.stringify(overlay[ono])]);
  });
  if (rows.length) sh.getRange(1, 1, rows.length, 2).setValues(rows);
  logProgress_('Sellic', '오버레이 시트 저장: ' + rows.length + '개 주문');
}

function loadSellicOverlay_() {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(ETL_SHEET_SELLIC);
  if (!sh) return {};
  var data = sh.getDataRange().getValues();
  var overlay = {};
  data.forEach(function(r) {
    if (!r[0]) return;
    try { overlay[r[0]] = JSON.parse(r[1]); } catch (e) {}
  });
  return overlay;
}

// ============================================================
// 6. 토큰 유사도 헬퍼
// ============================================================
function _sellicTokens_(str) {
  // 한글/영숫자 2자 이상 토큰 추출
  if (!str) return [];
  var s = String(str).toUpperCase();
  // 구분자 통일 → 공백 split
  s = s.replace(/[\[\]()_\-\/,·]/g, ' ').replace(/\s+/g, ' ').trim();
  return s.split(' ').filter(function(t) { return t.length >= 2; });
}

function _sellicTokenOverlap_(a, b) {
  // Jaccard 유사도 (0~1)
  var ta = _sellicTokens_(a);
  var tb = _sellicTokens_(b);
  if (!ta.length || !tb.length) return 0;
  var setB = {};
  tb.forEach(function(t) { setB[t] = 1; });
  var inter = 0;
  ta.forEach(function(t) { if (setB[t]) inter++; });
  var uni = ta.length + tb.length - inter;
  return uni > 0 ? inter / uni : 0;
}

// ============================================================
// 7. 단일 row ↔ sellic entry 매칭 스코어링
//
//   가중치 철학: "상품명/옵션/사이즈 일치도 → 가장 일치하는 것 선정"
//     → 상품명(최대 +125) + 옵션(cc +30) + 사이즈(+30) 축이 주력
//     → pb는 채널 파싱 신뢰도 낮으니 보조 신호 취급 (+60/+40)
//     → qty는 부차 신호 (+10)
//
//   신호 목록:
//     S1. pb 정확일치                 +60
//     S2. RAW 상품명에서 추출한 pb 일치  +40
//     S3. 상품명 정규화 완전일치       +100
//     S4. 상품명 Jaccard ≥ 0.7         +80
//     S5. 상품명 Jaccard ≥ 0.5         +60
//     S6. 상품명 Jaccard ≥ 0.3         +30
//     S7. 상품명 substring 포함         +45 (S4~S6와 중복 가능)
//     S8. cc 일치                    +30
//     S9. sz 일치                    +30
//     S10. 옵션원본 substring          +25
//     S11. 옵션원본 Jaccard ≥ 0.5     +15
//     S12. qty 정확일치                +10
// ============================================================
function _sellicScore_(row, e) {
  var sc = 0;

  // 품번 (보조 신호)
  if (row.pb && e.pb && row.pb === e.pb) sc += 60;
  var rawPb = extractPbFromRegex_(row.productName || '');
  if (rawPb && e.pb && rawPb === e.pb) sc += 40;

  // 상품명 (주력 신호, 최대 125점)
  var nA = normalizeNameKey_(row.productName || '');
  var nB = normalizeNameKey_(e.productName || '');
  if (nA && nB) {
    if (nA === nB) {
      sc += 100;
    } else {
      var jacc = _sellicTokenOverlap_(row.productName, e.productName);
      if (jacc >= 0.7) sc += 80;
      else if (jacc >= 0.5) sc += 60;
      else if (jacc >= 0.3) sc += 30;

      if (nA.length >= 4 && nB.length >= 4) {
        if (nA.indexOf(nB) >= 0 || nB.indexOf(nA) >= 0) sc += 45;
      }
    }
  }

  // 사이즈 (옵션바코드의 sz부분)
  if (row.sz && e.sz && row.sz === e.sz) sc += 30;

  // 컬러코드 (옵션바코드의 cc부분)
  if (row.cc && e.cc && row.cc === e.cc) sc += 30;

  // 옵션 원본 문자열 (채널 RAW의 옵션 표기)
  if (row.colorRaw && e.optionRaw) {
    var a = String(row.colorRaw).toUpperCase();
    var b = String(e.optionRaw).toUpperCase();
    if (a && b) {
      if (a === b) sc += 25;
      else if (a.indexOf(b) >= 0 || b.indexOf(a) >= 0) sc += 25;
      else {
        var jOpt = _sellicTokenOverlap_(a, b);
        if (jOpt >= 0.5) sc += 15;
      }
    }
  }

  // 수량
  if (row.qty && e.qty && toNum_(row.qty) === toNum_(e.qty)) sc += 10;

  return sc;
}

// ============================================================
// 8. 주문번호 그룹 단위 최적 배정
//
//   철학:
//     - 셀릭 = Ground Truth. 옵션바코드(pb+cc+sz 12자) 기준으로 정확히 덮어씀.
//     - 같은 주문에 N개 상품이 있으면 N:N 최적 배정으로
//       "각 채널 row ↔ 가장 일치하는 셀릭 entry" 연결.
//
//   알고리즘:
//     ① 1:1 → 즉시 적용
//     ② N,M ≤ MAX_PERMUTE(7) → 전수조사 (모든 할당 조합의 총 스코어 최대화)
//     ③ N,M > MAX_PERMUTE → greedy (고득점 페어부터 그리디 소비)
//     ④ 매칭 안 된 row (entry 부족) → 채널 원본 유지
//
//   덮어쓰기는 pb/cc/sz 개별 필드 단위로 수행 (_overwriteRow_).
// ============================================================
var SELLIC_MAX_PERMUTE = 7;  // N! 전수조사 상한 (7! = 5040)

// 스코어 행렬 생성
function _sellicBuildMatrix_(rowsInOrder, entries) {
  var M = [];
  for (var i = 0; i < rowsInOrder.length; i++) {
    var row = [];
    for (var j = 0; j < entries.length; j++) {
      row.push(_sellicScore_(rowsInOrder[i], entries[j]));
    }
    M.push(row);
  }
  return M;
}

// 순열 생성 헬퍼 (재귀)
function _permutations_(arr) {
  if (arr.length <= 1) return [arr.slice()];
  var result = [];
  for (var i = 0; i < arr.length; i++) {
    var rest = arr.slice(0, i).concat(arr.slice(i + 1));
    var perms = _permutations_(rest);
    for (var k = 0; k < perms.length; k++) {
      result.push([arr[i]].concat(perms[k]));
    }
  }
  return result;
}

// 전수조사 최적 배정 (총 스코어 합 최대)
function _sellicOptimalAssign_(matrix, nRow, nEntry) {
  var size = Math.min(nRow, nEntry);
  var rowIdxs = [];
  for (var i = 0; i < nRow; i++) rowIdxs.push(i);
  var entryIdxs = [];
  for (var j = 0; j < nEntry; j++) entryIdxs.push(j);

  // row 쪽이 entry 쪽보다 많은 경우: entry 기준 순열
  // entry 쪽이 더 많은 경우: row 기준 순열
  var assign = {};
  var bestScore = -1;
  var bestAssign = null;

  if (nRow <= nEntry) {
    // row마다 entry 배정: entry 순열 중 앞 nRow개를 각 row에 대응
    var perms = _permutations_(entryIdxs);
    for (var p = 0; p < perms.length; p++) {
      var total = 0;
      var tmp = {};
      for (var r = 0; r < nRow; r++) {
        tmp[r] = perms[p][r];
        total += matrix[r][perms[p][r]];
      }
      if (total > bestScore) { bestScore = total; bestAssign = tmp; }
    }
  } else {
    // entry마다 row 배정
    var permsR = _permutations_(rowIdxs);
    for (var q = 0; q < permsR.length; q++) {
      var total2 = 0;
      var tmp2 = {};
      for (var e = 0; e < nEntry; e++) {
        tmp2[permsR[q][e]] = e;
        total2 += matrix[permsR[q][e]][e];
      }
      if (total2 > bestScore) { bestScore = total2; bestAssign = tmp2; }
    }
  }
  return {assign: bestAssign, totalScore: bestScore};
}

// Greedy (N 큰 경우 fallback)
function _sellicGreedyAssign_(matrix, nRow, nEntry) {
  var pairs = [];
  for (var i = 0; i < nRow; i++) {
    for (var j = 0; j < nEntry; j++) {
      pairs.push({r: i, e: j, s: matrix[i][j]});
    }
  }
  pairs.sort(function(a, b) { return b.s - a.s; });
  var usedR = {}, usedE = {}, assign = {};
  pairs.forEach(function(p) {
    if (usedR[p.r] || usedE[p.e]) return;
    assign[p.r] = p.e;
    usedR[p.r] = true;
    usedE[p.e] = true;
  });
  return assign;
}

function _applySellicGroup_(rowsInOrder, entries, stats) {
  // ① 1:1
  if (rowsInOrder.length === 1 && entries.length === 1) {
    _overwriteRow_(rowsInOrder[0], entries[0]);
    stats.auto1to1++;
    return;
  }

  var nR = rowsInOrder.length;
  var nE = entries.length;
  var matrix = _sellicBuildMatrix_(rowsInOrder, entries);

  // ② / ③ 최적 배정
  var assigned;
  if (nR <= SELLIC_MAX_PERMUTE && nE <= SELLIC_MAX_PERMUTE) {
    var opt = _sellicOptimalAssign_(matrix, nR, nE);
    assigned = opt.assign || {};
    stats.optimalMatched += Object.keys(assigned).length;
  } else {
    assigned = _sellicGreedyAssign_(matrix, nR, nE);
    stats.greedyMatched += Object.keys(assigned).length;
  }

  // 스코어별 집계 (0점 매칭 = 채널 파싱 오류 교정된 케이스)
  Object.keys(assigned).forEach(function(rIdx) {
    var s = matrix[rIdx][assigned[rIdx]];
    if (s > 0) stats.scorePositive++;
    else stats.scoreZero++;
  });

  // ④ 매칭 못 받은 row (entry 부족) — 채널 원본 유지
  for (var r = 0; r < nR; r++) {
    if (assigned[r] == null) stats.rowUnmatched++;
  }

  // 실제 적용 (옵션바코드 분리 단위로 pb/cc/sz 개별 덮어쓰기)
  Object.keys(assigned).forEach(function(rIdx) {
    _overwriteRow_(rowsInOrder[rIdx], entries[assigned[rIdx]]);
  });
}

function _overwriteRow_(row, e) {
  var changed = false;
  if (e.pb && e.pb !== row.pb) { row._origPb = row.pb; row.pb = e.pb; changed = true; }
  if (e.cc && e.cc !== row.cc) { row._origCc = row.cc; row.cc = e.cc; changed = true; }
  if (e.sz && e.sz !== row.sz) { row._origSz = row.sz; row.sz = e.sz; changed = true; }
  if (changed) {
    row._sellicOverride = true;
    row._sellicBarcode = e.barcode;
  }
}

// ============================================================
// 9. 전체 rows 배열에 일괄 적용
// ============================================================
function applySellicOverlayToAll_(rows, overlay) {
  if (!overlay || !Object.keys(overlay).length) {
    logProgress_('Sellic', '오버레이 비어있음 — skip');
    return {overridden: 0, total: rows.length};
  }

  // 1) orderNo별 row 그룹핑
  var byOrder = {};
  for (var i = 0; i < rows.length; i++) {
    var ono = rows[i].orderNo;
    if (!ono) continue;
    if (!byOrder[ono]) byOrder[ono] = [];
    byOrder[ono].push(rows[i]);
  }

  // 2) 주문 단위로 _applySellicGroup_ 호출
  var stats = {auto1to1: 0, optimalMatched: 0, greedyMatched: 0,
               scorePositive: 0, scoreZero: 0, rowUnmatched: 0};
  Object.keys(overlay).forEach(function(ono) {
    var rowGroup = byOrder[ono];
    if (!rowGroup || !rowGroup.length) return;
    _applySellicGroup_(rowGroup, overlay[ono], stats);
  });

  var total = stats.auto1to1 + stats.optimalMatched + stats.greedyMatched;
  logProgress_('Sellic',
    '오버레이: 1:1=' + stats.auto1to1 +
    ', 최적배정=' + stats.optimalMatched +
    ', greedy=' + stats.greedyMatched +
    ', 점수>0=' + stats.scorePositive +
    ', 점수=0(파싱교정)=' + stats.scoreZero +
    ', 셀릭미기록=' + stats.rowUnmatched +
    ' | 총변경 ' + total + '/' + rows.length +
    ' (' + (total / rows.length * 100).toFixed(2) + '%)');
  return {overridden: total, total: rows.length, stats: stats};
}

// ============================================================
// 8. Step2b 엔트리포인트
//    Step2_readRAW 완료 후 호출 → 오버레이 구축 → 시트 저장
//    Step3 진입 시 applySellicOverlayToAll_() 호출
// ============================================================
function Step2b_buildSellicOverlay() {
  var t0 = Date.now();
  logProgress_('Step2b', 'START');
  try {
    var overlay = buildSellicOverlay_();
    saveSellicOverlay_(overlay);
    setEtlState_('Step3', {
      'ETL_SELLIC_READY': 'true',
      'ETL_SELLIC_ORDER_COUNT': String(Object.keys(overlay).length)
    });
    logProgress_('Step2b', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
  } catch (e) {
    logProgress_('Step2b', 'ERROR: ' + e.message);
    throw e;
  }
}

// ============================================================
// 9. 자가 검증
// ============================================================
function test_Sellic_() {
  // 1) 폴더 파일 리스트
  var files = listXlsxInFolder_(SELLIC_FOLDER_ID, false);
  console.log('셀릭 RAW 파일: ' + files.length + '개');
  files.slice(0, 5).forEach(function(f) { console.log('  - ' + f.name); });

  // 2) 첫 파일 헤더
  if (!files.length) { console.log('파일 없음'); return; }
  var rows = readXlsxAsRows_(files[0].id);
  console.log('첫 파일 행수: ' + rows.length);
  console.log('헤더 일부: ' + (rows[0] || []).slice(0, 15).join(' | '));

  // 3) 오버레이 구축 (소량 100행)
  var mini = [rows[0]].concat(rows.slice(1, Math.min(101, rows.length)));
  var overlay = {};
  var stats = {files: 1, total: 0, indexed: 0, skipStatus: 0, skipBarcode: 0,
               skipOrderNo: 0, skipOld: 0, missingCol: 0};
  readSellicIntoOverlay_(mini, overlay, stats);
  console.log('100행 인덱스: 고유주문=' + Object.keys(overlay).length +
    ', total=' + stats.total + ', indexed=' + stats.indexed +
    ', 상태제외=' + stats.skipStatus + ', 바코드불량=' + stats.skipBarcode);

  // 4) 샘플 엔트리
  var sampleOno = Object.keys(overlay)[0];
  if (sampleOno) {
    console.log('샘플 주문번호 ' + sampleOno + ': ' + JSON.stringify(overlay[sampleOno]));
  }

  // 5) applySellicOverrideToRow_ 시뮬
  var fakeRow = {orderNo: sampleOno, pb: 'OLDPB001', cc: 'XX', sz: 'XX', productName: '테스트'};
  applySellicOverrideToRow_(fakeRow, overlay);
  console.log('오버라이드 결과: ' + JSON.stringify(fakeRow));
}
