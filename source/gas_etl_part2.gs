/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part2
 * 마스터 데이터 로더 (종합재고 / 매장구분 코드 / 셀메이트)
 * 요구: Part1 (gas_etl_part1.gs) 이미 주입됨
 * ========================================================================
 */

// ============================================================
// 1. 최신 파일 선별 (파일명 패턴 + MMDD + lastUpdated)
// ============================================================
function findLatestByPattern_(folderId, pattern) {
  var folder = DriveApp.getFolderById(folderId);
  var files = folder.getFiles();
  var matched = [];
  while (files.hasNext()) {
    var f = files.next();
    var name = f.getName();
    if (pattern.test(name)) {
      matched.push({id: f.getId(), name: name, updated: f.getLastUpdated()});
    }
  }
  if (!matched.length) return null;
  // 1) 파일명의 MMDD 추출 (없으면 0), 2) lastUpdated, 복합 정렬
  matched.sort(function(a, b) {
    var ma = (a.name.match(/_(\d{4})(?!\d)/) || [,'0'])[1];
    var mb = (b.name.match(/_(\d{4})(?!\d)/) || [,'0'])[1];
    if (ma !== mb) return parseInt(mb, 10) - parseInt(ma, 10);
    return b.updated.getTime() - a.updated.getTime();
  });
  return matched[0];
}

// ============================================================
// 2. 종합재고 로더 (loadInventory_)
// ============================================================
// 파일 구조: L1=헤더 카테고리, L2=컬럼명, L3+=데이터
// 컬럼: A=상품바코드, B=품명, C=품번, D=색상(cc), E=색상명,
//       F=사이즈(sz), G=사전원가, H=사전원가(V+), I=사후원가,
//       J=최종원가, K=최초판매가, L=택가, M=현판매가
function loadInventory_(folderId) {
  var latest = findLatestByPattern_(folderId, /2026_클로드용_종합재고_\d{4}\.xlsx$/);
  if (!latest) throw new Error('종합재고 파일을 찾을 수 없습니다 (패턴: 2026_클로드용_종합재고_MMDD.xlsx)');
  logProgress_('Part2', '종합재고 로딩: ' + latest.name);

  var rows = readXlsxAsRows_(latest.id);
  if (rows.length < 3) throw new Error('종합재고 행수 부족: ' + rows.length);

  var inv = {};         // "pb|cc|sz" -> {name, colorName, cost, tag}
  var invPb = {};       // pb -> [[cc, sz], ...]
  var invColors = {};   // pb -> {cc: colorName}
  var nameToPb = {      // 브랜드별 name->pb (재매칭용)
    'CT': {}, 'CU': {}, 'CA': {}, 'CM': {},
    'AT': {}, 'AX': {}
  };
  var reissueSiblings = {};
  var reissuePrefer = {};

  // 데이터는 L3(인덱스 2)부터
  for (var i = 2; i < rows.length; i++) {
    var r = rows[i];
    var barcode = cleanStr_(r[0]);
    var name = cleanStr_(r[1]);
    var pb = cleanStr_(r[2]).toUpperCase();
    var ccRaw = cleanStr_(r[3]);
    var colorName = cleanStr_(r[4]);
    var szRaw = cleanStr_(r[5]);
    var cost = toNum_(r[7]);  // H=사전원가(V+)
    var tag = toNum_(r[11]);  // L=택가

    if (!pb) continue;
    // pb 형식 검증
    if (!PB_RE.test(pb)) continue;

    var cc = parseCc_(ccRaw);
    var sz = normSz_(szRaw);
    var key = pb + '|' + cc + '|' + sz;

    inv[key] = {
      name: name,
      colorName: colorName,
      cost: cost,
      tag: tag,
      barcode: barcode
    };

    // invPb 축적 (중복 방지)
    if (!invPb[pb]) invPb[pb] = [];
    var exists = invPb[pb].some(function(p) { return p[0] === cc && p[1] === sz; });
    if (!exists) invPb[pb].push([cc, sz]);

    // invColors
    if (!invColors[pb]) invColors[pb] = {};
    if (cc && !invColors[pb][cc]) invColors[pb][cc] = colorName;

    // nameToPb (브랜드별)
    var p2 = pb.substring(0, 2);
    if (nameToPb[p2]) {
      var nKey = normalizeNameKey_(name);
      if (nKey && !nameToPb[p2][nKey]) nameToPb[p2][nKey] = pb;
    }
  }

  logProgress_('Part2', '종합재고 완료: inv=' + Object.keys(inv).length +
    ', pb=' + Object.keys(invPb).length);

  return {
    inv: inv,
    invPb: invPb,
    invColors: invColors,
    nameToPb: nameToPb,
    reissueSiblings: reissueSiblings,
    reissuePrefer: reissuePrefer,
    _sourceFile: latest.name
  };
}

// ============================================================
// 3. 매장구분 코드 로더 (loadStoreCode_)
// ============================================================
// 파일 구조: B=재무코드, C=ERP코드, D=구분, E=채널명, F=동일명칭(alias)
// 키: "구분|채널명" -> {fc, erp}
function loadStoreCode_(folderId) {
  var latest = findLatestByPattern_(folderId, /2026_매장구분\s*코드_\d{4}.*\.xlsx$/);
  if (!latest) throw new Error('매장구분 코드 파일을 찾을 수 없습니다');
  logProgress_('Part2', '매장구분 로딩: ' + latest.name);

  var rows = readXlsxAsRows_(latest.id);
  var storeLookup = {};
  var aliasMap = {};
  var erpToChannel = {};

  // 헤더 1~2행 스킵, 데이터 L3부터
  for (var i = 2; i < rows.length; i++) {
    var r = rows[i];
    var fc = cleanStr_(r[1]);     // B
    var erp = cleanStr_(r[2]);    // C
    var div = cleanStr_(r[3]);    // D
    var ch = cleanStr_(r[4]);     // E
    var aliases = cleanStr_(r[5]); // F

    if (!div || !ch) continue;

    var k = div + '|' + ch;
    storeLookup[k] = {fc: fc, erp: erp};

    // alias (쉼표/세로바/세미콜론 구분)
    if (aliases) {
      aliases.split(/[,|;]/).forEach(function(a) {
        var aTrim = a.trim();
        if (aTrim) {
          aliasMap[aTrim] = ch;
          aliasMap[aTrim.toLowerCase()] = ch;
        }
      });
    }

    // ERP 역인덱스 (오프라인 fallback용)
    if (erp) {
      erpToChannel[erp] = [ch, div];
    }
  }

  logProgress_('Part2', '매장구분 완료: store=' + Object.keys(storeLookup).length +
    ', alias=' + Object.keys(aliasMap).length);

  return {
    storeLookup: storeLookup,
    aliasMap: aliasMap,
    erpToChannel: erpToChannel,
    _sourceFile: latest.name
  };
}

// ============================================================
// 4. 셀메이트 로더 (loadSellmate_)
// ============================================================
// 종합재고 미등록 품번 fallback용
// 파일 구조: 상품코드=품번, 상품명, 원가, 택가 (정확한 컬럼은 실파일 확인 필요)
function loadSellmate_(folderId) {
  var latest = findLatestByPattern_(folderId, /2026_셀메이트_상품정보_\d{4}\.xlsx$/);
  if (!latest) {
    logProgress_('Part2', '셀메이트 파일 없음 (선택 사항)');
    return {sellmate: {}, _sourceFile: null};
  }
  logProgress_('Part2', '셀메이트 로딩: ' + latest.name);

  var rows = readXlsxAsRows_(latest.id);
  if (!rows.length) return {sellmate: {}, _sourceFile: latest.name};

  // 헤더 행에서 컬럼 위치 감지
  var header = rows[0].map(function(h) { return String(h || '').trim(); });
  var pbCol = -1, nameCol = -1, costCol = -1, tagCol = -1;
  for (var j = 0; j < header.length; j++) {
    var h = header[j];
    if (pbCol < 0 && /품번|상품코드|자체상품/i.test(h)) pbCol = j;
    if (nameCol < 0 && /품명|상품명/.test(h)) nameCol = j;
    if (costCol < 0 && /원가/.test(h) && !/사후/.test(h)) costCol = j;
    if (tagCol < 0 && /택가|정상가|판매가/.test(h)) tagCol = j;
  }
  if (pbCol < 0 || nameCol < 0) {
    logProgress_('Part2', '셀메이트 헤더 인식 실패 (pbCol=' + pbCol + ', nameCol=' + nameCol + ')');
    return {sellmate: {}, _sourceFile: latest.name};
  }

  var sellmate = {};
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var pb = cleanStr_(r[pbCol]).toUpperCase();
    if (!pb || !PB_RE.test(pb)) continue;
    sellmate[pb] = {
      name: cleanStr_(r[nameCol]),
      cost: costCol >= 0 ? toNum_(r[costCol]) : 0,
      tag: tagCol >= 0 ? toNum_(r[tagCol]) : 0
    };
  }

  logProgress_('Part2', '셀메이트 완료: ' + Object.keys(sellmate).length + '건');
  return {sellmate: sellmate, _sourceFile: latest.name};
}

// ============================================================
// 5. 하드코드 보강 상수 (Python SELLMATE_ENRICH_HARDCODE 포팅)
//    Red 잔존 행 구제용 — Part4/Step4 NAME_OVERRIDE 직후 적용
// ============================================================
var SELLMATE_ENRICH_HARDCODE = {
  'ATA4O02': {name: '(ATA4) 뷔스티에 긴팔 롱 원피스', cost: 57045, tag: 219000},
  'ATA3O02': {name: '(ATA3) 스퀘어넥 플레어 롱 원피스', cost: 61966, tag: 239000},
  'CTA0AC05': {name: '(CTA0) [3PACK] 장목 양말', cost: 3500, tag: 15000}
  // 추가는 Part4/Step4 구현 시 Python 참조하여 확장
};

var NAME_OVERRIDE = {
  // 키: RAW 상품명 (정규화 전 원문), 값: 품번
  '(ATA4) 뷔스티에 긴팔 롱 원피스': 'ATA4O02',
  '[22FW] 스퀘어넥 플레어 롱 원피스_BLACK': 'ATA3O02',
  '[3PACK] CITYBREEZE 장목 양말_MIX': 'CTA0AC05'
  // 추가는 Part4/Step4에서 확장
};

// ============================================================
// 6. 임시 시트 저장 (PropertiesService는 용량 부족 → 시트 저장)
// ============================================================
var ETL_SHEET_INV = '_etlInv';
var ETL_SHEET_INVPB = '_etlInvPb';
var ETL_SHEET_INVCOLORS = '_etlInvColors';
var ETL_SHEET_NAMETOPB = '_etlNameToPb';
var ETL_SHEET_STORE = '_etlStore';
var ETL_SHEET_ALIAS = '_etlAlias';
var ETL_SHEET_ERPCH = '_etlErpCh';
var ETL_SHEET_SELLMATE = '_etlSellmate';

function getOrCreateSheet_(name) {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  var sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  else sh.clear();
  return sh;
}

function writeKV_(sheet, obj, valSerializer) {
  var rows = [];
  Object.keys(obj).forEach(function(k) {
    rows.push([k, valSerializer ? valSerializer(obj[k]) : obj[k]]);
  });
  if (!rows.length) return;
  sheet.getRange(1, 1, rows.length, 2).setValues(rows);
}

function saveMasters_(masters, storeMasters, sellmateObj) {
  // inv: pb|cc|sz, name, colorName, cost, tag
  var invSh = getOrCreateSheet_(ETL_SHEET_INV);
  var invRows = [];
  Object.keys(masters.inv).forEach(function(k) {
    var d = masters.inv[k];
    invRows.push([k, d.name, d.colorName, d.cost, d.tag]);
  });
  if (invRows.length) invSh.getRange(1, 1, invRows.length, 5).setValues(invRows);

  // invPb: pb -> JSON([[cc,sz],...])
  writeKV_(getOrCreateSheet_(ETL_SHEET_INVPB), masters.invPb, JSON.stringify);
  // invColors: pb -> JSON({cc:colorName})
  writeKV_(getOrCreateSheet_(ETL_SHEET_INVCOLORS), masters.invColors, JSON.stringify);
  // nameToPb: 단일 객체 → 브랜드 prefix별 "prefix|nKey" 키
  var nameSh = getOrCreateSheet_(ETL_SHEET_NAMETOPB);
  var nameRows = [];
  Object.keys(masters.nameToPb).forEach(function(p2) {
    var sub = masters.nameToPb[p2];
    Object.keys(sub).forEach(function(nk) {
      nameRows.push([p2 + '|' + nk, sub[nk]]);
    });
  });
  if (nameRows.length) nameSh.getRange(1, 1, nameRows.length, 2).setValues(nameRows);

  // Store
  writeKV_(getOrCreateSheet_(ETL_SHEET_STORE), storeMasters.storeLookup, JSON.stringify);
  writeKV_(getOrCreateSheet_(ETL_SHEET_ALIAS), storeMasters.aliasMap);
  writeKV_(getOrCreateSheet_(ETL_SHEET_ERPCH), storeMasters.erpToChannel, JSON.stringify);

  // Sellmate
  writeKV_(getOrCreateSheet_(ETL_SHEET_SELLMATE), sellmateObj.sellmate, JSON.stringify);

  logProgress_('Part2', '마스터 저장 완료 (시트 8개)');
}

function loadMastersFromSheets_() {
  var ss = SpreadsheetApp.openById(ETL_SPREADSHEET_ID);
  function readSh(name) {
    var sh = ss.getSheetByName(name);
    if (!sh) return [];
    var r = sh.getDataRange().getValues();
    return r.filter(function(row) { return row[0] !== ''; });
  }
  var inv = {}, invPb = {}, invColors = {}, nameToPb = {'CT':{},'CU':{},'CA':{},'CM':{},'AT':{},'AX':{}};
  var storeLookup = {}, aliasMap = {}, erpToChannel = {}, sellmate = {};

  readSh(ETL_SHEET_INV).forEach(function(r) {
    inv[r[0]] = {name: r[1], colorName: r[2], cost: toNum_(r[3]), tag: toNum_(r[4])};
  });
  readSh(ETL_SHEET_INVPB).forEach(function(r) {
    try { invPb[r[0]] = JSON.parse(r[1]); } catch(e) {}
  });
  readSh(ETL_SHEET_INVCOLORS).forEach(function(r) {
    try { invColors[r[0]] = JSON.parse(r[1]); } catch(e) {}
  });
  readSh(ETL_SHEET_NAMETOPB).forEach(function(r) {
    var parts = String(r[0]).split('|');
    var p2 = parts[0], nk = parts.slice(1).join('|');
    if (nameToPb[p2]) nameToPb[p2][nk] = r[1];
  });
  readSh(ETL_SHEET_STORE).forEach(function(r) {
    try { storeLookup[r[0]] = JSON.parse(r[1]); } catch(e) {}
  });
  readSh(ETL_SHEET_ALIAS).forEach(function(r) { aliasMap[r[0]] = r[1]; });
  readSh(ETL_SHEET_ERPCH).forEach(function(r) {
    try { erpToChannel[r[0]] = JSON.parse(r[1]); } catch(e) {}
  });
  readSh(ETL_SHEET_SELLMATE).forEach(function(r) {
    try { sellmate[r[0]] = JSON.parse(r[1]); } catch(e) {}
  });

  return {
    inv: inv, invPb: invPb, invColors: invColors, nameToPb: nameToPb,
    storeLookup: storeLookup, aliasMap: aliasMap, erpToChannel: erpToChannel,
    sellmate: sellmate
  };
}

// ============================================================
// 7. Step1 엔트리포인트
// ============================================================
function Step1_loadMasters() {
  var t0 = Date.now();
  logProgress_('Step1', 'START');
  try {
    var masters = loadInventory_(PRODUCT_INFO_FOLDER_ID);
    var storeMasters = loadStoreCode_(PRODUCT_INFO_FOLDER_ID);
    var sellmateObj = loadSellmate_(PRODUCT_INFO_FOLDER_ID);

    saveMasters_(masters, storeMasters, sellmateObj);

    setEtlState_('Step2', {
      'ETL_BATCH_IDX': '0',
      'ETL_STARTED_AT': String(t0),
      'ETL_MASTERS_READY': 'true',
      'ETL_INV_SOURCE': masters._sourceFile || '',
      'ETL_STORE_SOURCE': storeMasters._sourceFile || ''
    });

    logProgress_('Step1', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
    // Step2로 체이닝 (Part6에서 Step2_readRAW 정의 후 활성화)
    // scheduleNextStep_('Step2_readRAW', 1000);
  } catch (e) {
    logProgress_('Step1', 'ERROR: ' + e.message);
    throw e;
  }
}

// ============================================================
// 8. 자가 검증
// ============================================================
function test_Part2_() {
  logProgress_('test_Part2_', 'START');

  // 1) 종합재고 파일 탐색
  var inv = findLatestByPattern_(PRODUCT_INFO_FOLDER_ID, /2026_클로드용_종합재고_\d{4}\.xlsx$/);
  console.log('종합재고 최신: ' + (inv ? inv.name : 'NOT FOUND'));

  // 2) 매장구분 파일 탐색
  var st = findLatestByPattern_(PRODUCT_INFO_FOLDER_ID, /2026_매장구분\s*코드_\d{4}.*\.xlsx$/);
  console.log('매장구분 최신: ' + (st ? st.name : 'NOT FOUND'));

  // 3) 셀메이트 파일 탐색
  var sm = findLatestByPattern_(PRODUCT_INFO_FOLDER_ID, /2026_셀메이트_상품정보_\d{4}\.xlsx$/);
  console.log('셀메이트 최신: ' + (sm ? sm.name : 'NOT FOUND'));

  logProgress_('test_Part2_', 'END');
}
