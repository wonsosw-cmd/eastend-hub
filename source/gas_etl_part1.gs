/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part1
 * Config + Color/Size 매핑 + 공통 유틸리티 + Store Resolver
 * Source of Truth: /HTML 다시 생성/GAS_ETL_SKILL.md
 * ========================================================================
 */

// ============================================================
// 1. Drive 폴더 / 파일 Config
// ============================================================
var PRODUCT_INFO_FOLDER_ID = '1aetOzMtCIadZMZud-BvYvGMxySNX52X4';
var RAW_FOLDER_ID          = '1BO2ISYD-DCosaJWhzIhBj_RlXLxd78-a';
var SELLIC_FOLDER_ID       = '1CRIvaQF6tmHUGN7fUoyb-Ss948s2CQvU';
var OUTPUT_FOLDER_ID       = '1Kx43LicJEfPOJW0VyUfeI167jXdHOwmc';
var ETL_SPREADSHEET_ID     = '1GvaFMIVhRwjIfbw5u5bx49twdf1BCXK9';
var ETL_CUTOFF_DATE        = '2026-01-01';
var REMAP_CUTOFF           = '2026-01-01';
var ETL_TZ                 = 'Asia/Seoul';

// HUB 배포 (Part8)
var HUB_GITHUB_OWNER       = 'wonsosw-cmd';
var HUB_GITHUB_REPO        = 'eastend-hub';
var HUB_GITHUB_BRANCH      = 'main';
var HUB_GITHUB_TOKEN       = '';  // ★ GAS 프로젝트에서 직접 입력  // eastend-hub PAT
var HUB_URL                = 'https://wonsosw-cmd.github.io/eastend-hub/';

// Step별 시간 예산 (ms)
var STEP_BUDGET_MS = 5 * 60 * 1000;

// 출력 컬럼 인덱스 (0-based, _tempResult 시트 기준)
var COL = {
  NO: 0, FC: 1, ERP: 2, DIV: 3, CHANNEL: 4, SEASON: 5, CATEGORY: 6,
  PB: 7, NAME: 8, COLOR_NAME: 9, CC: 10, SZ: 11,
  COST: 12, TAG: 13, QTY: 14,
  TRANS: 15, TRANS_COUPON: 16, TRANS_SELF: 17, TRANS_PAY: 18,
  ORDER_DATE: 19, ORDER_MONTH: 20, TUE_WEEK: 21, ORDER_NO: 22,
  BLANK: 23, ERROR_FLAG: 24
};

// 정규식
var PB_RE = /[A-Z]{2,3}\d[A-Z]{2}\d{2}/;
var SET_RE = /SET|1\+1|2PACK|\[2PACK\]|세트|셋트/;

// ============================================================
// 2. Color 매핑 (WRONG / COLOR / KR_COLOR)
// ============================================================
var WRONG_COLOR_MAP = {
  'OS': 'BE', 'FS': 'BK', 'NS': 'NV',
  'CREAM': 'CR', 'CHARCOAL': 'CH'
};

var COLOR_MAP = {
  'BLACK':'BK','WHITE':'WH','IVORY':'IV','CREAM':'CR','OATMEAL':'OM',
  'BEIGE':'BE','LIGHTBEIGE':'LB','DARKBEIGE':'DB',
  'BROWN':'BR','LIGHTBROWN':'LR','DARKBROWN':'DR','CAMEL':'CM','MOCHA':'MC',
  'KHAKI':'KH','OLIVE':'OV','DARKKHAKI':'DK',
  'GREEN':'GN','LIGHTGREEN':'LG','DARKGREEN':'DG','MINT':'MT','EMERALD':'EM',
  'NAVY':'NV','BLUE':'BL','LIGHTBLUE':'LU','DARKBLUE':'DU','SKY':'SK','SKYBLUE':'SK','DENIM':'DM',
  'RED':'RD','DARKRED':'DR','PINK':'PK','LIGHTPINK':'LP','HOTPINK':'HP','DEEPPINK':'DP','CORAL':'CO',
  'YELLOW':'YE','LEMON':'LM','MUSTARD':'MS','ORANGE':'OR',
  'PURPLE':'PP','LAVENDER':'LV','VIOLET':'VL',
  'GREY':'GY','GRAY':'GY','LIGHTGREY':'LY','DARKGREY':'DY','CHARCOAL':'CH','SILVER':'SV',
  'WINE':'WN','BURGUNDY':'BG','MAROON':'MR',
  'MELANGE':'MG','MIX':'MX','MULTI':'MU',
  'GOLD':'GO','ROSEGOLD':'RG',
  'CHOCO':'CC','CHOCOLATE':'CC','TAN':'TN','NUDE':'ND','SAND':'SD','STONE':'ST',
  'TURQUOISE':'TQ','TEAL':'TL','AQUA':'AQ',
  'LIGHTGRAY':'LY','DARKGRAY':'DY'
};

var KR_COLOR_MAP = {
  '블랙':'BK','화이트':'WH','아이보리':'IV','크림':'CR','오트밀':'OM',
  '베이지':'BE','라이트베이지':'LB','다크베이지':'DB',
  '브라운':'BR','라이트브라운':'LR','다크브라운':'DR','카멜':'CM','모카':'MC',
  '카키':'KH','올리브':'OV','다크카키':'DK',
  '그린':'GN','라이트그린':'LG','다크그린':'DG','민트':'MT','에메랄드':'EM',
  '네이비':'NV','블루':'BL','라이트블루':'LU','다크블루':'DU','스카이':'SK','스카이블루':'SK','데님':'DM',
  '레드':'RD','핑크':'PK','라이트핑크':'LP','핫핑크':'HP','딥핑크':'DP','코랄':'CO',
  '옐로우':'YE','옐로':'YE','레몬':'LM','머스타드':'MS','오렌지':'OR',
  '퍼플':'PP','라벤더':'LV','바이올렛':'VL',
  '그레이':'GY','라이트그레이':'LY','다크그레이':'DY','차콜':'CH','실버':'SV',
  '와인':'WN','버건디':'BG','마룬':'MR',
  '멜란지':'MG','믹스':'MX','멀티':'MU',
  '골드':'GO','로즈골드':'RG',
  '초코':'CC','초콜릿':'CC','탄':'TN','누드':'ND','샌드':'SD','스톤':'ST',
  '터쿼이즈':'TQ','틸':'TL','아쿠아':'AQ'
};

// 사이즈 매핑
var SZ_MAP = {
  'FREE':'0F','F':'0F','프리':'0F','자유':'0F','ONE SIZE':'0F','ONESIZE':'0F','OS':'0F',
  'S':'0S','SMALL':'0S',
  'M':'0M','MEDIUM':'0M',
  'L':'0L','LARGE':'0L',
  'XS':'XS',
  'XL':'XL',
  'XXL':'XX','2XL':'XX',
  'XXXL':'X3','3XL':'X3'
};

// ============================================================
// 3. 공통 유틸리티
// ============================================================
function extractPbFromRegex_(text) {
  if (!text) return '';
  var m = String(text).toUpperCase().match(PB_RE);
  return m ? m[0] : '';
}

function detectBrandFromPb_(pb) {
  if (!pb) return '';
  var p2 = String(pb).substring(0, 2).toUpperCase();
  if (['CT','CU','CA','CM'].indexOf(p2) >= 0) return '시티브리즈';
  if (['AT','AX'].indexOf(p2) >= 0) return '아티드';
  return '';
}

function detectSet_(name) {
  return SET_RE.test(String(name || '').toUpperCase());
}

// parseCc_: 컬러 원본을 2자 코드로 정규화
function parseCc_(text) {
  if (text === null || text === undefined) return '';
  var s = String(text).trim().toUpperCase();
  if (!s) return '';

  // 1. WRONG_COLOR_MAP (오류 교정) — 소문자 키도 체크
  if (WRONG_COLOR_MAP[s]) return WRONG_COLOR_MAP[s];

  // 2. 이미 2자 알파벳이면 그대로
  if (/^[A-Z]{2}$/.test(s)) {
    return WRONG_COLOR_MAP[s] || s;
  }

  // 3. 영문 COLOR_MAP 직접 매칭
  var compact = s.replace(/\s+/g, '');
  if (COLOR_MAP[compact]) return COLOR_MAP[compact];
  if (COLOR_MAP[s]) return COLOR_MAP[s];

  // 4. 한글 KR_COLOR_MAP 직접 매칭
  var sKr = String(text).trim().replace(/\s+/g, '');
  if (KR_COLOR_MAP[sKr]) return KR_COLOR_MAP[sKr];

  // 5. Fuzzy substring (키 길이 DESC)
  var allKeys = Object.keys(COLOR_MAP).concat(Object.keys(KR_COLOR_MAP));
  allKeys.sort(function(a, b) { return b.length - a.length; });
  for (var i = 0; i < allKeys.length; i++) {
    var k = allKeys[i];
    if (k.length < 2) continue;
    if (s.indexOf(k) >= 0) return COLOR_MAP[k] || KR_COLOR_MAP[k];
    if (sKr.indexOf(k) >= 0) return COLOR_MAP[k] || KR_COLOR_MAP[k];
  }

  // 6. Fallback: 첫 2자
  return s.substring(0, 2);
}

// normSz_: 사이즈 정규화
function normSz_(text) {
  if (text === null || text === undefined) return '';
  var s = String(text).trim().toUpperCase();
  if (!s) return '';

  // 1. 괄호 메모 제거
  s = s.replace(/\([^)]*\)/g, '').trim();
  if (!s) return '';

  // 2. SZ_MAP 직접
  if (SZ_MAP[s]) return SZ_MAP[s];

  // 3. 숫자 사이즈 (24~44)
  if (/^\d{2}$/.test(s)) {
    var n = parseInt(s, 10);
    if (n >= 24 && n <= 44) return s;
  }

  // 4. 이미 정규화 형식
  if (/^(0[FSML]|XL|XS|XX|X3)$/.test(s)) return s;

  // 5. SZ_MAP 내 키 substring
  var keys = Object.keys(SZ_MAP);
  keys.sort(function(a, b) { return b.length - a.length; });
  for (var i = 0; i < keys.length; i++) {
    if (s.indexOf(keys[i]) >= 0) return SZ_MAP[keys[i]];
  }

  // 6. Fallback 첫 2자
  return s.substring(0, 2);
}

// normalizeDate_: YYYY-MM-DD
function normalizeDate_(val) {
  if (val === null || val === undefined || val === '') return '';
  if (val instanceof Date) {
    return Utilities.formatDate(val, ETL_TZ, 'yyyy-MM-dd');
  }
  var s = String(val).trim();
  // 공백 이후(시간) 제거
  s = s.split(' ')[0].split('T')[0];
  // 구분자 통일
  s = s.replace(/[./]/g, '-');
  // YYYY-MM-DD 검증
  var m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (m) {
    return m[1] + '-' + ('0' + m[2]).slice(-2) + '-' + ('0' + m[3]).slice(-2);
  }
  // YYYYMMDD
  var m2 = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m2) return m2[1] + '-' + m2[2] + '-' + m2[3];
  return s;
}

// calcTuesdayWeek_: 화요주차 (화요일 시작)
function calcTuesdayWeek_(dateStr) {
  if (!dateStr) return '';
  var d = new Date(dateStr);
  if (isNaN(d.getTime())) return '';
  // 화요일=2. 기준일부터 직전 화요일까지 되감기
  var day = d.getDay(); // Sun=0..Sat=6
  var diff = (day - 2 + 7) % 7;
  d.setDate(d.getDate() - diff);
  var yy = d.getFullYear();
  // 1/1 포함 주 기준: 연초 화요일 찾기
  var jan1 = new Date(yy, 0, 1);
  var jan1Day = jan1.getDay();
  var firstTueDiff = (2 - jan1Day + 7) % 7;
  var firstTue = new Date(yy, 0, 1 + firstTueDiff);
  var weekNum = Math.floor((d.getTime() - firstTue.getTime()) / (7 * 24 * 3600 * 1000)) + 1;
  return yy + '-W' + ('0' + weekNum).slice(-2);
}

// ============================================================
// 4. 스케줄링 & 상태 관리
// ============================================================
function scheduleNextStep_(fnName, delayMs) {
  ScriptApp.newTrigger(fnName).timeBased().after(delayMs || 1000).create();
}

function shouldYield_(startTime, budgetMs) {
  return (Date.now() - startTime) > (budgetMs || STEP_BUDGET_MS);
}

function setEtlState_(state, extra) {
  var props = PropertiesService.getScriptProperties();
  props.setProperty('ETL_STATE', state);
  if (extra) {
    Object.keys(extra).forEach(function(k) {
      props.setProperty(k, String(extra[k]));
    });
  }
}

function getEtlState_(key) {
  return PropertiesService.getScriptProperties().getProperty(key);
}

function logProgress_(step, msg) {
  var ts = Utilities.formatDate(new Date(), ETL_TZ, 'yyyy-MM-dd HH:mm:ss');
  console.log('[ETL ' + step + '] ' + ts + ' - ' + msg);
}

function clearEtlTriggers_() {
  var triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(function(t) {
    var fn = t.getHandlerFunction();
    if (fn && fn.indexOf('Step') === 0) {
      ScriptApp.deleteTrigger(t);
    }
  });
}

// ============================================================
// 5. Store Resolver
// ============================================================
function resolveStore_(brand, channelName, isOffline, storeLookup, aliasMap, erpToChannel) {
  var ch = String(channelName || '').trim();
  if (aliasMap && aliasMap[ch]) ch = aliasMap[ch];
  if (aliasMap && aliasMap[ch.toLowerCase()]) ch = aliasMap[ch.toLowerCase()];

  var div = brand + ' ' + (isOffline ? '오프라인' : '온라인');

  // 1차: 정확 매칭
  var k = div + '|' + ch;
  if (storeLookup[k]) {
    return {fc: storeLookup[k].fc, erp: storeLookup[k].erp, div: div, ch: ch};
  }

  // 2차: 소문자
  var kl = div + '|' + ch.toLowerCase();
  if (storeLookup[kl]) {
    return {fc: storeLookup[kl].fc, erp: storeLookup[kl].erp, div: div, ch: ch};
  }

  // 3차: ERP 코드 역방향 (오프라인)
  if (isOffline && erpToChannel && erpToChannel[ch]) {
    var pair = erpToChannel[ch];
    var rc = pair[0], rd = pair[1];
    var k3 = rd + '|' + rc;
    if (storeLookup[k3]) {
      return {fc: storeLookup[k3].fc, erp: storeLookup[k3].erp, div: rd, ch: rc};
    }
  }

  // 4차: 기타
  var k4 = div + '|기타';
  if (storeLookup[k4]) {
    return {fc: storeLookup[k4].fc, erp: storeLookup[k4].erp, div: div, ch: '기타'};
  }

  return {fc: '', erp: '', div: div, ch: ch};
}

// ============================================================
// 6. 채널 감지 (파일명 기반)
// ============================================================
function detectChannel_(filename) {
  var f = String(filename || '').toLowerCase();
  if (f.indexOf('자사몰') >= 0) return 'jasamo';
  if (f.indexOf('29cm') >= 0 || f.indexOf('29씨엠') >= 0) return '29cm';
  if (f.indexOf('무신사') >= 0) return 'musinsa';
  if (f.indexOf('w컨셉') >= 0 || f.indexOf('wconcept') >= 0 || f.indexOf('더블유컨셉') >= 0) return 'wconc';
  if (f.indexOf('롯데') >= 0) return 'lotteon';
  if (f.indexOf('하고') >= 0) return 'hago';
  if (f.indexOf('네이버') >= 0) return 'naver';
  if (f.indexOf('지그재그') >= 0) return 'zigzag';
  if (f.indexOf('카카오') >= 0) return 'kakao';
  if (f.indexOf('드립') >= 0) return 'drip';
  if (f.indexOf('eql') >= 0) return 'eql';
  if (f.indexOf('cj') >= 0) return 'cj';
  if (f.indexOf('오픈북') >= 0) return 'openbook';
  if (f.indexOf('gs') >= 0 || f.indexOf('브론테') >= 0) return 'gsshop';
  if (f.indexOf('퀸잇') >= 0) return 'queenit';
  if (f.indexOf('오프라인') >= 0 || f.indexOf('매장') >= 0) return 'offline';
  return null;
}

// ============================================================
// 7. xlsx → 2D 배열 변환 (Drive Advanced Service)
// ============================================================
function readXlsxAsRows_(fileId) {
  var copy = Drive.Files.copy(
    {mimeType: 'application/vnd.google-apps.spreadsheet', title: '_tmp_' + fileId},
    fileId
  );
  try {
    var ss = SpreadsheetApp.openById(copy.id);
    var sheet = ss.getSheets()[0];
    var range = sheet.getDataRange();
    return range.getValues();
  } finally {
    try { Drive.Files.remove(copy.id); } catch (e) { /* noop */ }
  }
}

// 파일명에서 MMDD(또는 날짜) 추출해서 최신파일 선별용 키로 변환
function extractDateKeyFromFilename_(filename) {
  var m = String(filename).match(/(\d{4})[-_]?(\d{2})[-_]?(\d{2})/);
  if (m) return m[1] + m[2] + m[3];
  var m2 = String(filename).match(/_(\d{4})\.?x?/); // _MMDD
  if (m2) return '2026' + m2[1];
  return '';
}

function listXlsxInFolder_(folderId, includeSubfolders) {
  var folder = DriveApp.getFolderById(folderId);
  var out = [];
  var files = folder.getFilesByType(MimeType.MICROSOFT_EXCEL);
  while (files.hasNext()) {
    var f = files.next();
    out.push({id: f.getId(), name: f.getName(), date: f.getLastUpdated()});
  }
  if (includeSubfolders) {
    var subs = folder.getFolders();
    while (subs.hasNext()) {
      var sub = subs.next();
      var inner = listXlsxInFolder_(sub.getId(), true);
      inner.forEach(function(r) { r.subfolder = sub.getName(); out.push(r); });
    }
  }
  return out;
}

// ============================================================
// 8. 공통 헬퍼 (숫자 파싱 등)
// ============================================================
function toNum_(val) {
  if (val === null || val === undefined || val === '') return 0;
  if (typeof val === 'number') return val;
  var s = String(val).replace(/[,\s원₩]/g, '');
  var n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function cleanStr_(val) {
  if (val === null || val === undefined) return '';
  return String(val).replace(/\u3000/g, ' ').trim();
}

function normalizeNameKey_(name) {
  if (!name) return '';
  return String(name)
    .replace(/\([A-Z]{2,3}\d[A-Z]{0,2}\d{0,2}\)\s*/g, '')
    .replace(/\[[^\]]+\]\s*/g, '')
    .replace(/\[리퍼브\]/g, '')
    .replace(/\s+/g, '')
    .toLowerCase();
}

// ============================================================
// 9. 자가 검증 (배포 전 sanity test)
// ============================================================
function test_Part1_() {
  var out = [];
  // Color
  out.push('parseCc_(BLACK)=' + parseCc_('BLACK') + ' (expect BK)');
  out.push('parseCc_(블랙)=' + parseCc_('블랙') + ' (expect BK)');
  out.push('parseCc_(OS)=' + parseCc_('OS') + ' (expect BE)');
  out.push('parseCc_(BK)=' + parseCc_('BK') + ' (expect BK)');
  // Size
  out.push('normSz_(FREE)=' + normSz_('FREE') + ' (expect 0F)');
  out.push('normSz_(S)=' + normSz_('S') + ' (expect 0S)');
  out.push('normSz_(2XL)=' + normSz_('2XL') + ' (expect XX)');
  out.push('normSz_(S(01.13 예약))=' + normSz_('S(01.13 예약)') + ' (expect 0S)');
  // PB
  out.push('extractPbFromRegex_(CTH3KT93 캐시미어)=' + extractPbFromRegex_('CTH3KT93 캐시미어') + ' (expect CTH3KT93)');
  out.push('detectBrandFromPb_(CTH3KT93)=' + detectBrandFromPb_('CTH3KT93') + ' (expect 시티브리즈)');
  out.push('detectBrandFromPb_(ATA4O02)=' + detectBrandFromPb_('ATA4O02') + ' (expect 아티드)');
  // Set
  out.push('detectSet_([2PACK] 양말)=' + detectSet_('[2PACK] 양말') + ' (expect true)');
  // Date
  out.push('normalizeDate_(2026.03.15 13:20)=' + normalizeDate_('2026.03.15 13:20') + ' (expect 2026-03-15)');
  out.push('normalizeDate_(20260315)=' + normalizeDate_('20260315') + ' (expect 2026-03-15)');
  console.log(out.join('\n'));
  return out.join('\n');
}
