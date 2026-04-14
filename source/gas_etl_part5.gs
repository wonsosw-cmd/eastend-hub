/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part5
 * SET / 1+1 / 2PACK 분해 로직
 *
 * 3단계 우선순위:
 *   ① SLACKS_3_MAP       — 링클 프리 슬랙스 3종 variant
 *   ② SPECIAL_SET_MAP    — 4개 고정 규칙 (울에센셜/실켓/캐시미어라인/제인레더버킷백)
 *   ③ 범용 SET/1+1/2PACK — detectSet_ 트리거 + 다중옵션 파싱 → N행 분리
 *
 * 입력: 표준 row 객체
 * 출력: row 배열 (분해 시 N개, 미해당 시 [row] 그대로)
 * ========================================================================
 */

// ============================================================
// 1. SLACKS_3_MAP
// ============================================================
var SLACKS_3_MAP = {
  '스트레이트': {pb: 'CTG3PT01', name: '(CTG3) 링클 프리 베이직 스트레이트 슬랙스'},
  '원턱':       {pb: 'CTG3PT02', name: '(CTG3) 링클 프리 베이직 원 턱 와이드 히든 밴딩 슬랙스'},
  '투턱':       {pb: 'CTG3PT03', name: '(CTG3) 링클 프리 베이직 투 턱 와이드 히든 밴딩 슬랙스'}
};
var SLACKS_PB_SET = {'CTG3PT01':1,'CTG3PT02':1,'CTG3PT03':1};

function _isSlacks3jong_(row) {
  var n = String(row.productName || '');
  if (n.indexOf('슬랙스') < 0) return false;
  if (n.indexOf('3종') >= 0) return true;
  if (detectSet_(n)) return true;
  if (SLACKS_PB_SET[row.pb]) return true;
  return false;
}

function _detectSlacksVariant_(text) {
  var s = String(text || '');
  if (s.indexOf('스트레이트') >= 0) return '스트레이트';
  if (s.indexOf('원턱') >= 0 || s.indexOf('원 턱') >= 0) return '원턱';
  if (s.indexOf('투턱') >= 0 || s.indexOf('투 턱') >= 0) return '투턱';
  return '';
}

// ============================================================
// 2. SPECIAL_SET_MAP
// ============================================================
var SPECIAL_SET_MAP = [
  {match: '울 에센셜', mode: 'decompose',
    variants: {'라운드': 'CTH3KT94', '브이넥': 'CTH3KT95'},
    defaultPb: 'CTH3KT94'},
  {match: '실켓', mode: 'decompose',
    variants: {'레귤러': 'ATD1TS86', '크롭': 'ATD2TS01'},
    defaultPb: 'ATD1TS86'},
  {match: '캐시미어 라인', mode: 'decompose',
    variants: {'가디건': 'CTH3KT90', '브이넥': 'CTH3KT93',
               '라운드': 'CTH3KT92', '오픈카라': 'CTH3KT91'},
    defaultPb: 'CTH3KT93'},
  {match: '제인 레더 버킷백', mode: 'remap',
    variants: {'스웨이드': 'ATD3BG83', '기본': 'ATC4BG02'},
    defaultPb: 'ATC4BG02'}
];

// ============================================================
// 3. 다중 옵션 파싱
//   입력 예: "[OPTION 1]BLACK/F[OPTION 2]WHITE/F"
//           "OPITON 1=WHITE:F, OPITON 2=BLUE:F"
//           "COLOR 1=NV, COLOR 2=BK"
// ============================================================
function parseMultiOption_(text) {
  if (!text) return null;
  var s = String(text);

  // Pattern 1: [OPTION N]...
  var m1 = s.match(/\[OPTION\s*\d+\][^\[]+/gi);
  if (m1 && m1.length > 1) {
    return m1.map(function(x) { return x.replace(/\[OPTION\s*\d+\]/i, '').trim(); });
  }

  // Pattern 2: OPITON/OPTION/COLOR N=...
  var m2 = s.match(/(?:OPITON|OPTION|COLOR)\s*\d+\s*=\s*[^,;]+/gi);
  if (m2 && m2.length > 1) {
    return m2.map(function(x) { return x.replace(/(?:OPITON|OPTION|COLOR)\s*\d+\s*=\s*/i, '').trim(); });
  }

  // Pattern 3: "A / B / C" (슬래시 여러 개)
  if ((s.match(/\s\/\s/g) || []).length >= 1 && s.split(/\s*\/\s*/).length >= 2) {
    // 단, option 필드 자체가 "BLACK/F"인 경우를 제외하려면 쉼표 or 세미콜론 기반만 받기
    // → pass, 아래에서 처리
  }

  // Pattern 4: 쉼표/세미콜론 구분
  var parts = s.split(/[,;]/);
  if (parts.length >= 2) {
    var trimmed = parts.map(function(x) { return x.trim(); }).filter(function(x) { return x.length > 0; });
    if (trimmed.length >= 2) return trimmed;
  }

  return null;
}

// ============================================================
// 4. 단일 옵션 문자열에서 cc/sz 분리
//    "BLACK/F" / "BLACK:F" / "BLACK F" / "BLACK"
// ============================================================
function parseColorSize_(optStr) {
  if (!optStr) return {cc: '', sz: ''};
  var s = String(optStr).trim();
  // 슬래시/콜론/등호 구분자
  var m = s.match(/^([^\/:=]+)\s*[\/:=]\s*([^\/:=]+)$/);
  if (m) {
    return {cc: parseCc_(m[1].trim()), sz: normSz_(m[2].trim())};
  }
  // 공백 구분 (색상 사이즈)
  var m2 = s.match(/^(\S+)\s+(\S+)$/);
  if (m2) {
    var csz = normSz_(m2[2]);
    if (/^(0[FSML]|X[SLX3]|\d{2})$/.test(csz)) {
      return {cc: parseCc_(m2[1]), sz: csz};
    }
  }
  // 단일 토큰 — 사이즈 형태면 sz, 아니면 cc
  var norm = normSz_(s);
  if (/^(0[FSML]|X[SLX3]|\d{2})$/.test(norm)) return {cc: '', sz: norm};
  return {cc: parseCc_(s), sz: ''};
}

// ============================================================
// 5. 분해된 자식 row 생성 (거래액/수량 N등분)
// ============================================================
function _cloneRow_(row) {
  var c = {};
  Object.keys(row).forEach(function(k) { c[k] = row[k]; });
  return c;
}

function _splitTransByN_(row, n) {
  ['trans','transCoupon','transSelf','transPay'].forEach(function(k) {
    row[k] = Math.round((toNum_(row[k]) || 0) / n);
  });
  row.qty = Math.max(1, Math.floor((toNum_(row.qty) || 1) / n));
}

// ============================================================
// 6. 슬랙스 3종 분해
// ============================================================
function _decomposeSlacks_(row) {
  var optSource = row.optionRaw || row.colorRaw || '';
  var opts = parseMultiOption_(optSource);

  // 다중 옵션 → 각 옵션의 variant별 분리
  if (opts && opts.length > 1) {
    var children = [];
    opts.forEach(function(opt) {
      var variant = _detectSlacksVariant_(opt);
      var info = SLACKS_3_MAP[variant];
      if (!info) info = SLACKS_3_MAP['스트레이트'];  // fallback
      var cs = parseColorSize_(opt);
      var c = _cloneRow_(row);
      c.pb = info.pb;
      c.productName = info.name;
      c.cc = cs.cc || c.cc;
      c.sz = cs.sz || c.sz;
      c._decomposed = 'slacks3';
      children.push(c);
    });
    children.forEach(function(c) { _splitTransByN_(c, opts.length); });
    return children;
  }

  // 단일 variant remap (pb가 이미 3종 중 하나인 경우)
  var variant = _detectSlacksVariant_(row.colorRaw || row.optionRaw || row.productName);
  if (variant && SLACKS_3_MAP[variant]) {
    row.pb = SLACKS_3_MAP[variant].pb;
    row.productName = SLACKS_3_MAP[variant].name;
    var cs2 = parseColorSize_(row.colorRaw || row.optionRaw || '');
    if (cs2.cc) row.cc = cs2.cc;
    if (cs2.sz) row.sz = cs2.sz;
    row._decomposed = 'slacksRemap';
    return [row];
  }

  return [row];
}

// ============================================================
// 7. SPECIAL_SET_MAP 분해 / remap
// ============================================================
function _decomposeSpecial_(row, rule) {
  var optSource = row.optionRaw || row.colorRaw || '';
  var opts = parseMultiOption_(optSource);

  if (rule.mode === 'remap') {
    // 분해 없이 variant에 따라 pb만 교체
    var variantKey = _detectVariantKey_(rule.variants, row.colorRaw || row.optionRaw || row.productName);
    row.pb = rule.variants[variantKey] || rule.defaultPb;
    row._decomposed = 'specialRemap';
    return [row];
  }

  // mode=decompose
  if (opts && opts.length > 1) {
    var children = [];
    opts.forEach(function(opt) {
      var variantKey = _detectVariantKey_(rule.variants, opt);
      var pb = rule.variants[variantKey] || rule.defaultPb;
      var cs = parseColorSize_(opt);
      var c = _cloneRow_(row);
      c.pb = pb;
      c.cc = cs.cc || c.cc;
      c.sz = cs.sz || c.sz;
      c._decomposed = 'specialDecompose';
      children.push(c);
    });
    children.forEach(function(c) { _splitTransByN_(c, opts.length); });
    return children;
  }

  // 옵션 파싱 실패 시 fallback: color_raw에서 variant 추출
  var variantKey2 = _detectVariantKey_(rule.variants, row.colorRaw || row.optionRaw || row.productName);
  if (variantKey2) {
    row.pb = rule.variants[variantKey2];
    row._decomposed = 'specialRemapFallback';
  } else {
    row.pb = rule.defaultPb;
    row._decomposed = 'specialDefault';
  }
  return [row];
}

function _detectVariantKey_(variants, text) {
  var s = String(text || '');
  var keys = Object.keys(variants);
  // 길이 내림차순 (긴 키부터)
  keys.sort(function(a, b) { return b.length - a.length; });
  for (var i = 0; i < keys.length; i++) {
    if (s.indexOf(keys[i]) >= 0) return keys[i];
  }
  return '';
}

// ============================================================
// 8. 범용 SET/1+1/2PACK 분해
// ============================================================
function _decomposeGeneric_(row) {
  var optSource = row.optionRaw || row.colorRaw || '';
  var opts = parseMultiOption_(optSource);
  if (!opts || opts.length < 2) return [row];

  var children = [];
  opts.forEach(function(opt) {
    var cs = parseColorSize_(opt);
    var c = _cloneRow_(row);
    if (cs.cc) c.cc = cs.cc;
    if (cs.sz) c.sz = cs.sz;
    c._decomposed = 'generic';
    children.push(c);
  });
  children.forEach(function(c) { _splitTransByN_(c, opts.length); });
  return children;
}

// ============================================================
// 9. 메인 디스패처: decomposeRow_
// ============================================================
function decomposeRow_(row) {
  // ① 슬랙스 3종
  if (_isSlacks3jong_(row)) return _decomposeSlacks_(row);

  // ② SPECIAL_SET_MAP
  for (var i = 0; i < SPECIAL_SET_MAP.length; i++) {
    var rule = SPECIAL_SET_MAP[i];
    if (String(row.productName || '').indexOf(rule.match) >= 0) {
      return _decomposeSpecial_(row, rule);
    }
  }

  // ③ 범용 SET
  if (detectSet_(row.productName)) return _decomposeGeneric_(row);

  return [row];
}

// ============================================================
// 10. 주문번호별 SET 거래액 균등배분 (Step4에서 호출)
//   RAW에서 이미 N행으로 들어온 SET 주문의 거래액 4종을
//   합산 → N등분 (round)
// ============================================================
function rebalanceSetTransByOrder_(rows) {
  var groups = {};
  for (var i = 0; i < rows.length; i++) {
    var r = rows[i];
    if (!r.orderNo || !detectSet_(r.productName || '')) continue;
    var key = r.orderNo;
    if (!groups[key]) groups[key] = [];
    groups[key].push(r);
  }
  var cnt = 0;
  Object.keys(groups).forEach(function(ono) {
    var g = groups[ono];
    if (g.length < 2) return;
    var n = g.length;
    ['trans','transCoupon','transSelf','transPay'].forEach(function(k) {
      var total = g.reduce(function(s, r) { return s + toNum_(r[k]); }, 0);
      var per = Math.round(total / n);
      g.forEach(function(r) { r[k] = per; });
    });
    cnt += n;
  });
  logProgress_('Part5', 'SET 거래액 균등배분: ' + cnt + '행 (' +
    Object.keys(groups).filter(function(k) { return groups[k].length >= 2; }).length + '주문)');
  return cnt;
}

// ============================================================
// 11. 자가 검증
// ============================================================
function test_Part5_() {
  // 1. detectSet
  console.log('detectSet_(2PACK 양말): ' + detectSet_('[2PACK] 양말'));
  console.log('detectSet_(1+1 티셔츠): ' + detectSet_('1+1 티셔츠'));
  console.log('detectSet_(일반 원피스): ' + detectSet_('일반 원피스'));

  // 2. 슬랙스 3종 remap
  var r1 = {productName: '링클 프리 베이직 슬랙스 3종', optionRaw: '원턱/28', qty: 1,
            trans: 99000, transCoupon: 99000, transSelf: 99000, transPay: 99000};
  var d1 = decomposeRow_(r1);
  console.log('슬랙스 단일 remap: pb=' + d1[0].pb + ' cc=' + d1[0].cc + ' sz=' + d1[0].sz);

  // 3. 슬랙스 다중 분해
  var r2 = {productName: '링클 프리 슬랙스 3종 SET', optionRaw: '[OPTION 1]원턱:28[OPTION 2]투턱:30', qty: 3,
            trans: 297000, transCoupon: 297000, transSelf: 297000, transPay: 297000};
  var d2 = decomposeRow_(r2);
  console.log('슬랙스 다중: ' + d2.length + '행, 각 trans=' + (d2[0] && d2[0].trans));

  // 4. 범용 1+1
  var r3 = {productName: '링클프리 베이직 셔츠 1+1', optionRaw: 'OPITON 1=WHITE:F, OPITON 2=BLUE:F', qty: 2,
            trans: 59900, transCoupon: 59900, transSelf: 59900, transPay: 59900};
  var d3 = decomposeRow_(r3);
  console.log('범용 1+1: ' + d3.length + '행 [' + d3.map(function(x){return x.cc+'/'+x.sz;}).join(', ') + ']');

  // 5. SPECIAL_SET_MAP (실켓)
  var r4 = {productName: '실켓 반팔 티셔츠 SET', optionRaw: '[OPTION 1]크롭:F[OPTION 2]레귤러:F', qty: 2,
            trans: 78000, transCoupon: 78000, transSelf: 78000, transPay: 78000};
  var d4 = decomposeRow_(r4);
  console.log('실켓 분해: ' + d4.length + '행, pb=[' + d4.map(function(x){return x.pb;}).join(',') + ']');

  // 6. SET 거래액 균등배분
  var rows = [
    {orderNo: 'T1', productName: '2PACK 양말', trans: 10000, transCoupon: 10000, transSelf: 10000, transPay: 10000},
    {orderNo: 'T1', productName: '2PACK 양말', trans: 0,     transCoupon: 0,     transSelf: 0,     transPay: 0}
  ];
  rebalanceSetTransByOrder_(rows);
  console.log('균등배분 후: ' + rows.map(function(r) { return r.trans; }).join(', '));
}
