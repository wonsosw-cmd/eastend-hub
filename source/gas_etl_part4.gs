/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part4
 * 16 채널 리더 (자사몰/29CM/무신사/W컨셉/롯데온/하고/네이버/지그재그/
 *                카카오/드립/EQL/CJ/오픈북/GS/퀸잇/오프라인)
 *
 * 모든 리더: (rows 2D 배열) → 표준 row 객체 배열 반환
 * 표준 row: {brand,channel,pb,cc,sz,productName,colorRaw,sizeRaw,qty,
 *            trans,transCoupon,transSelf,transPay,orderDate,orderNo,
 *            isSet,isOffline,optionRaw,erpCode}
 * ========================================================================
 */

// ============================================================
// 컬럼 탐색 헬퍼
// ============================================================
function _findCol_(header, patterns) {
  for (var p = 0; p < patterns.length; p++) {
    var re = patterns[p];
    for (var i = 0; i < header.length; i++) {
      if (re.test(String(header[i] || '').trim())) return i;
    }
  }
  return -1;
}
function _getCol_(row, idx) { return idx < 0 ? '' : row[idx]; }

// ============================================================
// 상품명에서 pb/cc/sz/option 추출 (범용)
// ============================================================
function _extractFromProductName_(name, option) {
  var pb = extractPbFromRegex_(name);
  var opt = String(option || '').trim();
  // option 분리: "BLACK/F" 또는 "BLACK:F" 또는 "컬러=BLACK,사이즈=F"
  var cc = '', sz = '';
  var m = opt.match(/^([^\/:=]+)[\/:=]([^\/:=]+)$/);
  if (m) {
    cc = parseCc_(m[1].trim());
    sz = normSz_(m[2].trim());
  } else if (opt) {
    // 단일값 — 사이즈 같으면 sz, 아니면 cc
    var oneNorm = normSz_(opt);
    if (/^(0[FSML]|X[SLX3]|\d{2})$/.test(oneNorm)) sz = oneNorm;
    else cc = parseCc_(opt);
  }
  return {pb: pb, cc: cc, sz: sz};
}

// ============================================================
// 1. 자사몰 (2-pass — 주문서쿠폰/적립금 비율 배분)
// ============================================================
function readJasamo_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/^주문번호$/]);
  var cSt = _findCol_(H, [/^주문\s*상태$/]);
  var cPcode = _findCol_(H, [/자체상품코드/]);
  var cOpt = _findCol_(H, [/^상품옵션$/]);
  var cQty = _findCol_(H, [/^수량$/]);
  var cRawP = _findCol_(H, [/상품구매금액/]);
  var cExtra = _findCol_(H, [/상품별\s*추가할인/]);
  var cAppDisc = _findCol_(H, [/앱\s*상품할인\s*금액/]);
  var cOrderCpn = _findCol_(H, [/주문서\s*쿠폰/]);
  var cLoy = _findCol_(H, [/적립금\s*사용|사용한\s*적립금/]);
  var cDt = _findCol_(H, [/^주문일시$/]);
  var cPname = _findCol_(H, [/^상품명$/]);

  var EXCL_EXACT = {'취소 완료':1,'반품 완료 - 환불완료':1,'취소 접수':1,'취소 처리중 - 환불전':1};

  // Pass1: 유효 행 수집 + 주문별 합계
  var valid = [];
  var orderNetSum = {};  // orderNo → sum(itemNet)
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var st = cleanStr_(_getCol_(r, cSt));
    if (EXCL_EXACT[st] || st.indexOf('취소') >= 0 || st.indexOf('반품 완료') >= 0) continue;
    var ono = cleanStr_(_getCol_(r, cOno));
    if (!ono) continue;
    var rawP = toNum_(_getCol_(r, cRawP));
    var extra = toNum_(_getCol_(r, cExtra));
    var appDisc = toNum_(_getCol_(r, cAppDisc));
    var itemNet = rawP - extra - appDisc;
    orderNetSum[ono] = (orderNetSum[ono] || 0) + itemNet;
    valid.push({r: r, ono: ono, itemNet: itemNet});
  }

  // Pass2: 쿠폰/적립금 배분
  var out = [];
  valid.forEach(function(v) {
    var r = v.r;
    var orderCpn = toNum_(_getCol_(r, cOrderCpn));
    var loyalty = toNum_(_getCol_(r, cLoy));
    var totalNet = orderNetSum[v.ono] || v.itemNet;
    var ratio = totalNet > 0 ? (v.itemNet / totalNet) : 1;
    var cpnAlloc = orderCpn * ratio;
    var loyAlloc = loyalty * ratio;
    var trans = v.itemNet - cpnAlloc;
    var orderDate = normalizeDate_(_getCol_(r, cDt));
    if (orderDate && orderDate < ETL_CUTOFF_DATE) return;

    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var ex = _extractFromProductName_(pname, optRaw);
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var pb = ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : '');

    out.push({
      brand: brand, channel: '자사몰', isOffline: false, erpCode: '',
      pb: pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: trans, transCoupon: trans, transSelf: trans, transPay: trans - loyAlloc,
      orderDate: orderDate, orderNo: v.ono,
      isSet: detectSet_(pname)
    });
  });
  return out;
}

// ============================================================
// 2. 29CM
// ============================================================
function read29cm_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cOpt = _findCol_(H, [/^옵션$/]);
  var cQty = _findCol_(H, [/^수량$/]);
  var cSales = _findCol_(H, [/^판매액$/]);
  var cCpn = _findCol_(H, [/쿠폰\s*할인/]);
  var cRealSales = _findCol_(H, [/실\s*판매액|^실판매액$/]);
  var cPreLoy = _findCol_(H, [/적립금\s*선할인/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'취소대기':1,'반품완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var sales = toNum_(_getCol_(r, cSales));
    var cpn = toNum_(_getCol_(r, cCpn));
    var realSales = toNum_(_getCol_(r, cRealSales)) || sales;
    var preLoy = toNum_(_getCol_(r, cPreLoy));
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var ex = _extractFromProductName_(pname, optRaw);

    out.push({
      brand: brand, channel: '29CM', isOffline: false,
      pb: ex.pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: sales, transCoupon: sales - cpn, transSelf: sales - cpn,
      transPay: realSales - preLoy,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 3. 무신사 (클레임상태 이중 필터)
// ============================================================
function readMusinsa_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cStyle = _findCol_(H, [/스타일넘버|품번/]);
  var cOpt = _findCol_(H, [/^옵션$/]);
  var cQty = _findCol_(H, [/수량/]);
  var cPrice = _findCol_(H, [/^판매가$/]);
  var cCpn = _findCol_(H, [/^상품쿠폰$/]);
  var cVendorCpn = _findCol_(H, [/업체부담쿠폰/]);
  var cSales = _findCol_(H, [/매출금액/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var st = cleanStr_(_getCol_(r, cSt));
    if (st.indexOf('취소') >= 0 || st.indexOf('반품') >= 0 || st === '결제오류') continue;
    if (cClaim >= 0 && cleanStr_(_getCol_(r, cClaim)) === '환불완료') continue;

    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var price = toNum_(_getCol_(r, cPrice));
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var cpn = toNum_(_getCol_(r, cCpn));
    var vendorCpn = toNum_(_getCol_(r, cVendorCpn));
    var sales = toNum_(_getCol_(r, cSales));
    var trans = price * qty;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var style = cleanStr_(_getCol_(r, cStyle)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);
    var pb = ex.pb || (PB_RE.test(style) ? style.match(PB_RE)[0] : '');

    out.push({
      brand: brand, channel: '무신사', isOffline: false,
      pb: pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: qty, trans: trans, transCoupon: trans - cpn,
      transSelf: trans - vendorCpn, transPay: sales || trans,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 4. W컨셉
// ============================================================
function readWconc_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cItem = _findCol_(H, [/아이템코드|상품코드/]);
  var cOpt1 = _findCol_(H, [/색상|옵션1/]);
  var cOpt2 = _findCol_(H, [/사이즈|옵션2/]);
  var cQty = _findCol_(H, [/수량/]);
  var cAmt = _findCol_(H, [/^금액$/]);
  var cSellerCpn = _findCol_(H, [/판매자쿠폰/]);
  var cCompCpn = _findCol_(H, [/당사쿠폰/]);
  var cPay = _findCol_(H, [/고객결제금액/]);
  var cDt = _findCol_(H, [/결제일자/]);

  var EXCL = {'환불완료':1,'취소완료':1,'취소대기중':1,'취소':1,'반품완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var amt = toNum_(_getCol_(r, cAmt));
    var sCpn = toNum_(_getCol_(r, cSellerCpn));
    var cCpn = toNum_(_getCol_(r, cCompCpn));
    var pay = toNum_(_getCol_(r, cPay));
    var pname = cleanStr_(_getCol_(r, cPname));
    var opt1 = cleanStr_(_getCol_(r, cOpt1));
    var opt2 = cleanStr_(_getCol_(r, cOpt2));
    var item = cleanStr_(_getCol_(r, cItem)).toUpperCase();

    out.push({
      brand: brand, channel: 'W컨셉', isOffline: false,
      pb: extractPbFromRegex_(item) || extractPbFromRegex_(pname),
      cc: parseCc_(opt1), sz: normSz_(opt2),
      productName: pname, colorRaw: opt1, sizeRaw: opt2, optionRaw: opt1 + '/' + opt2,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: amt, transCoupon: amt - sCpn - cCpn, transSelf: amt - sCpn, transPay: pay,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 5. 롯데온
// ============================================================
function readLotteon_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문진행단계|주문상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cOpt = _findCol_(H, [/옵션정보|옵션/]);
  var cQty = _findCol_(H, [/수량/]);
  var cTotal = _findCol_(H, [/총판매금액/]);
  var cSInst = _findCol_(H, [/셀러즉시할인/]);
  var cSDisc = _findCol_(H, [/상품할인.*셀러부담/]);
  var cLDisc = _findCol_(H, [/상품할인.*롯데ON|롯데.*부담/]);
  var cFee = _findCol_(H, [/제휴수수료/]);
  var cDt = _findCol_(H, [/주문접수일/]);

  var EXCL = {'취소':1,'반품완료':1,'반품접수':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var ono = cleanStr_(_getCol_(r, cOno));
    if (!ono) continue;
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var total = toNum_(_getCol_(r, cTotal));
    var sInst = toNum_(_getCol_(r, cSInst));
    var sDisc = toNum_(_getCol_(r, cSDisc));
    var lDisc = toNum_(_getCol_(r, cLDisc));
    var fee = toNum_(_getCol_(r, cFee));
    var trans = total - sInst;
    var transCoupon = trans - sDisc - lDisc;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var ex = _extractFromProductName_(pname, optRaw);

    out.push({
      brand: brand, channel: '롯데온', isOffline: false,
      pb: ex.pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: trans, transCoupon: transCoupon, transSelf: trans - sDisc, transPay: transCoupon - fee,
      orderDate: od, orderNo: ono, isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 6. 하고
// ============================================================
function readHago_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태|^상태$/]);
  var cPcode = _findCol_(H, [/제휴사상품코드/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cOpt = _findCol_(H, [/^옵션$/]);
  var cOrdQty = _findCol_(H, [/주문수량/]);
  var cCanQty = _findCol_(H, [/취소수량/]);
  var cRetQty = _findCol_(H, [/반품수량/]);
  var cExQty = _findCol_(H, [/교환수량/]);
  var cPrice = _findCol_(H, [/1차\s*할인가/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (cleanStr_(_getCol_(r, cSt)).indexOf('취소') >= 0) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var net = toNum_(_getCol_(r, cOrdQty)) - toNum_(_getCol_(r, cCanQty))
              - toNum_(_getCol_(r, cRetQty)) + toNum_(_getCol_(r, cExQty));
    if (net <= 0) continue;
    var trans = toNum_(_getCol_(r, cPrice)) * net;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);

    out.push({
      brand: brand, channel: '하고', isOffline: false,
      pb: ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : ''),
      cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: net, trans: trans, transCoupon: trans, transSelf: trans, transPay: trans,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 7. 네이버 (부분 포함 필터)
// ============================================================
function readNaver_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태|주문세부상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cPcode = _findCol_(H, [/판매자상품코드|옵션관리코드/]);
  var cOpt = _findCol_(H, [/옵션정보/]);
  var cQty = _findCol_(H, [/수량/]);
  var cPrice = _findCol_(H, [/상품가격/]);
  var cSellerDisc = _findCol_(H, [/판매자\s*부담\s*할인/]);
  var cFinal = _findCol_(H, [/최종\s*상품별/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var st = cleanStr_(_getCol_(r, cSt));
    if (st.indexOf('취소') >= 0 || st.indexOf('반품') >= 0) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var price = toNum_(_getCol_(r, cPrice));
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var sellerDisc = toNum_(_getCol_(r, cSellerDisc));
    var finalAmt = toNum_(_getCol_(r, cFinal));
    var trans = price * qty - sellerDisc;
    var tc = finalAmt || trans;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);

    out.push({
      brand: brand, channel: '네이버 스마트스토어', isOffline: false,
      pb: ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : ''),
      cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: qty, trans: trans, transCoupon: tc, transSelf: tc, transPay: tc,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 8. 지그재그 (이중 필터 + (원) 접미사)
// ============================================================
function readZigzag_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/^주문상태$/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cPcode = _findCol_(H, [/자체상품코드|상품코드/]);
  var cOpt = _findCol_(H, [/옵션정보/]);
  var cQty = _findCol_(H, [/수량/]);
  var cAmt = _findCol_(H, [/상품주문액/]);
  var cCpn = _findCol_(H, [/쿠폰\s*할인\s*금액/]);
  var cStoreBurden = _findCol_(H, [/스토어\s*부담/]);
  var cMileage = _findCol_(H, [/마일리지\s*할인/]);
  var cColor = _findCol_(H, [/^색상$/]);
  var cSize = _findCol_(H, [/^사이즈$/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var CLAIM_EXCL = {'취소완료':1,'반품완료':1,'환불완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    if (cClaim >= 0 && CLAIM_EXCL[cleanStr_(_getCol_(r, cClaim))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var amt = toNum_(_getCol_(r, cAmt));
    var cpn = toNum_(_getCol_(r, cCpn));
    var sb = toNum_(_getCol_(r, cStoreBurden));
    var mil = toNum_(_getCol_(r, cMileage));
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var color = cleanStr_(_getCol_(r, cColor));
    var size = cleanStr_(_getCol_(r, cSize));

    out.push({
      brand: brand, channel: '지그재그', isOffline: false,
      pb: extractPbFromRegex_(pcode) || extractPbFromRegex_(pname),
      cc: parseCc_(color) || _extractFromProductName_(pname, optRaw).cc,
      sz: normSz_(size) || _extractFromProductName_(pname, optRaw).sz,
      productName: pname, colorRaw: color || optRaw, sizeRaw: size, optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: amt, transCoupon: amt - cpn, transSelf: amt - sb, transPay: amt - cpn - mil,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 9. 카카오선물하기
// ============================================================
function readKakao_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cOpt = _findCol_(H, [/옵션/]);
  var cQty = _findCol_(H, [/수량/]);
  var cSettle = _findCol_(H, [/정산기준금액/]);
  var cDt = _findCol_(H, [/주문일/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var CLAIM_EXCL = {'취소완료':1,'반품완료':1,'환불완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    if (cClaim >= 0 && CLAIM_EXCL[cleanStr_(_getCol_(r, cClaim))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var settle = toNum_(_getCol_(r, cSettle));
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var ex = _extractFromProductName_(pname, optRaw);
    out.push({
      brand: brand, channel: '카카오선물하기', isOffline: false,
      pb: ex.pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: settle, transCoupon: settle, transSelf: settle, transPay: settle,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 10. 드립 (option1/2 swap)
// ============================================================
var DRIP_SZ_TOKENS = /^(S|M|L|F|XS|XL|XXL|FREE|원사이즈|\d{2}|S\(.+\))$/i;

function _dripExtractColorFromName_(name) {
  if (!name) return '';
  var parts = String(name).split('_');
  if (parts.length < 2) return '';
  var last = parts[parts.length - 1].trim();
  if (/^\d+COLORS$/i.test(last)) return '';
  return last;
}

function readDrip_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cPcode = _findCol_(H, [/상품코드/]);
  var cOpt1 = _findCol_(H, [/색상|옵션1/]);
  var cOpt2 = _findCol_(H, [/사이즈|옵션2/]);
  var cPay = _findCol_(H, [/^결제가$|^판매가$/]);
  var cCpn = _findCol_(H, [/쿠폰할인/]);
  var cLoy = _findCol_(H, [/적립금사용|사용적립금/]);
  var cDt = _findCol_(H, [/결제완료일|배송시작일|주문일/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var CLAIM_EXCL = {'취소완료':1,'반품완료':1,'환불완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    if (cClaim >= 0 && CLAIM_EXCL[cleanStr_(_getCol_(r, cClaim))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;

    var opt1 = cleanStr_(_getCol_(r, cOpt1));
    var opt2 = cleanStr_(_getCol_(r, cOpt2));
    var color = opt1, size = opt2;
    var pname = cleanStr_(_getCol_(r, cPname));

    // option swap 감지
    if (DRIP_SZ_TOKENS.test(opt1) && (!opt2 || opt2 === '-')) {
      size = opt1;
      color = _dripExtractColorFromName_(pname);
    } else if (opt1 === '-' && opt2 === '-') {
      size = 'FREE';
      color = _dripExtractColorFromName_(pname);
    }

    var pay = toNum_(_getCol_(r, cPay));
    var cpn = toNum_(_getCol_(r, cCpn));
    var loy = toNum_(_getCol_(r, cLoy));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();

    out.push({
      brand: brand, channel: '드립', isOffline: false,
      pb: extractPbFromRegex_(pcode) || extractPbFromRegex_(pname),
      cc: parseCc_(color), sz: normSz_(size),
      productName: pname, colorRaw: color, sizeRaw: size, optionRaw: opt1 + '/' + opt2,
      qty: 1,
      trans: pay, transCoupon: pay - cpn, transSelf: pay, transPay: pay - cpn - loy,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 11. EQL
// ============================================================
function readEql_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태|배송상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cPcode = _findCol_(H, [/업체상품코드|상품코드/]);
  var cOpt = _findCol_(H, [/^옵션$/]);
  var cQty = _findCol_(H, [/주문수량|수량/]);
  var cSales = _findCol_(H, [/판매금액/]);
  var cDt = _findCol_(H, [/주문일시/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var trans = toNum_(_getCol_(r, cSales)) * qty;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);
    out.push({
      brand: brand, channel: 'EQL', isOffline: false,
      pb: ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : ''),
      cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: qty, trans: trans, transCoupon: trans, transSelf: trans, transPay: trans,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 12. CJ
// ============================================================
function readCj_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태|배송상태/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cPcode = _findCol_(H, [/상품코드|옵션코드/]);
  var cOpt = _findCol_(H, [/옵션명/]);
  var cQty = _findCol_(H, [/수량/]);
  var cPrice = _findCol_(H, [/^판매가$/]);
  var cPay = _findCol_(H, [/^결제가$/]);
  var cDt = _findCol_(H, [/결제일|주문일/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var CLAIM_EXCL = {'취소완료':1,'반품완료':1,'환불완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    if (cClaim >= 0 && CLAIM_EXCL[cleanStr_(_getCol_(r, cClaim))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var price = toNum_(_getCol_(r, cPrice));
    var pay = toNum_(_getCol_(r, cPay));
    var trans = price * qty;
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);
    out.push({
      brand: brand, channel: 'CJ온스타일', isOffline: false,
      pb: ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : ''),
      cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: qty, trans: trans, transCoupon: pay, transSelf: trans, transPay: pay,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 13. 오픈북 (거래액 4종 = 품목별 결제금액으로 통일)
// ============================================================
function readOpenbook_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/주문상태/]);
  var cClaim = _findCol_(H, [/클레임.*상태/]);
  var cPname = _findCol_(H, [/주문상품명|^상품명$/]);
  var cPcode = _findCol_(H, [/자체상품코드/]);
  var cOpt = _findCol_(H, [/상품옵션|옵션/]);
  var cQty = _findCol_(H, [/수량/]);
  var cSettle = _findCol_(H, [/품목별\s*결제금액/]);
  var cDt = _findCol_(H, [/주문일시|주문일/]);

  var EXCL = {'취소':1,'취소완료':1,'환불완료':1,'반품완료':1,'배송취소':1};
  var CLAIM_EXCL = {'취소완료':1,'반품완료':1,'환불완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    if (cClaim >= 0 && CLAIM_EXCL[cleanStr_(_getCol_(r, cClaim))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var settle = toNum_(_getCol_(r, cSettle));
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var pcode = cleanStr_(_getCol_(r, cPcode)).toUpperCase();
    var ex = _extractFromProductName_(pname, optRaw);
    out.push({
      brand: brand, channel: '오픈북', isOffline: false,
      pb: ex.pb || (PB_RE.test(pcode) ? pcode.match(PB_RE)[0] : ''),
      cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: toNum_(_getCol_(r, cQty)) || 1,
      trans: settle, transCoupon: settle, transSelf: settle, transPay: settle,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 14. 브론테 / GS SHOP
// ============================================================
function readGsshop_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cOno = _findCol_(H, [/주문번호/]);
  var cSt = _findCol_(H, [/^상태$|주문상태/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cOpt = _findCol_(H, [/옵션정보|옵션/]);
  var cQty = _findCol_(H, [/수량/]);
  var cPrice = _findCol_(H, [/^판매가$/]);
  var cPay = _findCol_(H, [/결제금액|고객결제액/]);
  var cDt = _findCol_(H, [/주문일자|주문일/]);

  var EXCL = {'취소완료':1,'반품완료':1};
  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    if (EXCL[cleanStr_(_getCol_(r, cSt))]) continue;
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var trans = toNum_(_getCol_(r, cPrice)) * qty;
    var pay = toNum_(_getCol_(r, cPay));
    var pname = cleanStr_(_getCol_(r, cPname));
    var optRaw = cleanStr_(_getCol_(r, cOpt));
    var ex = _extractFromProductName_(pname, optRaw);
    var tcRest = pay || trans;
    out.push({
      brand: brand, channel: 'GS SHOP', isOffline: false,
      pb: ex.pb, cc: ex.cc, sz: ex.sz,
      productName: pname, colorRaw: optRaw, sizeRaw: '', optionRaw: optRaw,
      qty: qty, trans: trans, transCoupon: tcRest, transSelf: tcRest, transPay: tcRest,
      orderDate: od, orderNo: cleanStr_(_getCol_(r, cOno)),
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 15. 퀸잇 (GS와 유사)
// ============================================================
function readQueenit_(rows, brand) {
  var out = readGsshop_(rows, brand);
  out.forEach(function(x) { x.channel = '퀸잇'; });
  return out;
}

// ============================================================
// 16. 오프라인
// ============================================================
function readOffline_(rows, brand) {
  if (!rows || rows.length < 2) return [];
  var H = rows[0];
  var cStore = _findCol_(H, [/매장명/]);
  var cStoreCode = _findCol_(H, [/매장코드/]);
  var cPb = _findCol_(H, [/^품번$|상품코드/]);
  var cPname = _findCol_(H, [/^상품명$/]);
  var cColor = _findCol_(H, [/^색상$|컬러/]);
  var cSize = _findCol_(H, [/^사이즈$/]);
  var cQty = _findCol_(H, [/^수량$/]);
  var cSales = _findCol_(H, [/실판매금액/]);
  var cDt = _findCol_(H, [/판매일자/]);

  var out = [];
  for (var i = 1; i < rows.length; i++) {
    var r = rows[i];
    var od = normalizeDate_(_getCol_(r, cDt));
    if (od && od < ETL_CUTOFF_DATE) continue;
    var qty = toNum_(_getCol_(r, cQty)) || 1;
    var sales = toNum_(_getCol_(r, cSales));
    var storeName = cleanStr_(_getCol_(r, cStore));
    var pb = cleanStr_(_getCol_(r, cPb)).toUpperCase();
    if (!PB_RE.test(pb)) pb = extractPbFromRegex_(pb);
    var color = cleanStr_(_getCol_(r, cColor));
    var size = cleanStr_(_getCol_(r, cSize));
    var pname = cleanStr_(_getCol_(r, cPname));

    out.push({
      brand: brand, channel: storeName, isOffline: true,
      erpCode: cleanStr_(_getCol_(r, cStoreCode)),
      pb: pb, cc: parseCc_(color), sz: normSz_(size),
      productName: pname, colorRaw: color, sizeRaw: size, optionRaw: color + '/' + size,
      qty: qty, trans: sales, transCoupon: sales, transSelf: sales, transPay: sales,
      orderDate: od, orderNo: storeName + '|' + od + '|' + i,  // 인공 orderNo
      isSet: detectSet_(pname)
    });
  }
  return out;
}

// ============================================================
// 17. 채널 리더 디스패치
// ============================================================
var CHANNEL_READERS = {
  'jasamo':   readJasamo_,
  '29cm':     read29cm_,
  'musinsa':  readMusinsa_,
  'wconc':    readWconc_,
  'lotteon':  readLotteon_,
  'hago':     readHago_,
  'naver':    readNaver_,
  'zigzag':   readZigzag_,
  'kakao':    readKakao_,
  'drip':     readDrip_,
  'eql':      readEql_,
  'cj':       readCj_,
  'openbook': readOpenbook_,
  'gsshop':   readGsshop_,
  'queenit':  readQueenit_,
  'offline':  readOffline_
};

// ============================================================
// 18. 브랜드 감지 (파일명 기준)
// ============================================================
function detectBrandFromFilename_(filename) {
  var f = String(filename || '');
  if (/시티브리즈|citybreeze|CT/i.test(f)) return '시티브리즈';
  if (/아티드|artid|AT/i.test(f)) return '아티드';
  return '시티브리즈'; // 기본
}

// ============================================================
// 19. 자가 검증
// ============================================================
function test_Part4_() {
  console.log('채널 리더 등록 수: ' + Object.keys(CHANNEL_READERS).length);
  Object.keys(CHANNEL_READERS).forEach(function(k) {
    console.log('  ' + k + ' → ' + CHANNEL_READERS[k].name);
  });
  // 파일명 감지 테스트
  ['자사몰_시티브리즈_0413.xlsx','29CM_0413.xls','브론테_0413.xlsx','오프라인_매장_0413.xlsx'].forEach(function(fn) {
    console.log(fn + ' → channel=' + detectChannel_(fn) + ' brand=' + detectBrandFromFilename_(fn));
  });
}
