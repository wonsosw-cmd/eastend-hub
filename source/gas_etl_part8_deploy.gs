/**
 * ========================================================================
 * EASTEND ETL (GAS 버전) — Part8
 * HUB GitHub Pages 배포 모듈
 *
 * 역할:
 *   - ETL 결과 xlsx를 JSON으로 변환하여 GitHub 레포에 push
 *   - 여러 소스(ETL 결과 / Sheets / 재고 파일)를 하나로 결합
 *   - eastend-hub 레포의 index.html + data/*.json 자산 업데이트
 *   - Step8_deployHub() → runFullETL의 마지막 단계
 *
 * 필요 전역 상수 (Part1에 추가됨):
 *   HUB_GITHUB_OWNER, HUB_GITHUB_REPO, HUB_GITHUB_TOKEN,
 *   HUB_GITHUB_BRANCH, HUB_URL
 * ========================================================================
 */

// ============================================================
// 1. GitHub API 헬퍼
// ============================================================
function _ghApi_(method, path, payload) {
  var url = 'https://api.github.com/repos/' + HUB_GITHUB_OWNER + '/' + HUB_GITHUB_REPO + path;
  var options = {
    method: method,
    headers: {
      Authorization: 'token ' + HUB_GITHUB_TOKEN,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28'
    },
    muteHttpExceptions: true
  };
  if (payload) {
    options.contentType = 'application/json';
    options.payload = JSON.stringify(payload);
  }
  var res = UrlFetchApp.fetch(url, options);
  var code = res.getResponseCode();
  var body = res.getContentText();
  return {code: code, body: body, json: function() {
    try { return JSON.parse(body); } catch(e) { return null; }
  }};
}

function _ghGetFileSha_(path) {
  var res = _ghApi_('GET', '/contents/' + encodeURIComponent(path) +
    '?ref=' + HUB_GITHUB_BRANCH, null);
  if (res.code !== 200) return null;
  var j = res.json();
  return j && j.sha ? j.sha : null;
}

function _ghPutFile_(path, contentBase64, message) {
  var sha = _ghGetFileSha_(path);
  var payload = {
    message: message || ('Update ' + path + ' @ ' + new Date().toISOString()),
    content: contentBase64,
    branch: HUB_GITHUB_BRANCH
  };
  if (sha) payload.sha = sha;
  var res = _ghApi_('PUT', '/contents/' + encodeURIComponent(path), payload);
  if (res.code < 200 || res.code >= 300) {
    throw new Error('GitHub PUT 실패 ' + res.code + ' ' + path + ': ' + res.body.substring(0, 200));
  }
  return res.json();
}

// ============================================================
// 2. 최신 ETL 결과물 조회 (OUTPUT_FOLDER_ID에서 최신)
// ============================================================
function _findLatestEtlOutput_() {
  var folder = DriveApp.getFolderById(OUTPUT_FOLDER_ID);
  var files = folder.getFilesByType(MimeType.MICROSOFT_EXCEL);
  var latest = null;
  while (files.hasNext()) {
    var f = files.next();
    var name = f.getName();
    if (!/2026_로우데이터_결과물_\d{4}(_v\d+)?\.xlsx$/.test(name)) continue;
    if (!latest || f.getLastUpdated() > latest.getLastUpdated()) latest = f;
  }
  return latest;
}

// ============================================================
// 3. xlsx → JSON 변환 (Drive Advanced Service)
// ============================================================
function _etlXlsxToJson_(fileId) {
  var copy = Drive.Files.copy(
    {mimeType: 'application/vnd.google-apps.spreadsheet', title: '_hub_tmp_' + fileId},
    fileId
  );
  try {
    var ss = SpreadsheetApp.openById(copy.id);
    var sheet = ss.getSheets()[0];
    var data = sheet.getDataRange().getValues();
    if (data.length < 2) return {header: [], rows: []};
    var H = data[0];
    var rows = [];
    for (var i = 1; i < data.length; i++) {
      var row = {};
      for (var j = 0; j < H.length; j++) {
        var v = data[i][j];
        if (v instanceof Date) v = Utilities.formatDate(v, ETL_TZ, 'yyyy-MM-dd');
        row[H[j]] = v;
      }
      rows.push(row);
    }
    return {header: H, rows: rows};
  } finally {
    try { Drive.Files.remove(copy.id); } catch(e) {}
  }
}

// ============================================================
// 4. 상품별 집계 (③ 상품별 현황 뷰용)
// ============================================================
function _aggregateByProduct_(etlJson) {
  var map = {};
  etlJson.rows.forEach(function(r) {
    var pb = r['품번'];
    if (!pb) return;
    var k = pb;
    if (!map[k]) {
      map[k] = {
        pb: pb, name: r['상품명'] || '', category: r['카테고리'] || '',
        season: r['시즌'] || '', brand: (r['구분'] || '').split(' ')[0] || '',
        qty: 0, trans: 0, transCoupon: 0, transSelf: 0, transPay: 0,
        costSum: 0, tagSum: 0, daily: {}, options: {}
      };
    }
    var e = map[k];
    var q = toNum_(r['수량']);
    e.qty += q;
    e.trans += toNum_(r['거래액']);
    e.transCoupon += toNum_(r['거래액(쿠폰포함)']);
    e.transSelf += toNum_(r['거래액(자체쿠폰포함)']);
    e.transPay += toNum_(r['거래액(결제금액)']);
    e.costSum += toNum_(r['원가(VAT+)']);
    e.tagSum += toNum_(r['정상가(TAG가)']);

    var od = r['주문일'];
    if (od) {
      if (!e.daily[od]) e.daily[od] = {qty: 0, trans: 0};
      e.daily[od].qty += q;
      e.daily[od].trans += toNum_(r['거래액']);
    }

    var optKey = (r['컬러코드'] || '') + '|' + (r['사이즈'] || '');
    if (!e.options[optKey]) e.options[optKey] = {cc: r['컬러코드'], sz: r['사이즈'], qty: 0, trans: 0};
    e.options[optKey].qty += q;
    e.options[optKey].trans += toNum_(r['거래액']);
  });
  return Object.keys(map).map(function(k) { return map[k]; });
}

// ============================================================
// 5. HUB 메타 구축 (요약 통계)
// ============================================================
function _buildHubMeta_(etlJson, outputFile) {
  var total = etlJson.rows.length;
  var red = 0, yellow = 0, transTotal = 0;
  var channelMap = {}, brandMap = {};
  etlJson.rows.forEach(function(r) {
    var flag = String(r['오류/오류점검'] || '');
    if (flag === 'red') red++;
    else if (flag === 'yellow') yellow++;
    var ch = r['채널명'] || '(기타)';
    var br = (r['구분'] || '').split(' ')[0] || '(기타)';
    channelMap[ch] = (channelMap[ch] || 0) + toNum_(r['거래액']);
    brandMap[br] = (brandMap[br] || 0) + toNum_(r['거래액']);
    transTotal += toNum_(r['거래액']);
  });
  return {
    generatedAt: new Date().toISOString(),
    etlFile: outputFile ? outputFile.getName() : '',
    etlFileUrl: outputFile ? outputFile.getUrl() : '',
    etlFileId: outputFile ? outputFile.getId() : '',
    totalRows: total,
    red: red,
    yellow: yellow,
    transTotal: transTotal,
    byChannel: channelMap,
    byBrand: brandMap,
    hubUrl: HUB_URL
  };
}

// ============================================================
// 6. Step8: HUB 배포 (엔트리포인트)
// ============================================================
function Step8_deployHub() {
  var t0 = Date.now();
  logProgress_('Step8', 'START (HUB 배포)');

  try {
    if (!HUB_GITHUB_TOKEN || HUB_GITHUB_TOKEN.indexOf('ghp_') !== 0 && HUB_GITHUB_TOKEN.indexOf('github_pat_') !== 0) {
      logProgress_('Step8', 'HUB_GITHUB_TOKEN 미설정 — HUB 배포 스킵');
      setEtlState_('DONE', {});
      scheduleNextStep_('Step7_cleanup', 1000);
      return;
    }

    // 1. 최신 ETL 결과물 찾기
    var latestFile = _findLatestEtlOutput_();
    if (!latestFile) throw new Error('ETL 결과 xlsx 없음 — Step6 먼저 실행되어야 함');
    logProgress_('Step8', '대상: ' + latestFile.getName());

    // 2. xlsx → JSON 변환
    var etlJson = _etlXlsxToJson_(latestFile.getId());
    logProgress_('Step8', 'JSON 변환 완료: ' + etlJson.rows.length + '행');

    // 3. 상품별 집계
    var products = _aggregateByProduct_(etlJson);
    logProgress_('Step8', '상품별 집계: ' + products.length + '개 품번');

    // 4. 메타 정보
    var meta = _buildHubMeta_(etlJson, latestFile);

    // 5. GitHub push
    // 5-1. data/meta.json
    var metaB64 = Utilities.base64Encode(JSON.stringify(meta, null, 2), Utilities.Charset.UTF_8);
    _ghPutFile_('data/meta.json', metaB64, 'ETL meta: ' + meta.generatedAt);

    // 5-2. data/products.json (gzip 고려 용량이 크면 분할)
    var prodStr = JSON.stringify(products);
    var prodSize = Utilities.newBlob(prodStr).getBytes().length;
    logProgress_('Step8', 'products.json 크기: ' + (prodSize / 1024).toFixed(0) + 'KB');

    if (prodSize < 50 * 1024 * 1024) {
      // 50MB 이내: 직접 업로드
      var prodB64 = Utilities.base64Encode(prodStr, Utilities.Charset.UTF_8);
      _ghPutFile_('data/products.json', prodB64, 'ETL products: ' + meta.generatedAt);
    } else {
      // 50MB 초과 시 gzip
      var gzBlob = Utilities.gzip(Utilities.newBlob(prodStr, 'application/json', 'products.json'));
      var gzB64 = Utilities.base64Encode(gzBlob.getBytes());
      _ghPutFile_('data/products.json.gz', gzB64, 'ETL products (gzip): ' + meta.generatedAt);
    }

    // 6. 상태 저장 + 다음 단계
    setEtlState_('Step7', {
      'HUB_LAST_DEPLOY_AT': String(Date.now()),
      'HUB_LAST_META': HUB_URL + '/data/meta.json'
    });

    logProgress_('Step8', 'HUB 배포 완료 → ' + HUB_URL);
    logProgress_('Step8', 'END (' + ((Date.now() - t0) / 1000).toFixed(1) + 's)');
    scheduleNextStep_('Step7_cleanup', 1000);

  } catch (e) {
    logProgress_('Step8', 'ERROR: ' + e.message + '\n' + (e.stack || ''));
    throw e;
  }
}

// ============================================================
// 7. index.html 초기 배포 (최초 1회 수동 실행)
//    대표님이 /hub/index.html 로컬 파일을 Drive에 업로드한 후
//    initHubHtml() 함수를 수동 실행하면 GitHub에 초기 배포됨
// ============================================================
var HUB_HTML_INIT_FILE_NAME = 'hub_index.html';  // Drive에 이 이름으로 올려둘 것

function initHubHtml() {
  var folder = DriveApp.getFolderById(OUTPUT_FOLDER_ID);
  var files = folder.getFilesByName(HUB_HTML_INIT_FILE_NAME);
  if (!files.hasNext()) throw new Error(HUB_HTML_INIT_FILE_NAME + ' 파일 없음');
  var htmlFile = files.next();
  var htmlContent = htmlFile.getBlob().getDataAsString('UTF-8');
  var b64 = Utilities.base64Encode(htmlContent, Utilities.Charset.UTF_8);
  _ghPutFile_('index.html', b64, 'Init HUB index.html');
  logProgress_('initHubHtml', '✓ index.html 초기 배포 완료');
}

// ============================================================
// 8. 토큰 검증 유틸
// ============================================================
function verifyHubToken() {
  if (!HUB_GITHUB_TOKEN) return '토큰 미설정';
  var res = _ghApi_('GET', '', null);
  if (res.code === 200) {
    var j = res.json();
    return '✓ 레포 접근 OK: ' + j.full_name + ' (private=' + j.private + ')';
  }
  return '✗ ' + res.code + ': ' + res.body.substring(0, 200);
}
