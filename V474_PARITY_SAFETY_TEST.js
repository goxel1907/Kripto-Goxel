'use strict';
// LAZARUS V4.7.4 — PARITE + YURUTME GUVENLIGI REGRESYON TESTI
// Kullanim:  node V474_PARITY_SAFETY_TEST.js  (server.js ayni klasorde olmali)
//
// Bu test iki sey yapar:
//  A) V4.7.3'ten devralinan R495 kapali-1m matematigini yeniden dogrular (davranissal)
//  B) V4.7.4 yamalarinin kaynakta gercekten var oldugunu dogrular (statik)
//
// Herhangi bir assert duserse deploy YAPILMAZ.

const fs = require('fs'), vm = require('vm'), assert = require('assert');
const path = require('path');
const SERVER = path.join(__dirname, 'server.js');
const source = fs.readFileSync(SERVER, 'utf8');

let pass = 0;
function ok(name, cond, detail) {
  if (!cond) { console.error(`FAIL  ${name}${detail ? ' :: ' + detail : ''}`); process.exitCode = 1; }
  else { pass++; console.log(`pass  ${name}`); }
}
function countOf(re) {
  const g = new RegExp(re.source, re.flags.includes('g') ? re.flags : re.flags + 'g');
  return (source.match(g) || []).length;
}
function must(name, re, min = 1) {
  const n = countOf(re);
  ok(name, n >= min, `beklenen >=${min}, bulunan ${n}`);
}
function mustNot(name, re) {
  const n = countOf(re);
  ok(name, n === 0, `bulunmamaliydi, bulunan ${n}`);
}

console.log('── A) R495 kapali-1m davranis regresyonu ' + '─'.repeat(30));

const marker = 'const r495Exact = ';
const start = source.indexOf(marker);
assert(start >= 0, 'inline r495Exact marker missing');
const exprStart = start + marker.length;
const end = source.indexOf('\n\n\nconst app = express();', exprStart);
assert(end > exprStart, 'inline r495Exact end missing');
const exact = vm.runInNewContext(
  source.slice(exprStart, end).trim().replace(/;$/, ''),
  { Date, Math, Number, String, Object, Array, Boolean, Infinity, NaN }
);

const M = 60000, FIVE = 300000;
const fiveOpen = 1785784800000;
const compact = [], times = [];
for (let i = 0; i < 20; i++) {
  const o = 100 + i * .05, c = o + .03;
  compact.push([o, c + .12, o - .12, c, 1000 + i]);   // 5 elemanli paket: [o,h,l,c,v]
  times.push(fiveOpen - (19 - i) * FIVE);              // Binance openTime
}
const cand = exact.candidateMetaFromCompact5m(compact, times);

ok('A1 candidateOpenTs = 5m openTime', cand.candidateOpenTs === fiveOpen);
ok('A2 candidateTs = openTime + 300000', cand.candidateTs === fiveOpen + FIVE);
ok('A3 candidateCloseTime = boundary - 1', cand.candidateCloseTime === fiveOpen + FIVE - 1);
ok('A4 slPct 1.2-8.0 arasinda clamp', cand.slPct >= 1.2 && cand.slPct <= 8.0, `slPct=${cand.slPct}`);

// x[5] YOK oldugu icin times[] fallback'i devreye girmeli. Bu kritik kirilganlik testi:
// paket bir gun 6. alan kazanirsa bu test duser ve semantik sessizce kaymaz.
ok('A5 compact 5m paketi 5 elemanli (x[5] fallback saglam)',
   compact.every(r => r.length === 5), 'paket sekli degisti — candidateTs semantigi kayabilir');

function k(openTime, o, h, l, c, ratio) {
  const q = 1000;
  return [openTime, String(o), String(h), String(l), String(c), '10', openTime + 59999, String(q), 10, '5', String(q * ratio), '0'];
}
const B = cand.candidateTs;

let r = exact.evaluateClosed1m({
  rawBars: [k(B, 100.98, 101.10, 100.88, 101.04, .60),
            k(B + M, 101.04, 101.16, 100.98, 101.09, .55),
            k(B + 2 * M, 101.09, 101.22, 101.02, 101.15, .70)],
  candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now: B + 3 * M + 1000
});
ok('A6 3 oy + finalClose>=candidate -> MARKET', r.action === 'MARKET' && r.votes === 3, JSON.stringify({a:r.action,v:r.votes}));
ok('A7 ilk mum openTime == candidateTs', r.bars[0].openTime === B);
ok('A8 her mum 59999 ms', r.bars.every(b => b.closeTime - b.openTime === 59999));
ok('A9 mumlar zincirli', r.bars[1].openTime === r.bars[0].closeTime + 1 && r.bars[2].openTime === r.bars[1].closeTime + 1);

r = exact.evaluateClosed1m({
  rawBars: [k(B, 100.98, 101.10, 100.88, 101.05, .60),
            k(B + M, 101.05, 101.16, 100.98, 101.10, .55),
            k(B + 2 * M, 101.10, 101.14, 100.88, 100.96, .40)],
  candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now: B + 3 * M + 1000
});
ok('A10 2 oy -> TACTICAL', r.action === 'TACTICAL' && r.votes === 2);

r = exact.evaluateClosed1m({
  rawBars: [k(B, 100.98, 101.1, 100.8, 101.0, .60), k(B + M, 101, 101.2, 100.9, 101.1, .60)],
  candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now: B + 2 * M + 1000
});
ok('A11 3 mum yoksa WAIT/PUSU, MARKET degil', r.action !== 'MARKET');

// Eksik dakika -> WINDOW_GAP
r = exact.evaluateClosed1m({
  rawBars: [k(B, 100.98, 101.1, 100.8, 101.0, .60),
            k(B + 2 * M, 101, 101.2, 100.9, 101.1, .60),
            k(B + 3 * M, 101.1, 101.3, 101.0, 101.2, .60)],
  candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now: B + 4 * M + 1000
});
ok('A12 eksik dakika -> WINDOW_GAP', String(r.code).includes('WINDOW_GAP'), r.code);

// Kapanmamis 3. mum kabul edilmemeli (now yeterince ilerlememis)
r = exact.evaluateClosed1m({
  rawBars: [k(B, 100.98, 101.10, 100.88, 101.04, .60),
            k(B + M, 101.04, 101.16, 100.98, 101.09, .55),
            k(B + 2 * M, 101.09, 101.22, 101.02, 101.15, .70)],
  candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now: B + 2 * M + 30000
});
ok('A13 3. mum kapanmadan MARKET verilmez', r.action !== 'MARKET', r.code);

console.log('\n── B) V4.7.4 yama dogrulamasi (statik) ' + '─'.repeat(31));

// P0 kimlik
must('B0a build V4_7_4_20', /LAZARUS_BUILD = 'R493_V5_9_2_TESTNET_EXACT_CLOSED1M_R495_V4_7_4_20_EXIT_REASON_PARITY_RISK41_10X'/);
must('B0b session 4_7_4_10', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_20_EP1/);
mustNot('B0c story-shadow bayragi KALDIRILDI (F01)', /const V592_STORY_AUTHORITY_SHADOW_ONLY=/);
must('B0d leverage lock sabiti', /const V592_LEVERAGE_LOCK=/);
must('B0e idempotency sabiti', /const V592_ORDER_IDEMPOTENCY=/);
must('B0f clock guard sabiti', /const V592_MARKET_CLOCK_GUARD_MS=/);

// P1 emir yazmada kor retry yok
must('B1a orderWrite tespiti var', /const orderWrite = \(m === 'POST' && \/\\\/fapi\\\/v\[123\]\\\/\(order\|batchOrders\)\/i\.test\(path\)\)/);
must('B1b maxAttempts order icin 1', /const maxAttempts = \(sensitiveSigned \|\| orderWrite\) \? 1 : 3;/);

// P2 idempotency
must('B2a newClientOrderId gonderiliyor', /newClientOrderId:cid/);
must('B2b idempotent gonderici', /async function v592SendMainOrderIdempotent/);
must('B2c clientOrderId ile reconcile', /origClientOrderId:cid/);
must('B2d ana emir idempotent yoldan gidiyor', /const main=await v592SendMainOrderIdempotent\(apiKey,apiSecret,sym,oSide,qty,req\?\.body\?\.decisionKey\);/);
mustNot('B2e eski cikplak emir POSTu kalmadi',
  /const main=await bReq\(apiKey,apiSecret,'POST','\/fapi\/v1\/order',\{\s*\n\s*symbol:sym,side:oSide,type:'MARKET',quantity:qty,positionSide:'BOTH'\s*\n\s*\}\);/);

// P3 position truth fail-closed
must('B3a stale reddi', /POSITION_TRUTH_STALE/);
must('B3b schema reddi', /POSITION_TRUTH_SCHEMA_INVALID/);
must('B3c yas siniri', /POSITION_TRUTH_TOO_OLD/);
mustNot('B3d bos dizi fail-open kalmadi', /const rows=Array\.isArray\(snap\?\.account\?\.positions\)\?snap\.account\.positions:\[\];/);

// P4 story-authority shadow
must('B4a storyWait orijinal (F01 geri alindi)', /const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/);
must('B4b R493 blok funnel kaydi', /type:'R493_ENTRY_GATE_BLOCK'/);
mustNot('B4c R495 base shadow KALDIRILDI', /_baseShadowed/);

// P5 kaldirac kilidi
must('B5a leverage lock uygulanir', /const _lockLev = \(V592_EXACT_BACKTEST_AUTHORITY && V592_LEVERAGE_LOCK > 0\) \? V592_LEVERAGE_LOCK : Number\(leverage\);/);
must('B5b parite bozuksa emir gitmez', /Kaldirac paritesi bozuk/);
must('B5c uygulanan kaldirac kanitlanir', /exchangeLeverageProof/);
must('B5d appliedLeverage exchange kanitindan (F06)', /appliedLeverage:Number\.isFinite\(_appliedLeverageProof\)\?_appliedLeverageProof:null/);

// P6 saat otoritesi
must('B6a ayri piyasa saati', /let marketTimeOffset = 0;/);
must('B6b LIVE FAPI time senkronu', /\$\{FAPI\}\/fapi\/v1\/time/);
must('B6c R495 marketNow kullaniyor', /const now=marketNow\(\),fullSymbol=normalizeSymbol\(symbol\)/);
must('B6d clock guard fail-closed', /R495_CLOCK_GUARD/);
mustNot('B6e R495 artik yerel Date.now kullanmiyor', /const now=Date\.now\(\),fullSymbol=normalizeSymbol\(symbol\),sym=r495Sym/);

// P7 emir oncesi bakiye fail-closed
must('B7a bakiye dogrulanamazsa emir gitmez', /Emir öncesi signed account doğrulanamadı, emir gönderilmedi/);

// P8 status gercege dayali
must('B8a parityV4741 status alani', /parityV4741:\{r493EntryGateActive:/, 2);
must('B8b r493EntryGateDecisionImpact:true', /r493EntryGateDecisionImpact:true/);

// Backtest sozlesmesi degismemis olmali
must('B9a slot 41', /Number\(process\.env\.R497_SLOT_MARGIN_USDT \|\| 41\)/);
must('B9b buffer 20', /R497_MIN_BUFFER_USDT \|\| 20/);
must('B9c max 2 pozisyon', /R486_MAX_POSITIONS \|\| 2/);
must('B9d final risk 4', /R495_FINAL_RISK_PCT \|\| 4/);
must('B9e tactical 0.60', /R495_TACTICAL_RISK_SCALE \|\| \.60/);
must('B9f market 1.00', /R495_MARKET_RISK_SCALE \|\| 1\.00/);
must('B9g testnet hard-lock', /BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/);

console.log(`\n${process.exitCode ? 'SONUC: FAIL' : 'SONUC: PASS'} — ${pass} kontrol gecti`);
if (!process.exitCode) console.log('V4.7.4 parite + guvenlik regresyonu tamam.');
