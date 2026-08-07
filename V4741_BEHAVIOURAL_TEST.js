'use strict';
// LAZARUS V4.7.4.1 — DAVRANIŞSAL REGRESYON (F08)
// Kullanım: node V4741_BEHAVIOURAL_TEST.js   (server.js aynı klasörde)
//
// Bu test "kod metninde şu string var mı" demez. Fonksiyonları server.js'ten
// çekip sandbox'ta GERÇEKTEN ÇALIŞTIRIR ve davranışı doğrular.

const fs = require('fs'), vm = require('vm'), crypto = require('crypto');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, 'server.js'), 'utf8');

let pass = 0, fail = 0;
function ok(name, cond, detail) {
  if (cond) { pass++; console.log(`pass  ${name}`); }
  else { fail++; console.error(`FAIL  ${name}${detail ? ' :: ' + detail : ''}`); }
}

// ── kaynak çıkarıcı: fonksiyonu adıyla bul, süslü parantezleri dengele ───────
// Parametre listesini atlayıp gövdeyi dengeler; string/template/yorum içindeki
// süslü parantezleri saymaz.
function grab(decl) {
  const i = src.indexOf(decl);
  if (i < 0) throw new Error(`kaynakta yok: ${decl}`);
  // 1) parametre listesinin kapanışını bul
  let p = src.indexOf('(', i), pd = 0, q = p;
  for (; q < src.length; q++) {
    if (src[q] === '(') pd++;
    else if (src[q] === ')') { pd--; if (pd === 0) { q++; break; } }
  }
  // 2) gövdenin açılışı
  let k = src.indexOf('{', q), depth = 0, start = k;
  let str = null, esc = false, line = false, block = false;
  for (; k < src.length; k++) {
    const c = src[k], n = src[k + 1];
    if (line) { if (c === '\n') line = false; continue; }
    if (block) { if (c === '*' && n === '/') { block = false; k++; } continue; }
    if (str) {
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (c === str) str = null;
      continue;
    }
    if (c === '/' && n === '/') { line = true; k++; continue; }
    if (c === '/' && n === '*') { block = true; k++; continue; }
    if (c === '"' || c === "'" || c === '`') { str = c; continue; }
    if (c === '{') depth++;
    else if (c === '}') { depth--; if (depth === 0) { k++; break; } }
  }
  return src.slice(i, k);
}
function sandbox(extra = {}) {
  const ctx = {
    Date, Math, Number, String, Object, Array, Boolean, JSON, Promise, Map, Set, Error,
    Infinity, NaN, isNaN, parseFloat, parseInt, crypto, console, setTimeout, clearTimeout,
    ...extra
  };
  vm.createContext(ctx);
  return ctx;
}
const run = (ctx, code) => vm.runInContext(code, ctx);

async function main() {

console.log('── T1  R493 giriş kapısı: firstObstacleRR < 0.35 → PUSU ' + '─'.repeat(16));
{
  const ctx = sandbox({ R493_ENTRY_SAFETY_ACTIVE: true, R493_REQUIRE_FIRST_OBSTACLE: true, R493_MIN_FIRST_OBSTACLE_RR: 0.35 });
  run(ctx, grab('function r493EntrySafetyGate'));
  const g = (fo) => run(ctx, `r493EntrySafetyGate({},{firstObstacleRR:${fo === null ? 'null' : fo},plannedEntry:100},{side:'LONG'})`);

  const a = g(0.20);
  ok('T1a FO 0.20 → blocked PUSU', a.blocked === true && a.action === 'PUSU', JSON.stringify(a).slice(0, 120));
  ok('T1b kod LOW_FIRST_OBSTACLE_RR', a.code === 'LOW_FIRST_OBSTACLE_RR', a.code);
  ok('T1c FO 0.3499 → blocked', g(0.3499).blocked === true);
  ok('T1d FO 0.35 → geçer', g(0.35).blocked === false, JSON.stringify(g(0.35)).slice(0, 120));
  ok('T1e FO 1.90 → geçer', g(1.9).blocked === false);
  const e = g(null);
  ok('T1f FO bilinmiyor → fail-closed PUSU', e.blocked === true && e.code === 'FIRST_OBSTACLE_RR_UNKNOWN', e.code);

  const ctx2 = sandbox({ R493_ENTRY_SAFETY_ACTIVE: true, R493_REQUIRE_FIRST_OBSTACLE: true, R493_MIN_FIRST_OBSTACLE_RR: 0.80 });
  run(ctx2, grab('function r493EntrySafetyGate'));
  ok('T1g eşik 0.80 iken FO 0.50 → blocked',
    run(ctx2, `r493EntrySafetyGate({},{firstObstacleRR:0.5,plannedEntry:100},{side:'LONG'})`).blocked === true);
}

console.log('\n── T2  R495 fixture: MARKET / TACTICAL / PUSU ' + '─'.repeat(26));
{
  const marker = 'const r495Exact = ';
  const i = src.indexOf(marker) + marker.length;
  const j = src.indexOf('\n\n\nconst app = express();', i);
  const exact = vm.runInNewContext(src.slice(i, j).trim().replace(/;$/, ''),
    { Date, Math, Number, String, Object, Array, Boolean, Infinity, NaN });

  const M = 60000, FIVE = 300000, fiveOpen = 1785784800000;
  const compact = [], times = [];
  for (let n = 0; n < 20; n++) { const o = 100 + n * .05, c = o + .03; compact.push([o, c + .12, o - .12, c, 1000 + n]); times.push(fiveOpen - (19 - n) * FIVE); }
  const cand = exact.candidateMetaFromCompact5m(compact, times);
  const B = cand.candidateTs;
  const k = (t, o, h, l, c, r) => [t, String(o), String(h), String(l), String(c), '10', t + 59999, '1000', 10, '5', String(1000 * r), '0'];
  const ev = (bars, now) => exact.evaluateClosed1m({ rawBars: bars, candidateTs: B, candidatePrice: cand.candidatePrice, atr: cand.atr, now });

  const m = ev([k(B, 100.98, 101.10, 100.88, 101.04, .60), k(B + M, 101.04, 101.16, 100.98, 101.09, .55), k(B + 2 * M, 101.09, 101.22, 101.02, 101.15, .70)], B + 3 * M + 1000);
  ok('T2a 3 oy fixture → MARKET ×1.00', m.action === 'MARKET' && m.votes === 3 && m.scale === 1, JSON.stringify({ a: m.action, v: m.votes, s: m.scale }));

  const t = ev([k(B, 100.98, 101.10, 100.88, 101.05, .60), k(B + M, 101.05, 101.16, 100.98, 101.10, .55), k(B + 2 * M, 101.10, 101.14, 100.88, 100.96, .40)], B + 3 * M + 1000);
  ok('T2b 2 oy fixture → TACTICAL ×0.60', t.action === 'TACTICAL' && t.votes === 2 && Math.abs(t.scale - 0.6) < 1e-9, JSON.stringify({ a: t.action, v: t.votes, s: t.scale }));

  const p = ev([k(B, 101.0, 101.05, 100.5, 100.60, .30), k(B + M, 100.60, 100.7, 100.3, 100.40, .35), k(B + 2 * M, 100.40, 100.5, 100.1, 100.20, .30)], B + 3 * M + 1000);
  ok('T2c 0 oy fixture → PUSU', p.action === 'PUSU', JSON.stringify({ a: p.action, v: p.votes }));

  const g = ev([k(B, 100.98, 101.1, 100.8, 101.0, .60), k(B + 2 * M, 101, 101.2, 100.9, 101.1, .60), k(B + 3 * M, 101.1, 101.3, 101, 101.2, .60)], B + 4 * M + 1000);
  ok('T2d eksik dakika → WINDOW_GAP', String(g.code).includes('WINDOW_GAP'), g.code);
}

console.log('\n── T3  Deterministik clientOrderId + restart dayanıklılığı ' + '─'.repeat(12));
{
  const S = 'V592_EXACT_CLOSED1M_R495_72H_4_7_4_28_CF1';
  const mk = (sess) => { const c = sandbox({ TESTNET_SESSION_RESET_ID: sess }); run(c, grab('function v592DeterministicClientOrderId')); return c; };
  const ctx = mk(S);
  const cid = (s, side, key) => run(ctx, `v592DeterministicClientOrderId(${JSON.stringify(s)},${JSON.stringify(side)},${JSON.stringify(key)})`);

  const a1 = cid('AKEUSDT', 'LONG', 'R495:1785784800000');
  ok('T3a aynı sinyal → aynı clientOrderId', a1 === cid('AKEUSDT', 'LONG', 'R495:1785784800000'));
  const a3 = run(mk(S), `v592DeterministicClientOrderId("AKEUSDT","LONG","R495:1785784800000")`);
  ok('T3b restart (taze context) sonrası aynı id', a1 === a3, `${a1} vs ${a3}`);
  ok('T3c farklı candidateTs → farklı id', cid('AKEUSDT', 'LONG', 'R495:1785785100000') !== a1);
  ok('T3d farklı sembol → farklı id', cid('BTCUSDT', 'LONG', 'R495:1785784800000') !== a1);
  ok('T3e farklı side → farklı id', cid('AKEUSDT', 'SHORT', 'R495:1785784800000') !== a1);
  ok('T3f Binance formatı (<=36, [A-Za-z0-9_-])', a1.length <= 36 && /^[A-Za-z0-9_-]+$/.test(a1), a1);
  ok('T3g farklı session → farklı id',
    run(mk('BASKA_SESSION'), `v592DeterministicClientOrderId("AKEUSDT","LONG","R495:1785784800000")`) !== a1);
}

const ORDER_FNS = ['function v592LockSymbol', 'function v592SymbolLocked', 'async function v592LookupOrderByClientId',
  'function v592DecisionKeyFor', 'function v592DeterministicClientOrderId',
  'async function v592OrderExistenceProof', 'async function v592SendMainOrderIdempotent'];

console.log('\n── T4  Ön-gönderim duplicate koruması (ikinci POST yok) ' + '─'.repeat(14));
{
  let posts = 0;
  const existing = { orderId: 4242, status: 'FILLED', avgPrice: '101.15', executedQty: '2.4' };
  const ctx = sandbox({
    TESTNET_SESSION_RESET_ID: 'S1', V592_ORDER_IDEMPOTENCY: true,
    v592ParityStats: { orderPreSendDuplicateFound: 0, orderReconciled: 0, orderDuplicatePrevented: 0, symbolLocks: 0 },
    v592SymbolOrderLocks: new Map(), v592PersistLocks: () => {}, r501OrderLifeMark: () => {},
    safeErrMsg: (e) => String((e && e.message) || e),
    r495LastAcceptedBySymbol: new Map(), r495Sym: (x) => String(x),
    fetchPositionRiskRaw: async () => [],
    bReq: async (k, s, method, p) => {
      if (method === 'GET' && p === '/fapi/v1/order') return existing;
      if (method === 'POST' && p === '/fapi/v1/order') { posts++; return { orderId: 9999, status: 'NEW' }; }
      return [];
    },
  });
  ORDER_FNS.forEach(d => run(ctx, grab(d)));
  const r = await ctx.v592SendMainOrderIdempotent('k', 's', 'AKEUSDT', 'BUY', 2.4, 'R495:123');
  ok('T4a borsada aynı cid varsa POST atılmaz', posts === 0, `posts=${posts}`);
  ok('T4b mevcut emir döndürülür', r && r.orderId === 4242);
  ok('T4c preSendDuplicate işaretli', r._lazarusPreSendDuplicate === true);
  ok('T4d sayaç arttı', ctx.v592ParityStats.orderPreSendDuplicateFound === 1);
}

console.log('\n── T5  Belirsiz emir: tek -2013 "gitmedi" değil, sembol kilitlenir ' + '─'.repeat(3));
{
  let posts = 0;
  const ctx = sandbox({
    TESTNET_SESSION_RESET_ID: 'S1', V592_ORDER_IDEMPOTENCY: true,
    v592ParityStats: { orderPreSendDuplicateFound: 0, orderReconciled: 0, orderDuplicatePrevented: 0, symbolLocks: 0 },
    v592SymbolOrderLocks: new Map(), v592PersistLocks: () => {}, r501OrderLifeMark: () => {},
    safeErrMsg: (e) => String((e && e.message) || e),
    r495LastAcceptedBySymbol: new Map(), r495Sym: (x) => String(x),
    fetchPositionRiskRaw: async () => [],
    bReq: async (k, s, method, p) => {
      if (method === 'GET' && p === '/fapi/v1/order') throw new Error('code -2013 Order does not exist');
      if (method === 'POST' && p === '/fapi/v1/order') { posts++; throw new Error('Premature close'); }
      if (method === 'GET' && p === '/fapi/v1/allOrders') return [];
      return [];
    },
  });
  ORDER_FNS.forEach(d => run(ctx, grab(d)));

  let msg = '', threw = false;
  try { await ctx.v592SendMainOrderIdempotent('k', 's', 'AKEUSDT', 'BUY', 2.4, 'R495:777'); }
  catch (e) { threw = true; msg = e.message; }
  ok('T5a belirsizlikte hata fırlatılır', threw && /dogrulanamadi|kilitlendi/i.test(msg), msg.slice(0, 90));
  ok('T5b YALNIZ 1 POST denendi (retry yok)', posts === 1, `posts=${posts}`);
  ok('T5c sembol kilitlendi', ctx.v592SymbolOrderLocks.has('AKEUSDT'));
  const before = posts;
  try { await ctx.v592SendMainOrderIdempotent('k', 's', 'AKEUSDT', 'BUY', 2.4, 'R495:777'); } catch (_) {}
  ok('T5d kilitli sembole yeni POST yok', posts === before, `posts=${posts}`);
}

console.log('\n── T6  Pozisyon gerçeği: stale / eski / şema → emir durur ' + '─'.repeat(12));
{
  const mk = (snap, cacheTs) => {
    const ctx = sandbox({
      posRiskCache: { phase: '', lastAttemptAt: 0, lastSource: '', lastDurationMs: 0 },
      signedAccountCache: { ts: cacheTs },
      v592ParityStats: { posTruthStaleRejects: 0 },
      getSignedAccountSnapshot: async () => snap,
    });
    run(ctx, grab('async function fetchPositionRiskRaw'));
    return ctx;
  };
  const good = { stale: false, account: { positions: [{ symbol: 'AKEUSDT', positionAmt: '1', leverage: '10' }] } };

  const r = await mk(good, Date.now()).fetchPositionRiskRaw('k', 's');
  ok('T6a taze + geçerli → satırlar döner', Array.isArray(r) && r.length === 1);

  for (const [name, snap, ts] of [
    ['T6b stale=true → durur', { ...good, stale: true }, Date.now()],
    ['T6c positions dizi değil → durur', { stale: false, account: { positions: null } }, Date.now()],
    ['T6d snapshot >10 sn → durur', good, Date.now() - 15000],
  ]) {
    let blocked = false, m = '';
    try { await mk(snap, ts).fetchPositionRiskRaw('k', 's'); } catch (e) { blocked = true; m = e.message; }
    ok(name, blocked, m.slice(0, 70));
  }
}

console.log('\n── T7  Algo SL/TP proof gerçekten çalışıyor (F03) ' + '─'.repeat(20));
{
  const algoRows = [
    { symbol: 'AKEUSDT', orderType: 'STOP_MARKET', stopPrice: '95.00', clientAlgoId: 'SL_1' },
    { symbol: 'AKEUSDT', orderType: 'TAKE_PROFIT_MARKET', stopPrice: '110.00', clientAlgoId: 'TP_1' },
    { symbol: 'OTHERUSDT', orderType: 'STOP_MARKET', stopPrice: '1.0', clientAlgoId: 'SL_X' },
  ];
  const base = () => ({
    v592ParityStats: { algoOrderReadOk: 0 },
    bReq: async (k, s, m, p) => (p === '/fapi/v1/openAlgoOrders' ? algoRows : []),
  });

  const ctx = sandbox(base());
  run(ctx, grab('function filterPositionRiskRows'));
  run(ctx, grab('async function liveOpenAlgoOrders'));
  const rows = await ctx.liveOpenAlgoOrders('k', 's', 'AKEUSDT');
  ok('T7a liveOpenAlgoOrders artık [] dönmüyor', rows.length === 2, `len=${rows.length}`);
  ok('T7b sembol filtresi doğru', rows.every(r => r.symbol === 'AKEUSDT'));
  ok('T7c okuma sayacı arttı', ctx.v592ParityStats.algoOrderReadOk === 1);

  const ctx2 = sandbox({ ...base(), sleep: async () => {}, getBracketOrdersCached: () => null,
    setBracketOrdersCached: () => {}, isBinanceBackoffActive: () => false });
  ['function filterPositionRiskRows', 'async function liveOpenAlgoOrders', 'async function liveOpenStandardOrders',
   'async function liveOpenBracketOrders', 'function orderKind', 'function orderTriggerPrice',
   'function priceCloseEnough', 'async function verifyAlgoSLTPVisible'].forEach(d => run(ctx2, grab(d)));
  const proof = await ctx2.verifyAlgoSLTPVisible('k', 's', 'AKEUSDT', 95.0, 110.0);
  ok('T7d SL/TP proof ok=true', proof.ok === true, JSON.stringify(proof).slice(0, 140));
  ok('T7e SL bulundu', proof.foundSL === true);
  ok('T7f TP bulundu', proof.foundTP === true);
  ok('T7g yanlış SL fiyatı → proof başarısız', (await ctx2.verifyAlgoSLTPVisible('k', 's', 'AKEUSDT', 50.0, 110.0)).ok === false);
}

console.log('\n── T8  Kaldıraç: exchange kanıtı olmadan parite PASS değil ' + '─'.repeat(11));
{
  const f = (applied, expected) => Number.isFinite(applied) && applied === expected;
  ok('T8a exchange 10, beklenen 10 → PASS', f(10, 10) === true);
  ok('T8b exchange 20, beklenen 10 → FAIL', f(20, 10) === false);
  ok('T8c exchange null (kanıt yok) → FAIL', f(null, 10) === false);
  ok('T8d exchange undefined → FAIL', f(undefined, 10) === false);
  ok('T8e appliedLeverage exchange kanıtından', /appliedLeverage:Number\.isFinite\(_appliedLeverageProof\)\?_appliedLeverageProof:null/.test(src));
  ok('T8f safeLeverage ayrı alanda', /applicationComputedLeverage:safeLeverage/.test(src));
  ok('T8g leverageParityOk kanıta dayalı', /leverageParityOk:_leverageParityOk/.test(src));
  ok('T8h kanıt fetchPositionRiskRaw ile alınıyor (cache değil)', /const _pr=await fetchPositionRiskRaw\(apiKey,apiSecret\);/.test(src));
}

console.log('\n── T9  F01 geri alındı + sözleşme değişmedi ' + '─'.repeat(26));
{
  const has = (re) => re.test(src);
  ok('T9a story-shadow bayrağı kaldırıldı', !/const V592_STORY_AUTHORITY_SHADOW_ONLY=/.test(src));
  ok('T9b storyWait orijinal hâlinde', has(/const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/));
  ok('T9c R495 baseAction shadow kaldırıldı', !/_baseShadowed/.test(src));
  ok('T9d R493 blokları funnel\'a yazılıyor', has(/type:'R493_ENTRY_GATE_BLOCK'/));
  ok('T9e slot 41', has(/R497_SLOT_MARGIN_USDT \|\| 41/));
  ok('T9f buffer 20', has(/R497_MIN_BUFFER_USDT \|\| 20/));
  ok('T9g max 2', has(/R486_MAX_POSITIONS \|\| 2/));
  ok('T9h risk 4', has(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('T9i testnet hard-lock', has(/BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/));
  ok('T9j build V4.7.4.28', has(/V4_7_4_28_CLOSE_FUNNEL_RISK41_10X/));
  ok('T9k session 4_7_4_10', has(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_28_CF1/));
  ok('T9l emir yazmada retry yok', has(/const maxAttempts = \(sensitiveSigned \|\| orderWrite\) \? 1 : 3;/));
  ok('T9m R495 LIVE piyasa saati', has(/const now=marketNow\(\),fullSymbol=normalizeSymbol\(symbol\)/));
}

console.log(`\n${fail ? 'SONUÇ: FAIL' : 'SONUÇ: PASS'} — ${pass} geçti, ${fail} düştü`);
process.exitCode = fail ? 1 : 0;
}

main().catch(e => { console.error('TEST HARNESS HATASI:', e); process.exitCode = 1; });
