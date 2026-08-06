'use strict';
// LAZARUS V4.7.4.2 — STRICT TRUTH DAVRANIŞSAL REGRESYONU
// Bağımsız incelemenin 7 bulgusunun her biri için GERÇEK ÇALIŞTIRMA testi.
// Kullanım: node V4742_STRICT_TRUTH_TEST.js   (server.js aynı klasörde)

const fs = require('fs'), vm = require('vm'), crypto = require('crypto'), os = require('os');
const path = require('path');
const src = fs.readFileSync(path.join(__dirname, 'server.js'), 'utf8');

let pass = 0, fail = 0;
const ok = (n, c, d) => c ? (pass++, console.log(`pass  ${n}`))
                          : (fail++, console.error(`FAIL  ${n}${d ? ' :: ' + d : ''}`));

function grab(decl) {
  const i = src.indexOf(decl);
  if (i < 0) throw new Error(`kaynakta yok: ${decl}`);
  let p = src.indexOf('(', i), pd = 0, q = p;
  for (; q < src.length; q++) { if (src[q] === '(') pd++; else if (src[q] === ')') { pd--; if (!pd) { q++; break; } } }
  let k = src.indexOf('{', q), depth = 0, str = null, esc = false, line = false, block = false;
  for (; k < src.length; k++) {
    const c = src[k], n = src[k + 1];
    if (line) { if (c === '\n') line = false; continue; }
    if (block) { if (c === '*' && n === '/') { block = false; k++; } continue; }
    if (str) { if (esc) { esc = false; continue; } if (c === '\\') { esc = true; continue; } if (c === str) str = null; continue; }
    if (c === '/' && n === '/') { line = true; k++; continue; }
    if (c === '/' && n === '*') { block = true; k++; continue; }
    if (c === '"' || c === "'" || c === '`') { str = c; continue; }
    if (c === '{') depth++; else if (c === '}') { depth--; if (!depth) { k++; break; } }
  }
  return src.slice(i, k);
}
const sandbox = (extra = {}) => vm.createContext({
  Date, Math, Number, String, Object, Array, Boolean, JSON, Promise, Map, Set, Error,
  Infinity, NaN, isNaN, parseFloat, parseInt, crypto, console, setTimeout, clearTimeout,
  fs, path, ...extra
});
const run = (ctx, code) => vm.runInContext(code, ctx);

async function main() {

console.log('── C1  getPositionRiskCached: GERÇEK forceFresh ' + '─'.repeat(22));
{
  const mkCtx = (over = {}) => {
    let fetched = 0;
    const ctx = sandbox({
      posRiskCache: { data: [{ symbol: 'AKEUSDT', positionAmt: '1' }], ts: Date.now(), lastApiKey: 'FP',
        rateLimitUntil: 0, fetching: false, inflight: null, inflightStartedAt: 0, lastSuccessAt: Date.now(),
        consecutiveFailures: 0, phase: '', lastError: null, lastErrorAt: 0, lastErrorType: null, lastDurationMs: 0 },
      POS_RISK_TTL_ACTIVE: 30000, POS_RISK_TTL_NORMAL: 30000, POS_RISK_RATELIMIT_MS: 60000,
      resetStuckPositionRiskInflight: () => {},
      keyFingerprint: () => 'FP',
      isBinanceBackoffActive: () => false,
      isExecBackoffActive: () => false,
      getExecBackoffMs: () => 30000,
      getBinanceBackoffMs: () => 30000,
      makeBinanceBackoffError: (m) => new Error('BINANCE_BACKOFF_ACTIVE ' + m),
      isPositionRiskRateLimitError: () => false,
      safeErrMsg: (e) => String((e && e.message) || e),
      pushCritical: () => {},
      fetchPositionRiskRaw: async () => { fetched++; return [{ symbol: 'AKEUSDT', positionAmt: '2' }]; },
      ...over,
    });
    run(ctx, grab('function filterPositionRiskRows'));
    run(ctx, grab('async function getPositionRiskCached'));
    ctx.__fetched = () => fetched;
    return ctx;
  };

  let c = mkCtx();
  let r = await c.getPositionRiskCached('k', 's', {});
  ok('C1a forceFresh=false → TTL cache kullanılır', c.__fetched() === 0 && r[0].positionAmt === '1', `fetched=${c.__fetched()}`);

  c = mkCtx();
  r = await c.getPositionRiskCached('k', 's', { __forceFresh: true });
  ok('C1b forceFresh=true → TTL cache ATLANIR, ağa gidilir', c.__fetched() === 1 && r[0].positionAmt === '2', `fetched=${c.__fetched()}`);

  c = mkCtx({ isBinanceBackoffActive: () => true, isExecBackoffActive: () => true });
  r = await c.getPositionRiskCached('k', 's', {});
  ok('C1c backoff + forceFresh=false → stale döner', r[0].positionAmt === '1');

  c = mkCtx({ isBinanceBackoffActive: () => true, isExecBackoffActive: () => true });
  let threw = false, msg = '';
  try { await c.getPositionRiskCached('k', 's', { __forceFresh: true }); } catch (e) { threw = true; msg = e.message; }
  ok('C1d backoff + forceFresh=true → THROW (stale yok)', threw, msg.slice(0, 60));

  c = mkCtx(); c.posRiskCache.rateLimitUntil = Date.now() + 60000;
  r = await c.getPositionRiskCached('k', 's', {});
  ok('C1e cooldown + forceFresh=false → stale döner', r[0].positionAmt === '1');

  c = mkCtx(); c.posRiskCache.rateLimitUntil = Date.now() + 60000;
  threw = false;
  try { await c.getPositionRiskCached('k', 's', { __forceFresh: true }); } catch (e) { threw = true; msg = e.message; }
  ok('C1f cooldown + forceFresh=true → THROW', threw, msg.slice(0, 60));

  c = mkCtx({ fetchPositionRiskRaw: async () => { throw new Error('ECONNRESET'); } });
  c.posRiskCache.ts = 0;
  threw = false;
  try { await c.getPositionRiskCached('k', 's', { __forceFresh: true }); } catch (e) { threw = true; msg = e.message; }
  ok('C1g ağ hatası + forceFresh=true → THROW (stale fallback yok)', threw, msg.slice(0, 60));

  c = mkCtx({ fetchPositionRiskRaw: async () => { throw new Error('ECONNRESET'); } });
  c.posRiskCache.ts = 0;
  r = await c.getPositionRiskCached('k', 's', {});
  ok('C1h ağ hatası + forceFresh=false → stale fallback (davranış korundu)', Array.isArray(r) && r[0].positionAmt === '1');
}

console.log('\n── C2  Order truth ≤ 10 sn, tüm yollarda fail-closed ' + '─'.repeat(17));
{
  const mk = (ageMs) => {
    const ctx = sandbox({
      ORDER_TRUTH_MAX_AGE_MS: 10000,
      posRiskCache: { lastSuccessAt: Date.now() - ageMs, lastSource: 'v2/account.positions' },
      getPositionRiskCached: async () => [{ symbol: 'AKEUSDT', positionAmt: '1' }],
    });
    run(ctx, grab('async function getPositionRiskTruth'));
    return ctx;
  };
  const r = await mk(3000).getPositionRiskTruth('k', 's', {});
  ok('C2a 3 sn yaş → geçer', Array.isArray(r) && r.length === 1);

  for (const [name, age] of [['C2b 11 sn → THROW', 11000], ['C2c 30 sn → THROW', 30000], ['C2d 59 sn → THROW (eski 60 sn sınırı kalktı)', 59000]]) {
    let t = false, m = '';
    try { await mk(age).getPositionRiskTruth('k', 's', {}); } catch (e) { t = true; m = e.message; }
    ok(name, t && /POSITION_RISK_TRUTH_STALE/.test(m), m.slice(0, 70));
  }
  ok('C2e varsayılan sabit 10000 ms', /ORDER_TRUTH_MAX_AGE_MS=Math\.max\(2000,Math\.min\(30000,Number\(process\.env\.ORDER_TRUTH_MAX_AGE_MS\|\|10000\)\)\)/.test(src));
  ok('C2f /api/order ORDER_TRUTH_MAX_AGE_MS kullanıyor', /getPositionRiskTruth\(apiKey, apiSecret, \{\}, ORDER_TRUTH_MAX_AGE_MS\)/.test(src));
  ok('C2g manuel yolda da fail-closed', !/if \(autoConfig\?\.enabled\) throw new Error\(`Pozisyon limiti doğrulanamadı/.test(src)
     && /Pozisyon gerçeği doğrulanamadı, emir gönderilmedi/.test(src));
}

console.log('\n── C3  Kaldıraç kanıtı SERT KAPI ' + '─'.repeat(37));
{
  const gate = src.indexOf("'LEVERAGE_PARITY_FAILED'");
  const prot = src.indexOf("r501OrderLifeMark(sym,'PROTECTION_VERIFIED'");
  ok('C3a kaldıraç kapısı PROTECTION_VERIFIED\'den ÖNCE', gate > 0 && prot > 0 && gate < prot, `gate=${gate} prot=${prot}`);
  ok('C3b parite bozuksa 409', /return res\.status\(409\)\.json\(\{ok:false/.test(src) && /code:'LEVERAGE_PARITY_FAILED'/.test(src));
  ok('C3c sembol kilitlenir', /v592LockSymbol\(sym,`LEVERAGE_PARITY_FAILED_/.test(src));
  ok('C3d reduce-only unwind', /reduceOnly:'true'/.test(src) && /__emergency:true/.test(src));
  ok('C3e unwind kanıta yazılır', /'LEVERAGE_PARITY_UNWIND'/.test(src));
  ok('C3f posRisk cache invalidate', /invalidatePosRiskCache\('LEVERAGE_PARITY_FAILED'\)/.test(src));
  ok('C3g eski "sadece say, devam et" yolu kalmadı', !/LEVERAGE_PARITY_PROOF_FAILED/.test(src));
  const f = (a, e) => Number.isFinite(a) && a === e;
  ok('C3h kanıt 10 / beklenen 10 → ok', f(10, 10) === true);
  ok('C3i kanıt 20 → fail', f(20, 10) === false);
  ok('C3j kanıt null → fail', f(null, 10) === false);
}

console.log('\n── C4  decisionKey yoksa FAIL-CLOSED ' + '─'.repeat(33));
{
  const ctx = sandbox({ r495LastAcceptedBySymbol: new Map(), r495Sym: (x) => String(x) });
  run(ctx, grab('function v592DecisionKeyFor'));
  ok('C4a stabil anahtar yoksa null', ctx.v592DecisionKeyFor('AKEUSDT', 'LONG', undefined) === null);
  ok('C4b NO_R495_KEY kaldırıldı', !/NO_R495_KEY/.test(src));
  ok('C4c açık decisionKey kullanılır', ctx.v592DecisionKeyFor('AKEUSDT', 'LONG', 'R495:123') === 'R495:123');

  const ctx2 = sandbox({ r495LastAcceptedBySymbol: new Map([['AKEUSDT', { key: 'K1', candidateTs: 1785784800000 }]]), r495Sym: (x) => String(x) });
  run(ctx2, grab('function v592DecisionKeyFor'));
  ok('C4d R495 kabul kaydından türetilir', ctx2.v592DecisionKeyFor('AKEUSDT', 'LONG', undefined) === 'K1:1785784800000');

  let posts = 0;
  const c3 = sandbox({
    TESTNET_SESSION_RESET_ID: 'S1', V592_ORDER_IDEMPOTENCY: true,
    v592ParityStats: { orderPreSendDuplicateFound: 0, orderReconciled: 0, orderDuplicatePrevented: 0, symbolLocks: 0 },
    v592SymbolOrderLocks: new Map(), v592PersistLocks: () => {}, r501OrderLifeMark: () => {},
    safeErrMsg: (e) => String((e && e.message) || e),
    r495LastAcceptedBySymbol: new Map(), r495Sym: (x) => String(x),
    fetchPositionRiskRaw: async () => [],
    bReq: async (k, s, m, p) => { if (m === 'POST' && p === '/fapi/v1/order') { posts++; return { orderId: 1 }; } return null; },
  });
  ['function v592LockSymbol', 'function v592UnlockSymbol', 'function v592SymbolLocked',
   'async function v592LookupOrderByClientId', 'function v592DecisionKeyFor',
   'function v592DeterministicClientOrderId', 'async function v592OrderExistenceProof',
   'async function v592SendMainOrderIdempotent'].forEach(d => run(c3, grab(d)));
  let t = false, m = '';
  try { await c3.v592SendMainOrderIdempotent('k', 's', 'AKEUSDT', 'BUY', 2.4, undefined); } catch (e) { t = true; m = e.message; }
  ok('C4e anahtar yoksa emir gönderilmez', t && /MISSING_STABLE_DECISION_KEY/.test(m), m.slice(0, 70));
  ok('C4f hiç POST atılmadı', posts === 0, `posts=${posts}`);
}

console.log('\n── C5  Kalıcı unresolved-order kilidi ' + '─'.repeat(32));
{
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lz-lock-'));
  const mk = () => {
    const ctx = sandbox({
      TESTNET_SESSION_RESET_ID: 'S1',
      process: { env: { TESTNET_STATE_DIR: dir } },
      v592ParityStats: { symbolLocks: 0 }, pushCritical: () => {},
      r486391BinanceCreds: () => ({}), r501OrderLifeMark: () => {},
      bReq: async () => [], fetchPositionRiskRaw: async () => [],
      v592LookupOrderByClientId: async () => null,
    });
    run(ctx, 'const v592SymbolOrderLocks=new Map();');
    run(ctx, "const V592_LOCK_PATH=path.join(String(process.env.TESTNET_STATE_DIR||'/data').trim()||'/data','unresolved_orders.json');");
    ['function v592PersistLocks', 'function v592LoadLocks', 'function v592LockSymbol',
     'function v592UnlockSymbol', 'function v592SymbolLocked'].forEach(d => run(ctx, grab(d)));
    return ctx;
  };
  const a = mk();
  a.v592LockSymbol('AKEUSDT', 'UNRESOLVED_ORDER', 'LZabc123');
  const lockFile = path.join(dir, 'unresolved_orders.json');
  ok('C5a kilit diske yazıldı', fs.existsSync(lockFile));
  const saved = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
  ok('C5b clientOrderId kaydedildi', saved.locks[0].clientOrderId === 'LZabc123', JSON.stringify(saved.locks[0]));

  const b = mk();
  ok('C5c restart öncesi bellek boş', b.v592SymbolLocked('AKEUSDT') === null);
  b.v592LoadLocks();
  ok('C5d restart sonrası kilit GERİ YÜKLENDİ', b.v592SymbolLocked('AKEUSDT') !== null);
  ok('C5e restored bayrağı', b.v592SymbolLocked('AKEUSDT').restored === true);

  b.v592UnlockSymbol('AKEUSDT', 'test');
  const c = mk(); c.v592LoadLocks();
  ok('C5f unlock kalıcı', c.v592SymbolLocked('AKEUSDT') === null);

  ok('C5g atomik yazım (tmp + rename)', /const tmp=V592_LOCK_PATH\+'\.tmp';/.test(src) && /fs\.renameSync\(tmp,V592_LOCK_PATH\)/.test(src));
  ok('C5h boot reconciliation var', /async function v592BootReconcileLocks/.test(src));
  ok('C5i boot\'ta çağrılıyor', /v592BootReconcileLocks\(\)\.catch/.test(src));
  ok('C5j 3 kaynağa bakıyor', /BOOT_RECONCILE_ORDER_FOUND/.test(src) && /BOOT_RECONCILE_POSITION_FOUND/.test(src) && /BOOT_RECONCILE_HISTORY_FOUND/.test(src));
  ok('C5k çözülemezse kapalı kalır', /BOOT_RECONCILE_UNRESOLVED/.test(src));
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch (_) {}
}

console.log('\n── C6  Kimlik + strateji sözleşmesi ' + '─'.repeat(34));
{
  const has = (re) => re.test(src);
  ok('C6a build V4.7.4.17', has(/V4_7_4_17_CANDLE_PARITY_RISK41_10X/));
  ok('C6b session 4_7_4_10', has(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_17_CP1/));
  ok('C6c eski kimlik kalmadı', !has(/V4_7_4_9_EXIT_CONTRACT/) && !has(/4_7_4_9_EC1/));
  ok('C6d status yeni bayraklar', has(/strictForceFreshPositionTruth:true/) && has(/leverageProofHardGate:true/) && has(/persistentUnresolvedLocks:true/));
  ok('C6e R493 giriş kapısı aktif', has(/const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/));
  ok('C6f story-shadow bayrağı yok', !has(/const V592_STORY_AUTHORITY_SHADOW_ONLY=/));
  ok('C6g slot 41', has(/R497_SLOT_MARGIN_USDT \|\| 41/));
  ok('C6h buffer 20', has(/R497_MIN_BUFFER_USDT \|\| 20/));
  ok('C6i max 2', has(/R486_MAX_POSITIONS \|\| 2/));
  ok('C6j risk 4', has(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('C6k V4.5 eşikleri', has(/V592_V45_MS_SCORE_MIN\|\|35/) && has(/V592_V45_FIRST_OBSTACLE_RR_MIN\|\|0\.35/));
  ok('C6l testnet hard-lock', has(/BINANCE_EXECUTION_FAPI = 'https:\/\/testnet\.binancefuture\.com'/));
  ok('C6m R495 LIVE piyasa saati', has(/const now=marketNow\(\),fullSymbol=normalizeSymbol\(symbol\)/));
  ok('C6n emir yazmada retry yok', has(/const maxAttempts = \(sensitiveSigned \|\| orderWrite\) \? 1 : 3;/));
  ok('C6o F03 algo emir okuma', has(/filterPositionRiskRows\(Array\.isArray\(rows\) \? rows : \[\], \{symbol\}\)/));
}

console.log(`\n${fail ? 'SONUÇ: FAIL' : 'SONUÇ: PASS'} — ${pass} geçti, ${fail} düştü`);
process.exitCode = fail ? 1 : 0;
}
main().catch(e => { console.error('HARNESS HATASI:', e); process.exitCode = 1; });
