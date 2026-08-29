const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.join(__dirname, '..');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const panel = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const env = fs.readFileSync(path.join(root, 'CANLI.env'), 'utf8');

test('latest archive contract is fail-closed and visible at runtime', () => {
  assert.match(server, /V6_4_7_15M_HAFIZA_KARARA_GIRDI/);
  assert.match(server, /CANLI_KALDIRAC_7X_DEGIL/);
  assert.match(server, /CANLI_MAX_POZ_1_DEGIL/);
  assert.match(server, /CANLI_TEPE_VETO_KAPALI_DEGIL/);
  assert.match(server, /CANLI_ATR_TAVAN_8_DEGIL/);
  assert.match(server, /CANLI_KAR_TASIMA_KAPALI/);
  assert.match(server, /CANLI_MUTLAK_50_TABANI_BOZUK/);
  assert.match(server, /CANLI_RISK8_SL_UYUMU_KAPALI/);
  assert.match(server, /decisionContract:\{scanMode:'TOP24'/);
});

test('TOP10 core, ranks 11-24 and workers cannot be narrowed by stale panel storage', () => {
  assert.match(server, /const r54ScanMode = 'TOP24';/);
  assert.match(server, /const r54ScanLimit = 24;/);
  assert.match(server, /autoConfig\.scanMode = 'TOP24';/);
  assert.match(panel, /scanMode:\s+'TOP24'/);
  assert.match(panel, /set\('auto-scanmode',\s+'TOP24'\)/);
  assert.match(env, /^IZLEME_SLOT="24"$/m);
});

test('tiered scan keeps TOP10 deep, prefilters ranks 11-24 and promotes workers independently', () => {
  assert.match(server, /scanMode==='TOP24'\?24:R33_TOP_GAINER_LOCK_COUNT/);
  assert.match(server, /r54Bucket:'TOP24_RANK_11_24'/);
  assert.match(server, /function v640Top24LightPrefilter/);
  assert.match(server, /const top10Deep=ordered\.filter/);
  assert.match(server, /\.slice\(0,V640_TOP24_PROMOTE_MAX\)/);
  assert.match(server, /workerPolicy:'PRIORITY_DEEP_INDEPENDENT'/);
  assert.match(server, /extraBinanceRequests:0/);
  assert.match(env, /^V640_TOP24_PROMOTE_MAX="6"$/m);
  assert.match(env, /^V640_TOP24_MIN_PROOFS="3"$/m);
});

test('scan reset has one configured watchdog authority, not the old 75 second side gate', () => {
  assert.doesNotMatch(server, /age\s*>\s*75_000/);
  assert.match(server, /watchdogPending:age>V511_SCAN_WATCHDOG_MS/);
  assert.match(env, /^V511_SCAN_WATCHDOG_MS="360000"$/m);
  assert.match(env, /^V604_ANALYZE_TIMEOUT_MS="60000"$/m);
});

test('unresolved order locks need repeated fresh negative exchange proof before release', () => {
  assert.match(server, /V639_LOCK_NEGATIVE_CONFIRMATIONS/);
  assert.match(server, /FLAT_NO_OPEN_ENTRY_NO_ORDER_PROOF/);
  assert.match(server, /negativeProofs>=V639_LOCK_NEGATIVE_CONFIRMATIONS/);
  assert.match(server, /const openOrders=await bReq\([^\n]+\/fapi\/v1\/openOrders/);
  assert.match(server, /positionRows=await fetchPositionRiskRaw\(apiKey,apiSecret\)/);
  assert.match(server, /lock\.negativeProofs=0;v592PersistLocks\(\)/);
});

test('a terminal historical clientOrderId cannot masquerade as a new fill', () => {
  assert.match(server, /DUPLICATE_SIGNAL_ALREADY_RESOLVED/);
  assert.match(server, /MAIN_ORDER_CONFIRMED_NOT_FILLED/);
  assert.match(server, /proof\.resolvedNoFill/);
  assert.match(server, /duplicate POST gonderilmedi/);
});

test('deploy env carries the exact latest archive values', () => {
  assert.match(env, /^V625_TEPE_VETO="0"$/m);
  assert.match(env, /^V625_KAR_TASIMA="1"$/m);
  assert.match(env, /^V628_ATR_TAVAN="8\.0"$/m);
  assert.match(env, /^R486_MAX_POSITIONS="1"$/m);
  assert.match(env, /^R486_MIN_LEVERAGE="7"$/m);
  assert.match(env, /^V592_LEVERAGE_LOCK="7"$/m);
  assert.match(env, /^V623_BAKIYE_UYUMLU_MARJ="0"$/m);
  assert.match(env, /^V624_BUTCE_UYUMLU_SL="1"$/m);
  assert.match(env, /^V601_MARJ_TABAN="50"$/m);
  assert.match(env, /^V601_MARJ_TAVAN="100"$/m);
  assert.match(env, /^R497_SLOT_MARGIN_USDT="50"$/m);
  assert.match(env, /^R486_ABSOLUTE_MIN_MARGIN="50"$/m);
  assert.match(env, /^V644_POSITION_FOCUS_ACTIVE="1"$/m);
  assert.match(env, /^V644_15M_MEMORY_ACTIVE="1"$/m);
});

test('runtime ignores stale panel capital, direction and universe overrides', () => {
  assert.match(server, /const leverage = R486_MIN_LEVERAGE;/);
  assert.match(server, /const allowShort = false;/);
  assert.match(server, /autoConfig\.maxPositions = R486_MAX_POSITIONS;/);
  assert.match(server, /autoConfig\.leverage = R486_MIN_LEVERAGE;/);
  assert.match(server, /autoConfig\.allowShort = false;/);
  assert.match(server, /const r54ScanMode = 'TOP24';/);
  assert.match(server, /LONG_ONLY_CONTRACT_SHORT_BLOCKED/);
});

test('stale duplicate parity route is not registered', () => {
  assert.match(server, /if \(false\) \{\s*app\.get\('\/api\/backtest-parity\/status'/);
  assert.match(server, /hardLockedTestnet:BINANCE_EXECUTION_ENV==='TESTNET'/);
});
