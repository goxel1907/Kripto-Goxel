const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.join(__dirname, '..');
const source = fs.readFileSync(path.join(root, 'server.js'), 'utf8');

function loadAlgoIdHelpers() {
  const start = source.indexOf('function v643AlgoClientId(');
  const end = source.indexOf('async function buildAlgoCloseParams(', start);
  assert.ok(start >= 0 && end > start, 'V6.4.3 algo kimlik yardımcıları bulunmalı');
  const context = { crypto, TESTNET_SESSION_RESET_ID: 'LIVE_TEST_SESSION' };
  vm.runInNewContext(`${source.slice(start, end)}\nthis.makeId=v643AlgoClientId;this.normalizeId=v643NormalizeAlgoClientId;`, context);
  return context;
}

test('Unicode paritelerin SL/TP clientAlgoId değerleri Binance biçimine uyar', () => {
  const { makeId } = loadAlgoIdHelpers();
  const legal = /^[.A-Z:/a-z0-9_-]{1,36}$/;
  for (const symbol of ['我踏马来了USDT', '龙虾USDT', 'MELANIAUSDT']) {
    const sl = makeId('SL', symbol, 1724830000000);
    const tp = makeId('TP', symbol, 1724830000000);
    assert.match(sl, legal);
    assert.match(tp, legal);
    assert.notEqual(sl, tp);
    assert.ok(sl.length <= 36);
    assert.ok(tp.length <= 36);
  }
});

test('yasa dışı eski kimlik build sınırında hashlenir, yasal kimlik korunur', () => {
  const { normalizeId } = loadAlgoIdHelpers();
  const legal = /^[.A-Z:/a-z0-9_-]{1,36}$/;
  const fixed = normalizeId('SL_龙虾USDT_1724830000000', '龙虾USDT', 'STOP_MARKET');
  assert.match(fixed, legal);
  assert.ok(fixed.startsWith('SL_'));
  assert.equal(normalizeId('SL_existing-safe_1', 'ABCUSDT', 'STOP_MARKET'), 'SL_existing-safe_1');
});

test('Unicode teşhis sembolleri ASCII temizliğiyle yalnız USDT değerine çökmez', () => {
  assert.match(source, /function r49356Sym\(v\)\{ return normalizeSymbol\(v\); \}/);
  assert.doesNotMatch(source, /function r49356Sym\(v\).*replace\(\/\[\^A-Z0-9\]\/g/);
});
