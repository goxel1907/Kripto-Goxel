'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KARAR: kaldirac 7x -> 5x. Gerekce ARITMETIK, tercih degil.
//   risk$ = marj x kaldirac x stop%.  Marj (50$) ve risk (%23) sabitken kaldirac,
//   stopun ne kadar GENIS olabilecegini belirleyen TEK serbest degisken:
//     odenebilir stop = risk% / ((marj/ozsermaye) x kaldirac)
//       7x -> %6,57 -> ATR x2,35 ile karsilanabilen max ATR(1s) = %2,80
//       5x -> %9,20 ->                                            %3,91
//       3x -> %15,3 ->                                            %6,52
//   EGLD ATR %5,30 idi: 7x'te sinirin cok disinda, stop 0,58 x ATR'ye sikisti (-%26).
//
// OLCUM (879 grafik, ortusmeyen 6.153 nokta, TP %9):
//   stop 0,5xATR -> stop yeme %83,1 · 1x -> %67,9 · 2x -> %42,5 · 4x -> %14,6
//
// Kaldirac dusurmek RISKI ARTIRMAZ: risk$ ayni, stopa nefes alani acilir.
// Bedeli: ayni marjda nominal %29 kucuk (350$ -> 250$). R katsayisi degismez.

const lev = (l) => 23 / (0.50 * l) / 2.35;   // max ATR(1s)

test('V693: aritmetik — 5x sinirinin nereden geldigi', () => {
  assert.ok(Math.abs(lev(7) - 2.796) < 0.01, '7x -> %2,80');
  assert.ok(Math.abs(lev(5) - 3.915) < 0.01, '5x -> %3,91');
  assert.ok(Math.abs(lev(3) - 6.525) < 0.01, '3x -> %6,52');
  assert.ok(5.30 > lev(7), 'EGLD 7x sinirinin disindaydi');
});

test('V693: GERCEK varsayilan S() enjeksiyonunda — sabitin fallback degeri degil', () => {
  // Once sabitin `|| 7` fallback'ini degistirdim ve acilis logu HALA 7x yazdi:
  // S() process.env'i onceden dolduruyor, fallback hic calismiyor.
  assert.match(server, /S\('R486_MIN_LEVERAGE','5'\);/, 'S() enjeksiyonu 5 olmali');
  assert.ok(!/S\('R486_MIN_LEVERAGE','7'\);/.test(server), 'eski 7 kalmamali');
});

test('V693: BORSAYA giden kaldirac da 5 (asil tehlike buydu)', () => {
  // Emir yolunda: _lockLev = V592_LEVERAGE_LOCK, R486_MIN_LEVERAGE DEGIL.
  // Sadece R486 degistirilseydi: log 5x der, Binance'e 7x giderdi.
  assert.ok(server.includes('const _lockLev = (V592_EXACT_BACKTEST_AUTHORITY && V592_LEVERAGE_LOCK > 0) ? V592_LEVERAGE_LOCK : Number(leverage);'),
    'emir yolu lock kullaniyor olmali');
  assert.match(server, /S\('V592_LEVERAGE_LOCK','5'\);/, 'lock da 5 olmali');
  assert.ok(!/S\('V592_LEVERAGE_LOCK','7'\);/.test(server), 'eski 7 kalmamali');
});

test('V693: ikisi ayrisirsa acilista UYARI veriyor', () => {
  const i = server.indexOf('V693 ═══ Emir kaldiraci ile hesap kaldiraci AYNI olmali');
  assert.ok(i > 0, 'tutarlilik kontrolu olmali');
  const blok = server.slice(i, i + 800);
  assert.ok(blok.includes('V592_LEVERAGE_LOCK !== R486_MIN_LEVERAGE'), 'karsilastirma olmali');
  assert.ok(blok.includes('KALDIRAC AYRISMASI'), 'net dille uyarmali');
});

test('V693: ENV ile geri alinabilir', () => {
  assert.ok(server.includes('process.env.R486_MIN_LEVERAGE'), 'env yolu durmali');
  assert.ok(server.includes('process.env.V592_LEVERAGE_LOCK'), 'env yolu durmali');
});
