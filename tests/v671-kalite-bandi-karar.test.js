'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ÖLÇÜM — haziran 6TF backtest, senaryo R495_3M_ACCEPT, MAX 1 POZİSYON:
//   kapı yok       n=305  WR %66,9  PF 1,91  net +716,31$  işlem-başı 2,35$  maxDD 73,86$
//   LOW bandı ele  n=229  WR %67,2  PF 2,79  net +836,38$  işlem-başı 3,65$  maxDD 57,62$
// Üç eksende birden üstün: daha çok para, daha yüksek PF, daha sığ dip.
// Backtest bantları canlıyla BİREBİR aynı eşikte: HIGH>=0,660 · LOW<0,550.

test('V671: DÜŞÜK kalite bandı adayı ELİYOR (sadece marj kırpmıyor)', () => {
  assert.match(server, /const V671_DUSUK_KALITE_ELE = String\(process\.env\.V671_DUSUK_KALITE_ELE \?\? '1'\) !== '0';/);
  const i = server.indexOf('V671_DUSUK_KALITE_ELE && _q493 < R493_LOW_MAX');
  assert.ok(i > 0, 'eleme koşulu bulunmalı');
  const blok = server.slice(i, i + 1200);
  assert.ok(blok.includes('V671_LOW_QUALITY_BLOCK'), 'kanıt hunisine yazmalı');
  assert.ok(blok.includes('markAutoSkip'), 'markAutoSkip çağırmalı');
  assert.ok(blok.includes('continue;'), 'adayı gerçekten atlamalı');
  // eleme, marj çarpanından ÖNCE gelmeli; yoksa yine hiçbir şey elemez
  assert.ok(blok.indexOf('continue;') < blok.indexOf('let f493'), 'eleme f493 hesabından önce olmalı');
});

test('V671: eşikler R493 sabitlerinden geliyor, elde yazılmıyor', () => {
  assert.match(server, /const R493_LOW_MAX     = r491EnvNumber\('R493_LOW_MAX', 0\.55, 0\.30, 0\.65\);/);
  assert.match(server, /const R493_HIGH_MIN    = r491EnvNumber\('R493_HIGH_MIN', 0\.66, 0\.55, 0\.90\);/);
});

test('V672: kelepçeler env değerini artık sessizce yutmuyor', () => {
  assert.match(server, /V628_ATR_TAVAN        = Math\.max\(1\.0, Math\.min\(20\.0,/);
  assert.match(server, /R495_TAKER_RATIO_MIN = Math\.max\(\.05, Math\.min\(\.70,/);
  for (const re of [/V619_TOP10_FAST_SCORE_MIN = Math\.max\(30,/,
                    /V619_TOP24_FAST_SCORE_MIN = Math\.max\(30,/,
                    /V619_EXPLOSION_FAST_SCORE_MIN = Math\.max\(30,/])
    assert.match(server, re, 'hızlı eşik tabanı artık başka bir sabit olmamalı');
});

test('V672: parite kapısı tam-eşitlik yerine ARALIK istiyor', () => {
  for (const [ad, re] of [
    ['kaldıraç',  /if\(!\(Number\(V592_LEVERAGE_LOCK\)>=7&&Number\(V592_LEVERAGE_LOCK\)<=10\)\)/],
    ['V45 skor',  /if\(!\(V592_V45_MS_SCORE_MIN>=20&&V592_V45_MS_SCORE_MIN<=60\)\)/],
    ['max poz',   /if\(!\(Number\(R486_MAX_POSITIONS\)>=1&&Number\(R486_MAX_POSITIONS\)<=2\)\)/],
    ['ATR tavan', /if\(!\(V628_ATR_TAVAN>=1\.0&&V628_ATR_TAVAN<=20\.0\)\)/],
    ['drift max', /if\(!\(Number\(R495_MAX_ENTRY_DRIFT_ATR\)>=0\.30&&Number\(R495_MAX_ENTRY_DRIFT_ATR\)<=1\.50\)\)/],
    ['drift min', /if\(!\(Number\(R495_MIN_ENTRY_DRIFT_ATR\)>=-1\.50&&Number\(R495_MIN_ENTRY_DRIFT_ATR\)<=-0\.05\)\)/],
    ['adverse',   /if\(!\(Number\(R495_MAX_ADVERSE_ATR\)>=0\.30&&Number\(R495_MAX_ADVERSE_ATR\)<=1\.50\)\)/],
  ]) assert.match(server, re, `${ad} aralık kontrolü olmalı`);
  for (const eski of ['CANLI_KALDIRAC_7X_DEGIL','V45_SCORE_35_DEGIL','CANLI_MAX_POZ_1_DEGIL',
                      'CANLI_ATR_TAVAN_8_DEGIL','DRIFT_MAX_BACKTEST_ALTI','DRIFT_MIN_BACKTEST_USTU',
                      'ADVERSE_MAX_BACKTEST_ALTI'])
    assert.ok(!server.includes(eski), `${eski} kalmamalı — env denemesi boot'u fail-closed ederdi`);
});

test('V672: hiçbir VARSAYILAN değişmedi — bugünkü davranış birebir aynı', () => {
  assert.ok(server.includes('process.env.V592_LEVERAGE_LOCK||7'));
  assert.ok(server.includes('process.env.V592_V45_MS_SCORE_MIN||35'));
  assert.ok(server.includes('process.env.V628_ATR_TAVAN || 8.0'));
  assert.ok(server.includes('process.env.R495_MAX_ENTRY_DRIFT_ATR || .85'));
  assert.ok(server.includes('process.env.R495_MAX_ADVERSE_ATR || .85'));
  assert.ok(server.includes('process.env.R495_TAKER_RATIO_MIN || .20'));
  assert.ok(server.includes('process.env.R486_MAX_POSITIONS || 1'));
});

test('V671: build etiketi package.json surumuyle uyusuyor', () => {
  // Sabit build stringi her surumde bu testi kirdi (v670'te de oldu). Surumden turetiliyor.
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build, 'LAZARUS_BUILD tanimli olmali');
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
