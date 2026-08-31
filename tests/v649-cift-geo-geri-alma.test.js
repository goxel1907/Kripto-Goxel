'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ── V646/V648-D neden geri alindi ───────────────────────────────────────────
// `breakEvenAt` OPERATIF esik degil, TABAN. Zincir:
//   breakEvenAt -> r192BreakEvenAt -> r283DynamicBE (geo ORADA uygulanir)
// V646 geo'yu tabana da ekleyince geo IKI KEZ carpildi.

test('V649: breakEvenAt artik SADECE taban (geo uygulanmaz)', () => {
  assert.match(server, /const breakEvenAt = _v646BeTaban;/);
  assert.ok(!/const breakEvenAt = V646_BE_GEO \?/.test(server),
    'V646 cift-geo satiri kalmamali');
});

test('V649: geo TEK noktada, r283DynamicBE icinde uygulanir', () => {
  // runner ve non-runner dallari backtest formulunun aynisi olmali
  assert.match(server, /\? Math\.max\(r192BreakEvenAt \* r339GeoScale, 2\.0 \* r339GeoScale, \(r339AiManaged \? r344SlPctVal \* 0\.75 : 0\)\)/);
  assert.match(server, /Math\.max\(r192BreakEvenAt, 0\.65\)\) \* r339GeoScale/);
});

test('V649: r192BreakEvenAt tabandan turer (zincir kopmamis)', () => {
  assert.match(server, /const r192BreakEvenAt = r192MomentumBreath \? Math\.max\(breakEvenAt,/);
});

test('V649: kar-kilit merdiveni ktm*geo ile zaten backtest paritesinde', () => {
  // BU ZATEN DOGRUYDU — yanlislikla "eksik" sanip ikinci kez eklemeyelim diye kilitliyoruz.
  assert.match(server, /const _runnerMult = state\.aiRunner \? 1\.8 : 1\.0;/);
  assert.match(server, /const kT1 = karTasima1 \* _runnerMult \* r339GeoScale, kT2 = karTasima2 \* _runnerMult \* r339GeoScale, kT3 = karTasima3 \* _runnerMult \* r339GeoScale;/);
  // tabanlar geo/ktm ICERMEMELI (onlar merdivende uygulaniyor)
  assert.match(server, /const karTasima1    = \(Number\(cfg\.aiKT1\) > 0 \? Number\(cfg\.aiKT1\) : 1\.5\) \* r390K;/);
});

test('V649: trailing de zaten backtest paritesinde (geo + runner 1.8x / min %2.5)', () => {
  assert.match(server, /let trailPctEff = Math\.min\(trailPct \* r339GeoScale, Math\.max\(trailPct, Number\(state\.slPct \|\| slPct\) \* 1\.1\)\);/);
  assert.match(server, /if \(state\.aiRunner\) trailPctEff = Math\.max\(trailPctEff \* 1\.8, 2\.5\);/);
});

test('V649: MAX_SURE cikisi OPERATIF esigi kullanir (backtest: real < be_thr*.5)', () => {
  assert.match(server, /const _v649BeThr = r282RunnerMode/);
  assert.match(server, /if \(openMinutes > r339MaxSureDk && realProfitPct < _v649BeThr \* 0\.5\) \{/);
  assert.ok(!/realProfitPct < breakEvenAt \* 0\.5/.test(server),
    'ham taban ile karsilastirma kalmamali');
});

test('V649: MAX_SURE suresi backtestle ayni (runner 240 / normal 150)', () => {
  assert.match(server, /const _v601MaxSureTaban = r339AiManaged \? \(state\.aiRunner \? 240 : 150\) : 90;/);
});

test('V649: operatif esik pozisyon basina bir kez loglanir', () => {
  assert.match(server, /V649 BE OPERATIF esik/);
  assert.match(server, /state\._v649Log = 1;/);
});

test('V649: geo matematigi — TEK kez uygulanmali, iki kez DEGIL', () => {
  const clamp = (v,a,b)=>Math.max(a,Math.min(b,v));
  const bt = (slPct, r390=1, runner=false) => {
    const geo = clamp(slPct/1.7,1,3), base = 0.8*r390;
    return runner ? Math.max(base*geo, 2*geo, slPct*0.75) : Math.max(base,0.65)*geo;
  };
  const v646Hatali = (slPct, r390=1, runner=false) => {
    const geo = clamp(slPct/1.7,1,3), taban = Math.max(0.8*r390,0.65)*geo; // geo 1. kez
    return runner ? Math.max(taban*geo, 2*geo, slPct*0.75) : Math.max(taban,0.65)*geo; // 2. kez
  };
  // non-runner slPct %5,5: backtest %2,40 · hatali surum %7,20 (3 kat gec)
  assert.ok(Math.abs(bt(5.5) - 2.40) < 0.01, 'backtest %2,40 olmali');
  assert.ok(Math.abs(v646Hatali(5.5) - 7.20) < 0.01, 'hatali surum %7,20 idi');
  // ZKP: runner slPct %3,47 -> iki surumde de %4,08 (2*geo bastiriyor) — bu yuzden canlida gorunmedi
  assert.ok(Math.abs(bt(3.47,1,true) - v646Hatali(3.47,1,true)) < 0.001,
    'runner %3,47te fark yok — hatanin canlida neden gizlendigini kayit altina alir');
});

test('V649: sozlesme ve ilgisiz yamalar yerinde', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(require('fs').readFileSync(require('path').join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'),
    `LAZARUS_BUILD (${_b && _b[1]}) package.json (${_pkg.version}) ile uyusmuyor`);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);
  assert.match(server, /V637_PUSU_R495E_DEVRET/);
  assert.match(server, /V646_ZAYIF_KURULUMLAR/);   // zayif kurulum elemesi — ayri is, duruyor
  assert.match(server, /V648_EN_YAKIN_ENGEL/);     // en yakin engel — ayri is, duruyor
  assert.match(server, /V648_PIVOT_ENGEL/);        // swing pivot — ayri is, duruyor
});
