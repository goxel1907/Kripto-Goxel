'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ÖLÇÜM — dataset v3.44, 85 kapanmış işlem. Geri verme (zirve − kapanış) ile
// sonuç arasındaki ilişki TEK YÖNLÜ:
//    <1 puan : 16 işlem · 14 kazanan · net  +61,13 $
//   1–3 puan :  9 işlem ·  6 kazanan · net  +31,26 $
//   3–6 puan :  4 işlem ·  4 kazanan · net  +19,48 $
//  6–10 puan :  7 işlem ·  5 kazanan · net  +11,65 $
//  10–20 puan: 25 işlem ·  7 kazanan · net  −52,41 $
//   20+ puan : 24 işlem ·  1 kazanan · net −192,70 $
// Kazananların geri verme medyanı 2,8 · kaybedenlerin 19,2.
//
// PARAMETRE kapalı-form veriden ÇIKARILAMIYOR: tarama hep daha küçük G'ye doğru
// iyileşiyor, çünkü zirve geriye dönük biliniyor. Canlı takip eden zirve her an
// nihai zirveden düşüktür. Bu blok tam o farkı ölçer — KOŞAN zirveyle.

test('V677: gölge kaydedici emre DOKUNMUYOR', () => {
  const i = server.indexOf('V677 ═══ GOLGE: gercek bir takip eden');
  assert.ok(i > 0, 'V677 bloğu bulunmalı');
  const blok = server.slice(i, i + 2200);
  for (const yasak of ['closePosition','sendOrder','v592SendMainOrder','cancelOrder',
                       'setStopLoss','markAutoSkip','trailingState.set','reduceOnly'])
    assert.ok(!blok.includes(yasak), `gölge blok ${yasak} içermemeli`);
  // Icerdigi TEK `continue` G dongusunun muhafizidir; adayi/islemi atlayan bir
  // continue degildir. Sayisini sabitliyorum ki ileride sessizce eklenmesin.
  assert.strictEqual((blok.match(/continue;/g) || []).length, 1, 'yalnız döngü muhafızı olmalı');
  assert.ok(blok.includes('if(_st.fired[_g]) continue;'), 'o continue G döngüsünün muhafızı olmalı');
  assert.ok(blok.includes('logAuto'), 'yalnız loglamalı');
  assert.ok(blok.includes('_st.fired[_g]='), 'tetiklemeyi kaydetmeli');
});

test('V677: ROI yöneticinin kendi sayısından okunuyor, fiyattan TÜRETİLMİYOR', () => {
  // ZORA kaydında fiyattan türetilen ROI %0,27 iken yönetici %2,72 diyordu.
  // Gölge, karşılaştırılacağı peakRoi/roiPct ile aynı birimde olmak zorunda.
  assert.match(server, /const _roi=Number\(manager\?\.r91Exit\?\.pnlPct\);/);
  assert.ok(!server.includes("const _roi=(_long?(_p/_e-1):(_e/_p-1))*_l*100;"),
    'fiyattan türetme kalmamalı');
});

test('V677: bayraklar ve eşikler env ile ayarlanabilir', () => {
  assert.match(server, /const V677_GERI_VERME_GOLGE = String\(process\.env\.V677_GERI_VERME_GOLGE \?\? '1'\) !== '0';/);
  assert.match(server, /const V677_MIN_ZIRVE = Math\.max\(0\.5, Math\.min\(20, Number\(process\.env\.V677_MIN_ZIRVE \|\| 3\)\)\);/);
  assert.match(server, /V677_G_LISTESI = Object\.freeze\(String\(process\.env\.V677_G_LISTESI \|\| '6,8,10,12'\)/);
});

test('V677: her G bir kez tetiklenir, min zirve altında hiç tetiklenmez', () => {
  const i = server.indexOf('for(const _g of V677_G_LISTESI)');
  assert.ok(i > 0);
  const blok = server.slice(i, i + 500);
  assert.ok(blok.includes('if(_st.fired[_g]) continue;'), 'tekrar tetiklenmemeli');
  assert.ok(blok.includes('_st.peak>=V677_MIN_ZIRVE'), 'min zirve koşulu olmalı');
  assert.ok(blok.includes('_roi<=_st.peak-_g'), 'koşan zirveden G puan düşüş koşulu');
});

test('V677: dataset.csv gölge kolonları', () => {
  const i = server.indexOf('V677: GOLGE geri-verme stopu');
  assert.ok(i > 0);
  const blok = server.slice(i, i + 1400);
  for (const k of ["'AtMs'","'Roi'","'Peak'","'PeakAtMs'"]) {
    const ad = k.replace(/'/g,'');
    assert.ok(blok.includes("_o['gb'+_g+'" + ad + "']"), `gb<G>${ad} kolonu olmalı`);
  }
  assert.ok(blok.includes('gbLivePeak'), 'koşan zirve de dışa verilmeli');
  assert.ok(blok.includes('catch(_){ return {}; }'), 'fail-open olmalı');
});

test('V677: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
