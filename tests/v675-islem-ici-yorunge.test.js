'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// NEDEN: `samples` her işlemde 1 Hz kayıtlı — kanıt indeksi bunu doğruluyor
// (PROM 2957 örnek / 2953 sn işlem, COLLECT 2251, BTW 2260, ZORA 597).
// Ama 2,6 MB'lik kaydın içinde ve HTTP tarafı belgeyi ~70.000. baytta kesiyor.
// Bu kolonlar seriyi dataset.csv'ye taşır — GERİYE DÖNÜK: 87 kapalı işlem için
// de dolu gelir, yeni kayıt beklemeye gerek yok.
//
// ZORA'nın gerçek 64 örneği üzerinde doğrulandı:
//   cvdRatio0=47 · cvdRatioMin=45,7 @20.328ms · cvdRatioMax=48,8
//   peakRoiSeen=2,72 @27.330ms · sampleSpanMs=61.160

test('V675: samples dizisinden yörünge kolonları türetiliyor', () => {
  const i = server.indexOf('V675: ISLEM ICI CVD YORUNGESI');
  assert.ok(i > 0, 'V675 bloğu bulunmalı');
  const blok = server.slice(i, i + 3000);
  assert.ok(blok.includes('rec.samples'), 'samples dizisinden okumalı');
  assert.ok(blok.includes("s?.cvd?.stream?.ratio"), 'CVD oranı doğru yoldan okunmalı');
  assert.ok(blok.includes("s?.manager?.peakRoi"), 'kâr tepesi manager.peakRoi\'den okunmalı');
  for (const k of ['cvdRatio0','cvdRatio1m','cvdRatio5m','cvdRatioMin','cvdRatioMinAtMs',
                   'cvdDrop1m','cvdDrop5m','peakRoiSeen','peakRoiAtMs','givebackPct',
                   'sampleCountUsed','sampleSpanMs'])
    assert.ok(blok.includes(k + ':'), `${k} kolonu olmalı`);
  assert.ok(blok.includes('catch(_){ return {}; }'), 'fail-open olmalı');
});

test('V675: işlem o dakikaya ulaşmadıysa null döner (sessiz bozulma koruması)', () => {
  // Bu olmadan 61 saniyelik bir işlem cvdRatio5m olarak SON degeri dondururdu ve
  // "5. dakikadaki CVD" ile "islemin son CVD'si" karisirdi.
  const i = server.indexOf('const at=ms=>{');
  assert.ok(i > 0);
  const satir = server.slice(i, i + 220);
  assert.ok(satir.includes('if(!(span>=ms))return null;'), 'span kontrolü ilk sırada olmalı');
});

test('V675: yalnız dışa aktarım — karar yolu ve parite değişmedi', () => {
  const i = server.indexOf('V675: ISLEM ICI CVD YORUNGESI');
  const ds = server.indexOf('function r501DatasetRows');
  assert.ok(ds > 0 && i > ds && i - ds < 12000, 'dataset üreticisinin gövdesinde olmalı');
  // V682: parite kipi artık ENV'den geliyor ve varsayılanı KAPALI (veri açık).
  assert.match(server, /const V592_POLICY_PARITY_MODE = !V682_PIYASA_VERISI;/);
});

test('V675: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
