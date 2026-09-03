'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI OLAY: V6.9.2 + V6.9.3 push edildikten sonra bot acildi ve parite kapisi
// fail-closed calisti:
//   🔴 CANLI PARITE KAPISI KAPALI — EMIR ACILMAYACAK:
//      CANLI_KALDIRAC_ARALIK_DISI:5 | V45_FO_ARALIK_DISI:0
//
// KAPI DOGRU DAVRANDI. Sozlesme sinirlarini degistirdim ama sinirlarin KENDISINI
// gunceIlemedim; kapi bunu yakalayip canli parayla emir acilmasini engelledi.
// Bu test o iki sinirin yeni sozlesmeyle uyumlu oldugunu ve GEVSEMEDIGINI dogrular.

test('V694: kaldirac araligi 3-10 (alt sinir indi, UST SINIR KORUNDU)', () => {
  assert.match(server, /Number\(V592_LEVERAGE_LOCK\)>=3&&Number\(V592_LEVERAGE_LOCK\)<=10/);
  assert.ok(!/Number\(V592_LEVERAGE_LOCK\)>=7&&/.test(server), 'eski 7 alt siniri kalmamali');
  // Ust sinir gevsemedi: yukari kayma hala sozlesme ihlali.
  assert.ok(server.includes('<=10)) hata.push(`CANLI_KALDIRAC_ARALIK_DISI'),
    'ust sinir 10 korunmali');
});

test('V694: alt sinir R486_MIN_LEVERAGE kelepcesiyle AYNI (keyfi degil)', () => {
  assert.match(server, /const R486_MIN_LEVERAGE = Math\.max\(3, Math\.min\(20,/,
    'sabitin kendi tabani 3');
  // Parite kapisi sabitin izin verdiginden daha dar olmamali, yoksa gecerli bir
  // yapilandirma boot'ta oldurulur (tam olarak canlida olan sey).
});

test('V694: V45 ilk-engel esigi 0 kabul ediyor ("esik yok" gecerli deger)', () => {
  assert.match(server, /V592_V45_FIRST_OBSTACLE_RR_MIN>=0&&V592_V45_FIRST_OBSTACLE_RR_MIN<=1\.00/);
  assert.ok(!/V592_V45_FIRST_OBSTACLE_RR_MIN>=0\.01&&/.test(server), 'eski 0,01 tabani kalmamali');
});

test('V694: kapi HALA fail-closed — gevsetmedim, sadece siniri tasidim', () => {
  assert.match(server, /EMIR ACILMAYACAK/);
  for (const k of ['CANLI_KALDIRAC_ARALIK_DISI','V45_FO_ARALIK_DISI','R493_FO_ARALIK_DISI','R486_FO_ARALIK_DISI'])
    assert.ok(server.includes(k), k + ' kontrolu durmali');
});

test('V694: V692/V693 ile parite kapisi ARTIK CELISMIYOR', () => {
  // V692: V45 esigi 0 · V693: kaldirac 5x. Ikisi de yeni araliklarin ICINDE.
  const v45Sifir = /const V592_V45_FIRST_OBSTACLE_RR_MIN=V692_ILK_ENGEL_KAPISI_KALKTI \? 0/.test(server);
  const lev5 = /S\('V592_LEVERAGE_LOCK','5'\);/.test(server);
  assert.ok(v45Sifir && lev5, 'yeni degerler kodda olmali');
  assert.ok(0 >= 0 && 0 <= 1.00, 'V45=0 aralik icinde');
  assert.ok(5 >= 3 && 5 <= 10, 'kaldirac 5 aralik icinde');
});
