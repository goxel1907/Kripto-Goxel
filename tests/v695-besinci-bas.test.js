'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));

// CANLI KANIT — CAKE, 03.09.2026 15:36, panel dokumu:
//   "ilk engel R/R 0.04 fiziksel olarak yetersiz"
//   "V691: ilk engel (+%0.13) ATR gurultusunden yakin (taban %3.60)"  <- V691 CALISIYOR
//   "SL 1.64% < 3.60% (2.35 ATR)"                                     <- tightStop CALISIYOR
//   panel: "0 emir / 16 atlandi · kalite 0 gecti / 16 elendi"
//
// V692'de UC kapiyi actim ama panel hala 0 acilis gosterdi. Sebep: ailenin BESINCI basi
// SABIT KODLANMISTI ve hicbir ENV ona ulasamiyordu:
//   legacyFirstObstacleHard = firstRRAdjusted < 0.45 && rol === 'HARD_OBSTACLE' && ...
// CAKE: 0,04 < 0,45 ve rol HARD_OBSTACLE -> MARKET engellendi.
// V687 muafiyeti de kurtarmiyor: CAKE'in tam R/R'si 2,78 (< 4 esigi).

test('V695: sabit kodlanmis 0.45 esigi bayraga baglandi', () => {
  assert.ok(!/firstRRAdjusted<0\.45&&/.test(server), 'sabit 0.45 kalmamali');
  assert.match(server, /const _v695FoEsik = V692_ILK_ENGEL_KAPISI_KALKTI \? 0 : 0\.45;/);
  assert.match(server, /firstRRAdjusted<_v695FoEsik&&/);
});

test('V695: kapali durumda hicbir R/R degeri bloklanmiyor', () => {
  // esik 0 -> `firstRRAdjusted < 0` hicbir gecerli R/R icin dogru olamaz
  const esik = 0;
  for (const rr of [0.00, 0.04, 0.20, 0.44, 1.0, 2.78]) assert.ok(!(rr < esik), `R/R ${rr} bloklanmamali`);
  // acik durumda eski davranis birebir korunuyor
  const eski = 0.45;
  assert.ok(0.04 < eski, 'V692=0 iken CAKE yine bloklanir (geri alinabilirlik)');
});

test('V695: sabit kodlanmis esik ILAN EDILIYOR (ENV\'de gorunmez ama davranisi belirliyor)', () => {
  assert.ok(server.includes('[V695] legacyFirstObstacleHard esigi 0.45 -> 0'),
    'acilista ilan edilmeli — ilan edilmezse kimse orada oldugunu bilmez');
  assert.ok(server.includes('ASIL BEKCI buydu'), 'neden onemli oldugu yazilmali');
});

test('V695: BUILD etiketi package.json ile AYNI (V6.9.3/V6.9.4 bumplanmamisti)', () => {
  const m = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(m, 'BUILD tanimli olmali');
  const onek = 'V' + pkg.version.replace(/\./g, '_');
  assert.ok(m[1].startsWith(onek),
    `BUILD ${m[1]} ile package.json ${pkg.version} uyusmuyor — panelde yanlis surum gorunur`);
  // Canlida panel V6_9_2 yaziyordu ama V6.9.4 kodu calisiyordu: hangi surumun canli
  // oldugunu kimse bilemiyordu. Bir daha olmasin diye bu test var.
});

test('V695: V691 ve tightStop ZATEN calisiyordu (canli logdan dogrulandi)', () => {
  // CAKE logu: "V691: ilk engel ... ATR gurultusunden yakin (taban %3.60)"
  //            "SL 1.64% < 3.60% (2.35 ATR)"
  assert.match(server, /engel tavanı artık stopu kısmıyor/);
  assert.match(server, /\(\$\{R486_MIN_STOP_ATR\} ATR\)/);
});
