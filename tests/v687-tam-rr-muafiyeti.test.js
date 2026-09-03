'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const panel = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

// OLCUM — 88 canli islem, dort kutu (tam R/R x ilk engel R/R):
//   tam R/R>=4 & ilk engel <0,65 : n=29 WR %62 ortROI +3,10% net  +48,5$  <- EN IYI
//   tam R/R>=4 & ilk engel >=0,65: n=25 WR %36 ortROI -2,14% net  -15,7$
//   tam R/R<4  & ilk engel <0,65 : n=18 WR %33 ortROI -14,92% net -120,3$
//   tam R/R<4  & ilk engel >=0,65: n=16 WR %38 ortROI -4,91% net  -43,7$
//
// tam R/R>=4 icinde ilk engel DUSTUKCE sonuc IYILESIYOR (kapi TERS calisiyor):
//   0-0,45   WR %83  +28,3$
//   0,45-0,65 WR %57 +20,2$
//   0,65-1,0  WR %44 +11,7$
//   1,0+      WR %14 -27,3$
//
// CANLI ORNEK 03.09.2026 10:28 BRUSDT: tam R/R 4,99 · ilk engel 0,42
//   -> "ilk engel R/R 0.42 fiziksel olarak yetersiz" diye REDDEDILDI.
//   O kutu (R/R>=4 & foRR 0-0,45) botun en iyi kutusu: WR %83.

test('V687: tam R/R saglikliyken ilk-engel vetosu SERT olmuyor', () => {
  const i = server.indexOf('const _v687Muaf =');
  assert.ok(i > 0, 'muafiyet degiskeni olmali');
  const blok = server.slice(i, i + 500);
  assert.ok(blok.includes('fullRR!==null'), 'tam R/R okunmali');
  assert.ok(blok.includes('>= V687_MUAF_RR'), 'esikle karsilastirilmali');
  assert.ok(server.includes('const firstObstacleHard=_v687Muaf ? false : ('),
    'muafiyet varken firstObstacleHard false olmali');
});

test('V687: esik olculen degerde ve ENV ile kapatilabilir', () => {
  assert.match(server, /const V687_MUAF_RR = Math\.max\(1, Math\.min\(10, Number\(process\.env\.V687_MUAF_RR \|\| 4\)\)\);/);
  assert.match(server, /const V687_TAM_RR_MUAFIYET = String\(process\.env\.V687_TAM_RR_MUAFIYET \?\? '1'\) !== '0';/);
});

test('V687: erken-risk bayragi da muafiyeti dinliyor', () => {
  assert.ok(server.includes('(firstObstacleSoft&&!_v687Muaf)'),
    'earlyRisk icinde soft veto muaf tutulmali');
});

test('V687: gerekce metni kararin NEDEN degistigini yaziyor', () => {
  assert.ok(server.includes('V687: tam R/R'), 'muafiyet gerekcede gorunmeli');
  assert.ok(server.includes('vetosu kaldirildi'), 'acikca yazmali');
  assert.ok(server.includes('legacyFirstObstacleHard&&!_v687Muaf'),
    'muafiyet varken eski red metni yazilmamali');
});

// PANEL DUZELTMELERI — canli panelde yakalanan yanlislar
test('V687-panel: okuma olmayan aday "kalite gecti" DEMIYOR', () => {
  assert.ok(panel.includes("k.skor==null?'okuma yok (kapıdan önce elendi)'"),
    'skor yoksa okuma yok yazmali');
});

test('V687-panel: funding CIFT olceklenmiyor', () => {
  assert.ok(!server.includes('funding ${(P.funding*100)'), 'x100 kaldirilmali');
  assert.ok(server.includes('funding ${P.funding.toFixed(4)}%'), 'ham yuzde yazilmali');
});

test('V687-panel: bayat ENV metni duzeldi (30$ -> 50$, tavan yok)', () => {
  assert.ok(!panel.includes('R497_SLOT_MARGIN_USDT=30'), 'eski 30$ metni kalmamali');
  assert.ok(panel.includes('R497_SLOT_MARGIN_USDT=50'), 'dogru deger yazmali');
  assert.ok(!panel.includes('max 2 pozisyon'), 'max 2 poz metni kalmamali');
});
