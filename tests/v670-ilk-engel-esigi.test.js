'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ÖLÇÜM — LAZARUS_HAZIRAN_2026_6TF_BACKTEST_PAKETI / all_scenario_trades.csv
// senaryo R495_3M_ACCEPT, 555 işlem, SONUÇLAR AYNI, yalnız foRR kapısı değişiyor:
//   kapı yok    n=555  WR %65,8  PF 1,83  net +1186,77$  işlem-başı 2,14$  maxDD  97,69$
//   foRR>=0,10  n=289  WR %68,5  PF 2,44  net  +911,56$  işlem-başı 3,15$  maxDD  53,29$
//   foRR>=0,20  n=200  WR %70,0  PF 2,34  net  +613,68$  işlem-başı 3,07$  maxDD  50,22$
//   foRR>=0,35  n=118  WR %64,4  PF 1,90  net  +290,25$  işlem-başı 2,46$  maxDD  58,29$   <- CANLIDA BUYDU
// max 1 pozisyon (canlı gerçeği, zaman sıralı, çakışan atlanır):
//   kapı yok    n=305  PF 1,91  net +716,31$  maxDD 73,86$
//   foRR>=0,10  n=210  PF 2,23  net +606,12$  maxDD 64,10$   <- en iyi PF + en düşük DD
//   foRR>=0,35  n= 99  PF 1,61  net +163,96$  maxDD 58,18$
// 0,35 kapısı 437 işlemi (net +896,52$, WR %66,1) eliyordu.

test('V670: R493 sert kapısı 0,10 varsayılan ve 0,01 tabanına kadar inebiliyor', () => {
  // V692: bu kapi TAMAMEN kalkti. Uretim hunisi 13.711 kayitta TARGET_TOO_NEAR'i
  // 1.161 blokla en buyuk adlandirilmis sebep gosterdi; olcum ise ilk engel R/R'nin
  // ROI ile iliskisini SIFIR (r=-0,016) buldu. Eski ifade env yolunda korunuyor.
  assert.match(server,
    /const R493_MIN_FIRST_OBSTACLE_RR = V692_ILK_ENGEL_KAPISI_KALKTI \? 0\.01/,
    'V692 acikken esik 0.01 (fiilen kapali) olmali');
  assert.ok(server.includes("r491EnvNumber('R493_MIN_FIRST_OBSTACLE_RR', 0.10, 0.01, 3.00)"),
    'eski yol ENV ile geri alinabilir kalmali');
  // Eski taban 0.10 idi: env 0.05 yazılsa sessizce 0.10'a yükseltilirdi.
  assert.ok(!server.includes("r491EnvNumber('R493_MIN_FIRST_OBSTACLE_RR', 0.35, 0.10, 3.00)"),
    'eski 0.35/0.10 imzası kalmamalı');
});

test('V670: V4.5 seçicisinin KENDİ foRR eşiği de indi (R495 ÖNCESİ çalışır)', () => {
  // Bu eşik R493 kapısından ÖNCE aday eler. Yalnız R493 indirilseydi hiçbir şey değişmezdi.
  // V692: bu da kalkti (huni: V45_SELECTOR 2.258 blok — R493'ten ONCE eliyordu).
  assert.match(server, /const V592_V45_FIRST_OBSTACLE_RR_MIN=V692_ILK_ENGEL_KAPISI_KALKTI \? 0/);
  assert.ok(server.includes("Number(process.env.V592_V45_FIRST_OBSTACLE_RR_MIN||0.10)"),
    'eski yol ENV ile geri alinabilir kalmali');
  assert.match(server, /S\('V592_V45_FIRST_OBSTACLE_RR_MIN','0\.10'\);/);
});

test('V670: R486 yumuşak bayrağın 0,35 TABANI kaldırıldı', () => {
  // Aynı hata sınıfı bir kat aşağıda duruyordu: Math.max(0.35, ...) env<0.35'i etkisiz kılıyordu.
  assert.match(server,
    /const R486_FIRST_OBSTACLE_MIN_RR = Math\.max\(0\.01, Math\.min\(2\.0, Number\(process\.env\.R486_FIRST_OBSTACLE_MIN_RR \|\| 0\.10\)\)\);/);
  assert.ok(!/R486_FIRST_OBSTACLE_MIN_RR = Math\.max\(0\.35/.test(server),
    '0.35 tabanı kalmamalı');
});

test('V670: BOOT PARITY GATE üç eşiği de aralık olarak kontrol ediyor (fail-closed değil)', () => {
  // Bu üç satır "eq(...,0.35)" kalsaydı yeni değerle bot EMİR AÇMAZDI.
  for (const [ad, re] of [
    ['V45',  /if\(!\(V592_V45_FIRST_OBSTACLE_RR_MIN>=0\.01&&V592_V45_FIRST_OBSTACLE_RR_MIN<=1\.00\)\) hata\.push\(`V45_FO_ARALIK_DISI/],
    ['R493', /if\(!\(R493_MIN_FIRST_OBSTACLE_RR>=0\.01&&R493_MIN_FIRST_OBSTACLE_RR<=1\.00\)\) hata\.push\(`R493_FO_ARALIK_DISI/],
    ['R486', /if\(!\(R486_FIRST_OBSTACLE_MIN_RR>=0\.01&&R486_FIRST_OBSTACLE_MIN_RR<=1\.00\)\) hata\.push\(`R486_FO_ARALIK_DISI/],
  ]) assert.match(server, re, `${ad} boot kontrolü aralık olmalı`);
  for (const eski of ['V45_FO_035_DEGIL', 'R493_FO_035_DEGIL', 'R486_FO_035_DEGIL'])
    assert.ok(!server.includes(eski), `${eski} kalmamalı — fail-closed açılışa sebep olurdu`);
});

test('V670: kapıyı okuyan iki karar noktası da aynı sabiti kullanıyor', () => {
  // r493EntrySafetyGate sert bloğu
  assert.match(server, /_v647Known&&_v647RR<R493_MIN_FIRST_OBSTACLE_RR/);
  // R619 hardReasons
  assert.match(server, /if\(!\(Number\.isFinite\(firstRR\)&&firstRR>=R493_MIN_FIRST_OBSTACLE_RR\)\)hardReasons\.push\('FIRST_OBSTACLE_RR'\)/);
});

test('V670: build etiketi package.json surumuyle uyusuyor', () => {
  // Build stringini sabitlemek her surumde bu testi kirdi. Artik surumden turetiliyor.
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build, 'LAZARUS_BUILD tanimli olmali');
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
