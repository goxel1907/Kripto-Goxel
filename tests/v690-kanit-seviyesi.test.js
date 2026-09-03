'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KAYNAK: Viaggi, "A Standardized R-Multiple Framework for the Statistical Validation
// of Trading Edge in Retail Trading Systems" (SSRN 6653758).
//
// Makalenin bize bakan iki sonucu:
//   1) N_min = z^2 * b / e^2   — RR 1:3 ve gercek edge +0,10R icin tek-test 812 islem,
//      secim sonrasi (M_eff=10) 1.731 islem (Tablo A.17). Bizde 92 kapali islem var.
//   2) Tablo A.16: 20 aday varyant denendiyse orta-kanit esigi 1,645 degil 2,28.
//
// BU YUZDEN kendi olcumlerimi yeniden yargiladim:
//   - "36.918 nokta" SISMISTI: 4 mum adim + 24 mum ileri ufuk = her sonuc ~6 kez sayildi.
//   - Gercek bagimsiz birim GRAFIK. Ortusmeyen ornek: 6.153 nokta / 879 grafik.
//   - Kume-saglam t (grafik ici etki -> grafikler arasi t-testi), esik 2,28:
//       V680 dikey uzama   t=-9,25  GECTI
//       V688 desteksiz -8  t=+1,96  GECMEDI ve ISARET TERS
//       V688 EQL +14       t=+1,06  GECMEDI
//       V689 fitilsiz +5   t=+0,49  GECMEDI
//
// KARAR: kanit ne kadarsa agirlik o kadar.

test('V690: V680 dikey uzama cezasi DOKUNULMADI (tek gecen terim)', () => {
  assert.match(server, /const V680_PARABOLIK_CEZA = String\(process\.env\.V680_PARABOLIK_CEZA \?\? '1'\) !== '0';/);
  assert.match(server, /const V680_CEP_CEZA   = Math\.max\(0, Math\.min\(60, Number\(process\.env\.V680_CEP_CEZA \|\| 25\)\)\);/);
});

test('V690: EQL agirligi kume-saglam tahmine cekildi (14 -> 6)', () => {
  const m = server.match(/^const V688_EQL_PUAN\s*=.*$/m);
  assert.ok(m, 'sabit bulunmali');
  assert.ok(/\|\| 6\)/.test(m[0]), 'varsayilan 6 olmali: ' + m[0]);
  assert.ok(/V690/.test(m[0]), 'satirda gerekce olmali');
});

test('V690: isareti kararsiz terim KAPATILDI (desteksiz cezasi 8 -> 0)', () => {
  const m = server.match(/^const V688_DESTEKSIZ_CEZA\s*=.*$/m);
  assert.ok(/\|\| 0\)/.test(m[0]), 'varsayilan 0 olmali: ' + m[0]);
});

test('V690: etkisiz terim KAPATILDI (fitilsiz dip 5 -> 0)', () => {
  const m = server.match(/^const V689_PUAN\s*=.*$/m);
  assert.ok(/\|\| 0\)/.test(m[0]), 'varsayilan 0 olmali: ' + m[0]);
});

test('V690: terimler SILINMEDI, env ile geri acilabilir', () => {
  assert.ok(server.includes('process.env.V688_DESTEKSIZ_CEZA'), 'sabit durmali');
  assert.ok(server.includes('process.env.V689_PUAN'), 'sabit durmali');
  assert.ok(server.includes('v688DestekOkumasi'), 'fonksiyon durmali');
  assert.ok(server.includes('altDip:_v689'), 'olcum durmali — raporlanmaya devam');
});

test('V690: orneklem sismesi ve secim-sonrasi esik kodda YAZILI', () => {
  const i = server.indexOf('V690 ═══ KANIT SEVIYESINE CEKILME');
  assert.ok(i > 0, 'gerekce blogu olmali');
  const blok = server.slice(i, i + 2600);
  assert.ok(blok.includes('SISMIS'), 'orneklem sismesi kabul edilmeli');
  assert.ok(blok.includes('2,28'), 'secim-sonrasi esik yazilmali');
  assert.ok(blok.includes('879 grafik'), 'gercek bagimsiz birim yazilmali');
  assert.ok(blok.includes('-9,25'), 'gecen terimin t degeri yazilmali');
});
