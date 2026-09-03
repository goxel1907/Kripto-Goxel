'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KAYNAK: Trader Dale — "Order Flow Trading Setups", Trading Setup #5: Unfinished Business.
// Kitabin iddiasi (s.73): "You don't want to enter a Long trade when there is the
// Unfinished Business below your entry... you would risk the price shooting past your
// trade entry to test it." (bitmemis dip = miknatis)
//
// OLCUM — 879 grafik / 36.918 nokta (15m, 24 mum ileri, %3 SL / %9 TP), dikey uzama
// (ret6<3) kontrollu, EN YAKIN alttaki pivot dip AYNI MESAFEDEyken karsilastirildi:
//
//   mesafe     fitilsiz dip            normal dip           fark
//   0-1 ATR    n=894  bek 1,047        n=20566 bek 0,954    +4 puan
//   1-2 ATR    n=324  bek 1,114        n= 6408 bek 0,891    +9 puan
//   2-3 ATR    n=115  bek 1,043        n= 2074 bek 0,947    +4 puan
//   3-5 ATR    n= 62  bek 1,242        n=  978 bek 1,055    +7 puan
//   TOPLAM     n=1395 bek 1,071        n=30026 bek 0,943    +5,1 puan
//
// KARARLILIK: yarilar +5,1 / +4,8 · ucteler +9,5 / +4,7 / +1,2 — HIC ters donmuyor.
// (Karsilastir: V686'nin iliskisi ilk yaride tersine donuyordu.)
//
// SONUC: kitap TERS soyluyor. 15dk kriptoda fitilsiz dip "bitmemis muzayede" degil,
// dususun tek mumda durdurulup geri alindigi SERT SAVUNULMUS seviye.
// Bu yuzden ceza degil ODUL kondu: +5.
//
// AYNI KITAPTAN OLCULUP REDDEDILENLER (skora KONMADI):
//   - "sadece ILK dokunusta islem yap": olcum tam tersi — 1. gelis bek -0,090 (kaz %37),
//     5+ gelis bek +0,882 (kaz %68). ret6 kontrollu de ayni (0,098 vs 1,037).
//   - hacim kumesi (HVN) uzakligi: ham veride monoton, ret6 kontrolunde sifirlaniyor.
//   - absorpsiyon (yuksek hacim + kucuk menzil): n=73, kontrolde +2 puan — gurultu.
//   - hacim tabanli TP (ustteki ilk HVN): tum bantlar 0,88-0,99 — duz, sinyal yok.

function yukle() {
  const isim = ['V680_PARABOLIK_CEZA','V680_RET6_TABAN','V680_CEZA_TAVAN','V680_CEP_RET6','V680_CEP_CEZA',
                'V686_TAM_RR_SKORA','V686_RR_ESIK','V686_RR_CEZA_TAVAN',
                'V688_ARALIK_DUZELT','V688_DESTEK_OKUMA','V688_EQL_PUAN','V688_DESTEKSIZ_CEZA',
                'V689_FITILSIZ_DIP','V689_FITIL_ESIK','V689_MAX_ATR','V689_PUAN'];
  const satir = isim.map(n => {
    const m = server.match(new RegExp('^const\\s+' + n + '\\s*=.*$', 'm'));
    assert.ok(m, n + ' sabiti olmali');
    return m[0];
  });
  const i = server.indexOf('function v688AralikPuani');
  const son = server.indexOf('\nfunction r486EntryTruthGuard', i + 10);
  const ctx = { process: { env: {} }, Math, Number, String, Object };
  vm.createContext(ctx);
  vm.runInContext(satir.join('\n') + '\n' + server.slice(i, son) + '\nglobalThis.f = v678GrafikKalitesi;', ctx);
  return ctx.f;
}

const hik = (dip) => ({
  tf: { '15m': { rangePos: 0.35, ret6: 0.5, trend: 'RANGE_MIXED', trendline: {}, altDip: dip } },
  liquidity: { below: 100, belowType: 'SWING_LOW', belowDist: 2.0 }
});

test('V689: sabitler olculen degerlerde', () => {
  assert.match(server, /const V689_FITIL_ESIK   = Math\.max\(0, Math\.min\(1, Number\(process\.env\.V689_FITIL_ESIK \|\| 0\.10\)\)\);/);
  // V690: 5 -> 0. Ortusmeyen/kume-saglam testte t=0,49 (esik 2,28) — kanit yok.
  assert.match(server, /const V689_PUAN         = Math\.max\(0, Math\.min\(30, Number\(process\.env\.V689_PUAN \|\| 0\)\)\);/);
});

test('V689: yapi motoru en yakin alt dibin fitilini artik olcuyor', () => {
  const i = server.indexOf('V689 ═══ en yakin ALTTAKI pivot dibin');
  assert.ok(i > 0, 'r484Structure icinde hesaplanmali');
  const blok = server.slice(i, i + 700);
  assert.ok(blok.includes('(Math.min(b.o,b.c)-b.l)/_rg'), 'alt fitil orani hesaplanmali');
  assert.ok(server.includes('altDip:_v689'), 'tf durumuna eklenmeli');
});

test('V690: fitilsiz dip OLCULUYOR ama agirligi 0 (kanit yetmedi)', () => {
  const f = yukle();
  const fitilsiz = f(hik({ uzaklikAtr: 1.4, fitilOrani: 0.02 }), 2.0);
  const normal   = f(hik({ uzaklikAtr: 1.4, fitilOrani: 0.45 }), 2.0);
  assert.strictEqual(normal.fitilsizDip, null, 'fitilli dip isaretlenmez');
  assert.ok(fitilsiz.fitilsizDip, 'fitilsiz dip RAPORLANMAYA devam ediyor');
  assert.strictEqual(fitilsiz.fitilsizDip.puan, 0, 'V690: agirlik 0');
  assert.strictEqual(+(fitilsiz.skor - normal.skor).toFixed(1), 0, 'skora etkisi yok');
});

test('V689: uzaktaki dip odul almaz (olcum yalniz <5 ATR bandinda)', () => {
  const f = yukle();
  const uzak = f(hik({ uzaklikAtr: 9.0, fitilOrani: 0.01 }), 2.0);
  assert.strictEqual(uzak.fitilsizDip, null, '5 ATR uzerinde odul yok');
});

test('V689: veri yoksa terim sessiz (fail-open)', () => {
  const f = yukle();
  const k = f(hik(null), 2.0);
  assert.strictEqual(k.fitilsizDip, null);
  assert.ok(k.ok && k.skor > 0);
});

test('V689: REDDEDILEN kitap kurallari koda GIRMEDI', () => {
  // "sadece ilk dokunus" kurali: olcum tam tersini soyluyor, kod bunu uygulamamali
  assert.ok(!/ILK_DOKUNUS_KURALI|firstTouchOnly|ilkDokunusZorunlu/i.test(server), 'ilk-dokunus kapisi olmamali');
  assert.ok(!/V689_ABSORPSIYON|absorptionScore/i.test(server), 'absorpsiyon terimi olmamali (n=73)');
});
