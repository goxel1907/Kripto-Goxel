'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// OLCUM — 879 gercek 15m grafik / 36.918 ilk-dokunus noktasi, ileri 24 mum,
// uc geometri (SL/TP: 3/9, 2/5, 4/12), beklenti R-katsayisi cinsinden.
//
// V678'in en buyuk ARTI terimi "fiyat araligin tepesinde" (+18,4 puana kadar).
// Olcum bu terimin KOSULLU oldugunu gosterdi:
//   rangePos>=%85 & son 6 mum (90dk) getirisi <%3   -> +0,314  (n=3.584)
//   rangePos>=%85 & son 6 mum getirisi %3-8         -> +0,177  (n=1.618)
//   rangePos>=%85 & son 6 mum getirisi %8-15        -> -0,084  (n=  365)
//   rangePos>=%85 & son 6 mum getirisi >=%15        -> -0,250  (n=  191)
// Yani tepede olmak, oraya YAVAS gelindiyse iyi; DIKEY gelindiyse zarar.
//
// CANLI KANIT (AKEUSDT, 02.09.2026 17:14): rangePos %96, ATR %3,81,
// son 90dk ~%28 dikey. V678 skoru 53,8 -> GECTI. 15 dakika sonra fiyat
// 0,0110'dan 0,0087'ye dustu (-%21). Yeni skor 25,8 -> ELENIR.

function yukle() {
  // V686 fonksiyona tam R/R terimini ekledi; sabitleri de yuklenmeli
  const isim = ['V680_PARABOLIK_CEZA','V680_RET6_TABAN','V680_CEZA_TAVAN','V680_CEP_RET6','V680_CEP_CEZA',
                'V686_TAM_RR_SKORA','V686_RR_ESIK','V686_RR_CEZA_TAVAN',
                'V688_ARALIK_DUZELT','V688_DESTEK_OKUMA','V688_EQL_PUAN','V688_DESTEKSIZ_CEZA'];
  const satir = isim.map(n => {
    const re = new RegExp('^const\\s+' + n + '\\s*=.*$', 'm');
    const m = server.match(re);
    assert.ok(m, n + ' sabiti bulunmali');
    return m[0];
  });
  const i = server.indexOf('function v688AralikPuani');   // V688 yardimcilari da gerekli
  assert.ok(i > 0);
  const son = server.indexOf('\nfunction r486EntryTruthGuard', i + 10);
  const ctx = { process: { env: {} }, Math, Number, String, Object };
  vm.createContext(ctx);
  vm.runInContext(satir.join('\n') + '\n' + server.slice(i, son) +
    '\nglobalThis.f = v678GrafikKalitesi;', ctx);
  return ctx.f;
}

const hikaye = (rp, ret6, atr = 2.0, trend = 'COMPRESSION') =>
  ({ tf: { '15m': { rangePos: rp, ret6, trend, trendline: {} } } });

test('V680: sabitler olculen degerlerde', () => {
  assert.match(server, /const V680_RET6_TABAN = Math\.max\(0, Math\.min\(20, Number\(process\.env\.V680_RET6_TABAN \|\| 4\)\)\);/);
  assert.match(server, /const V680_CEP_RET6   = Math\.max\(3, Math\.min\(50, Number\(process\.env\.V680_CEP_RET6 \|\| 10\)\)\);/);
  assert.match(server, /const V680_CEP_CEZA   = Math\.max\(0, Math\.min\(60, Number\(process\.env\.V680_CEP_CEZA \|\| 25\)\)\);/);
});

test('V680: AKE anI artik ELENIYOR (canli kanit)', () => {
  const f = yukle();
  const k = f(hikaye(0.957, 28), 3.81);
  assert.ok(k.ok, 'skor uretilmeli');
  assert.ok(k.skor < 40, `AKE skoru ${k.skor} — 40 esiginin ALTINDA olmali`);
  assert.ok(k.parabolikCeza >= 25, 'cepte sert ceza uygulanmali');
  assert.strictEqual(k.ret6, 28, 'ret6 disa aktarilmali');
});

test('V680: YAVAS gelinen tepe cezalandirilmiyor (olcum: +0,314)', () => {
  const f = yukle();
  const k = f(hikaye(0.957, 1.5), 2.0);
  assert.strictEqual(k.parabolikCeza, 0, 'ret6 dusukken ceza YOK');
  // V688: eski (rp-0,5)*40 terimi bu adaya +18,3 veriyordu; olcum ise ret6<3 kontrolunde
  // 95+ bandini 1,099 (alt-ort 0,943) gosteriyor -> +6. Skor 56: esigin (40) rahat ustunde.
  assert.ok(k.skor >= 50, `yavas tepe skoru ${k.skor} — esigin ustunde kalmali`);
});

test('V680: ceza ret6 ile birlikte artiyor ve tavanla sinirli', () => {
  const f = yukle();
  const c = r6 => f(hikaye(0.80, r6), 2.0).parabolikCeza;
  assert.strictEqual(c(3), 0, 'taban altinda ceza yok');
  assert.ok(c(10) > c(6) && c(6) > c(4), 'ceza monoton artmali');
  assert.ok(c(100) <= 28, 'tavan asilmamali');
});

test('V680: aralik ortasinda (rangePos<.70) ceza yok — kural KOSULLU', () => {
  const f = yukle();
  assert.strictEqual(f(hikaye(0.55, 20), 2.0).parabolikCeza, 0);
});

test('V680: ret6 yoksa fail-open (eski davranis)', () => {
  const f = yukle();
  const k = f(hikaye(0.90, undefined), 2.0);
  assert.strictEqual(k.parabolikCeza, 0);
  assert.strictEqual(k.ret6, null);
});

test('V680: hikaye geriden gelen yapi etiketini soyluyor', () => {
  const i = server.indexOf('function v679Hikaye');
  const govde = server.slice(i, server.indexOf('// ═══ V678 ═══ Grafik kalite kapisi', i));
  assert.ok(govde.includes('DIKEY uzama'), 'dikey uzama cumlesi olmali');
  assert.ok(govde.includes('HENUZ GORMEDI'), 'yapi etiketinin geriden geldigi soylenmeli');
});

test('V680: kayit ret6 ve cezayi tasiyor', () => {
  const i = server.indexOf('v679Kaydet({symbol:coin.symbol');
  const blok = server.slice(i, i + 900);
  assert.ok(blok.includes('ret6:_k680?.ret6??null'), 'ret6 kaydedilmeli');
  assert.ok(blok.includes('parabolikCeza:_k680?.parabolikCeza??0'), 'ceza kaydedilmeli');
});
