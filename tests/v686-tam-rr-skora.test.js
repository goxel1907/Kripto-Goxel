'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// OLCUM — 88 canli islem (lazarus_v592_testnet_research_dataset_v3_45.csv),
// entryReason metninden ayristirildi:
//
//   tam R/R      <-> ROI   r = +0,343   <- en guclu tekil isaret
//   skor         <-> ROI   r = +0,179
//   ILK ENGEL R/R<-> ROI   r = -0,016   <- SIFIR; ve bot KAPIYI BURAYA KOYUYOR
//   SKOR         <-> tam R/R  r = +0,146  <- skor bu sayiyi duymuyordu
//
//   tam R/R < 4 : n=34  WR %35  ort ROI -10,21%  net -164,1$
//   tam R/R >= 4: n=54  WR %50  ort ROI  +0,67%  net  +32,8$
//   (oturumun TUM zarari R/R<4'te; en buyuk kayip cikarilinca da duruyor: -131,0$)
//
// CANLI ORNEK — 4USDT ucluSU:
//   +8,64%  tam R/R 5,39  skor 71
//   +17,18% tam R/R 6,10  skor 62
//   -55,85% tam R/R 1,68  skor 78   <- EN YUKSEK SKOR, EN DUSUK R/R
//
// UYARI: iliski verinin ilk yarisinda tersine donuyor (1.yari R/R>=4 net -37,9$).
// Bu yuzden SERT KAPI degil KADEMELI ceza kondu.

function skorFn() {
  const isim = ['V680_PARABOLIK_CEZA','V680_RET6_TABAN','V680_CEZA_TAVAN','V680_CEP_RET6','V680_CEP_CEZA',
                'V686_TAM_RR_SKORA','V686_RR_ESIK','V686_RR_CEZA_TAVAN',
                'V688_ARALIK_DUZELT','V688_DESTEK_OKUMA','V688_EQL_PUAN','V688_DESTEKSIZ_CEZA'];
  const satir = isim.map(n => {
    const m = server.match(new RegExp('^const\\s+' + n + '\\s*=.*$', 'm'));
    assert.ok(m, n + ' sabiti olmali');
    return m[0];
  });
  const i = server.indexOf('function v688AralikPuani');   // V688 yardimcilari da gerekli
  const son = server.indexOf('\nfunction r486EntryTruthGuard', i + 10);
  const ctx = { process: { env: {} }, Math, Number, String, Object };
  vm.createContext(ctx);
  vm.runInContext(satir.join('\n') + '\n' + server.slice(i, son) + '\nglobalThis.f = v678GrafikKalitesi;', ctx);
  return ctx.f;
}
const hik = (rr, rp = 0.5, ret6 = 0) =>
  ({ tf: { '15m': { rangePos: rp, ret6, trend: 'RANGE_MIXED', trendline: {} } },
     entryTruth: { fullRR: rr } });

test('V686: sabitler olculen degerlerde', () => {
  assert.match(server, /const V686_RR_ESIK = Math\.max\(1, Math\.min\(10, Number\(process\.env\.V686_RR_ESIK \|\| 4\)\)\);/);
  assert.match(server, /const V686_RR_CEZA_TAVAN = Math\.max\(0, Math\.min\(40, Number\(process\.env\.V686_RR_CEZA_TAVAN \|\| 18\)\)\);/);
});

test('V686: dusuk tam R/R skoru dusuruyor (4USDT kaybedeni)', () => {
  const f = skorFn();
  const kotu = f(hik(1.68), 2.0);   // -55,85% olan islem
  const iyi  = f(hik(6.10), 2.0);   // +17,18% olan islem
  assert.ok(kotu.rrCezasi > 0, 'R/R 1,68 cezalanmali');
  assert.strictEqual(iyi.rrCezasi, 0, 'R/R 6,10 cezalanmamali');
  assert.ok(kotu.skor < iyi.skor, `${kotu.skor} < ${iyi.skor} olmali`);
  assert.strictEqual(kotu.tamRR, 1.68, 'tam R/R disa aktarilmali');
});

test('V686: ceza kademeli ve tavanli — sert kapi DEGIL', () => {
  const f = skorFn();
  const c = rr => f(hik(rr), 2.0).rrCezasi;
  assert.ok(c(1) > c(2) && c(2) > c(3) && c(3) > c(3.9), 'ceza monoton azalmali');
  assert.strictEqual(c(4), 0, 'esikte ceza bitmeli');
  assert.strictEqual(c(9), 0, 'esik ustunde ceza yok');
  assert.ok(c(0.1) <= 18, 'tavan asilmamali');
});

test('V686: R/R yoksa fail-open (eski davranis)', () => {
  const f = skorFn();
  const k = f({ tf: { '15m': { rangePos: 0.5, ret6: 0, trend: 'RANGE_MIXED', trendline: {} } } }, 2.0);
  assert.strictEqual(k.rrCezasi, 0);
  assert.strictEqual(k.tamRR, null);
});

test('V686: hikayenin KENDI hesabini okuyor, yeniden hesaplamiyor', () => {
  const i = server.indexOf('V686 ═══ TAM R/R: hikaye');
  assert.ok(i > 0);
  const blok = server.slice(i, i + 700);
  assert.ok(blok.includes("story?.entryTruth?.fullRR"), 'entryTruth.fullRR okunmali');
});
