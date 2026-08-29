'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ── YAMA 1: basa-bas esigi backtestin geo carpanini kullanir ────────────────
// Backtest (h6tf_core.py:182):  be_thr = max(.8*r390, .65) * clamp(slPct/1.7, 1, 3)
// Canlida geo YOKTU -> BE 3 kat erken kuruluyordu (MAGMA %1,28 · ROBO +0,71$).

test('V646: basa-bas esigi geo carpanini ve 0.65 tabanini icerir', () => {
  assert.match(server, /const V646_BE_GEO = String\(process\.env\.V646_BE_GEO \?\? '1'\) !== '0';/);
  assert.match(server, /const _v646Geo = _v646Sl > 0 \? Math\.max\(1, Math\.min\(3, _v646Sl \/ 1\.7\)\) : 1;/);
  assert.match(server, /const breakEvenAt = V646_BE_GEO \? Math\.max\(_v646BeTaban, 0\.65\) \* _v646Geo : _v646BeTaban;/);
});

test('V646/V648: stop genisligi gercek fiyatlardan turetilir, oncelik PLAN stopu', () => {
  // V646: state.slPct'e guvenilmez (o yol geo'yu 1'de birakip yamayi no-op yapardi).
  // V648: oncelik initialSL (plan stopu) — backtestte slpct islem boyunca SABIT.
  assert.match(server, /_v646Sl = \(_e - _s\) \/ _e \* 100; _v646Kaynak = \(_s0 > 0 \? 'initialSL' : 'currentSL'\);/);
  assert.match(server, /const _s = _s0 > 0 \? _s0 : \(Number\(state\?\.currentSL\) \|\| 0\);/);
  assert.match(server, /else if \(Number\(cfg\.slPct\) > 0\)/);
});

test('V646: BE esigi her pozisyonda loglanir (sessiz no-op tespiti icin)', () => {
  assert.match(server, /V646 BE esigi/);
  assert.match(server, /geo \$\{_v646Geo\.toFixed\(2\)\}/);
});

test('V646: geo matematigi backtestle ayni sonucu verir', () => {
  const geo = (slPct) => Math.max(1, Math.min(3, slPct / 1.7));
  const be = (taban, slPct) => Math.max(taban, 0.65) * geo(slPct);
  // plan SL %5,5 · r390K=1 · aiBE varsayilan 0,8  -> backtest %2,40
  assert.ok(Math.abs(be(0.8, 5.5) - 2.4) < 0.001, 'SL %5,5 -> BE %2,40 olmali');
  // dar stop: geo tabanda kalir, eski davranisla ayni
  assert.ok(Math.abs(be(0.8, 1.7) - 0.8) < 1e-9);
  // geo 3'te tavanlanir
  assert.ok(Math.abs(be(0.8, 99) - 2.4) < 1e-9, 'geo 3te tavanlanmali');
  // eski canli davranis (geo yok) cok daha erken kurardi
  assert.ok(0.8 < be(0.8, 5.5), 'eski esik yeni esikten kucuk olmali');
});

// ── YAMA 2: backtestte DE zarar eden imzalar elenir ─────────────────────────
// SWEEP_RECLAIM_HIGH_ATR  n=16  -7,119$/islem   (canli: PF 0,74 · n=24)
// CEKIC_ALT_FITIL         n=25  -2,163$/islem

test('V646: zayif kurulum listesi tanimli ve filtre aday listesine uygulaniyor', () => {
  assert.match(server, /const V646_ZAYIF_KURULUMLAR = Object\.freeze\(\['SWEEP_RECLAIM_HIGH_ATR','CEKIC_ALT_FITIL'\]\);/);
  assert.match(server, /r481Uygun = r481Uygun\.filter\(a => !V646_ZAYIF_KURULUMLAR\.includes\(a\.tip\)\);/);
});

test('V646: eleme k24 filtresinden SONRA gelir (mevcut PATLAMA kurali bozulmaz)', () => {
  const k24Idx = server.indexOf("a.tip !== 'PATLAMA'");
  const eleIdx = server.indexOf('V646_ZAYIF_KURULUMLAR.includes(a.tip)');
  assert.ok(k24Idx > 0 && eleIdx > 0, 'iki filtre de bulunmali');
  assert.ok(eleIdx > k24Idx, 'V646 elemesi k24 filtresinden sonra olmali');
});

test('V646: eleme env ile geri alinabilir', () => {
  assert.match(server, /const V646_ZAYIF_ELEME = String\(process\.env\.V646_ZAYIF_ELEME \?\? '1'\) !== '0';/);
});

test('V646: karli kurulumlar listede DEGIL', () => {
  const m = server.match(/const V646_ZAYIF_KURULUMLAR = Object\.freeze\(\[([^\]]*)\]\)/);
  assert.ok(m, 'liste bulunmali');
  const liste = m[1];
  for (const karli of ['KURU_SONRASI_SPIKE', 'CIFT_DIP', 'MOMENTUM_KIRILIMI', 'TEMIZ_KAPANIS', 'ARDISIK_HH', 'PATLAMA']) {
    assert.ok(!liste.includes(karli), `${karli} backtestte karli — elenmemeli`);
  }
});

// ── sozlesme korunuyor mu ───────────────────────────────────────────────────
test('V646: risk sozlesmesi ve onceki surumlerin duzeltmeleri yerinde', () => {
  assert.match(server, /const LAZARUS_BUILD = 'V6_4_8_EN_YAKIN_ENGEL_PIVOT'/);
  assert.match(server, /V637_PUSU_R495E_DEVRET/);   // PUSU -> R495 devri
  assert.match(server, /V634_TOPLAM_RISK_PCT/);      // 1 poz x %8 sozlesmesi
  assert.match(server, /V628_ATESLEME_KOPRUSU/);     // retestBelowStop koprusu
});
