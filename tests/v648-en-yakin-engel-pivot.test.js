'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ══ YAMA A — V647-A OLU KODDU: gate'e stop gecilmiyordu ════════════════════
// Bu testler, sessiz olu kodu yakalayan REGRESYON testleridir.

test('V648-A: r493EntrySafetyGate cagrisina recommendedSl VE sl gecilir', () => {
  const m = server.match(/const r493EntrySafety=r493EntrySafetyGate\((.*?)\);\r?\n/);
  assert.ok(m, 'gate cagrisi bulunmali');
  const cagri = m[1];
  assert.ok(cagri.includes('recommendedSl'), 'entryTruth icinde recommendedSl olmali');
  assert.ok(cagri.includes('originalSl:sl'),  'entryTruth icinde originalSl olmali');
  assert.ok(/\{side,entry:[^}]*sl:recommendedSl\}/.test(cagri), 'opts icinde sl gecilmeli');
});

test('V648-A: hafiza dalinin okudugu her alan cagri tarafinda mevcut', () => {
  // Dal su siralamayi okur: entryTruth.plannedEntry|originalEntry|opts.entry
  //                          entryTruth.recommendedSl|originalSl|opts.sl
  // Cagrida bunlardan EN AZ BIRI bulunmazsa dal sessizce olur.
  const m = server.match(/const r493EntrySafety=r493EntrySafetyGate\((.*?)\);\r?\n/);
  const cagri = m[1];
  const girisKaynak = ['plannedEntry', 'originalEntry', 'entry:'].some(k => cagri.includes(k));
  const stopKaynak  = ['recommendedSl', 'originalSl', 'sl:'].some(k => cagri.includes(k));
  assert.ok(girisKaynak, 'giris kaynagi gecilmeli');
  assert.ok(stopKaynak,  'stop kaynagi gecilmeli — V647-A tam burada oluyordu');
});

// ══ YAMA B — en YAKIN engel kazanir ═════════════════════════════════════════

test('V648-B: hafiza dali artik !_v647Known ile KAPATILMIYOR', () => {
  assert.ok(!/if \(V647_HAFIZA_ENGEL && !_v647Known && candidateMemory\?\.available\)/.test(server),
    'eski "yalniz hikaye korse" sarti kalmamali');
  assert.match(server, /if \(V647_HAFIZA_ENGEL && candidateMemory\?\.available\)/);
});

test('V648-B: en kucuk R/R kazanir (en yakin engel)', () => {
  assert.match(server, /\(!_v647Known \|\| \(V648_EN_YAKIN_ENGEL && _rr < _v647RR\)\)/);
});

test('V648-B: hikaye kaynagi da etiketlenir (HIKAYE)', () => {
  assert.match(server, /_v647Kaynak = firstKnown \? 'HIKAYE' : null;/);
});

test('V648-B: "15m hafizadan" metinleri yalniz gercek hafiza kaynaginda yazilir', () => {
  // 'HIKAYE' etiketi de truthy oldugu icin eski `_v647Kaynak?` kontrolleri
  // her gecise yanlislikla "hafizadan" yazardi.
  assert.ok(!/\$\{_v647Kaynak\?' \(15m hafiza\)':''\}/.test(server));
  assert.ok(!/\$\{_v647Kaynak\?' \(ilk engel 15m hafızadan\)':''\}/.test(server));
  assert.match(server, /if\(_v647Kaynak==='15M_HAFIZA_TARGET1'\)\{ try\{ logAuto\(/);
});

test('V648-B: KOMAUSDT matematigi — hafiza engeli isleme izin VERMEZDI', () => {
  const e = 0.015132, s = 0.01477018512;     // canli giris / plan stopu
  const hikayeRR = 2.40;                      // 0.01645 aralik tepesi (+%8,89)
  const t1 = 0.015167;                        // 15m hafiza TP1 (+%0,23)
  const hafizaRR = (t1 - e) / (e - s);
  const ESIK = 0.35;
  assert.ok(hafizaRR < hikayeRR, 'hafiza engeli daha yakin olmali');
  const secilen = Math.min(hikayeRR, hafizaRR);   // V648 kurali
  assert.ok(Math.abs(secilen - 0.0967) < 0.005, 'secilen R/R ~0,097 olmali');
  assert.ok(secilen < ESIK, 'V648 ile ENGELLENIRDI');
  assert.ok(hikayeRR >= ESIK, 'V647 ile gectigi dogrulanmali (yasanan durum)');
});

// ══ YAMA C — swing-high pivot engel adayi ═══════════════════════════════════

test('V648-C: swing-high pivotlar engel adaylarina eklenir', () => {
  assert.match(server, /const V648_PIVOT_ENGEL = String\(process\.env\.V648_PIVOT_ENGEL \?\? '1'\) !== '0';/);
  assert.match(server, /for \(const _pv of \(tfRow\.structure\?\.pivots\?\.high \|\| \[\]\)\) add\(Number\(_pv\?\.price\), 'SWING_HIGH_PIVOT', 'WEAK'\);/);
});

test('V648-C: pivot eklemesi r49356Obstacle icinde ve VAH satirindan SONRA', () => {
  const obsIdx   = server.indexOf('function r49356Obstacle');
  const vahIdx   = server.indexOf("add(tfRow.profile?.vah,'VAH','WEAK');");
  const pivIdx   = server.indexOf("'SWING_HIGH_PIVOT'");
  const sortIdx  = server.indexOf('c.sort((a,b)=>a.price-b.price)');
  assert.ok(obsIdx > 0 && vahIdx > obsIdx, 'VAH satiri r49356Obstacle icinde olmali');
  assert.ok(pivIdx > vahIdx, 'pivot eklemesi VAH sonrasi olmali');
  assert.ok(sortIdx > pivIdx, 'pivotlar SIRALAMADAN once eklenmeli — yoksa en yakin secilemez');
});

test('V648-C: pivot verisi tfRow icinde gercekten uretiliyor (no-op degil)', () => {
  // r49356PerTf structure.pivots.high'i r484Structure'dan doldurur.
  assert.match(server, /pivots:\{high:r49356SafeArray\(st\.pivots\?\.H\)\.slice\(-12\)/);
  // r484Structure pivots'u r483Pivots'tan alir ve elemanlarin .price alani vardir.
  assert.match(server, /let p=r483Pivots\(rs,rs\.length>=14\?2:1\);/);
  assert.match(server, /pivots:p\}/);
});

// ══ YAMA D — geo plan stopundan (backtest gibi sabit) ═══════════════════════

test('V648-D: geo initialSL (plan stopu) uzerinden hesaplanir', () => {
  assert.match(server, /const V648_GEO_DONDUR = String\(process\.env\.V648_GEO_DONDUR \?\? '1'\) !== '0';/);
  assert.match(server, /const _s0 = V648_GEO_DONDUR \? \(Number\(state\?\.initialSL\) \|\| 0\) : 0;/);
  assert.match(server, /const _s = _s0 > 0 \? _s0 : \(Number\(state\?\.currentSL\) \|\| 0\);/);
  assert.match(server, /_v646Kaynak = \(_s0 > 0 \? 'initialSL' : 'currentSL'\);/);
});

test('V648-D: state.initialSL pozisyon acilisinda GERCEKTEN yaziliyor', () => {
  // V646'da state.slPct tuzagina dusmustuk; ayni hatayi tekrarlamamak icin
  // alanin yazildigi yeri test ediyoruz.
  assert.match(server, /_stOpen\.initialSL = Number\(_stOpen\.initialSL \|\| stopPrice \|\| _ai\.sl \|\| 0\) \|\| null;/);
});

test('V648-D: KOMAUSDT geo matematigi', () => {
  const geo = sl => Math.max(1, Math.min(3, sl / 1.7));
  const planSl = 2.391, kayanSl = 1.903;
  assert.ok(Math.abs(geo(planSl) - 1.4065) < 0.001, 'plan stopu geo 1,4065');
  assert.ok(Math.abs(geo(kayanSl) - 1.1196) < 0.001, 'kayan stop geo 1,1196');
  const erken = 1 - geo(kayanSl) / geo(planSl);
  assert.ok(erken > 0.20 && erken < 0.21, 'BE %20,4 erken kuruluyordu');
});

// ══ YAMA E — sessiz no-op alarmi ════════════════════════════════════════════

test('V648-E: hafiza var ama giris/stop yoksa UYARI loglanir', () => {
  assert.match(server, /\} else if \(!\(_e > 0 && _s > 0\)\) \{/);
  assert.match(server, /V648 hafiza engeli HESAPLANAMADI/);
});

// ══ sozlesme korunuyor mu ═══════════════════════════════════════════════════

test('V648: surum adi ve onceki surumlerin duzeltmeleri yerinde', () => {
  assert.match(server, /V6_4_8_EN_YAKIN_ENGEL_PIVOT/);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);      // 1 poz x %8
  assert.match(server, /V637_PUSU_R495E_DEVRET/);    // PUSU -> R495
  assert.match(server, /V646_ZAYIF_KURULUMLAR/);     // zayif kurulum elemesi
  assert.match(server, /V647_BOLGE_GIRISI/);         // bolge girisi retest kaniti
  assert.match(server, /V628_ATESLEME_KOPRUSU/);
});

test('V648: uc yama da env ile geri alinabilir', () => {
  for (const bayrak of ['V648_EN_YAKIN_ENGEL', 'V648_PIVOT_ENGEL', 'V648_GEO_DONDUR']) {
    assert.ok(new RegExp(`process\\.env\\.${bayrak} \\?\\? '1'`).test(server), `${bayrak} geri alinabilir olmali`);
  }
});
