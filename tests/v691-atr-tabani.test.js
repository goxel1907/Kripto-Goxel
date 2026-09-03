'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT — EGLD LONG, 3 Eylul 2026 10:33 -> 11:40, SL ile kapandi, ROI -%26,0.
//   giris 5,3370 · SL 5,17311 · cikis 5,1377 (planli stopun %0,68 altinda kayma)
//   ilk engel 5,345857 = girisin yalniz %0,166 ustunde
//   ATR (1h) %5,30 · kaldirac 7x
//
// HATA ZINCIRI (kod, kendi sayilariyla):
//   V662 engel tavani = 0,166 / 0,42 = %0,395
//   ATR gerekli       = 5,30 x 2,35  = %12,46
//   likidasyon tavani = (100/7)x0,90 = %12,86
//   minStopPct = max(0,80 ; MIN(12,86 ; 12,46 ; 0,395)) = %0,80   <-- ATR tabani EZILDI
//   gercek stop %3,07 > %0,80  ->  tightStop = false  ->  genisletme CALISMADI
//   %3,07 / %5,30 = 0,58 x ATR  ->  gurultunun icinde bir stop
//
// Hikayenin KENDISI zaten "PUSU" diyordu (5,194-5,223 bolgesine kuruldu) ve
// "R486 giris sozlesmesi MARKET'i engelledi" yaziyordu; buna ragmen 5,3370'ten
// market girildi.
//
// OLCUM (879 grafik, ortusmeyen 6.153 nokta, TP %9 sabit, stop k x ATR):
//   k=0,5 -> stop yeme %83,1 · k=1 -> %67,9 · k=2 -> %42,5 · k=4 -> %14,6
//   beklenti(R): 0,064 / 0,073 / 0,116 / 0,165  (monoton)
// Bu tablo YENI kural icin degil: botun ZATEN sahip oldugu R486_MIN_STOP_ATR
// kuralinin neden onemli oldugunu gosteriyor. V691 yeni kapi acmiyor.

const EGLD = { entry: 5.3370, sl: 5.17311, engel: 5.345857142857143, atrPct: 5.30, lev: 7 };

function hesap(o) {
  const MIN_STOP_ATR = 2.35, LIK_PAYI = 0.90, HEDEF_RR = 0.42, RISK_PCT = 23;
  const d = (o.engel - o.entry) / o.entry * 100;
  const engelTavan = d / HEDEF_RR;
  const gereken = o.atrPct * MIN_STOP_ATR;
  const likTavan = (100 / o.lev) * LIK_PAYI;
  const stopPct = (o.entry - o.sl) / o.entry * 100;
  return {
    d, engelTavan, gereken, likTavan, stopPct,
    eski: Math.max(0.80, Math.min(likTavan, gereken, engelTavan)),
    yeni: Math.max(0.80, Math.min(likTavan, gereken)),
    odenebilir: RISK_PCT / o.lev,
  };
}

test('V691: EGLD aritmetigi — engel tavani ATR tabanini gercekten ezmis', () => {
  const h = hesap(EGLD);
  assert.ok(Math.abs(h.d - 0.166) < 0.01, `engel mesafesi %${h.d.toFixed(3)}`);
  assert.ok(Math.abs(h.gereken - 12.46) < 0.05, `ATR gerekli %${h.gereken.toFixed(2)}`);
  assert.ok(Math.abs(h.eski - 0.80) < 0.01, `eski taban %${h.eski.toFixed(2)} — 0,80'e cokmus olmali`);
  assert.ok(Math.abs(h.yeni - 12.46) < 0.05, `yeni taban %${h.yeni.toFixed(2)}`);
  // Eski mantikta stop tabani asiyor -> hic genisletilmiyor
  assert.ok(h.stopPct > h.eski, 'eski: tightStop=false (bug)');
  // Yeni mantikta taban altinda -> genisletme/reddetme tetiklenir
  assert.ok(h.stopPct < h.yeni, 'yeni: tightStop=TRUE');
});

test('V691: stop gurultunun icindeydi (0,58 x ATR)', () => {
  const h = hesap(EGLD);
  const kAtr = h.stopPct / EGLD.atrPct;
  assert.ok(kAtr < 0.7, `stop ${kAtr.toFixed(2)} x ATR — olcumde bu bant %83 stop yeme demek`);
});

test('V691: sozlesme bu stopu odeyemiyordu (rapor, YENI KAPI DEGIL)', () => {
  // odenebilir stop = risk% / ((marj/ozsermaye) x kaldirac).
  // 50$/100$ · 7x · %23 risk -> %6,57 stop -> max ATR(1s) %2,80.
  // EGLD ATR %5,30 bunun cok ustundeydi.
  // NOT: bu bir KAPI DEGIL. Marj/ozsermaye orani entry-truth noktasinda bilinmiyor,
  // o yuzden orada kapi kurmadim; acilista TEK SATIR olarak yazdiriliyor ve
  // tightStop zaten mevcut yoldan erken-risk uretiyor.
  const odenebilir = 23 / (0.50 * EGLD.lev);
  const maxAtr = odenebilir / 2.35;
  assert.ok(Math.abs(odenebilir - 6.571) < 0.01, `odenebilir %${odenebilir.toFixed(2)}`);
  assert.ok(Math.abs(maxAtr - 2.796) < 0.01, `max ATR %${maxAtr.toFixed(2)}`);
  assert.ok(EGLD.atrPct > maxAtr, 'EGLD sinirin disindaydi');
  assert.ok(server.includes('STOPLA KORUNARAK ALINABILIR MAKSIMUM ATR'),
    'acilista tek satir olarak yazdirilmali');
});

test('V691: kodda taban artik engel tavaniyla EZILMIYOR', () => {
  assert.ok(!/minStopPct\s*=\s*Math\.max\(\.80,\s*Math\.min\(_v660LikTavan,_v660Gereken,_v662Tavan\)\)/.test(server),
    'eski uc-yollu Math.min kalmamali');
  assert.ok(server.includes('const _v691Taban = Math.max(.80, Math.min(_v660LikTavan, _v660Gereken));'),
    'taban yalniz likidasyon ve ATR den turemeli');
  assert.match(server, /const V691_ATR_TABANI_KORUNUR = String\(process\.env\.V691_ATR_TABANI_KORUNUR \?\? '1'\) !== '0';/);
});

test('V691: YENI KAPI EKLENMEDI — mevcut tightStop yolu yeter', () => {
  // Ilk yazimda marketAllowed'a yeni bir kapi koymustum ve odenebilirligi kaldiraca
  // bolmustum (marji unutarak) — 2 kat fazla sikiydi. Geri aldim.
  assert.ok(server.includes('_v649BolgeUstu||_v660Imkansiz);'),
    'marketAllowed listesi V691 ONCESI haliyle kalmali');
  assert.ok(!server.includes('_v660Imkansiz||_v691Karsilanamaz)'), 'yeni kapi olmamali');
  assert.ok(server.includes('marj/ozsermaye'), 'neden kapi kurulmadigi kodda yazili olmali');
});

test('V691: eski davranis ENV ile geri alinabilir (fail-open kacis)', () => {
  assert.ok(server.includes("process.env.V691_ATR_TABANI_KORUNUR"), 'anahtar olmali');
  assert.ok(server.includes("process.env.V691_KARSILANAMAZ_PUSU"), 'anahtar olmali');
  assert.ok(server.includes('Math.min(_v660LikTavan, _v660Gereken, _v662Tavan)'), 'eski yol korunmali');
});
