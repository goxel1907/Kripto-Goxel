'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KULLANICI SORUSU: "btc ve eth ye islem aciyor neden"
//
// CANLI KANIT (panel logu):
//   "PATLAMA 5M TEYIT: BTC · skor 28 · 1m hacim 0.13x · mum %0.031 · delta -37.5
//    · 5m ters degil — oncelikli grafik hikayesine gonderildi"
//   "PATLAMA 5M TEYIT: SUI · skor 27 ... oncelikli grafik hikayesine gonderildi"
//
// SEBEP: kod dev-coin listesini ZATEN tanimliyordu (`excludedCore`, satir ~26556)
// ama YALNIZCA fallback yolunda kullaniyordu (tek referans: fallbackCore filtresi).
// Worker evreni (R328 patlama / R370 / R385 / R366) hicbir yerde uygulamiyordu.
// Worker evreninin tek filtresi R4863_STABLE_OR_INDEX_BASES idi — icinde BTC/ETH YOK,
// yalniz stablecoin ve endeks urunleri (USDC, BTCDOM, DEFI...).
//
// Bu YENI politika degil: kodun KENDI beyan ettigi niyeti isler hale getiriyor.

test('V696: dev coin listesi modul seviyesinde ve worker evrenine uygulaniyor', () => {
  assert.match(server, /const V696_DEV_COIN_HARIC = String\(process\.env\.V696_DEV_COIN_HARIC \?\? '1'\) !== '0';/);
  assert.match(server, /const V696_DEV_COIN_BASES = new Set\(String\(process\.env\.V696_DEV_COIN_LISTE/);
  for (const b of ['BTC','ETH','BNB','XRP','SOL','DOGE'])
    assert.ok(server.includes(b + ','), b + ' listede olmali');
});

test('V696: filtre worker evren zincirinde — enjeksiyonun TEK gecidi', () => {
  const i = server.indexOf('V696 ═══ dev coinler burada eleniyor');
  assert.ok(i > 0, 'filtre worker evreninde olmali');
  const blok = server.slice(i, i + 500);
  assert.ok(blok.includes('V696_DEV_COIN_BASES.has('), 'liste kontrol edilmeli');
  assert.ok(blok.includes("excluded.push(b+'(dev)')"), 'elenenler panelde gorunmeli');
  // Filtre, hacim/degisim filtrelerinden ONCE olmali ki elenenler listesine dussun
  const volIdx = server.indexOf('R4863_WORKER_MIN_QUOTE)', i);
  assert.ok(volIdx > i, 'dev filtresi hacim filtresinden once gelmeli');
});

test('V696: eski excludedCore hala duruyor (fallback yolu bozulmadi)', () => {
  assert.ok(server.includes("const excludedCore=new Set(['BTCUSDT','ETHUSDT'"),
    'fallback yolundaki liste kaldirilmadi');
  assert.ok(server.includes('fallbackCore=(rows||[]).filter(t=>!excludedCore.has('),
    'fallback filtresi calismaya devam etmeli');
});

test('V696: ENV ile kapatilabilir / liste degistirilebilir', () => {
  assert.ok(server.includes('process.env.V696_DEV_COIN_HARIC'), 'acma-kapama anahtari');
  assert.ok(server.includes('process.env.V696_DEV_COIN_LISTE'), 'liste ENV den gelebilmeli');
});

test('V696: TDZ yok — tanim kullanimdan ONCE', () => {
  const d = server.indexOf('const V696_DEV_COIN_BASES');
  const u = server.indexOf('V696_DEV_COIN_BASES.has(');
  assert.ok(d > 0 && u > d, 'sabit kullanimdan once tanimli olmali');
});
