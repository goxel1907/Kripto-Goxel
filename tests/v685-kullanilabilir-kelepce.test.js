'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT — oturum hunisi (lifecycle, 184 kayit):
//   ORDER_REQUEST_RECEIVED 38 · MAIN_ORDER_SEND 38 · MAIN_ORDER_ACK 7
//   ORDER_ROUTE_ERROR 31 · MAIN_ORDER_SEND_UNCERTAIN 30
//   MAIN_ORDER_UNRESOLVED_SYMBOL_LOCKED 30 · ORDER_REJECTED 62
// Binance'in dondurdugu metin: "/fapi/v1/order: Margin is insufficient. (-2019)"
//
// Yani bot 38 kez emir acmaya karar verdi, 7'si borsaya ulasti. "Sadece ayni
// coine giriyor" gorunmesinin sebebi secim degil: digerleri borsada oluyordu.
//
// Kok neden: boyut totalWalletBalance'tan hesaplaniyor (bilesik buyume kapanmis
// kari tasisin diye — kod yorumu bunu acikca soyluyor), Binance ise
// availableBalance'tan kesiyor. Acik pozisyon / komisyon / yetim emir ikisini
// ayirdiginda emir -2019 ile oluyor.

test('V685: hedef marj kullanilabilir bakiyeyle kelepceleniyor', () => {
  const i = server.indexOf('V685 ═══ CANLI KANIT');
  assert.ok(i > 0, 'V685 blogu olmali');
  const blok = server.slice(i, i + 1800);
  assert.ok(blok.includes('r372BakiyeCache.avail'), 'kullanilabilir bakiye okunmali');
  assert.ok(blok.includes('_tasinabilir < _v685Marj'), 'yalniz KUCUKSE kelepcelemeli');
  assert.ok(blok.includes('1.02 + 0.0005'), 'kayma payi + komisyon dusulmeli');
});

test('V685: kullanilabilir bakiye onbellege aliniyor', () => {
  assert.match(server, /let r372BakiyeCache = \{ value: 0, ts: 0, avail: 0 \};/);
  assert.ok(server.includes("const avail=Number(acc.availableBalance??usdtRow.availableBalance??usdtRow.maxWithdrawAmount??0);"),
    'snapshot\'tan availableBalance alinmali');
});

test('V685: bilesik buyume BOZULMAZ — kelepce yalniz tavan', () => {
  const i = server.indexOf('V685 ═══ CANLI KANIT');
  const blok = server.slice(i, i + 1800);
  // wallet tabanli hesap hala yapiliyor ve kayitta duruyor
  assert.ok(blok.includes('v685Walletmarj:contract.margin'), 'wallet hedefi kayitta kalmali');
  // kelepce ENV ile kapatilabilir
  assert.match(server, /const V685_KULLANILABILIR_KELEPCE = String\(process\.env\.V685_KULLANILABILIR_KELEPCE \?\? '1'\) !== '0';/);
});

test('V685: matematik — kelepce dogru hesapliyor', () => {
  const lev = 7, pay = 1.02 + 0.0005 * lev;      // 1.0235
  const tasi = av => Math.max(0, Math.floor((av / pay) * 100) / 100);
  // 50$ marj icin gereken kullanilabilir bakiye
  assert.ok(tasi(51.18) >= 50, '51,18$ kullanilabilir 50$ marji tasimali');
  assert.ok(tasi(45) < 50, '45$ kullanilabilir 50$ marji TASIYAMAZ');
  // buyudukce kelepce acilir
  assert.ok(tasi(300) > 290, 'kullanilabilir buyuyunce kelepce acilmali');
});
