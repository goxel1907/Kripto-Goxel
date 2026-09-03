'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const env = fs.readFileSync(path.join(__dirname, '..', 'CANLI.env'), 'utf8');

// ÖLÇÜM: backtest slPct/ATR ortanca 2.35 (1060 sinyal) · canlı 0.52 → 4,5 kat dar
// Backtest kendi verisi: stop/ATR 2.84 → +0.667R · 1.02 → +0.025R · 0.88 → −0.100R

test('V660: üç kelepçe de açıldı', () => {
  assert.match(server, /const V634_TOPLAM_RISK_PCT = Math\.max\(2, Math\.min\(30, Number\(process\.env\.V634_TOPLAM_RISK_PCT \|\| 8\)\)\);/);
  assert.match(server, /const R486_MIN_STOP_ATR = Math\.max\(0\.30, Math\.min\(3\.00, Number\(process\.env\.R486_MIN_STOP_ATR \|\| 2\.35\)\)\);/);
  assert.ok(!/Math\.min\(4\.5,atrPct\*R486_MIN_STOP_ATR\)/.test(server), 'sabit %4,5 tavanı kalmamalı');
});

test('V660: minStopPct likidasyondan türetilen tavanla kelepçeleniyor', () => {
  assert.match(server, /const _v660LikTavan = \(100 \/ _v660Lev\) \* V660_LIK_PAYI;/);
  assert.match(server, /const _v660Gereken = atrPct \* R486_MIN_STOP_ATR;/);
  // V691: _v662Tavan bu listeden CIKARILDI. EGLD kaybi gosterdi ki engel tavani
  // ATR tabanini eziyordu (12,46 -> 0,80). Taban artik yalniz likidasyon ve ATR'den.
  const _mi = server.indexOf('const _v691Taban = Math.max(.80, Math.min(');
  assert.ok(_mi > 0, 'V691 taban ifadesi bulunmali');
  const _ms = server.slice(_mi, _mi + 120);
  for (const t of ['_v660LikTavan','_v660Gereken']) {
    assert.ok(_ms.includes(t), `taban icinde ${t} olmali`);
  }
  assert.ok(!_ms.includes('_v662Tavan'), 'engel tavani TABANA girmemeli (V691)');
});

test('V660: durdurulamayan işlem MARKET açamaz', () => {
  assert.match(server, /const _v660Imkansiz = V660_DURDURULAMAZ_VETO && _v660Gereken > _v660LikTavan;/);
  assert.match(server, /r493EntrySafety\.blocked\|\|_v649BolgeUstu\|\|_v660Imkansiz\)/);
  assert.match(server, /V660 DURDURULAMAZ: ATR/);
  assert.match(server, /v660Imkansiz:_v660Imkansiz,v660Gereken:/);
});

test('V660: TDZ yok — sabitler kullanımdan ÖNCE tanımlı', () => {
  const tanim = (n) => server.indexOf('const ' + n + ' =');
  const kullanim = server.indexOf('const _v660LikTavan = ');
  for (const n of ['R486_MIN_LEVERAGE','R486_MIN_STOP_ATR','V660_LIK_PAYI','V660_DURDURULAMAZ_VETO']) {
    assert.ok(tanim(n) > 0 && tanim(n) < kullanim, `${n} kullanımdan önce tanımlı olmalı`);
  }
});

test('V660: boot parite kapısı sabit 8 yerine sözleşme sabitine bakıyor', () => {
  assert.ok(!/eq\(V634_TOPLAM_RISK_PCT,8\)/.test(server), 'sabit 8 kontrolü kalmamalı');
  assert.match(server, /const V660_SOZLESME_RISK_PCT = Math\.max\(2, Math\.min\(30, Number\(process\.env\.V660_SOZLESME_RISK_PCT \|\| 23\)\)\);/);
  assert.match(server, /if\(!eq\(V634_TOPLAM_RISK_PCT,V660_SOZLESME_RISK_PCT\)\) hata\.push\(`TOPLAM_RISK_SOZLESME_DISI/);
  // yapisal kontrol (poz x risk = toplam) DURMALI
  assert.match(server, /if\(Math\.abs\(_v634Toplam - V634_TOPLAM_RISK_PCT\) > 1e-6\)/);
});

test('V660: CANLI.env sözleşmeyle uyumlu (yoksa bot fail-closed olur)', () => {
  assert.match(env, /^R486_MIN_STOP_ATR="2\.35"$/m);
  assert.match(env, /^V634_TOPLAM_RISK_PCT="23"$/m);
  assert.match(env, /^R497_SLOT_MARGIN_USDT="50"$/m);
  assert.match(env, /^R486_MAX_POSITIONS="1"$/m);
});

test('V660: likidasyon aritmetiği', () => {
  const tavan = (lev, payi = 0.90) => (100 / lev) * payi;
  assert.ok(Math.abs(tavan(7) - 12.857) < 0.01, '7x → %12,86');
  assert.ok(Math.abs(tavan(10) - 9.0) < 0.01, '10x → %9,00');
  const gereken = (atr) => atr * 2.35;
  // ZORA: ATR %8,48 → gereken %19,93 → 7x tavanını (12,86) aşar → AÇILMAMALI
  assert.ok(gereken(8.48) > tavan(7), 'ZORA hiçbir 7x stopuyla korunamaz');
  assert.ok(gereken(8.48) > tavan(10), '10x ile de korunamaz');
  // ATR %4,00 → gereken %9,40 → 7x tavanının altında → açılabilir
  assert.ok(gereken(4.00) < tavan(7), 'ATR %4 7x ile korunabilir');
  // kullanicinin sectigi %23 butce: 50$ x 7x, bakiye 143$ -> izin verilen stop
  const butceStop = (eq, butce, marj, lev) => (eq * butce / 100) / (marj * lev) * 100;
  assert.ok(Math.abs(butceStop(143.14, 23, 50, 7) - 9.406) < 0.01, '%23 bütçe → stop %9,41');
  assert.ok(gereken(4.00) <= butceStop(143.14, 23, 50, 7) + 0.01, 'ATR %4 bütçeye de sığar');
});

test('V660: sözleşmenin dokunulmayan kısımları', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'));
  assert.match(server, /const V601_HARD_MARGIN_FLOOR_USDT = 50;/);   // 50$ sabit marj
  assert.match(server, /V649_BOLGEDEN_GIRIS/);                        // bölgeden giriş
  assert.match(server, /const breakEvenAt = _v646BeTaban;/);          // çift-geo geri alma
});
