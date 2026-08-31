'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KULLANICI KARARI: "ATR ne olursa olsun 50 dolar 7x, yeter ki bot fırsat görsün,
// stop mesafesi uzasın." → bütçe veto olmaktan çıkar, tek tavan likidasyon kalır.
// TESPİT: 100$ bakiye · %8 bütçe · 50$×7x → izin verilen stop %2,29.
// İKİ veto adayı öldürüyordu: V624 (SL'i daraltır) + R495_RISK_FLOOR_CONFLICT (continue).

test('V661: bütçe vetosu VARSAYILAN KAPALI, ATR vetosu da kapalı', () => {
  assert.match(server, /const V661_BUTCE_VETO = String\(process\.env\.V661_BUTCE_VETO \?\? '0'\) !== '0';/);
  assert.match(server, /const V660_DURDURULAMAZ_VETO = String\(process\.env\.V660_DURDURULAMAZ_VETO \?\? '0'\) !== '0';/);
});

test('V661: V624 tavanı artık likidasyon (bütçe SL daraltmıyor)', () => {
  assert.match(server, /const _lik661 = \(100\/_lev495\)\*V660_LIK_PAYI;/);
  assert.match(server, /const _slTavan624 = V661_BUTCE_VETO \? _butce661 : _lik661;/);
  assert.ok(!/const _slTavan624 = \(_eq495\*\(R495_FINAL_RISK_PCT\/100\)\)/.test(server),
    'eski koşulsuz bütçe tavanı kalmamalı');
});

test('V661: risk-floor vetosu yalnız bütçe vetosu AÇIKKEN adayı öldürür', () => {
  assert.match(server, /const _riskFloorConflict495 = V661_BUTCE_VETO && _eq495>0/);
  // veto hala var olmali (acilinca calissin), sadece kosullu
  assert.match(server, /R495_RISK_FLOOR_CONFLICT/);
  assert.match(server, /riskFloorConflict:true/);
});

test('V661: gerçek risk HER işlemde loglanıyor (gizlenmiyor)', () => {
  assert.match(server, /V661 RİSK: 50\$ × \$\{_lev495\}x × SL/);
  assert.match(server, /AŞILDI \(veto kapalı\)/);
  assert.match(server, /likidasyon tavanı/);
  // log, veto kontrolunden ONCE gelmeli ki atlanan adayda da gorunsun
  const logIdx = server.indexOf('V661 RİSK: 50$');
  const vetoIdx = server.indexOf('const _riskFloorConflict495 = V661_BUTCE_VETO');
  assert.ok(logIdx > 0 && logIdx < vetoIdx, 'risk logu veto kontrolünden önce olmalı');
});

test('V661: likidasyon tavanı KALDIRILMADI — fizik, tercih değil', () => {
  assert.match(server, /const _v660LikTavan = \(100 \/ _v660Lev\) \* V660_LIK_PAYI;/);
  assert.match(server, /minStopPct=Math\.max\(\.80,Math\.min\(_v660LikTavan,_v660Gereken\)\)/);
});

test('V661: 100$ bakiyede risk aritmetiği (kullanıcı bunu bilerek seçti)', () => {
  const eq = 100, marj = 50, lev = 7, KAT = 2.35, PAY = 0.90;
  const likTavan = (100 / lev) * PAY;
  const stop = (atr) => Math.min(likTavan, KAT * atr);
  const risk = (atr) => marj * lev * stop(atr) / 100;
  assert.ok(Math.abs(likTavan - 12.857) < 0.01, '7x tavan %12,86');
  assert.ok(Math.abs(risk(2.0) - 16.45) < 0.05, 'ATR %2 → 16,45$');
  assert.ok(Math.abs(risk(3.0) - 24.68) < 0.05, 'ATR %3 → 24,68$');
  assert.ok(Math.abs(risk(4.0) - 32.90) < 0.05, 'ATR %4 → 32,90$');
  assert.ok(Math.abs(risk(8.48) - 45.00) < 0.05, 'ATR %8,48 → tavanda 45,00$');
  // tek islemde kaybedilebilecek en fazla para bakiyenin %45'i
  assert.ok(risk(99) / eq <= 0.451, 'likidasyon tavanı kaybı %45,1 ile sınırlıyor');
});

test('V661: eski davranış env ile geri gelir', () => {
  assert.match(server, /process\.env\.V661_BUTCE_VETO/);
  assert.match(server, /process\.env\.V660_DURDURULAMAZ_VETO/);
});

test('V661: sözleşmenin dokunulmayanları', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'));
  assert.match(server, /const V601_HARD_MARGIN_FLOOR_USDT = 50;/);   // 50$ sabit
  assert.match(server, /const R486_MIN_STOP_ATR = Math\.max\(0\.30, Math\.min\(3\.00, Number\(process\.env\.R486_MIN_STOP_ATR \|\| 2\.35\)\)\);/);
  assert.match(server, /V649_BOLGEDEN_GIRIS/);
});
