'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KANIT 31.08.2026 11:15:28 (aynı saniye):
//   🎯 CLO V649 giriş bölgeye çekildi: 0.14103 → 0.13886 · sapma %1.56
//   ⚙️ CLO MEKANİK KARAR MOTORU: giriş:0.14103            ← çekilmemiş
//   🪤 CLO R442 PUSU kuruldu: fiyat 0.14103 bölgesine     ← çekilmemiş
// V649 entryTruth.plannedEntry'yi çekiyor; R486.3.9 anatomi yolu onu okumuyor.

test('V652: çekme r442PusuKur içinde — pusu seviyesinin tek kapısı', () => {
  const f = server.slice(server.indexOf('function r442PusuKur'), server.indexOf('function r442PusuKontrol'));
  assert.ok(f.length > 200, 'r442PusuKur bulunmalı');
  assert.match(f, /let entry = Number\(ai\?\.entry \|\| 0\);/);
  assert.match(f, /const _hf = v644CandidateMemoryContext\(symbolFull, entry, \{\}\);/);
  assert.match(f, /const _sap = \(entry - _tepe\) \/ _tepe \* 100;/);
  assert.match(f, /entry = _tepe;/);
  assert.match(f, /V652 pusu BÖLGEYE kuruldu/);
});

test('V652: r442PusuKur GERÇEKTEN tek kapı (başka çağrı yeri yok)', () => {
  const cagri = (server.match(/r442PusuKur\(/g) || []).length;
  assert.strictEqual(cagri, 2, 'biri tanım biri çağrı = 2 olmalı; fazlaysa tek kapı değil');
  assert.match(server, /if \(Number\(ai\?\.entry\) > 0\) r442PusuKur\(coin\.fullSymbol, ai,/);
});

test('V652: çekilen değer HARİTAYA yazılıyor (no-op değil)', () => {
  const f = server.slice(server.indexOf('function r442PusuKur'), server.indexOf('function r442PusuKontrol'));
  const cekme = f.indexOf('entry = _tepe;');
  const yaz = f.indexOf('r442PusuPlanlar.set(base, {');
  assert.ok(cekme > 0 && yaz > cekme, 'çekme, haritaya yazmadan ÖNCE olmalı');
  // ve harita `entry` degiskenini yaziyor olmali (yeniden Number(ai.entry) degil)
  assert.match(f, /r442PusuPlanlar\.set\(base, \{\r?\n?\s*entry,/);
});

test('V652: plan SL\'inin altına çekilmez', () => {
  const f = server.slice(server.indexOf('function r442PusuKur'), server.indexOf('function r442PusuKontrol'));
  assert.match(f, /const _planSl = Number\(ai\?\.sl \|\| 0\);/);
  assert.match(f, /if \(_tepe > 0 && \(!\(_planSl > 0\) \|\| _tepe > _planSl\)\)/);
});

test('V652: aynı tolerans bandı, ayrı bayrak yok', () => {
  const f = server.slice(server.indexOf('function r442PusuKur'), server.indexOf('function r442PusuKontrol'));
  assert.match(f, /if \(V649_BOLGEDEN_GIRIS\) \{/);
  assert.match(f, /_sap > V649_BOLGE_TOLERANS && _sap <= V649_BOLGE_MAX_SAPMA/);
});

test('V652: CLO ve AUCTION matematiği — ikisi de banda düşer', () => {
  const TOL = 0.5, MAX = 4;
  const sap = (e, t) => (e - t) / t * 100;
  const clo = sap(0.14103, 0.13886), auc = sap(3.558, 3.529);
  assert.ok(Math.abs(clo - 1.563) < 0.01, 'CLO sapması %1,56');
  assert.ok(Math.abs(auc - 0.822) < 0.01, 'AUCTION sapması %0,82');
  for (const s of [clo, auc]) assert.ok(s > TOL && s <= MAX, 'ikisi de bandın içinde');
});

test('V652: V649 marketAllowed kolu (B2) yerinde duruyor', () => {
  // V660: listeye _v660Imkansiz eklendi. Tam string yerine TERIM TERIM kontrol —
  // her yeni kol eklendiginde test kirilmasin, ama hicbir kol da sessizce dusmesin.
  const i = server.indexOf('marketAllowed=!(');
  assert.ok(i > 0, 'marketAllowed ifadesi bulunmali');
  const m = [null, server.slice(i, i + 260)];
  for (const kol of ['storyWait','planIncoherent','legacyFirstObstacleHard','directionAgainst',
                     'tightStop&&retestFar','r493EntrySafety.blocked','_v649BolgeUstu','_v660Imkansiz']) {
    assert.ok(m[1].includes(kol), `marketAllowed icinde ${kol} olmali`);
  }
});

test('V652: sözleşme yerinde', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'),
    `LAZARUS_BUILD (${_b && _b[1]}) package.json (${_pkg.version}) ile uyusmuyor`);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);
  assert.match(server, /const breakEvenAt = _v646BeTaban;/);
});
