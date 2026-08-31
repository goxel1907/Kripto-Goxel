'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KANIT: ZORA_1788162608211.evidence.json
//   plannedEntry 0.01060939025 · originalEntry 0.010209 · firstObstacle 0.0105935
//   hafıza bölgesi 0.009166-0.010065 · target1 0.0105174167 · v649Sapma 5.409 (ATLANDI)
// SEBEP: story.timing==='WAIT_BREAK_RETEST' iken plannedEntry = firstObstacle*1.0015

test('V651: V649 sapması GERÇEK girişten ölçülür', () => {
  assert.match(server, /const _v649Ref = Number\(entry\) > 0 \? Number\(entry\) : plannedEntry;/);
  assert.match(server, /_v649Sapma = \(_v649Ref - _tepe\) \/ _tepe \* 100;/);
  assert.ok(!/_v649Sapma = \(plannedEntry - _tepe\) \/ _tepe \* 100;/.test(server),
    'plannedEntry ile ölçüm kalmamalı');
});

test('V651: giriş asla YUKARI itilmez, yalnız bölgeye çekilir', () => {
  assert.match(server, /const _eski = plannedEntry; plannedEntry = Math\.min\(plannedEntry, _tepe\);/);
});

test('V651: kapı LONG\'u güncel fiyatın ÜSTÜNDEN değerlendirmez', () => {
  assert.match(server, /const _pe = Number\(entryTruth\?\.plannedEntry \|\| 0\);/);
  assert.match(server, /const _oe = Number\(entryTruth\?\.originalEntry \|\| opts\?\.entry \|\| 0\);/);
  assert.match(server, /const _e = \(_pe > 0 && \(!\(_oe > 0\) \|\| _pe <= _oe \* 1\.0005\)\) \? _pe : _oe;/);
});

test('V651: originalEntry kapıya gerçekten geçiliyor (no-op değil)', () => {
  const m = server.match(/const r493EntrySafety=r493EntrySafetyGate\((.*?)\);\r?\n/);
  assert.ok(m && m[1].includes('originalEntry:entry'), 'originalEntry gate çağrısında olmalı');
});

test('V651: ZORA matematiği — düzeltme kararı değiştirirdi', () => {
  const zHigh = 0.010065, planned = 0.01060939025, gercek = 0.010209;
  const TOL = 0.5, MAX = 4;
  const sapma = (ref) => (ref - zHigh) / zHigh * 100;
  // canlida olan: plannedEntry ile olculdu -> bayat sayildi
  assert.ok(sapma(planned) > MAX, 'plannedEntry ile %4 asiliyor (atlandi)');
  assert.ok(Math.abs(sapma(planned) - 5.409) < 0.01, 'kanit dosyasindaki 5.409 ile ayni');
  // duzeltmeden sonra: gercek giris ile banda duser
  assert.ok(sapma(gercek) > TOL && sapma(gercek) <= MAX, 'gercek giris ile banda dusmeli');
  assert.ok(Math.abs(sapma(gercek) - 1.431) < 0.01, 'sapma %1,43 olmali');
});

test('V651: ZORA — hafıza dalının neden öldüğü ve düzeltmeden sonra ne olacağı', () => {
  const planned = 0.01060939025, gercek = 0.010209, sl = 0.009758823936;
  const t1 = 0.0105174167, engel = 0.0105935;
  // canlida: target1 plannedEntry'nin ALTINDA -> `_t > _e` false -> dal atlandi
  assert.ok(!(t1 > planned), 'target1 plannedEntry altinda kalmis');
  // duzeltmeden sonra gercek girisle: dal calisir ve daha YAKIN engeli secer
  assert.ok(t1 > gercek, 'gercek girisle target1 ustte, dal calisir');
  const rr = (h, e) => (h - e) / (e - sl);
  assert.ok(rr(t1, gercek) < rr(engel, gercek), 'hafiza engeli daha yakin -> min() onu secer');
  assert.ok(Math.abs(rr(t1, gercek) - 0.685) < 0.01);
});

test('V651: sözleşme ve önceki düzeltmeler yerinde', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'),
    `LAZARUS_BUILD (${_b && _b[1]}) package.json (${_pkg.version}) ile uyusmuyor`);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);
  assert.match(server, /const breakEvenAt = _v646BeTaban;/);
  assert.match(server, /const V650_PIVOT_ENGEL = String\(process\.env\.V650_PIVOT_ENGEL \?\? '0'\) !== '0';/);
});
