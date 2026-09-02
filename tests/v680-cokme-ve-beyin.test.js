'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT - 02.09.2026 17:11:17, panel logu:
//   "SKYAI AI beyin baglama hatasi: Assignment to constant variable."
// V673-B (V6.7.2) `first` degiskenini yeniden atiyordu ama `first` const'ti.
// Sonuc: kosul her tuttugunda r486EntryTruthGuard TypeError firlatiyor, aday
// icin TUM giris-gercegi hesabi (recommendedSl / firstObstacleRR / marketAllowed)
// yok oluyor ve emir sessizce dusuyordu. Yani "onemsiz bir kural yuzunden
// kacirilan firsat" degil, dogrudan COKME idi.

test('V680-A: V673-B yeniden atadigi icin `first` let olmali', () => {
  assert.ok(server.includes('let first=firstObstacleDirectionValid'),
    '`first` let ile taniminmali');
  assert.ok(!server.includes('const first=firstObstacleDirectionValid'),
    '`first` const kalirsa V673-B calisirken TypeError firlar');
});

test('V680-A2: V673-B tutarlilik yamasi hala yerinde', () => {
  const i = server.indexOf('V673_ENGEL_HEDEF_TUTARLILIK && first > 0');
  assert.ok(i > 0, 'V673-B kosulu duruyor olmali');
  assert.ok(server.slice(i, i + 400).includes('first = target;'),
    'V673-B ilk engeli hedefe cekmeye devam etmeli');
});

// CANLI KANIT - ayni tur: "Taranan 16 / Acilan 0 / Atlanan 16" iken
// /api/v679/beyin "toplam:0" donuyordu. V679 kaydi dongunun EN SONUNDAydi;
// adaylarin hepsi daha once `continue` ile eleniyordu.
test('V680-C: beyin kaydi karar kapilarindan ONCE aciliyor', () => {
  const kayit = server.indexOf('V680-C');
  const r308i = server.indexOf('R308I TEK KAPI SIZDIRMAZLIK');
  const r325 = server.indexOf('R325 TEK BEYIN AMELIYATI');
  assert.ok(kayit > 0, 'V680-C blogu olmali');
  assert.ok(r308i > 0);
  assert.ok(kayit < r308i, 'kayit R308I kapisindan once olmali');
  if (r325 > 0) assert.ok(kayit < r325, 'kayit R325 kapisindan once olmali');
});

test('V680-C2: V678 blogundaki cift kayit kaldirildi', () => {
  const n = (server.match(/v679Kaydet\(\{symbol:coin\.symbol/g) || []).length;
  assert.strictEqual(n, 1, 'aday basina TEK v679Kaydet cagrisi olmali');
});

test('V680-D: her atlama sebebi beyne yaziliyor', () => {
  const i = server.indexOf('function markAutoSkip');
  assert.ok(i > 0);
  const govde = server.slice(i, i + 1800);
  assert.ok(govde.includes("v679SonKarar(symbol, 'ATLANDI'"),
    'markAutoSkip beyne ATLANDI yazmali');
});

test('V680-F: acilan emir de beyne yaziliyor', () => {
  const i = server.indexOf('function markAutoOpened');
  assert.ok(i > 0);
  assert.ok(server.slice(i, i + 1400).includes("v679SonKarar(symbol, 'EMIR'"),
    'markAutoOpened beyne EMIR yazmali');
});

test('V680-B: v679SonKarar karara etki etmez, yalniz kayit gunceller', () => {
  const i = server.indexOf('function v679SonKarar');
  assert.ok(i > 0);
  const govde = server.slice(i, server.indexOf('\n}', i));
  for (const yasak of ['markAutoSkip', 'return false', 'blocked', 'decisionImpact:true'])
    assert.ok(!govde.includes(yasak), `v679SonKarar icinde ${yasak} olmamali`);
  assert.ok(govde.includes('V679_SONUC_PENCERE_MS'), 'sonuc penceresi sinirli olmali');
});

test('V680: beyin ozeti karar dagilimini da veriyor', () => {
  const i = server.indexOf("app.get('/api/v679/beyin/:nonce'");
  assert.ok(i > 0);
  const govde = server.slice(i, i + 2000);
  for (const alan of ['emir:sayac.EMIR', 'atlandi:sayac.ATLANDI', 'enCokSebep'])
    assert.ok(govde.includes(alan), `ozet ${alan} icermeli`);
});
