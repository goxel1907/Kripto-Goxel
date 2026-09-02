'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// V592 parite kipi karar aninda CVD / emir defteri / OI / funding / likidasyon /
// iceberg alanlarini SIFIRLIYOR (r592NeutralizeDecisionData). Ama notrleyicinin
// cagrildigi yerde HAM veri `researchInput` icinde duruyor:
//     server.js  const researchInput = data;  data = r592NeutralizeDecisionData(data);
// V681 o ham degerleri karar anindan alip kayda yaziyor — karara HIC dokunmadan.
// Amac: 30-50 kapanmis islemde olcmek, "bu veri kazananla kaybedeni ayiriyor mu?"

test('V681: ham piyasa verisi researchInput uzerinden okunuyor', () => {
  const i = server.indexOf('v681Piyasa:(function(){');
  assert.ok(i > 0, 'v681Piyasa blogu olmali');
  const blok = server.slice(i, i + 1800);
  assert.ok(blok.includes('researchInput'), 'notrlenmemis kaynaktan okumali');
  assert.ok(!blok.includes('r592Neutral'), 'notrlenmis veriden okumamali');
});

test('V681: likidasyon alanlari kaydediliyor (kullanicinin asil istedigi)', () => {
  const i = server.indexOf('v681Piyasa:(function(){');
  const blok = server.slice(i, i + 1800);
  for (const alan of ['likLong5m', 'likShort5m', 'likLong1m', 'likShort1m',
                      'likDominance', 'likCascade'])
    assert.ok(blok.includes(alan), `${alan} kaydedilmeli`);
});

test('V681: akis/defter/OI/funding/iceberg da kaydediliyor', () => {
  const i = server.indexOf('v681Piyasa:(function(){');
  const blok = server.slice(i, i + 1800);
  for (const alan of ['cvdRatio', 'takerRatio', 'bookImb', 'oi1h', 'funding', 'icebergSignal'])
    assert.ok(blok.includes(alan), `${alan} kaydedilmeli`);
});

// MarkitTick script'inin olcumunden cikan tek islevsel bulgu: supurme sinyali
// yavas trend filtresi yukariyken cok daha guclu (botun stopuyla 3R +0,662,
// n=352, iki yarida tutarli). Bot'ta bu terim yoktu; once GOLGEDE olculuyor.
test('V681: yavas trend (50 saatlik ortalama) golgede olculuyor', () => {
  const i = server.indexOf('v681Piyasa:(function(){');
  const blok = server.slice(i, i + 1800);
  assert.ok(blok.includes("rows&&rows['1h']"), '1h mumlarindan turetilmeli');
  assert.ok(blok.includes('H.length>=50'), '50 mum yoksa null donmeli');
  assert.ok(blok.includes('yavasSapma') && blok.includes('yavasUstunde'));
});

test('V682: piyasa verisi karara giriyor, kayit da tutuluyor', () => {
  // V682: veri artik notrlenmiyor, karar yoluna DOGRUDAN giriyor.
  // v681Piyasa blogu ise kayit/panel icin ayni degerlerin anlik goruntusu.
  assert.match(server, /const V682_PIYASA_VERISI = String\(process\.env\.V682_PIYASA_VERISI \?\? '1'\) !== '0';/);
  assert.match(server, /const V592_POLICY_PARITY_MODE = !V682_PIYASA_VERISI;/);
  assert.match(server, /const V592_RESEARCH_PASSIVE_ONLY = !V682_PIYASA_VERISI;/);
  // notrleyici hala kodda ama artik bayrakla kapali — geri alinabilir olmali
  const n = server.indexOf('function r592NeutralizeDecisionData');
  assert.ok(server.slice(n, n + 200).includes('if(!V592_POLICY_PARITY_MODE)return data;'),
    'bayrak kapaliyken ham veri gecmeli');
});

test('V681: kapanmis islem CSV kolonlari eklendi', () => {
  const i = server.indexOf('v681_cvdRatio');
  assert.ok(i > 0, 'CSV kolonlari olmali');
  const blok = server.slice(i - 600, i + 1800);
  for (const k of ['v681_likLong5m', 'v681_likCascade', 'v681_yavasSapma', 'v681_oi1h', 'v681_bookImb'])
    assert.ok(blok.includes(k), `${k} kolonu olmali`);
  assert.ok(blok.includes('rp?.v681Piyasa'), 'researchPassive uzerinden okunmali');
});

test('V682: beyin karti piyasa verisini KARARA GIRIYOR etiketiyle gosteriyor', () => {
  const i = server.indexOf('function v679Hikaye');
  const govde = server.slice(i, server.indexOf('// ═══ V678 ═══ Grafik kalite kapisi', i));
  assert.ok(govde.includes('PIYASA VERISI - karara giriyor'), 'karara girdigi acikca yazmali');
  assert.ok(govde.includes('likidasyon'), 'likidasyon gosterilmeli');
  const k = server.indexOf('v679Kaydet({symbol:coin.symbol');
  assert.ok(server.slice(k, k + 1100).includes('piyasa:_s680?.researchPassive?.v681Piyasa'),
    'kayit piyasa anlik goruntusunu tasimali');
});
