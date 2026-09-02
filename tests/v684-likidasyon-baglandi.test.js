'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT — V6.8.3 deploy sonrasi ilk emir, /api/v679/beyin:
//   cvdRatio 68 · bookImb -16.5 · oi1h +2.29 · funding 0.005 · yavasSapma +16.37
//   likLong5m null · likShort5m null · likDominance "" · likCascade null
// Yani V682 piyasa verisini acti ama LIKIDASYONLAR yine bostu.
//
// Sebep V592 degildi: `r481MekanikVeri` — grafik/karar veri paketini kuran
// fonksiyon — `liquidations` alanini HIC tasimiyordu. Bot tum Binance futures
// likidasyonlarina (!forceOrder@arr) bagliydi, analiz ucu da donduruyordu,
// ama karar yoluna giden pakete konmamisti. Bos gelmesinin sebebi buydu.

test('V684: likidasyonlar karar veri paketine baglandi', () => {
  const i = server.indexOf('function r481MekanikVeri');
  assert.ok(i > 0);
  const govde = server.slice(i, i + 6000);
  assert.ok(govde.includes('liquidations:analysis?.liquidations || null'),
    'r481MekanikVeri liquidations tasimali');
});

test('V684: eksik olan diger piyasa alanlari da baglandi', () => {
  const i = server.indexOf('function r481MekanikVeri');
  const govde = server.slice(i, i + 6000);
  for (const alan of ['oiChange15m:Number(analysis?.oiChange15m',
                      'oiChange4h:Number(analysis?.oiChange4h',
                      'fundingSlope:Number(analysis?.fundingSlope',
                      'icebergSignal:String(analysis?.icebergSignal'])
    assert.ok(govde.includes(alan), `${alan} baglanmali`);
});

test('V684: kaynak zincir bozulmadi — analiz ucu hala liquidations donduruyor', () => {
  assert.match(server, /liquidations: liqData,/);
  assert.match(server, /const liqData = getLiqData\(full\);/);
  // gercek akis: !forceOrder@arr tum coinler
  assert.match(server, /!forceOrder@arr/);
});

test('V684: V681 kaydi bu alanlari okuyor (zincir tam)', () => {
  const i = server.indexOf('v681Piyasa:(function(){');
  const blok = server.slice(i, i + 1800);
  assert.ok(blok.includes('L=R.liquidations||{}'), 'researchInput.liquidations okunmali');
  for (const k of ['likLong5m:N(L.longLiq5m)', 'likShort5m:N(L.shortLiq5m)',
                   'likDominance:String(L.dominance||\'\')'])
    assert.ok(blok.includes(k), `${k} olmali`);
});
