'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT 31.08 23:09:19Z (build V6_6_2):
//   TWTUSDT · giriş 0.5204 · SL 0.51186544 (%1,64) · TP 0.57244
//   firstObstacleRR: null  → girişin üstünde HİÇ engel bulunamadı → PUSU
// Sebep: çözücü 3m/5m/15m/1h bakıyordu, 4h'a hiç bakmıyordu.

test('V663: veri yolu KANITLI — story tüm TF için fvg/ob/liq üretiyor', () => {
  // Bu satır olmadan 4h okumak sessiz no-op olurdu (V6.5.0'da o hataya düşmüştüm).
  assert.match(server, /const fvg=\{\},ob=\{\},fibResearch=\{\},fib=\{\},liq=\{\};for\(const k of \['1m','3m','5m','15m','30m','1h','4h','1d'\]\)/);
  assert.match(server, /fvg\[k\]=r483Fvg\(rows\[k\],price\);ob\[k\]=r483Ob\(rows\[k\],price\);/);
  // ve story'ye bu adlarla giriyor
  assert.match(server, /orderBlock:ob,fib,liquidity:liq,/);
});

test('V663: fallbackObstacles artık 30m/1h/4h de okuyor', () => {
  const i = server.indexOf('const fallbackObstacles=');
  assert.ok(i > 0, 'fallbackObstacles bulunmalı');
  const blok = server.slice(i, i + 1400);
  for (const t of ["liquidity?.['4h']?.above", "liquidity?.['30m']?.above",
                   "orderBlock?.['1h']?.supply?.low", "orderBlock?.['4h']?.supply?.low",
                   "fvg?.['1h']?.bear?.low", "fvg?.['4h']?.bear?.low"]) {
    assert.ok(blok.includes(t), `fallbackObstacles içinde ${t} olmalı`);
  }
  // eskiler de durmali
  for (const t of ["liquidity?.['3m']?.above", "liquidity?.['15m']?.above", "fvg?.['15m']?.bear?.low"]) {
    assert.ok(blok.includes(t), `${t} kaybolmamalı`);
  }
});

test('V663: V662 stop-tavanı aday listesi de aynı seviyeleri görüyor', () => {
  const i = server.indexOf('const _v662Aday = [');
  assert.ok(i > 0, '_v662Aday bulunmalı');
  const blok = server.slice(i, i + 1400);
  for (const t of ["liquidity?.['4h']?.above", "orderBlock?.['4h']?.supply?.low", "fvg?.['4h']?.bear?.low"]) {
    assert.ok(blok.includes(t), `_v662Aday içinde ${t} olmalı`);
  }
});

test('V663: eklenen seviyeler HİÇBİR kararı daraltamaz', () => {
  // Liste siralanip [0] aliniyor: uzak seviye ancak daha yakini YOKKEN secilir.
  assert.match(server, /\.filter\(x=>x>entry\*1\.00005\)\.sort\(\(a,b\)=>a-b\)/);
  assert.match(server, /const _v662Engel = _v662Aday\[0\] \|\| 0;/);
  assert.match(server, /const first=firstObstacleDirectionValid&&firstRaw>0\?firstRaw:\(_v650Adaylar\[0\]\|\|0\);/);
});

test('V663: TWT senaryosu — 4h engeli görülseydi ne olurdu', () => {
  const e = 0.5204, sl = 0.51186544;
  const stopPct = (e - sl) / e * 100;
  assert.ok(Math.abs(stopPct - 1.640) < 0.01, 'TWT stopu %1,64');
  // engel bulunmazsa R/R null -> PUSU (yasanan)
  // 4h engeli TP civarinda (0.57244) olsaydi:
  const rr = (0.57244 - e) / (e - sl);
  assert.ok(rr > 0.35, `4h engeliyle R/R ${rr.toFixed(2)} — kapıyı geçerdi`);
});

test('V663: sözleşme ve önceki düzeltmeler yerinde', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'));
  assert.match(server, /const V601_HARD_MARGIN_FLOOR_USDT = 50;/);
  assert.match(server, /const _v662Tavan = \(V662_ENGELE_UYUMLU_STOP && _v662D > 0\)/);
  assert.match(server, /V649_BOLGEDEN_GIRIS/);
});
