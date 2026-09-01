'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI KANIT — dataset v3.43 (1 Eylül, 89 satır, 192 kolon):
// V6.7.4'ün 13 yörünge kolonundan yalnız ikisi (sampleCountUsed, sampleSpanMs)
// CSV'ye çıktı. Sebep: başlık `Object.keys(rows[0])` ile tek satırdan üretiliyordu
// ve indeksin ilk kaydı USELESS_1788256810605 sadece 2 örnek taşıyordu; V675'in
// erken dönüşü iki anahtarlı kısa şekil döndürdü ve DİĞER 88 SATIRIN kolonları
// atıldı. Oysa 89 kaydın 86'sında tam seri vardı (medyan 2.133 örnek, max 17.810).

test('V676-A: yörünge bloğunun şekli her zaman sabit', () => {
  const i = server.indexOf('V675: ISLEM ICI CVD YORUNGESI');
  assert.ok(i > 0);
  const blok = server.slice(i, i + 3400);
  assert.ok(blok.includes('const BOS='), 'eksik veri için sabit şekil tanımlanmalı');
  assert.ok(blok.includes('if(S.length<3) return BOS;'), 'erken dönüş sabit şekli döndürmeli');
  // BOS icinde her kolon bulunmali; biri eksikse tuzak geri gelir
  for (const k of ['sampleCountUsed','sampleSpanMs','cvdRatio0','cvdRatio1m','cvdRatio5m',
                   'cvdRatio15m','cvdRatioMin','cvdRatioMax','cvdRatioMinAtMs','cvdDrop1m',
                   'cvdDrop5m','peakRoiSeen','peakRoiAtMs','dipRoiSeen','givebackPct'])
    assert.ok(new RegExp(k + ':').test(blok.slice(blok.indexOf('const BOS='), blok.indexOf('if(S.length<3)'))),
      `BOS içinde ${k} olmalı`);
  assert.ok(!blok.includes('return {sampleCountUsed:S.length,sampleSpanMs:0};'),
    'eski kısa şekil kalmamalı');
});

test('V676-B: CSV başlığı TÜM satırların birleşimi (asıl sebep)', () => {
  // Tek satirdan turetilen baslik, degisken sekilli HER kolon icin ayni tuzak.
  assert.ok(!server.includes('Object.keys(rows[0])'),
    'hiçbir CSV başlığı tek satırdan türetilmemeli');
  const ds = server.indexOf("app.get('/api/evidence/dataset.csv'");
  assert.ok(ds > 0);
  assert.ok(server.slice(ds, ds + 400).includes('rows.reduce('), 'dataset.csv birleşim kullanmalı');
  const pc = server.indexOf("app.get('/api/evidence/passive.csv'");
  assert.ok(pc > 0);
  assert.ok(server.slice(pc, pc + 500).includes('reduce('), 'passive.csv de birleşim kullanmalı');
});

test('V676: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
