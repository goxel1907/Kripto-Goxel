'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// NEDEN: chartStory.researchPassive.fib her işlemde DOĞRU hesaplanıyor ve arşivde
// duruyor — ZORA kaydında 1m: ok=true, SHORT, derinlik %84, skor 23,48. Ama tek tek
// 2,6 MB'lik JSON'ların ~202.000. baytında; 87 işlemi geriye dönük okumak pratikte
// imkânsızdı (HTTP tarafı belgeyi ~70.000. baytta kesiyor). Bu sürüm onu
// dataset.csv'ye kolon yapar: tek indirmeyle bütün geçmiş ölçülebilir.
//
// KARAR ETKİSİ YOK: researchPassive zaten decisionImpact:false. Değişen tek şey
// dışa aktarım. chartStory.fib (karar şeridi) hâlâ V592_POLICY_PARITY_MODE ile boş.

test('V674: dataset.csv fib kolonları üretiyor', () => {
  const i = server.indexOf('V674: FIBONACCI OLCUM KOLONLARI');
  assert.ok(i > 0, 'V674 bloğu bulunmalı');
  const blok = server.slice(i, i + 1600);
  assert.ok(blok.includes("researchPassive?.fib"), 'pasif araştırma şeridinden okumalı');
  for (const tf of ['1m','5m','15m','1h']) assert.ok(blok.includes(`'${tf}'`), `${tf} kolonu olmalı`);
  for (const alan of ["Ok']","Side']","InZone']","Depth']","Score']","Eff']","Bos']"])
    assert.ok(blok.includes(alan), `fib*${alan} kolonu olmalı`);
  assert.ok(blok.includes('catch(_){ return {}; }'), 'fail-open olmalı — eksik kayıt CSV üretimini kırmamalı');
});

test('V674: r501DatasetRows içinde, karar yolunda değil', () => {
  const ds = server.indexOf('function r501DatasetRows');
  const v674 = server.indexOf('V674: FIBONACCI OLCUM KOLONLARI');
  assert.ok(ds > 0 && v674 > ds, 'blok dataset üreticisinin içinde olmalı');
  assert.ok(v674 - ds < 12000, 'aynı fonksiyonun gövdesinde olmalı');
});

test('V674: karar şeridindeki fib kilidine DOKUNULMADI', () => {
  // Bu sürüm ölçümü açar, pariteyi kırmaz. Karar hâlâ boş fib alıyor.
  assert.match(server, /fib\[k\]=V592_POLICY_PARITY_MODE\?\{ok:false,policyNeutral:true/);
  assert.match(server, /const V592_POLICY_PARITY_MODE = true;/);
});

test('V674: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
