'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// A: hafizadaki target1 ilk engel olarak kullanilir
test('V647-A: hafiza target1 ilk engel olarak devreye girer', () => {
  assert.match(server, /const V647_HAFIZA_ENGEL = String\(process\.env\.V647_HAFIZA_ENGEL \?\? '1'\) !== '0';/);
  assert.match(server, /const _t = Number\(candidateMemory\?\.target1 \|\| 0\);/);
  assert.match(server, /_v647Known = true; _v647Kaynak = '15M_HAFIZA_TARGET1';/);
});

test('V647-A: 0.35 esigi GEVSEMIYOR, sadece engel kaynagi degisiyor', () => {
  assert.match(server, /_v647RR<R493_MIN_FIRST_OBSTACLE_RR/);
  assert.ok(!/R493_MIN_FIRST_OBSTACLE_RR\s*\*\s*0\./.test(server), 'esik carpanla dusurulmemeli');
});

test('V647-A: hafizadan gelen engel loglanir (sessiz calismasin)', () => {
  assert.match(server, /V647 ilk engel 15m HAFIZADAN/);
});

// B: ENTRY_ZONE_RETEST pozitif kanit
test('V647-B: ENTRY_ZONE_RETEST + reclaim retestProof sayilir', () => {
  assert.match(server, /const V647_BOLGE_GIRISI = String\(process\.env\.V647_BOLGE_GIRISI \?\? '1'\) !== '0';/);
  assert.match(server, /candidateMemory\?\.state==='ENTRY_ZONE_RETEST'/);
  assert.match(server, /candidateMemory\?\.reclaim===true/);
});

test('V647-B: yalniz retestProof kumesine eklenir, kapi kaldirilmaz', () => {
  const m = server.match(/retestProof=!!\(([^;]*?)\),proof=/s);
  assert.ok(m, 'retestProof ifadesi bulunmali');
  assert.ok(m[1].includes('ENTRY_ZONE_RETEST'), 'bolge retesti kanit kumesinde olmali');
  assert.ok(m[1].includes('micro.confirmedPullback'), 'mevcut kanitlar korunmali');
});

// C: kaldirac araligi — VARSAYILAN KAPALI
test('V647-C: kaldirac araligi varsayilan KAPALI', () => {
  assert.match(server, /const V647_KALDIRAC_ARALIK = String\(process\.env\.V647_KALDIRAC_ARALIK \?\? '0'\) !== '0';/);
});

test('V647-C: aralik acikken parite ve emir yolu ayni siniri kullanir', () => {
  assert.match(server, /KALDIRAC_KILIDI_ARALIK_DISI/);
  assert.match(server, /const _v647LevOk = V647_KALDIRAC_ARALIK/);
  assert.match(server, /safeLeverage <= V647_MAX_KALDIRAC/);
});

test('V647-C: aralik KAPALIYKEN eski tam-esitlik davranisi korunur', () => {
  assert.match(server, /: \(safeLeverage === V592_LEVERAGE_LOCK\);/);
  assert.match(server, /} else if\(Number\(V592_LEVERAGE_LOCK\)!==_v602BekLev\)/);
});

test('V647: onceki surumlerin duzeltmeleri yerinde', () => {
  assert.match(server, /V6_4_7_15M_HAFIZA_KARARA_GIRDI/);
  assert.match(server, /V646_BE_GEO/);
  assert.match(server, /V646_ZAYIF_KURULUMLAR/);
  assert.match(server, /V637_PUSU_R495E_DEVRET/);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);
});
