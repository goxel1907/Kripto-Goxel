'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ÖLÇÜM — 879 gerçek 15m grafik (Ağustos 2026, botun seçtiği coinler),
// 36.918 değerlendirme noktası. Her noktada server.js'in KENDİ dedektörleri
// (r484Structure · r484Trendline · r483Ob · r483Liquidity) ileriye BAKMADAN
// koşturuldu; sonra 6 saatlik pencerede İLK DOKUNUŞ (TP mi SL mi önce) ölçüldü.
//
// Gönderilen fonksiyonun ürettiği bantlar (SL%3/TP%9):
//   skor  0-40 : n=8.155  WR %24,1  beklenti -0,071   <- TEK NEGATİF BANT
//   skor 40-50 : n=6.830  WR %29,6  beklenti +0,284
//   skor 50-60 : n=7.228  WR %32,6  beklenti +0,415   (taban +0,408)
//   skor 60-70 : n=7.321  WR %36,8  beklenti +0,640
//   skor 70+   : n=7.384  WR %43,2  beklenti +0,815
// <40 vetosu 8.155 noktayı (%22,1) eler; kalanın beklentisi +0,408 → +0,544.
//
// SAĞLAMLIK: çekirdek (rangePos>=0,6 + ATR<%2 + trend!=DOWN) üç ayrı geometride
// 2,01x · 1,98x · 1,98x; 18 sembolün 13'ünde iyi; sembol yarıya bölününce
// 2,00x ve 2,03x. Aşırı uydurma değil.

test('V678: skor yalnız botun KENDİ dedektörlerinden besleniyor', () => {
  const i = server.indexOf('function v678GrafikKalitesi(');
  assert.ok(i > 0, 'fonksiyon bulunmalı');
  const fn = server.slice(i, server.indexOf('function r486EntryTruthGuard(', i));
  for (const yol of ["story?.tf?.['15m']", 't.rangePos', 't.trend', 'tl.retestUp', 'tl.breakUp',
                     'tl.falseBreakUp', "story?.orderBlock?.['15m']?.supply?.inZone"])
    assert.ok(fn.includes(yol), `${yol} okunmalı`);
  assert.ok(fn.includes("return {ok:false,skor:null,sebep:'RANGEPOS_YOK'}"), 'veri yoksa fail-open');
  assert.ok(fn.includes('catch(e)'), 'hata yutulmalı');
});

test('V678: ölçülen katsayılar birebir kodda', () => {
  const i = server.indexOf('function v678GrafikKalitesi(');
  const fn = server.slice(i, server.indexOf('function r486EntryTruthGuard(', i));
  assert.ok(fn.includes('let s=50'), 'taban 50');
  assert.ok(fn.includes('s += (rp-0.5)*40'), 'aralık pozisyonu katsayısı 40');
  assert.ok(fn.includes('Math.max(-20, Math.min(15, (2.0-a)*8))'), 'ATR katsayısı 8, kelepçe -20..+15');
  assert.ok(fn.includes('s-=10'), 'düşüş yapısı -10');
  assert.ok(fn.includes('s+=12'), 'kırılım+retest +12');
  assert.ok(fn.includes('s+=8'), 'trend çizgisi kırılımı +8');
  assert.ok(fn.includes('s+=6'), 'arz OB +6');
  assert.ok(fn.includes('s-=8'), 'yanlış kırılım -8');
});

test('V678: veto YALNIZ ölçülen negatif banda (<40), env ile ayarlanabilir', () => {
  assert.match(server, /const V678_GRAFIK_KALITE = String\(process\.env\.V678_GRAFIK_KALITE \?\? '1'\) !== '0';/);
  assert.match(server, /const V678_MIN_SKOR = Math\.max\(0, Math\.min\(100, Number\(process\.env\.V678_MIN_SKOR \|\| 40\)\)\);/);
  const i = server.indexOf('GRAFIK KALITE KAPISI');
  assert.ok(i > 0, 'veto bloğu bulunmalı');
  const blok = server.slice(i, i + 2200);
  assert.ok(blok.includes('_k678.skor < V678_MIN_SKOR'), 'eşik altında elemeli');
  assert.ok(blok.includes('V678_LOW_CHART_QUALITY'), 'kanıt hunisine yazmalı');
  assert.ok(blok.includes('markAutoSkip'), 'markAutoSkip çağırmalı');
  assert.ok(blok.includes('continue;'), 'adayı gerçekten atlamalı');
});

test('V678: skor hesaplanamazsa aday KORUNUR (fail-open)', () => {
  const i = server.indexOf('GRAFIK KALITE KAPISI');
  const blok = server.slice(i, i + 2400);
  // veto yalnizca ok===true iken calisir
  assert.ok(blok.includes('_k678.ok && _k678.skor < V678_MIN_SKOR'), 'ok değilse elemez');
  assert.ok(/catch\(_e678\)[^]*aday KORUNDU/.test(blok), 'hata halinde aday korunmalı');
});

test('V678: kapı R493 kalite-sizing bloğundan ÖNCE', () => {
  // Sıra önemli: skor kapısı, marj hesabı yapılmadan önce elemeli.
  const v678 = server.indexOf('GRAFIK KALITE KAPISI');
  // DIKKAT: 'R493 KALİTE-SIZING' dizgisi dosyada iki kez geçiyor; ilki 7021'deki
  // sabit yorumu. Emir döngüsündekini tam metniyle ara.
  const r493 = server.indexOf('R493 KALİTE-SIZING — R480 skoruna');
  assert.ok(v678 > 0 && r493 > 0 && v678 < r493, 'V678 önce gelmeli');
});

test('V678: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
