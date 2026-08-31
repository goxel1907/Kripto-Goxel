'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// KULLANICI: "stop veya ATR yüzünden bariz fırsatlar asla kaçmayacak"
// TESPİT: V6.6.0'da ATR'ye göre genişlettiğim stop, R/R = engel/stop oranının
// paydasını büyütüp R493 kapısının aynı adayları elemesine yol açtı.
// ÖLÇÜM (10 aday, bölge girişi): plan stopu 7/10 · geniş stop 2/10 · V662 9/10

test('V662: bayraklar tanımlı, hedef R/R eşiğin üstünde', () => {
  assert.match(server, /const V662_ENGELE_UYUMLU_STOP = String\(process\.env\.V662_ENGELE_UYUMLU_STOP \?\? '1'\) !== '0';/);
  assert.match(server, /const V662_HEDEF_RR = Math\.max\(0\.36, Math\.min\(1\.5, Number\(process\.env\.V662_HEDEF_RR \|\| 0\.42\)\)\);/);
  // R493 esigi 0.35 — hedef ondan BUYUK olmali yoksa stop kendi islemini eler
  assert.match(server, /R493_MIN_FIRST_OBSTACLE_RR/);
});

test('V662: stop tavanı engel mesafesinden türetiliyor', () => {
  assert.match(server, /const _v662Engel = _v662Aday\[0\] \|\| 0;/);
  assert.match(server, /const _v662D = \(_v662Engel > 0 && entry > 0\) \? \(_v662Engel-entry\)\/entry\*100 : 0;/);
  assert.match(server, /const _v662Tavan = \(V662_ENGELE_UYUMLU_STOP && _v662D > 0\) \? _v662D \/ V662_HEDEF_RR : Infinity;/);
  assert.match(server, /minStopPct=Math\.max\(\.80,Math\.min\(_v660LikTavan,_v660Gereken,_v662Tavan\)\)/);
});

test('V662: engel HESAPLANABILIR — `first` sonra geldiği için yerinde türetiliyor', () => {
  const tavanIdx = server.indexOf('const _v662Tavan =');
  const firstIdx = server.indexOf('const first=firstObstacleDirectionValid');
  const minIdx   = server.indexOf('minStopPct=Math.max(.80,Math.min(_v660LikTavan');
  assert.ok(tavanIdx > 0 && minIdx > tavanIdx, 'tavan, minStopPct hesabından önce olmalı');
  assert.ok(firstIdx > minIdx, '`first` sonra geliyor — bu yüzden yerinde türetim şart');
  // kaynaklar story uzerinde ve entry opts'ta — ikisi de o noktada mevcut
  assert.match(server, /Number\(story\?\.firstObstacle\|\|0\),/);
  assert.match(server, /Number\(story\?\.liquidity\?\.\['15m'\]\?\.above\|\|0\)/);
});

test('V662: sessiz no-op alarmı var (engel bulunamazsa loglanır)', () => {
  assert.match(server, /V662 stop fırsata uyduruldu/);
  assert.match(server, /V662: girişin üstünde engel bulunamadı/);
});

test('V662: matematik — stop kendi işlemini diskalifiye edemez', () => {
  const HEDEF = 0.42, ESIK = 0.35, LIK = 12.86;
  const stop = (d, atrStop) => Math.max(0.80, Math.min(LIK, atrStop, d / HEDEF));
  const rr = (d, s) => d / s;
  // engel yakinken stop DARALIR ve islem GECER
  for (const [d, atrStop] of [[0.93, 9.4], [0.88, 9.4], [1.34, 9.4], [1.74, 9.4], [2.22, 9.4]]) {
    const s = stop(d, atrStop);
    assert.ok(rr(d, s) >= ESIK, `engel %${d} → stop %${s.toFixed(2)} → R/R ${rr(d,s).toFixed(3)} eşiği geçmeli`);
  }
  // yer VARSA stop genis kalir (ATR/likidasyon baglar)
  assert.ok(Math.abs(stop(13.49, 20) - LIK) < 0.01, 'LA: yer var → stop likidasyon tavanında');
  // gercekten yer yoksa taban devreye girer ve islem ELENIR (dogru davranis)
  const sNot = stop(0.26, 9.4);
  assert.ok(Math.abs(sNot - 0.80) < 0.01, 'NOT: taban %0,80');
  assert.ok(rr(0.26, sNot) < ESIK, 'NOT gerçekten fırsat değil — elenmeli');
});

test('V662: ATR artık hiçbir şeyi ENGELLEMİYOR', () => {
  // ATR vetosu kapali, butce vetosu kapali, ATR yalniz TAVAN adaylarindan biri
  assert.match(server, /const V660_DURDURULAMAZ_VETO = String\(process\.env\.V660_DURDURULAMAZ_VETO \?\? '0'\) !== '0';/);
  assert.match(server, /const V661_BUTCE_VETO = String\(process\.env\.V661_BUTCE_VETO \?\? '0'\) !== '0';/);
});

test('V662: kullanıcı sözleşmesi aynen duruyor', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'));
  assert.match(server, /const V601_HARD_MARGIN_FLOOR_USDT = 50;/);        // 50$ min
  assert.match(server, /const V601_HARD_MARGIN_CAP_USDT = 100;/);
  assert.match(server, /const _v660LikTavan = \(100 \/ _v660Lev\) \* V660_LIK_PAYI;/);  // likidasyon
  assert.match(server, /V649_BOLGEDEN_GIRIS/);                            // bölgeden giriş
});
