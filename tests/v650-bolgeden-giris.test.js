'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// ÖLÇÜM: 78 canlı işlem · net -90,88$ · kazanma %42,3  |  backtest %70,1
// Backtest sinyallerinde SADECE girişi kaydırma testi:
//   %0,50 -> %66,6 · %0,84 -> %50,6 · %1,30 -> %46,9 · %1,50 -> %40,8
// Canlıda ölçülen bölge-üstü kayma ortancası ~%1,3 -> canlı %42,3 ile örtüşüyor.

test('V650: bayraklar tanımlı ve env ile geri alınabilir', () => {
  assert.match(server, /const V649_BOLGEDEN_GIRIS = String\(process\.env\.V649_BOLGEDEN_GIRIS \?\? '1'\) !== '0';/);
  assert.match(server, /const V649_BOLGE_TOLERANS = Math\.max\(0\.05, Math\.min\(5, Number\(process\.env\.V649_BOLGE_TOLERANS \?\? 0\.5\)\)\);/);
  assert.match(server, /const V649_BOLGE_MAX_SAPMA = Math\.max\(1, Math\.min\(15, Number\(process\.env\.V649_BOLGE_MAX_SAPMA \?\? 4\)\)\);/);
});

test('V650: giriş hafıza bölgesinin TAVANINA çekilir', () => {
  assert.match(server, /const _hf = v644CandidateMemoryContext\(story\?\.symbol, Number\(entry \|\| 0\), story\);/);
  assert.match(server, /const _z = _hf\?\.available \? _hf\.entryZone : null, _tepe = Number\(_z\?\.high \|\| 0\);/);
  // V651: sapma artik GERCEK giristen olculuyor (plannedEntry kirilim projeksiyonu olabiliyor)
  assert.match(server, /_v649Sapma = \(_v649Ref - _tepe\) \/ _tepe \* 100;/);
  assert.match(server, /_v649Bolge = _z; _v649BolgeUstu = true;/);
  assert.match(server, /plannedEntry = Math\.min\(plannedEntry, _tepe\);/);
});

test('V650: SESSIZ NO-OP DEGIL — okudugu alanlari hafiza fonksiyonu gercekten uretiyor', () => {
  // V647'de `recommendedSl` gate'e hic gecilmedigi icin dal olu kalmisti.
  // Burada okunan her alan v644CandidateMemoryContext'in donus objesinde OLMALI.
  assert.match(server, /entryZone:zone\?\{low:r49356Round\(zone\.low,10\),high:r49356Round\(zone\.high,10\),type:zone\.type\}:null/);
  assert.match(server, /return \{active:true,available:true,symbol:S/);
  // ve fonksiyon, yamanin kullanildigi yerden ONCE tanimli olmali
  const tanim = server.indexOf('function v644CandidateMemoryContext');
  const kullanim = server.indexOf('const _hf = v644CandidateMemoryContext(story?.symbol');
  assert.ok(tanim > 0 && kullanim > tanim, 'fonksiyon kullanimdan once tanimli olmali');
});

test('V650: bölge üstündeyken MARKET emri yok (pusuya düşer)', () => {
  assert.match(server, /marketAllowed=!\(storyWait\|\|planIncoherent\|\|legacyFirstObstacleHard\|\|directionAgainst\|\|\(tightStop&&retestFar\)\|\|r493EntrySafety\.blocked\|\|_v649BolgeUstu\)/);
});

test('V650: pusu seviyesi zinciri kopuk degil (plannedEntry -> ai.entry -> r442)', () => {
  // r447 WAIT donusu plannedEntry'yi `entry` olarak verir
  assert.match(server, /entry:r\(entryTruth\.plannedEntry\|\|0\)/);
  // ai.entry r442PusuKur'a gider
  assert.match(server, /if \(Number\(ai\?\.entry\) > 0\) r442PusuKur\(coin\.fullSymbol, ai,/);
  // r442PusuKur pusu seviyesini ai.entry'den alir
  assert.match(server, /const entry = Number\(ai\?\.entry \|\| 0\);/);
});

test('V650: gerekçe loglara ve dönüş objesine giriyor', () => {
  assert.match(server, /if\(_v649BolgeUstu\)reasons\.push\(/);
  assert.match(server, /v649BolgeUstu:_v649BolgeUstu,v649Sapma:/);
  assert.match(server, /V649 giriş bölgeye çekildi/);
});

test('V650: bölge çok uzaksa DOKUNULMAZ (bayat hafıza) ve loglanır', () => {
  assert.match(server, /\} else if \(_v649Sapma > V649_BOLGE_MAX_SAPMA\) \{/);
  assert.match(server, /hafıza bölgesi çok uzak/);
});

test('V650: tolerans bandı matematiği', () => {
  const TOL = 0.5, MAX = 4;
  const karar = (sapma) => sapma > TOL && sapma <= MAX ? 'CEK' : (sapma > MAX ? 'BAYAT' : 'DOKUNMA');
  // canlida olculen gercek sapmalar
  assert.strictEqual(karar(0.84), 'CEK');   // KOMA
  assert.strictEqual(karar(1.28), 'CEK');   // ZKP
  assert.strictEqual(karar(1.30), 'CEK');   // TAC
  assert.strictEqual(karar(1.45), 'CEK');   // COLLECT
  assert.strictEqual(karar(2.24), 'CEK');   // PROM
  assert.strictEqual(karar(0.30), 'DOKUNMA');
  assert.strictEqual(karar(4.92), 'BAYAT'); // ZKP'nin eski bolgesi
});

test('V650: kazanma oranı / kayma ilişkisi (ölçümün kaydı)', () => {
  // backtest kaydirmasi -> kazanma orani (1060 sinyal uzerinde olculdu)
  const olcum = { 0: 70.1, 0.5: 66.6, 0.84: 50.6, 1.30: 46.9, 1.50: 40.8, 2.24: 33.4 };
  const canli = 42.3;
  assert.ok(olcum[1.30] > canli && canli > olcum[1.50],
    'canli kazanma orani %1,30-%1,50 kayma bandina dusmeli');
  assert.ok(olcum[0.5] - olcum[0.84] > 15, 'kirilma %0,5 ile %0,84 arasinda — tolerans oraya konuldu');
});

test('V650: sözleşme ve önceki düzeltmeler yerinde', () => {
  const _b = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  const _pkg = JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
  assert.ok(_b && _b[1].startsWith('V' + String(_pkg.version).replace(/\./g,'_') + '_'),
    `LAZARUS_BUILD (${_b && _b[1]}) package.json (${_pkg.version}) ile uyusmuyor`);
  assert.match(server, /V634_TOPLAM_RISK_PCT/);
  assert.match(server, /const breakEvenAt = _v646BeTaban;/);   // V649 cift-geo geri alma duruyor
  assert.match(server, /V648_EN_YAKIN_ENGEL/);
});

// ── V650-C: GRAFİK MOTORU — pivotlar karar vericiye açılır ──────────────────
// Motor `r484Structure(rows[k],k).pivots` ile swing pivotları ZATEN üretiyordu;
// story'ye konulmadığı için karar veren onları hiç görmüyordu.

test('V650-C: pivot verisi story\'ye ekleniyor (5m/15m/1h)', () => {
  assert.match(server, /const V650_PIVOT_GORUS = String\(process\.env\.V650_PIVOT_GORUS \?\? '1'\) !== '0';/);
  assert.match(server, /,pivots:V650_PIVOT_GORUS\?\{'5m':\{high:/);
  for (const tf of ['5m','15m','1h']) {
    assert.ok(server.includes(`(tf['${tf}']?.pivots?.H||[]).slice(-8)`), `${tf} pivotu okunmali`);
  }
});

test('V650-C: SESSIZ NO-OP DEGIL — pivot zinciri uctan uca mevcut', () => {
  // r483Pivots {H,L} uretir, elemanlarin .price alani vardir
  assert.match(server, /function r483Pivots\(rs=\[\],span=2\)\{/);
  assert.match(server, /H\.push\(\{i,ts:rs\[i\]\.ts\|\|0,price:rs\[i\]\.h/);
  // r484Structure onu pivots olarak dondurur
  assert.match(server, /trendline:tl,pivots:p\}/);
  // r483ChartStory her tf icin r484Structure cagirir -> tf[k].pivots.H dolu
  assert.match(server, /for\(const k of \['1m','5m','15m','1h','4h','1d'\]\)\{rows\[k\]=r483Rows\(c\[k\],times\[k\]\);tf\[k\]=r484Structure\(rows\[k\],k\);\}/);
});

test('V650-C2: pivot engeli VARSAYILAN KAPALI ve no-op alarmı var', () => {
  assert.match(server, /const V650_PIVOT_ENGEL = String\(process\.env\.V650_PIVOT_ENGEL \?\? '0'\) !== '0';/);
  assert.match(server, /const _v650Adaylar = fallbackObstacles\.concat\(_v650Pivot\)\.sort\(\(a,b\)=>a-b\);/);
  assert.match(server, /const first=firstObstacleDirectionValid&&firstRaw>0\?firstRaw:\(_v650Adaylar\[0\]\|\|0\);/);
  assert.match(server, /V650 pivot engeli AÇIK ama pivot listesi BOŞ/);
});

test('V650-C: engel adayları sıralamadan ÖNCE birleşiyor (en yakın seçilebilsin)', () => {
  const birlesim = server.indexOf('fallbackObstacles.concat(_v650Pivot)');
  const secim = server.indexOf('(_v650Adaylar[0]||0)');
  assert.ok(birlesim > 0 && secim > birlesim, 'birlesim secimden once olmali');
});
