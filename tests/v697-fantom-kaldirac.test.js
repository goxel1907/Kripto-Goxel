// V697: (1) V644 hafizasi ilk engeli "onceki hedef" diye ithal etmesin,
//       (2) gec-kovalama esigi varsayilan KAPALI (olcum ters cikti),
//       (3) R431/R461 haritasi kilit aciksa borsaya giden degeri EZMESIN.
const fs=require('fs'), path=require('path'), assert=require('assert');
const SRC=fs.readFileSync(path.join(__dirname,'..','server.js'),'utf8');

// 1) hafiza hedefi artik V692 bayragina bagli
assert.ok(/const target=Number\(\(V692_ILK_ENGEL_KAPISI_KALKTI\?0:Number\(tf15\.obstacle\?\.firstObstacle\?\.price\|\|0\)\)/.test(SRC),
  'V644 hafiza hedefi hala dogrudan ilk engelden okunuyor (ailenin 6. basi acik)');

// 2) lateEvidence icindeki 0.35 sabiti kalkti, esik env'e bagli ve varsayilan 0
assert.ok(!/distanceFromBreakoutAtr\|\|0\)>\.35/.test(SRC), 'gec-kovalama 0.35 sabiti hala kodda');
assert.ok(/V697_GEC_KOVALAMA_ATR>0&&Number\(currentStory\?\.distanceFromBreakoutAtr\|\|0\)>V697_GEC_KOVALAMA_ATR/.test(SRC),
  'gec-kovalama esigi V697_GEC_KOVALAMA_ATR uzerinden okunmuyor');
const m=SRC.match(/const V697_GEC_KOVALAMA_ATR = Math\.max\(0, Math\.min\(9, Number\(process\.env\.V697_GEC_KOVALAMA_ATR \?\? (\d+)\)\)\)/);
assert.ok(m, 'V697_GEC_KOVALAMA_ATR tanimi bulunamadi');
assert.strictEqual(m[1],'0','gec-kovalama varsayilani KAPALI (0) olmali — olcum t=3,66 ile TERS cikti');

// 3) R431 ve R461 kilit aciksa haritayi uygulamaz
assert.ok(/const _v697Kilit = V592_EXACT_BACKTEST_AUTHORITY && Number\(V592_LEVERAGE_LOCK\) > 0;/.test(SRC),
  'R431 fantom koruma satiri yok');
assert.ok(/if \(_v697Kilit && executeLeverage !== Number\(V592_LEVERAGE_LOCK\)\)/.test(SRC),
  'R431 kilit uygulamasi yok');
assert.ok(/V697: R461 de ayni fantomu uretiyordu/.test(SRC), 'R461 fantom koruma yorumu yok');
assert.ok(/if \(V592_EXACT_BACKTEST_AUTHORITY && Number\(V592_LEVERAGE_LOCK\) > 0\) aiTargetLev = Number\(V592_LEVERAGE_LOCK\);/.test(SRC),
  'R461 kilit uygulamasi yok');

// 4) TDZ: tanim kullanimdan ONCE olmali
assert.ok(SRC.indexOf('const V697_GEC_KOVALAMA_ATR =') < SRC.indexOf('V697_GEC_KOVALAMA_ATR>0&&Number('),
  'TDZ: V697_GEC_KOVALAMA_ATR kullanimdan sonra tanimlanmis');

// 5) sessiz no-op yasagi: acilista ilan ediliyor
assert.ok(/\[V697\] V644 HAFIZASI/.test(SRC) && /\[V697\] KALDIRAC KAYDI/.test(SRC),
  'V697 acilis ilanlari eksik');

// 6) BUILD etiketi package.json ile ayni surumu gostermeli
const pkg=JSON.parse(fs.readFileSync(path.join(__dirname,'..','package.json'),'utf8'));
const b=SRC.match(/const LAZARUS_BUILD = '([^']+)'/)[1];
assert.strictEqual(b.slice(0,5), 'V'+pkg.version.replace(/\./g,'_').slice(0,4), `BUILD ${b} != package ${pkg.version}`);
console.log('v697 fantom kaldirac + 6. bas: OK');
