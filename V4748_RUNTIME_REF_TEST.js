'use strict';
// V4.7.4.8 — CALISMA ZAMANI REFERANS TESTI
// node --check sozdizimini dogrular ama TANIMSIZ DEGISKEN referansini yakalamaz.
// V4.7.4.7'de R497_MIN_CHANGE_24H_PCT (ENV adi, kod sabiti degil) kullanildi ->
// iki status endpointi de 500 verdi. Bu test o sinifi kalici olarak kapatir.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));

const declared=new Set();
for(const re of [/^(?:const|let|var)\s+([A-Za-z_$][\w$]*)/gm,
                 /^(?:async\s+)?function\s+([A-Za-z_$][\w$]*)/gm,
                 /^\s+(?:const|let|var)\s+([A-Za-z_$][\w$]*)/gm,
                 /(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=/g])
  for(const m of src.matchAll(re)) declared.add(m[1]);

console.log('── I1  Status bloklarinda tanimsiz sabit YOK ' + '─'.repeat(26));
{
  const blocks=[...src.matchAll(/parityV4741:\{[\s\S]{0,1600}?\},(?:strictForceFresh|accountSnapshot|dualLane)/g)].map(x=>x[0]);
  ok('I1a iki status blogu bulundu', blocks.length===2, `${blocks.length}`);
  const bad=[];
  // V4.7.4.9: tirnak icindeki metinler SABIT DEGILDIR — taramadan cikar.
  const strip=t=>t.replace(/'(?:[^'\\]|\\.)*'/g,"''").replace(/"(?:[^"\\]|\\.)*"/g,'""').replace(/`(?:[^`\\]|\\.)*`/g,'``');
  // V4.7.4.17: nesne ANAHTARLARI (WORD:) da sabit degildir — taramadan cikar.
  const stripKeys=t=>t.replace(/\b[A-Z][A-Z0-9_]{4,}\s*:/g,'k:');
  for(const b of blocks) for(const m of stripKeys(strip(b)).matchAll(/\b([A-Z][A-Z0-9_]{4,})\b/g))
    if(!declared.has(m[1]) && !bad.includes(m[1])) bad.push(m[1]);
  ok('I1b tanimsiz BUYUK_HARF sabit yok', bad.length===0, bad.join(', '));
}
console.log('\n── I2  Bilinen tuzaklar ' + '─'.repeat(46));
{
  ok('I2a R497_MIN_CHANGE_24H_PCT kod sabiti olarak KULLANILMIYOR',
     !/[^.\w]R497_MIN_CHANGE_24H_PCT\b(?!\s*\|\|)/.test(src.replace(/process\.env\.R497_MIN_CHANGE_24H_PCT/g,'')),
     'ENV adi kod sabiti gibi kullanilmis');
  ok('I2b dogru sabit kullaniliyor', /minChange24hPct:R497_MIN_CHANGE_24H\}/.test(src));
  ok('I2c R497_MIN_CHANGE_24H tanimli', declared.has('R497_MIN_CHANGE_24H'));
  ok('I2d R497_PIT_MAX_RANK tanimli', declared.has('R497_PIT_MAX_RANK'));
  ok('I2e R497_PIT_MIN_HITS tanimli', declared.has('R497_PIT_MIN_HITS'));
  ok('I2f ORDER_TRUTH_MAX_AGE_MS tanimli', declared.has('ORDER_TRUTH_MAX_AGE_MS'));
  ok('I2g V592_LOCK_PATH tanimli', declared.has('V592_LOCK_PATH'));
  ok('I2h v592ParityStats tanimli', declared.has('v592ParityStats'));
  ok('I2i marketTimeOffset tanimli', declared.has('marketTimeOffset'));
  ok('I2j marketTimeSyncedAt tanimli', declared.has('marketTimeSyncedAt'));
}
console.log('\n── I3  Kimlik ' + '─'.repeat(56));
{
  ok('I3a build V4.7.4.29', /V4_7_4_29_CLOSE_PNL_RISK41_10X/.test(src));
  ok('I3b session 4_7_4_29_CP1', /V592_EXACT_CLOSED1M_R495_72H_4_7_4_29_CP1/.test(src));
  ok('I3c PIT hizalama duruyor', /R497_PIT_MAX_RANK=Math\.max/.test(src)&&/strictEligible/.test(src));
  ok('I3d G1/G2 koruma duruyor', /__forceFresh:true,/.test(src)&&/'POST_FILL_POSITION_PROOF'/.test(src));
  ok('I3e sozlesme degismedi', /R497_SLOT_MARGIN_USDT \|\| 41/.test(src)&&/R495_FINAL_RISK_PCT \|\| 4/.test(src));
}
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
