'use strict';
// V4.7.4.7 — PIT TOP_GAINER ESIK HIZALAMASI
const fs=require('fs'),vm=require('vm'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`pass  ${n}`)):(fail++,console.error(`FAIL  ${n}${d?' :: '+d:''}`));
const i=src.indexOf('function v592PitSelectionMeta');
let d=0,k=src.indexOf('{',src.indexOf(')',i));const st=k;
for(;k<src.length;k++){if(src[k]==='{')d++;else if(src[k]==='}'){d--;if(!d){k++;break;}}}
const body=src.slice(i,k);
const mk=(mr,mh)=>{const c=vm.createContext({R497_PIT_MAX_RANK:mr,R497_PIT_MIN_HITS:mh,Number,String,Math});
  vm.runInContext(body,c);return (r,h,ex)=>c.v592PitSelectionMeta({r497Rank:r,r497PersistenceHits:h,selectionReason:ex});};

console.log('── H1  VARSAYILAN = eski sabit davranis ' + '─'.repeat(30));
{
  const t=mk(10,2);
  ok('H1a rank3/hits3 -> TOP_GAINER', t(3,3).selectionReason==='TOP_GAINER');
  ok('H1b rank5/hits0 -> RED', t(5,0).selectionReason===null);
  ok('H1c rank12/hits3 -> RED', t(12,3).selectionReason===null);
  ok('H1d sinir rank10/hits2 -> TOP_GAINER', t(10,2).selectionReason==='TOP_GAINER');
  ok('H1e rank11/hits2 -> RED', t(11,2).selectionReason===null);
  ok('H1f rank10/hits1 -> RED', t(10,1).selectionReason===null);
  ok('H1g explicit etiket her zaman gecer', t(99,0,'TOP_GAINER').provenance==='EXPLICIT_TOP_GAINER');
}
console.log('\n── H2  BACKTEST HIZALI (99/0) ' + '─'.repeat(40));
{
  const t=mk(99,0);
  ok('H2a rank5/hits0 -> TOP_GAINER', t(5,0).selectionReason==='TOP_GAINER');
  ok('H2b rank45/hits0 -> TOP_GAINER', t(45,0).selectionReason==='TOP_GAINER');
  ok('H2c provenance esikleri yansitiyor', t(5,0).provenance==='R497_PIT_TOP99_0OF3', t(5,0).provenance);
}
console.log('\n── H3  strictEligible HER ZAMAN kaydediliyor ' + '─'.repeat(25));
{
  const loose=mk(99,0);
  ok('H3a gevsek modda rank5/hits0 strict=false', loose(5,0).strictEligible===false);
  ok('H3b gevsek modda rank3/hits3 strict=true', loose(3,3).strictEligible===true);
  ok('H3c gevsek modda rank10/hits2 strict=true', loose(10,2).strictEligible===true);
  ok('H3d thresholds kaydediliyor', JSON.stringify(loose(5,0).thresholds)==='{"maxRank":99,"minHits":0,"strictMaxRank":10,"strictMinHits":2}', JSON.stringify(loose(5,0).thresholds));
  ok('H3e strict esikleri SABIT (10/2)', loose(5,0).thresholds.strictMaxRank===10 && loose(5,0).thresholds.strictMinHits===2);
}
console.log('\n── H4  Kaynak + kimlik ' + '─'.repeat(47));
{
  const h=re=>re.test(src);
  ok('H4a ENV sabitleri', h(/R497_PIT_MAX_RANK=Math\.max\(1,Math\.min\(999,Number\(process\.env\.R497_PIT_MAX_RANK\|\|10\)\)\)/));
  ok('H4b minHits ENV', h(/R497_PIT_MIN_HITS=Math\.max\(0,Math\.min\(10,Number\(process\.env\.R497_PIT_MIN_HITS\?\?2\)\)\)/));
  ok('H4c sabit rank<=10&&hits>=2 kalmadi', !h(/if\(rank<=10&&hits>=2\)return \{selectionReason:'TOP_GAINER',provenance:'R497_PIT_TOP10_2OF3'/));
  ok('H4d status pitTopGainer blogu', h(/pitTopGainer:\{maxRank:R497_PIT_MAX_RANK,minHits:R497_PIT_MIN_HITS/));
  ok('H4e backtestAligned bayragi', h(/backtestAligned:\(R497_PIT_MAX_RANK>=99&&R497_PIT_MIN_HITS<=0\)/));
  ok('H4f build V4.7.4.21', h(/V4_7_4_21_UNPROTECTED_EXCEPTION_RISK41_10X/));
  ok('H4g session 4_7_4_21_UP1', h(/V592_EXACT_CLOSED1M_R495_72H_4_7_4_21_UP1/));
  ok('H4h V4.5 kurali degismedi', h(/V592_V45_MS_SCORE_MIN\|\|35/)&&h(/V592_V45_FIRST_OBSTACLE_RR_MIN\|\|0\.35/));
  ok('H4i R493 kapisi degismedi', h(/const storyWait = \['PUSU','REJECT'\]\.includes\(authority\.action\);/));
  ok('H4j sozlesme degismedi', h(/R497_SLOT_MARGIN_USDT \|\| 41/)&&h(/R486_MAX_POSITIONS \|\| 2/)&&h(/R495_FINAL_RISK_PCT \|\| 4/));
  ok('H4k G1/G2 korumasi duruyor', h(/__forceFresh:true,/)&&h(/'POST_FILL_POSITION_PROOF'/));
}
console.log(`\n${fail?'SONUC: FAIL':'SONUC: PASS'} — ${pass} gecti, ${fail} dustu`);
process.exitCode=fail?1:0;
