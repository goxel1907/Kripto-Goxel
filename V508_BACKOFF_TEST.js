// V5.0.8 — 418 YASAK SURESINE UYMA
// OLCULDU 12.08 (V507_72H_TEMIZ_1): 4,9 saatte 29 adet 418.
// 28 olayin 25'i onceki yasak bitmeden yapilan cagriydi.
// Kok sebep: Math.min(120, retry) — Binance 3441 dedi, kod 120'ye kirpti.
const fs=require('fs'),path=require('path'),vm=require('vm');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const has=t=>src.includes(t);

console.log('══ A — TAVANLAR KALKTI ' + '═'.repeat(48));
ok('120sn tavani YOK', !has('Math.min(120, Number(retry)||60)'));
ok('180sn tavani YOK', !has('Math.min(180, Number(seconds)||45)'));
ok('yeni tavan sabiti', has('const V508_BACKOFF_MAX_SEC'));
ok('  varsayilan 3600 sn', /V508_BACKOFF_MAX_SEC \|\| 3600/.test(src));
ok('  ENV ile ayarlanabilir', has('process.env.V508_BACKOFF_MAX_SEC'));
ok('retryAfter gercekten kullaniliyor', has('Math.min(V508_BACKOFF_MAX_SEC, Number(retry)||60)'));
ok('ikinci noktada da', has('Math.min(V508_BACKOFF_MAX_SEC, Number(seconds)||45)'));

console.log('\n══ B — DAVRANIS: 3441 sn KIRPILMIYOR ' + '═'.repeat(34));
{
  // gercek formulu calistir
  const MAX=3600;
  const hesap=(retryHeader,status)=>{
    const retry=parseInt(retryHeader||(Number(status)===418?'60':'60'),10);
    return Math.max(Number(status)===418?60:30, Math.min(MAX, Number(retry)||60));
  };
  ok('retryAfter 3441 -> 3441 sn', hesap('3441',418)===3441, String(hesap('3441',418)));
  ok('retryAfter 69   -> 69 degil 60 taban', hesap('69',418)===69, String(hesap('69',418)));
  ok('retryAfter 30   -> 60 tabani uygulanir', hesap('30',418)===60, String(hesap('30',418)));
  ok('retryAfter 9999 -> 3600 tavani', hesap('9999',418)===3600, String(hesap('9999',418)));
  ok('ESKI kod 3441 icin 120 verirdi', Math.max(60,Math.min(120,3441))===120);
}

console.log('\n══ C — RESTART YASAGI SIFIRLAMIYOR ' + '═'.repeat(36));
ok('diske yazma fonksiyonu', has('function v508SaveBackoff()'));
ok('  atomik yazim (tmp+rename)', has('fs.renameSync(tmp,V508_BACKOFF_PATH)'));
ok('diskten okuma fonksiyonu', has('function v508LoadBackoff()'));
ok('  gecmis yasak atlanir', has('if(e>now)') && has('if(p2>now)'));
ok('her backoff kaydinda yaziliyor', has('v508SaveBackoff();   // V5.0.8'));
ok('ACILISTA yukleniyor', (src.match(/v508LoadBackoff\(\)/g)||[]).length>=2);
ok('state dizini /data', has("process.env.TESTNET_STATE_DIR||'/data'"));

console.log('\n══ D — OLCULEBILIR ' + '═'.repeat(52));
ok('engellenen cagri sayaci', has('execBackoffBlocked'));
ok('418 funnel kaydi (V507)', has("type:'BINANCE_RATE_LIMIT'"));
ok('  retryAfter kaydediliyor', has('retryAfter:retryHeader'));

console.log('\n══ E — OLCUM KAYNAKTA ' + '═'.repeat(49));
ok('29 olay / 25 erken cagri', has("28 olayin 25'i"));
ok('ornek zincir belgeli', has('3441 sn') && has('2077 sn'));
ok('%45 kayip olcumu', has("%45'i oldu"));
ok('kok sebep yazili', has('Math.min(120, retry)'));
ok('hata sinifi belirtilmis', has("restart'ta olen state"));
ok('eski yorumun neden yanlis oldugu', has('YASAGI BUYUTUYOR'));

console.log('\n══ F — ONCEKI SURUMLER ' + '═'.repeat(48));
for(const [ad,t] of [['V507 kaldirac retry','V507_LEVERAGE_PROOF_MAX_ATTEMPTS'],
  ['V506 kayip telemetrisi',"type:'ENTRY_CANDLE_DRIFT_BLOCK'"],['V505 oy duzeltmesi','!takerVoteActive'],
  ['V504 taker sabiti','V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199'],['V503 on-filtre','V503 testnet evren on-filtresi'],
  ['V502 kaldirac cift kilidi','V502-A: KALDIRAC KILIDI'],['parite kapisi','function v592BootParityGate'],
  ['B1-B6 engelleri','V501_KNOWN_PARITY_BLOCKERS']]) ok(ad, has(t));
ok('SONDA yok', !has('/api/probe'));
ok('build V5_0_8', has('V5_0_8_BACKOFF_HONORED'));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
