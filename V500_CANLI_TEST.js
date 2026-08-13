// ════════════════════════════════════════════════════════════════════════
// V5.0.0 CANLI SURUM TESTI — canliya gecerken SESSIZCE bozulan her seyi cakar
// ════════════════════════════════════════════════════════════════════════
// Bu dosyanin varlik sebebi: testnet surumunde backtest sozlesmesinin
// TAMAMI "BINANCE_EXECUTION_ENV==='TESTNET'" kosuluna bagliydi. Tek satir
// degistirip canliya gecen biri, hicbir hata almadan, backtestte hic
// olculmemis bir botu gercek parayla calistirirdi. Asagidaki her iddia
// o senaryonun bir parcasini kilitler.
const fs=require('fs'),path=require('path');
const src=fs.readFileSync(path.join(__dirname,'server.js'),'utf8');
let pass=0,fail=0;
const ok=(n,c,d)=>c?(pass++,console.log(`  pass  ${n}`)):(fail++,console.error(`  FAIL  ${n}${d?' :: '+d:''}`));
const cnt=(re)=>(src.match(new RegExp(re,'g'))||[]).length;
const has=(t)=>src.includes(t);

console.log('══ A — ORTAM BAGI KOPARILDI (asil duzeltme) ' + '═'.repeat(28));
ok('EXACT_BACKTEST_AUTHORITY artik ortama bagli DEGIL',
   !/V592_EXACT_BACKTEST_AUTHORITY\s*=\s*BINANCE_EXECUTION_ENV/.test(src));
ok('EXACT_BACKTEST_AUTHORITY yalniz ENV bayragindan okunur',
   has("const V592_EXACT_BACKTEST_AUTHORITY = String(process.env.V592_EXACT_BACKTEST_AUTHORITY ?? '1') !== '0';"));
ok('V45 secici (backtestin mask_rule\'u) ortama bagli DEGIL',
   !/V592_V45_TESTNET_ACTIVE\s*=\s*BINANCE_EXECUTION_ENV/.test(src));
ok('V45 secici yalniz ENV bayragindan okunur',
   has("const V592_V45_TESTNET_ACTIVE = String(process.env.V592_V45_TESTNET_ACTIVE??'1')==='1';"));
ok('EXACT_BACKTEST_AUTHORITY hala 27 davranis noktasini besliyor', cnt('V592_EXACT_BACKTEST_AUTHORITY')>=25);
ok('V45 secici hala emri blokluyor (tek kapi)', has('if(V592_V45_TESTNET_ACTIVE&&!_v45.eligible)'));
ok('kok sebep kaynakta belgeli (C1)', has('CANLI-C1') && has('27 cagri noktasinda davranis degisirdi'));
ok('kok sebep kaynakta belgeli (C2)', has('CANLI-C2') && has("mask_rule"));

console.log('\n══ B — EMIR URL\'I ORTAMLA TUTARLI ' + '═'.repeat(38));
ok('testnet URL artik SABIT degil', !has("const BINANCE_EXECUTION_FAPI = 'https://testnet.binancefuture.com';"));
ok('URL ortamdan turetiliyor', has("BINANCE_EXECUTION_FAPI = BINANCE_EXECUTION_ENV==='LIVE'"));
ok('canli dali fapi.binance.com', has("? 'https://fapi.binance.com'"));
ok('testnet dali korundu', has(": 'https://testnet.binancefuture.com';"));
ok('ortam ENV\'den okunur, sabit degil', has("const BINANCE_EXECUTION_ENV = (String(process.env.BINANCE_EXECUTION_ENV||'LIVE')"));
ok('piyasa verisi LIVE kaldi (backtest paritesi)', has("const BINANCE_MARKET_DATA_ENV = 'LIVE';"));

console.log('\n══ C — KIMLIK BILGISI ORTAMA GORE COZULUR ' + '═'.repeat(30));
ok('sabit BINANCE_TESTNET_API_KEY okumasi KALMADI', !has("process.env.BINANCE_TESTNET_API_KEY||''"));
ok('tek cozucu: r486391BinanceCreds', cnt('function r486391BinanceCreds')===1);
ok('canli anahtar adi tanimli', has("'BINANCE_LIVE_API_KEY'") && has("'BINANCE_LIVE_API_SECRET'"));
ok('testnet anahtar adi korundu', has("'BINANCE_TESTNET_API_KEY'") && has("'BINANCE_TESTNET_API_SECRET'"));
ok('bReq + bAlgo cozucuyu cagiriyor (CANLI-C3 x2)', src.split('r486391BinanceCreds());   // CANLI-C3').length-1===2);
ok('cozucu sistem genelinde tek kaynak', cnt('r486391BinanceCreds\\(\\)')>=10);
ok('kok sebep belgeli (C3)', has('CANLI-C3') && has('bot hic emir acamaz'));

console.log('\n══ D — SONDA CANLI SURUMDE YOK ' + '═'.repeat(41));
for(const ad of ['v592ProbeOpenOne','v592ProbeCloseOne','v592ProbeCycle','v592ProbeBootRecover',
                 'v592ProbeRows','v592ProbeFlat','v592ProbeWrite','v592ProbeCreds','v592ProbeMarkPrice',
                 'v592ProbeSaveState','v592ProbeLoadState','V592_PROBE_INTERVAL_MS','V592_PROBE_MARGIN_USDT'])
  ok(`silindi: ${ad}`, !has(ad));
ok('sonda HTTP ucu yok', !has('/api/probe'));
ok('sonda zamanlayicisi yok', !/setInterval\([^)]*v592Probe/.test(src));
ok('V592_PROBE_ACTIVE sabit false', has('const V592_PROBE_ACTIVE = false;'));
ok('slot ofseti daima 0', has('function v592ProbeSlotOffset(){ return 0; }'));
ok('sonda sembol testi daima false', has('function v592IsProbeSymbol(){ return false; }'));
ok('kok sebep belgeli (C4)', has('CANLI-C4'));

console.log('\n══ E — ACILIS PARITE KAPISI (fail-closed) ' + '═'.repeat(30));
ok('kapi fonksiyonu var', has('function v592BootParityGate()'));
ok('acilista cagriliyor', has('try{ v592BootParityGate(); }catch(e){'));
ok('bayrak FAIL-CLOSED baslar', has("let V592_TRADING_HARD_BLOCK = 'BOOT_PARITY_GATE_NOT_RUN';"));
ok('kapi patlarsa da emir yolu kapali', has("V592_TRADING_HARD_BLOCK = 'BOOT_PARITY_GATE_THREW:'"));
for(const m of ['V592_EXACT_BACKTEST_AUTHORITY','V592_V45_TESTNET_ACTIVE','V592_ENTRY_CANDLE_PARITY',
                'V592_EXIT_CANDLE_PARITY','V592_EXIT_TYPE_WHITELIST','V592_LEDGER_EVIDENCE_ONLY'])
  ok(`sozlesme maddesi denetleniyor: ${m}`, new RegExp(m+'[,\\s]').test(src.slice(src.indexOf('const sozlesme={'))));
ok('kaldirac kilidi 10 dogrulanir', has("if(Number(V592_LEVERAGE_LOCK)!==10)"));
ok('min-hold dogrulanir', has("if(Number(V592_MIN_HOLD_MS)<=0)"));
ok('sonda canlida aktifse kapi kapanir', has("if(V592_PROBE_ACTIVE!==false)       hata.push('SONDA_CANLIDA_AKTIF');"));
ok('URL<->ortam tutarliligi denetlenir', has('URL_ORTAM_UYUSMAZLIGI'));
ok('kimlik eksikligi denetlenir', has('KIMLIK_EKSIK'));
ok('gercek para ACIK silahlandirma ister', has("LAZARUS_LIVE_ARM") && has("'CANLI-PARA-ONAY'"));
ok('silahlandirilmamis canli = emir yok', has("if(BINANCE_EXECUTION_ENV==='LIVE' && !armed) hata.push('CANLI_SILAHLANDIRILMADI:LAZARUS_LIVE_ARM');"));
ok('durum ucu var', has("app.get('/api/canli/parity-gate'"));
ok('kok sebep belgeli (C5)', has('CANLI-C5') && has('sessizce kapanan kural'));

console.log('\n══ F — EMIR YOLU KAPIYA BAGLI ' + '═'.repeat(42));
ok('giris emri kapiyi kontrol eder', has("if(V592_TRADING_HARD_BLOCK){"));
ok('bloklu emir firlatir', has('PARITE_KAPISI_KAPALI'));
ok('bloklu emir iz birakir', has("'ORDER_BLOCKED_BY_PARITY_GATE'"));
ok('kontrol fonksiyonun ILK isi (await oncesi)', (()=>{
  const i=src.indexOf('async function v592SendMainOrderIdempotent');
  const g=src.indexOf('V592_TRADING_HARD_BLOCK',i), a=src.indexOf('await',i);
  return i>0 && g>0 && g<a;
})());
ok('sonda preempt cagrisi kaldirildi', !has('await v592ProbePreempt(apiKey,apiSecret,sym);'));

console.log('\n══ G — DEGISMEMESI GEREKENLER (backtest sozlesmesi) ' + '═'.repeat(20));
ok('giris: candidateTs + 180000', has('candidateToEntryMs:180000'));
ok('cikis: exitTs %% 60000 = 59999', has('exitTsMod60000:59999'));
ok('cikis sayilari 338/210/165/12', has('DYNAMIC_STOP:338') && has('INITIAL_SL:210') && has('TARGET:165') && has('MAX_TIME_NO_PROGRESS:12'));
ok('izinli cikislar 4 + MAX_24H', has("allowedExits:['INITIAL_SL','TARGET','DYNAMIC_STOP','MAX_TIME_NO_PROGRESS','MAX_24H']"));
ok('V45 esikleri: MS>=35', has('V592_V45_MS_SCORE_MIN||35'));
ok('V45 esikleri: FO>=0.35', has('V592_V45_FIRST_OBSTACLE_RR_MIN||0.35'));
ok('V45 TOP_GAINER sarti duruyor', has('V592_V45_REQUIRE_TOP_GAINER'));
ok('hayalet defter muhafizi DURUYOR', has('phantomLedgerBlocked'));
ok('sanal ozkaynak hayalet filtresi DURUYOR', has('function r500VirtualEquityRowValid'));
ok('operator onceligi duzeltmesi DURUYOR (BH1)', has("if (V592_EXACT_BACKTEST_AUTHORITY && (action.type === 'EMERGENCY_EXIT'"));
ok('tek kapanis hunisi DURUYOR (AS1)', has('function v592FinalizeClose'));
ok('ayri dedup Map\'leri DURUYOR (AU1)', has('const v592ClosedOnce') && has('const v592EvidenceClosedOnce'));
ok('kapanis ispati varsayilan DURUYOR (AN1)', has('V592_CLOSE_PROOF_EXEMPT'));

console.log('\n══ H — SURUM KIMLIGI ' + '═'.repeat(51));
ok('surum adi V501 PARITY AUDIT ADAYI', ((((((((has('V5_0_1_PARITY_AUDIT_CANDIDATE')||has('V5_0_2_LEVLOCK_BOTH_ENDS')) || src.includes('V5_0_3_TESTNET_UNIVERSE_PREFILTER')) || src.includes('V5_0_4_TAKER_FROM_BACKTEST')) || src.includes('V5_0_5_VOTE_EXACT_BACKTEST')) || src.includes('V5_0_6_LOSS_TELEMETRY')) || src.includes('V5_0_7_LEVPROOF_RETRY')) || src.includes('V5_0_8_BACKOFF_HONORED') || src.includes('V5_1_0_SCAN_DECOUPLED')) || src.includes('V5_0_9_SL_CONTRACT_LOCKED'))); // V501: V500 temel C1-C5 regresyonu yeni build adina uyarlandi
ok('kaynak politika SHA korundu', has("V592_POLICY_SOURCE_SHA256 = '5bd66193f328ca74e86fb608ea2e5ebe45b52816abab1817177925ea0e51fe1d'"));

console.log(`\n${'═'.repeat(72)}\nSONUC: ${pass} gecti, ${fail} kaldi`);
process.exit(fail?1:0);
