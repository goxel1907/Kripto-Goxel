# -*- coding: utf-8 -*-
# LAZARUS CANLI (LIVE) BUILD — dogrulanmis transformasyon.
# Her donusum TAM BIR KEZ uygulanmazsa build COKER. "Gozumden kacti" imkansiz.
import pathlib, hashlib, sys, re

BASE = pathlib.Path(__file__).resolve().parent
SRC = BASE / 'SOURCE_V47443_PATCHED' / 'server.js'
OUT = BASE / 'server.rebuilt.js'
s = SRC.read_text(encoding='utf-8')
KAYNAK_SHA = hashlib.sha256(s.encode()).hexdigest()
assert KAYNAK_SHA == 'c0948fb161630101685e0c0d48f427393c58b40ef6650eab8e64b56f21b1d407', f'KAYNAK SHA UYUSMUYOR: {KAYNAK_SHA}'
log=[]
def rep(etiket, eski, yeni, adet=1):
    global s
    n = s.count(eski)
    if n != adet:
        sys.exit(f'BUILD DURDU [{etiket}]: beklenen {adet} eslesme, bulunan {n}')
    s = s.replace(eski, yeni)
    log.append(f'{etiket}: {adet} yer')

# ══ T1 — CALISMA ORTAMI ARTIK SABIT DEGIL ═══════════════════════════════
rep('T1_EXEC_ENV',
"const BINANCE_EXECUTION_ENV = 'TESTNET';",
"""const BINANCE_EXECUTION_ENV = (String(process.env.BINANCE_EXECUTION_ENV||'LIVE').trim().toUpperCase()==='TESTNET')?'TESTNET':'LIVE';""")

# ══ T2 — EMIR URL'I ORTAMDAN TURETILIR ══════════════════════════════════
rep('T2_EXEC_FAPI',
"const BINANCE_EXECUTION_FAPI = 'https://testnet.binancefuture.com';",
"""const BINANCE_EXECUTION_FAPI = BINANCE_EXECUTION_ENV==='LIVE'
  ? 'https://fapi.binance.com'
  : 'https://testnet.binancefuture.com';
// FAIL-CLOSED: parite kapisi calisip TEMIZ demeden hicbir emir gonderilemez.
let V592_TRADING_HARD_BLOCK = 'BOOT_PARITY_GATE_NOT_RUN';""")

# ══ T3 — BACKTEST SOZLESMESI ORTAMDAN AYRILDI (ASIL DUZELTME) ═══════════
rep('T3_EXACT_AUTHORITY',
"""const V592_EXACT_BACKTEST_AUTHORITY = BINANCE_EXECUTION_ENV === 'TESTNET'
  && String(process.env.V592_EXACT_BACKTEST_AUTHORITY ?? '1') !== '0';""",
"""// CANLI-C1: ESKIDEN "BINANCE_EXECUTION_ENV==='TESTNET' &&" ile basliyordu.
// Canliya gecince SESSIZCE false olurdu ve 27 cagri noktasinda davranis degisirdi:
// kaldirac kilidi, giris/cikis mum paritesi, cikis tipi beyaz listesi,
// min-hold guard, defter kanit-modu — hepsi kapanirdi. Backtestte olcumu
// yapilmamis bir bot gercek parayla calisirdi. Ortam bagi KALDIRILDI.
const V592_EXACT_BACKTEST_AUTHORITY = String(process.env.V592_EXACT_BACKTEST_AUTHORITY ?? '1') !== '0';""")

rep('T4_V45_SELECTOR',
"const V592_V45_TESTNET_ACTIVE = BINANCE_EXECUTION_ENV==='TESTNET' && String(process.env.V592_V45_TESTNET_ACTIVE??'1')==='1';",
"""// CANLI-C2: ESKIDEN ortama bagliydi. Bu secici backtestteki mask_rule'un TA KENDISI
// (msScore>=35 + TOP_GAINER + firstObstacleRR>=0.35). Canlida false olsaydi
// botun TEK filtresi kalkardi; 725 islem / PF 2.420 olcumunun canlida karsiligi
// kalmazdi. Ortam bagi KALDIRILDI.
const V592_V45_TESTNET_ACTIVE = String(process.env.V592_V45_TESTNET_ACTIVE??'1')==='1';""")

# ══ T5 — KIMLIK BILGISI ORTAMA GORE COZULUR ═════════════════════════════
rep('T5_CREDS_RESOLVER',
"""function r486391BinanceCreds() {
  const apiKey = cleanBinanceCredential(process.env.BINANCE_TESTNET_API_KEY || '');
  const apiSecret = cleanBinanceCredential(process.env.BINANCE_TESTNET_API_SECRET || '');
  return {apiKey,apiSecret,source:apiKey&&apiSecret?'RAILWAY_ENV_TESTNET':'MISSING_TESTNET_ENV'};
}""",
"""// CANLI-C3: ESKIDEN her zaman BINANCE_TESTNET_API_KEY okurdu. Canlida bu
// degiskenler yoktur -> imzali her uc olur, bot hic emir acamaz.
// Artik ortama gore cozulur. TEK cozucu; bReq ve bAlgo da bunu cagirir.
function r486391BinanceCreds() {
  const CANLI = BINANCE_EXECUTION_ENV === 'LIVE';
  const kAd = CANLI ? 'BINANCE_LIVE_API_KEY'    : 'BINANCE_TESTNET_API_KEY';
  const sAd = CANLI ? 'BINANCE_LIVE_API_SECRET' : 'BINANCE_TESTNET_API_SECRET';
  const apiKey = cleanBinanceCredential(process.env[kAd] || '');
  const apiSecret = cleanBinanceCredential(process.env[sAd] || '');
  return {apiKey, apiSecret, envVars:[kAd,sAd],
    source: apiKey&&apiSecret ? (CANLI?'RAILWAY_ENV_LIVE':'RAILWAY_ENV_TESTNET')
                              : (CANLI?'MISSING_LIVE_ENV':'MISSING_TESTNET_ENV')};
}""")

rep('T5b_bAlgo_CREDS',
"""  apiKey=cleanBinanceCredential(process.env.BINANCE_TESTNET_API_KEY||'');
  apiSecret=cleanBinanceCredential(process.env.BINANCE_TESTNET_API_SECRET||'');
  if(!apiKey||!apiSecret) throw new Error('BINANCE_TESTNET_API_KEY/SECRET eksik');""",
"""  ({apiKey,apiSecret}=r486391BinanceCreds());   // CANLI-C3
  if(!apiKey||!apiSecret) throw new Error(`${BINANCE_EXECUTION_ENV} API key/secret eksik (${r486391BinanceCreds().envVars.join('/')})`);""")

rep('T5c_bReq_CREDS',
"""  apiKey=cleanBinanceCredential(process.env.BINANCE_TESTNET_API_KEY||'');
  apiSecret=cleanBinanceCredential(process.env.BINANCE_TESTNET_API_SECRET||'');
  if(!apiKey||!apiSecret) throw new Error('Binance API key/secret boş veya maskeli');""",
"""  ({apiKey,apiSecret}=r486391BinanceCreds());   // CANLI-C3
  if(!apiKey||!apiSecret) throw new Error(`${BINANCE_EXECUTION_ENV} Binance API key/secret bos veya maskeli (${r486391BinanceCreds().envVars.join('/')})`);""")

s_before = s

# ══ T6 — SONDA TICARET KODU TAMAMEN SILINDI ═════════════════════════════
# Kullanici sarti: "CANLI VERSIYONA GECTIGIMIZDE OLMAYACAK BU OZELLIK".
# Blok bitisik: SONDA basligindan son endpoint'e kadar. Emir acan/kapatan
# hicbir sonda kodu canli surumde YOKTUR: open/close/cycle/timer/endpoint gitti.
# Geriye yalnizca 5 ETKISIZ SAPLAMA kalir. Neden silinmedi: bu saplamalar
# strateji kodunun 15 ayri yerinden cagriliyor. 15 yeri elle duzenlemek
# tam olarak yeni hata uretecegim yerdir. Saplamalar sabit deger doner.
L = s.split('\n')
bas = next(i for i,x in enumerate(L) if 'SONDA (PROBE)' in x) - 1      # ustundeki cerceve satiri
son = next(i for i,x in enumerate(L) if 'lazarus_sonda.csv' in x) + 1  # res.send satiri
if not L[bas].startswith('// \u2550'): sys.exit(f'BUILD DURDU [T6]: ust sinir yanlis -> {L[bas][:60]!r}')
if 'csv);});' not in L[son]:            sys.exit(f'BUILD DURDU [T6]: alt sinir yanlis -> {L[son][:60]!r}')
silinen_satir = son - bas + 1
if not (480 < silinen_satir < 560): sys.exit(f'BUILD DURDU [T6]: beklenmedik blok boyutu {silinen_satir} satir')
SAPLAMA = """// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
// CANLI-C4 \u2014 SONDA (PROBE) BU SURUMDE YOKTUR
// \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
// Testnet surumundeki kosulsuz ornekleme modulu (emir acma, kapatma, dongu,
// zamanlayici, HTTP uclari) CANLI SURUMDEN SILINDI. Sonda bir ARASTIRMA
// aracidir; gercek parayla kosula bakmadan islem acmaz.
// Asagidaki bes saplama yalnizca strateji kodundaki cagri noktalarinin
// cozulmesi icindir ve HEPSI SABIT/ETKISIZ deger doner.
const V592_PROBE_ACTIVE = false;
const v592ProbeOpen = new Map();                   // daima bos
const v592ProbeStats = Object.freeze({removed:true, reason:'CANLI_SURUMDE_YOK'});
function v592ProbeSlotOffset(){ return 0; }        // pozisyon limitine ofset eklemez
async function v592ProbePreempt(){ return false; } // kapatilacak sonda yok"""
s = '\n'.join(L[:bas] + SAPLAMA.split('\n') + L[son+1:])
log.append(f'T6_SONDA_SILINDI: {silinen_satir} satir -> {SAPLAMA.count(chr(10))} satir saplama')

# eski v592ProbePreempt tanimini (2038) kaldir: artik saplama var
ESKI_PREEMPT_BAS = "async function v592ProbePreempt(apiKey,apiSecret,sym){"
ESKI_PREEMPT_SON = "  }catch(_){ return false; }\n}\nasync function v592SendMainOrderIdempotent"
a = s.find(ESKI_PREEMPT_BAS); b = s.find(ESKI_PREEMPT_SON)
if a < 0 or b < 0 or b <= a: sys.exit('BUILD DURDU [T6b]: eski preempt tanimi bulunamadi')
s = s[:a] + "async function v592SendMainOrderIdempotent" + s[b+len(ESKI_PREEMPT_SON):]
if s.count('async function v592ProbePreempt') != 1: sys.exit('BUILD DURDU [T6b]: preempt tekil degil')
log.append('T6b_ESKI_PREEMPT_KALDIRILDI')

# ══ T7 — SONDA SEMBOL GUARDI: canlida sonda yok, sabit false ════════════
rep('T7_IS_PROBE_SYMBOL',
"""function v592IsProbeSymbol(symbol){
  try{ return v592ProbeOpen.has(normalizeSymbol(symbol)); }catch(_){ return false; }
}""",
"""// CANLI-C4: sonda yok -> daima false. HAYALET KAYIT muhafizi (asagida) AYRIDIR
// ve canlida da AKTIFTIR; onu kaldirmak 11.08'deki 120 hayalet satiri geri getirir.
function v592IsProbeSymbol(){ return false; }""")

# ══ T8 — ACILIS PARITE KAPISI + CANLI SILAHLANDIRMA ═════════════════════
# Bu, "gozumden kacti" hata sinifinin panzehiridir: backtest sozlesmesinin
# HERHANGI bir maddesi acilista dogru degilse bot EMIR ACMAZ.
KAPI = r"""

// ════════════════════════════════════════════════════════════════════════
// CANLI-C5 — ACILIS PARITE KAPISI (FAIL-CLOSED)
// ════════════════════════════════════════════════════════════════════════
// NEDEN VAR: bu kod tabaninda hatalarin en pahalisi "sessizce kapanan kural"
// oldu. Bir sabit false'a duser, kod calismaya devam eder, kimse fark etmez,
// haftalar sonra sonuclarin neden backtestle tutmadigi aranir.
// Bu kapi onu imkansiz kilar: backtest sozlesmesinin her maddesi acilista
// TEK TEK dogrulanir. Biri bile tutmuyorsa V592_TRADING_HARD_BLOCK dolar ve
// emir yolu kapanir. Bot calisir, panel calisir, kanit toplar — AMA ISLEM ACMAZ.
function v592BootParityGate(){
  const hata=[];
  const sozlesme={
    V592_EXACT_BACKTEST_AUTHORITY,   // 27 cagri noktasi: kaldirac/parite/cikis listesi
    V592_V45_TESTNET_ACTIVE,         // backtestin mask_rule'u = TEK giris filtresi
    V592_ENTRY_CANDLE_PARITY,        // candidateTs + 180000 (725/725)
    V592_EXIT_CANDLE_PARITY,         // exitTs % 60000 === 59999 (725/725)
    V592_EXIT_TYPE_WHITELIST,        // yalniz 4 backtest cikisi
    V592_LEDGER_EVIDENCE_ONLY
  };
  for(const [ad,deger] of Object.entries(sozlesme)) if(!deger) hata.push(`SOZLESME_KAPALI:${ad}`);

  if(Number(V592_LEVERAGE_LOCK)!==10) hata.push(`KALDIRAC_KILIDI_10_DEGIL:${V592_LEVERAGE_LOCK}`);
  if(Number(V592_MIN_HOLD_MS)<=0)     hata.push('MIN_HOLD_KAPALI');
  if(V592_PROBE_ACTIVE!==false)       hata.push('SONDA_CANLIDA_AKTIF');

  // V501: 725 portfoy replay'inin SAYISAL sozlesmesi. Boolean acik olmasi yetmez.
  const eq=(a,b,t=1e-9)=>Number.isFinite(Number(a))&&Math.abs(Number(a)-Number(b))<=t;
  if(!eq(V592_V45_MS_SCORE_MIN,35)) hata.push(`V45_SCORE_35_DEGIL:${V592_V45_MS_SCORE_MIN}`);
  if(!eq(V592_V45_FIRST_OBSTACLE_RR_MIN,0.35)) hata.push(`V45_FO_035_DEGIL:${V592_V45_FIRST_OBSTACLE_RR_MIN}`);
  if(V592_V45_REQUIRE_TOP_GAINER!==true) hata.push('V45_TOP_GAINER_ZORUNLU_DEGIL');
  if(R493_ENTRY_SAFETY_ACTIVE!==true) hata.push('R493_ENTRY_SAFETY_KAPALI');
  if(!eq(R493_MIN_FIRST_OBSTACLE_RR,0.35)) hata.push(`R493_FO_035_DEGIL:${R493_MIN_FIRST_OBSTACLE_RR}`);
  if(!eq(R486_FIRST_OBSTACLE_MIN_RR,0.35)) hata.push(`R486_FO_035_DEGIL:${R486_FIRST_OBSTACLE_MIN_RR}`);
  if(R497_FIXED_SLOT_ACTIVE!==true) hata.push('FIXED_SLOT_KAPALI');
  if(!eq(R497_SLOT_MARGIN_USDT,41)) hata.push(`SLOT_41_DEGIL:${R497_SLOT_MARGIN_USDT}`);
  if(!eq(R497_MIN_BUFFER_USDT,20)) hata.push(`BUFFER_20_DEGIL:${R497_MIN_BUFFER_USDT}`);
  if(String(R497_ABOVE_CAP_MODE)!=='HOLD_FIXED') hata.push(`ABOVE_CAP_HOLD_FIXED_DEGIL:${R497_ABOVE_CAP_MODE}`);
  if(Number(R486_MAX_POSITIONS)!==2) hata.push(`MAX_POS_2_DEGIL:${R486_MAX_POSITIONS}`);
  if(!eq(R495_FINAL_RISK_PCT,4)) hata.push(`RISK_4_DEGIL:${R495_FINAL_RISK_PCT}`);
  if(R490_DD_THROTTLE_ACTIVE!==true) hata.push('DD_THROTTLE_KAPALI');
  if(!eq(R490_DD_START,0.08)||!eq(R490_DD_FULL,0.30)||!eq(R490_DD_FLOOR,0.45)) hata.push(`DD_SOZLESME_UYUSMAZ:${R490_DD_START}/${R490_DD_FULL}/${R490_DD_FLOOR}`);
  if(!eq(R493_HIGH_FACTOR,1.00)||!eq(R493_MID_FACTOR,0.90)||!eq(R493_LOW_FACTOR,0.60)) hata.push(`QUALITY_FACTOR_UYUSMAZ:${R493_HIGH_FACTOR}/${R493_MID_FACTOR}/${R493_LOW_FACTOR}`);

  // Bu aday paket, kaynakta kanitlanamayan yapisal farklar bitmeden GERCEK PARA ACMAZ.
  const V501_KNOWN_PARITY_BLOCKERS=[
    'CANDIDATE_FEATURE_GENERATOR_NOT_INCLUDED',
    'SAME_TIMESTAMP_SLOT_PRIORITY_NOT_PROVEN',
    'COOLDOWN_AND_DAILY_STOP_RESTART_PERSISTENCE_NOT_PARITY',
    'TRAILING_STATE_RESTART_NOT_PARITY',
    'CLEAN_EXIT_ENGINE_PARITY_RUN_MISSING'
  ];
  if(BINANCE_EXECUTION_ENV==='LIVE') for(const x of V501_KNOWN_PARITY_BLOCKERS) hata.push(`BILINEN_PARITE_ENGELI:${x}`);

  // URL <-> ortam tutarliligi. Yanlis eslesme = yanlis borsaya emir.
  const bekURL = BINANCE_EXECUTION_ENV==='LIVE' ? 'https://fapi.binance.com' : 'https://testnet.binancefuture.com';
  if(BINANCE_EXECUTION_FAPI!==bekURL) hata.push(`URL_ORTAM_UYUSMAZLIGI:${BINANCE_EXECUTION_FAPI}`);

  // Kimlik: ortamin KENDI degiskenleri dolu mu.
  let kimlik=null; try{ kimlik=r486391BinanceCreds(); }catch(_){}
  if(!kimlik||!kimlik.apiKey||!kimlik.apiSecret) hata.push(`KIMLIK_EKSIK:${(kimlik&&kimlik.envVars||['?']).join('/')}`);

  // Gercek para icin ACIK silahlandirma sart. Yanlislikla deploy = islem acmaz.
  const armed = String(process.env.LAZARUS_LIVE_ARM||'').trim()==='CANLI-PARA-ONAY';
  if(BINANCE_EXECUTION_ENV==='LIVE' && !armed) hata.push('CANLI_SILAHLANDIRILMADI:LAZARUS_LIVE_ARM');

  V592_TRADING_HARD_BLOCK = hata.length ? hata.join(' | ') : null;
  const bas = BINANCE_EXECUTION_ENV==='LIVE' ? '🔴 CANLI' : '🟡 TESTNET';
  if(V592_TRADING_HARD_BLOCK){
    const m=`${bas} PARITE KAPISI KAPALI — EMIR ACILMAYACAK: ${V592_TRADING_HARD_BLOCK}`;
    try{logAuto('⛔ '+m);}catch(_){ console.error(m); }
    try{pushCritical('PARITE_KAPISI',m,{},'CRITICAL');}catch(_){}
  } else {
    const m=`${bas} parite kapisi TEMIZ — backtest sozlesmesi ve sayisal kilitlar dogrulandi, emir yolu acik.`;
    try{logAuto('✅ '+m);}catch(_){ console.log(m); }
  }
  return {ok:!V592_TRADING_HARD_BLOCK, hata, env:BINANCE_EXECUTION_ENV, fapi:BINANCE_EXECUTION_FAPI, armed};
}
try{ v592BootParityGate(); }catch(e){
  V592_TRADING_HARD_BLOCK = 'BOOT_PARITY_GATE_THREW:'+String(e&&e.message||e);
  console.error('⛔ PARITE KAPISI HATA VERDI — emir yolu kapali:', e);
}
app.get('/api/canli/parity-gate',(_req,res)=>{res.set('Cache-Control','no-store');
  res.json({ok:!V592_TRADING_HARD_BLOCK,build:LAZARUS_BUILD,executionEnvironment:BINANCE_EXECUTION_ENV,
    executionFapi:BINANCE_EXECUTION_FAPI,marketDataEnvironment:BINANCE_MARKET_DATA_ENV,
    hardBlock:V592_TRADING_HARD_BLOCK,probeRemoved:true,
    armed:String(process.env.LAZARUS_LIVE_ARM||'').trim()==='CANLI-PARA-ONAY',
    contract:{exactBacktestAuthority:V592_EXACT_BACKTEST_AUTHORITY,v45Selector:V592_V45_TESTNET_ACTIVE,
      entryCandleParity:V592_ENTRY_CANDLE_PARITY,exitCandleParity:V592_EXIT_CANDLE_PARITY,
      exitTypeWhitelist:V592_EXIT_TYPE_WHITELIST,leverageLock:V592_LEVERAGE_LOCK,minHoldMs:V592_MIN_HOLD_MS},
    serverTime:Date.now()});});
"""
s = s.rstrip() + "\n" + KAPI
log.append('T8_ACILIS_PARITE_KAPISI_EKLENDI')

# ══ T9 — EMIR YOLU KAPIYA BAGLANDI ══════════════════════════════════════
rep('T9_ORDER_HARD_BLOCK',
"""async function v592SendMainOrderIdempotent(apiKey,apiSecret,sym,oSide,qty,decisionKey){
  await v592ProbePreempt(apiKey,apiSecret,sym);
  if(!V592_ORDER_IDEMPOTENCY){""",
"""async function v592SendMainOrderIdempotent(apiKey,apiSecret,sym,oSide,qty,decisionKey){
  // CANLI-C5: parite kapisi TEMIZ demeden hicbir giris emri gitmez (fail-closed).
  if(V592_TRADING_HARD_BLOCK){
    try{r501OrderLifeMark(sym,'ORDER_BLOCKED_BY_PARITY_GATE',{reason:V592_TRADING_HARD_BLOCK});}catch(_){}
    throw new Error(`PARITE_KAPISI_KAPALI: ${V592_TRADING_HARD_BLOCK}`);
  }
  if(!V592_ORDER_IDEMPOTENCY){""")

# ══ T10 — SURUM ADI ═════════════════════════════════════════════════════
rep('T10_BUILD_ADI',
"const LAZARUS_BUILD = 'R493_V5_9_2_TESTNET_EXACT_CLOSED1M_R495_V4_7_4_43_AUDIT_FIX_RISK41_10X'",
"const LAZARUS_BUILD = 'R493_V5_9_2_V5_0_1_PARITY_AUDIT_CANDIDATE_NOPROBE_RISK41_10X'")


# ══ T11 — ANALIZ PAKETI: SILINEN FONKSIYON CAGRILARI TEMIZLENDI ═════════
# YAKALANDI: v592ProbeRows/v592ProbeFlat T6'da silindi ama analiz paketi
# onlari hala cagiriyordu. try/catch icinde oldugu icin sunucu cokmezdi —
# analiz paketi ucu SESSIZCE bozulurdu. Tam da kacirmamam gereken sinif.
rep('T11_BUNDLE_PROBE',
"""    const sr=v592ProbeRows().map(v592ProbeFlat);
    const sc=sr.length?Object.keys(sr[0]):['id','symbol','sonuc'];
    const probeCsv=[sc.join(','),...sr.map(r=>sc.map(c=>r501CsvCell(r[c])).join(','))].join('\\n');
""","")
import re as _re
_pat = r"\n\s*B\('probe_samples\.csv',[^\n]*\n"
_n = len(_re.findall(_pat, s))
if _n != 1: sys.exit(f'BUILD DURDU [T11b]: probe_samples.csv girdisi {_n} kez')
s = _re.sub(_pat, lambda m: "\n", s, count=1)
log.append('T11b_BUNDLE_ENTRY: 1 yer')
rep('T11c_BUNDLE_STATUS',
"""      B('probe_status.json',{active:V592_PROBE_ACTIVE,stats:v592ProbeStats,openNow:[...v592ProbeOpen.keys()]}),
""","")
rep('T11d_BUNDLE_OKU',
"""        +`probe_samples.csv      : ${sr.length} sonda ornegi, ${sc.length} sutun\\n`
""","")

# ══ T12 — OLU REFERANS TARAMASI (genel muhafiz) ═════════════════════════
# Silinen HER fonksiyon icin kalan cagri noktasi var mi? Varsa build coker.
SILINEN = ['v592ProbeOpenOne','v592ProbeCloseOne','v592ProbeCycle','v592ProbeBootRecover',
           'v592ProbeRows','v592ProbeFlat','v592ProbeWrite','v592ProbeSnapshot','v592ProbeCreds',
           'v592ProbeDecisionNow','v592ProbeMarkPrice','v592ProbeSaveState','v592ProbeLoadState',
           'V592_PROBE_INTERVAL_MS','V592_PROBE_HOLD_MS','V592_PROBE_TOP_N','V592_PROBE_MARGIN_USDT',
           'V592_PROBE_LEVERAGE','V592_PROBE_MAX_OPEN','V592_PROBE_PATH','V592_PROBE_STATE_PATH',
           'v592ProbeRotate','v592ProbeRetries']
olu=[]
for ad in SILINEN:
    kalan=[i+1 for i,satir in enumerate(s.split('\n')) if ad in satir]
    if kalan: olu.append(f'{ad} -> satir {kalan[:4]}')
if olu: sys.exit('BUILD DURDU [T12 OLU REFERANS]:\n   ' + '\n   '.join(olu))
log.append(f'T12_OLU_REFERANS_TARAMASI: {len(SILINEN)} silinen ad, 0 kalinti')

# ══ T13-T17 — V5.0.2 KALDIRAC/SL TUTARLILIK DÖNÜŞÜMLERİ ══════════════
# V5.0.2 daha önce server.js üzerine elle uygulanmıştı; bu blok aynı değişiklikleri
# kaynak V4.7.4.43 -> V5.0.1 dönüşümünün devamında yeniden üretilebilir hale getirir.
rep('T13_V502_BUILD_ADI',
"const LAZARUS_BUILD = 'R493_V5_9_2_V5_0_1_PARITY_AUDIT_CANDIDATE_NOPROBE_RISK41_10X'",
"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_2_LEVLOCK_BOTH_ENDS_NOPROBE_RISK41_10X'")

rep('T14_V502_LEV_LOCK_BAS',
"""        let executeLeverage = normalizeRequestedLeverage(leverage, 1);
        let leverageNote = `panel kaldıracı ${executeLeverage}x`;""",
"""        let executeLeverage = normalizeRequestedLeverage(leverage, 1);
        // ═══ V502-A: KALDIRAC KILIDI ZINCIRIN BASINDA ═══════════════════════
        // V5.0.1 kilidi zincirin SONUNA koydu. Ama SL turetme zinciri kilitten
        // ONCE calisiyor ve 24888'de userSLPct'yi o anki (kilitlenmemis)
        // executeLeverage'a boluyordu. Sonuc: SL lev=2 varsayimiyla turetilip
        // lev=10 ile uygulaniyordu. Olculdu — TACTICAL planda SL>=%6.01,
        // NORMAL planda SL>=%8.01 oldugunda tetikleniyor; R283 %11 ROI riski
        // hedeflerken gerceklesen %55 oluyordu. stopPrice ve userRR de bundan
        // tureyip bozuluyordu.
        // Backtest LEV=10 SABIT. O yuzden TUM turetme zinciri 10x gormeli.
        // Zincirin sonundaki kilit de duruyor: R283 cap'i 10'un altina
        // indirirse oradan geri 10'a cekilir. Iki nokta birlikte gerekli.
        if (V592_EXACT_BACKTEST_AUTHORITY && Number(V592_LEVERAGE_LOCK) > 0) {
          executeLeverage = Number(V592_LEVERAGE_LOCK);
        }
        let leverageNote = `panel kaldıracı ${executeLeverage}x`;""")

rep('T15_V502_R283_EXACT_BYPASS',
"""          const r282OldLev = executeLeverage;
          const r282CapLev = Math.max(2, Math.floor(Number(r282TradePlan.maxRoiRisk||15) / Math.max(0.45, Number(userSLPct||1.5))));
          executeLeverage = Math.min(executeLeverage, r282CapLev);
          if (executeLeverage < r282OldLev) {
            leverageNote += ` · R283 risk ${r282OldLev}x→${executeLeverage}x (SL×Lev≤${r282TradePlan.maxRoiRisk}%)`;
          }
          if (userSLPct * executeLeverage > Number(r282TradePlan.maxRoiRisk||15) + 1) {
            const oldSL282 = userSLPct;""",
"""          // ═══ V502-D: EXACT modda R283 kaldirac/SL mudahalesi KAPALI ═══════
          // Olculdu: R283 kaldiraci SL'e gore dusuruyor (cap), sonra 24902 SL'i
          // o DUSURULMUS kaldiraca gore yeniden yaziyor. Zincirin sonundaki
          // 10x kilidi kaldiraci geri cekince SL ile kaldirac UYUSMUYOR:
          // NORMAL planda R283 %15 ROI riski hedeflerken gerceklesen %75 idi.
          // Backtestte R283 KATMANI YOK; LEV sabit 10. O yuzden EXACT modda
          // bu iki mudahale hic calismaz. Bu bir TUTARLILIK duzeltmesidir;
          // SL'in backtestle ayni olacagini GARANTI ETMEZ (bkz. engel #6).
          const _v502ExactNoR283 = V592_EXACT_BACKTEST_AUTHORITY && Number(V592_LEVERAGE_LOCK) > 0;
          const r282OldLev = executeLeverage;
          const r282CapLev = Math.max(2, Math.floor(Number(r282TradePlan.maxRoiRisk||15) / Math.max(0.45, Number(userSLPct||1.5))));
          if (!_v502ExactNoR283) executeLeverage = Math.min(executeLeverage, r282CapLev);
          if (executeLeverage < r282OldLev) {
            leverageNote += ` · R283 risk ${r282OldLev}x→${executeLeverage}x (SL×Lev≤${r282TradePlan.maxRoiRisk}%)`;
          }
          if (!_v502ExactNoR283 && userSLPct * executeLeverage > Number(r282TradePlan.maxRoiRisk||15) + 1) {
            const oldSL282 = userSLPct;""")

rep('T16_V502_GATE_MARKER',
"""// emir yolu kapanir. Bot calisir, panel calisir, kanit toplar — AMA ISLEM ACMAZ.
function v592BootParityGate(){""",
"""// emir yolu kapanir. Bot calisir, panel calisir, kanit toplar — AMA ISLEM ACMAZ.
// V502: iki uclu kaldirac kilidi kaynakta dogrulanir (build-time degil run-time).
const V502_LEV_LOCK_BOTH_ENDS = true;
function v592BootParityGate(){""")

rep('T16b_V502_GATE_CHECK',
"""  if(Number(V592_MIN_HOLD_MS)<=0)     hata.push('MIN_HOLD_KAPALI');
  if(V592_PROBE_ACTIVE!==false)       hata.push('SONDA_CANLIDA_AKTIF');

  // V501: 725 portfoy replay'inin SAYISAL sozlesmesi.""",
"""  if(Number(V592_MIN_HOLD_MS)<=0)     hata.push('MIN_HOLD_KAPALI');
  if(V592_PROBE_ACTIVE!==false)       hata.push('SONDA_CANLIDA_AKTIF');
  // V502: kaldirac kilidi zincirin HER IKI ucunda mi? Tek uc yeterli degil —
  // SL turetme zinciri basta calisiyor. Kaynakta iki kilit de aranir.
  if(V592_EXACT_BACKTEST_AUTHORITY && !V502_LEV_LOCK_BOTH_ENDS) hata.push('KALDIRAC_KILIDI_TEK_UCTA');

  // V501: 725 portfoy replay'inin SAYISAL sozlesmesi.""")

rep('T17_V502_BLOCKER6',
"""    'TRAILING_STATE_RESTART_NOT_PARITY',
    'CLEAN_EXIT_ENGINE_PARITY_RUN_MISSING'
  ];""",
"""    'TRAILING_STATE_RESTART_NOT_PARITY',
    'CLEAN_EXIT_ENGINE_PARITY_RUN_MISSING',
  // V502 OLCULDU: SL/TP yuzdesi 22 ayri canli noktada turetilir, HICBIRI EXACT
  // kapsaminda degil. Backtestin 725 isleminde slPct %1,13-9,94 (medyan %4,26).
  // Canli zincirde R166 tavani %3,00 (backtestin %67,7'si USTUNDE) ve R175
  // tavani %0,95 (backtestin %100'u USTUNDE). Stop yerlesimi backtestle
  // ayni olamaz. Kaynak: SL kurali eksik candidates_radar_features uretecinde,
  // yani bu engel #1'in alt kumesi.
  'SL_TP_YUZDE_SOZLESMESI_DOGRULANMADI_22_NOKTA_R166_3PCT_R175_095PCT',
];""")

# ══ SON DOGRULAMALAR ════════════════════════════════════════════════════
kontrol = [
 ("testnet URL sabit kalmadi",        "const BINANCE_EXECUTION_FAPI = 'https://testnet.binancefuture.com';" not in s),
 ("EXACT ortama bagli degil",         "V592_EXACT_BACKTEST_AUTHORITY = BINANCE_EXECUTION_ENV" not in s),
 ("V45 ortama bagli degil",           "V592_V45_TESTNET_ACTIVE = BINANCE_EXECUTION_ENV" not in s),
 ("sabit testnet anahtari kalmadi",   "process.env.BINANCE_TESTNET_API_KEY||''" not in s),
 ("sonda emir acamaz (OpenOne yok)",  "v592ProbeOpenOne" not in s),
 ("sonda emir kapatamaz (CloseOne)",  "v592ProbeCloseOne" not in s),
 ("sonda dongusu yok",                "v592ProbeCycle" not in s),
 ("sonda endpointi yok",              "/api/probe" not in s),
 ("hayalet muhafizi duruyor",         "phantomLedgerBlocked" in s),
 ("parite kapisi var",                "v592BootParityGate" in s),
 ("emir yolu kapiya bagli",           "PARITE_KAPISI_KAPALI" in s),
]
for ad,ok in kontrol:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')

OUT.write_text(s, encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log))
print(f'\n  kaynak : {SRC.name}  {len(s_before.splitlines())} satir  sha {KAYNAK_SHA[:16]}')
print(f'  cikti  : {OUT.name}  {len(s.splitlines())} satir  sha {hashlib.sha256(s.encode()).hexdigest()[:16]}')
print(f'  dogrulama: {len(kontrol)}/{len(kontrol)} GECTI')
