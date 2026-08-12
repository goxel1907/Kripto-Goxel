# -*- coding: utf-8 -*-
# V5.0.7 — KALDIRAC ISPATI RETRY + 418 TELEMETRISI
import pathlib, hashlib, sys, re
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='eed7859178fed4ae3542657d70e4c5eee60963385c0616c04a5d5b92483ab965'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V507-A: SABITLER ═══════════════════════════════════════════════════
rep('V507_A_SABIT',
"const V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199;",
"""const V504_BACKTEST_TAKER_MIN_OBSERVED = 0.2199;
// ══ V5.0.7 — KALDIRAC ISPATI ARTIK TEK ATIS DEGIL ═══════════════════
// OLCULDU 12.08 (V506_72H_TEMIZ_1): BEAT islemi 12:14:29'da acildi, SL/TP
// yazildi, sonra 12:16:21'de LEVERAGE_PARITY_FAILED -> LEVERAGE_PARITY_UNWIND
// ile GERI KAPATILDI. Sebep: ispat fetchPositionRiskRaw'i TEK KEZ cagiriyordu;
// HTTP 418 nedeniyle cagri patlayinca proof=null oldu ve gercek pozisyon imha edildi.
//
// Kaldirac emir aninda setSymbolLeverageSafe ile ZATEN ayarlanmisti. Borsa 10x'i
// uyguladi; biz o anda okuyamadik. Ayni sey pozisyon henuz positionRisk'e
// yansimadiginda da olur (yayilma gecikmesi).
//
// 418 BIZDEN DEGIL — olculdu: 97 dakikada 15 imzali cagri = 0,16/dk = ~0,8
// weight/dk. Binance limiti 2400 weight/dk. Kendi trafigimiz binde bir bile
// degil. Railway cikis IP'si (208.77.246.50) paylasimli; limit ORTAK IP
// uzerinden yeniyor ve Binance testnet zaten mainnetten cok dar limitli.
// Yani 418'i onleyemeyiz — ona DAYANIKLI olmamiz gerekir.
//
// MUHAFIZ GEVSEMIYOR: N deneme sonunda hala kanit yoksa pozisyon yine geri
// sarilir. Yalnizca gecici hataya dayanikli hale geliyor.
const V507_LEVERAGE_PROOF_MAX_ATTEMPTS = Math.max(1, Math.min(10, Number(process.env.V507_LEVERAGE_PROOF_MAX_ATTEMPTS || 5)));
const V507_LEVERAGE_PROOF_RETRY_MS = Math.max(300, Math.min(10000, Number(process.env.V507_LEVERAGE_PROOF_RETRY_MS || 1800)));""")

# ══ V507-B: ISPAT DONGUSU ══════════════════════════════════════════════
rep('V507_B_ISPAT',
"""    let _appliedLeverageProof=null,_leverageParityOk=false;
    try{
      const _pr=await fetchPositionRiskRaw(apiKey,apiSecret);
      const _p=Array.isArray(_pr)?_pr.find(x=>String(x.symbol||'').toUpperCase()===sym&&Math.abs(Number(x.positionAmt||0))>0):null;
      _appliedLeverageProof=_p?(Number(_p.leverage||0)||null):null;
    }catch(_e){_appliedLeverageProof=null;}""",
"""    let _appliedLeverageProof=null,_leverageParityOk=false;
    // V5.0.7: tek atis yerine N deneme. Pozisyon bu asamada ZATEN korumali
    // (PROTECT_FIRST yukarida calisti), bu yuzden beklemek guvenli.
    let _lpAttempt=0,_lpLastErr=null;
    for(_lpAttempt=1;_lpAttempt<=V507_LEVERAGE_PROOF_MAX_ATTEMPTS;_lpAttempt++){
      try{
        const _pr=await fetchPositionRiskRaw(apiKey,apiSecret);
        const _p=Array.isArray(_pr)?_pr.find(x=>String(x.symbol||'').toUpperCase()===sym&&Math.abs(Number(x.positionAmt||0))>0):null;
        _appliedLeverageProof=_p?(Number(_p.leverage||0)||null):null;
        if(Number.isFinite(_appliedLeverageProof)) break;
        _lpLastErr='NO_POSITION_ROW';
      }catch(_e){ _appliedLeverageProof=null; _lpLastErr=safeErrMsg(_e).slice(0,140); }
      if(_lpAttempt<V507_LEVERAGE_PROOF_MAX_ATTEMPTS){
        try{ await new Promise(r=>setTimeout(r,V507_LEVERAGE_PROOF_RETRY_MS)); }catch(_){}
      }
    }
    v592ParityStats.leverageProofAttempts=(v592ParityStats.leverageProofAttempts||0)+_lpAttempt;
    if(_lpAttempt>1){
      v592ParityStats.leverageProofRetried=(v592ParityStats.leverageProofRetried||0)+1;
      try{ r501EvidenceFunnel({type:'LEVERAGE_PROOF_RETRY',action:'LEVERAGE_PROOF_RETRY',
        authority:'EXACT_V592_EXECUTION',symbol:normalizeSymbol(sym),decisionImpact:false,
        attempts:_lpAttempt,maxAttempts:V507_LEVERAGE_PROOF_MAX_ATTEMPTS,
        proof:_appliedLeverageProof,lastError:_lpLastErr,
        reason:`kaldirac ispati ${_lpAttempt} denemede ${Number.isFinite(_appliedLeverageProof)?'ALINDI':'ALINAMADI'}`}); }catch(_){}
    }""")

# ══ V507-C: 418/429 OLAYLARI FUNNEL'A ══════════════════════════════════
rep('V507_C_418',
"function registerHttpBackoffAndThrow(scope, status, retryHeader, domain='PUBLIC') {",
"""function registerHttpBackoffAndThrow(scope, status, retryHeader, domain='PUBLIC') {
  // V5.0.7: 418/429 olaylari funnel'a. Kendi trafigimiz olculdu (0,16 imzali
  // cagri/dk); 418 disaridan geliyorsa bu kayitlar zaman icinde bunu gosterir.
  try{ if(typeof r501EvidenceFunnel==='function') r501EvidenceFunnel({
    type:'BINANCE_RATE_LIMIT',action:'RATE_LIMIT',authority:domain==='EXEC'?'TESTNET_EXECUTION':'PUBLIC_MARKET',
    symbol:null,decisionImpact:true,orderBlocking:domain==='EXEC',
    httpStatus:status,scope:String(scope||'').slice(0,120),domain,retryAfter:retryHeader||null,
    reason:`HTTP ${status} ${domain} ${String(scope||'').slice(0,80)}`}); }catch(_){}""")

m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_7_LEVPROOF_RETRY_NOPROBE_RISK41_10X'")
log.append('V507_D_SURUM')

k=[("retry sabiti", "V507_LEVERAGE_PROOF_MAX_ATTEMPTS" in s),
   ("retry dongusu", "for(_lpAttempt=1;_lpAttempt<=V507_LEVERAGE_PROOF_MAX_ATTEMPTS" in s),
   ("basarida cikis", "if(Number.isFinite(_appliedLeverageProof)) break;" in s),
   ("deneme sayaci", "leverageProofAttempts" in s),
   ("retry sayaci", "leverageProofRetried" in s),
   ("retry funnel kaydi", "type:'LEVERAGE_PROOF_RETRY'" in s),
   ("418 funnel kaydi", "type:'BINANCE_RATE_LIMIT'" in s),
   ("418 EXEC/PUBLIC ayrimi", "domain==='EXEC'?'TESTNET_EXECUTION':'PUBLIC_MARKET'" in s),
   ("MUHAFIZ AYNEN DURUYOR", "if(V592_EXACT_BACKTEST_AUTHORITY&&V592_LEVERAGE_LOCK>0&&!_leverageParityOk){" in s),
   ("unwind aynen duruyor", "LEVERAGE_PARITY_UNWIND" in s),
   ("olcum kaynakta", "0,16/dk" in s and "2400 weight/dk" in s),
   ("BEAT olayi belgeli", "BEAT islemi 12:14:29" in s),
   ("onceki surumler", "!takerVoteActive" in s and "V503 testnet evren on-filtresi" in s and "V502-A: KALDIRAC KILIDI" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
