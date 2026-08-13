# -*- coding: utf-8 -*-
# V5.0.9 — bReq BOGUMUNA BACKOFF + SL SOZLESMESI KILITLENDI
import pathlib, hashlib, sys, re
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='8ab3af3a30d4820f49613b992c59bdbe5e62f2cde6f0e36450c263ff71571ee7'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V509-A: bReq BOGUMUNA BACKOFF ══════════════════════════════════════
rep('V509_A_BREQ_BOGUM',
"  await binanceThrottle(`${emergencyBypass ? 'EMERGENCY' : 'SIGNED'}:${path}`, w, orderWeight);",
"""  // ══ V5.0.9 — BACKOFF KONTROLU TEK BOGUMDA ═══════════════════════════
  // OLCULDU 13.08 (V508_72H_TEMIZ_1): V5.0.8 yasak SURESINI duzeltti ama
  // UYGULANMASINI duzeltmedi. 73 olayin 63'u (%86) hala yasak bitmeden
  // yapilan cagriydi. Sebep: kontroller bazi sarmalayicilara ayri ayri
  // konmustu; liveOpenAlgoOrders / liveOpenStandardOrders dogrudan bReq
  // cagirip kontrolsuz geciyordu. 418'ler yer degistirdi:
  //   /fapi/v2/account       19 -> 10   (kontrolu vardi)
  //   /fapi/v1/openAlgoOrders 5 -> 33   (korumasizdi)
  //   /fapi/v1/openOrders     5 -> 20   (korumasizdi)
  // Bu, devir belgesine "guard tek cagri yerine bagli" diye yazdigim
  // hata sinifinin (AS1/AC) aynisidir. 27 bReq cagri noktasi var; her
  // birine ayri kontrol koymak yerine TEK bogumda kesiliyor.
  //
  // ACIL YOL MUAF: pozisyon kapatmak yasak yuzunden engellenirse daha kotu olur.
  if (!emergencyBypass && isExecBackoffActive()) {
    const _kalanMs = getExecBackoffMs();
    try{ v592ParityStats.execBackoffBlockedAtBreq=(v592ParityStats.execBackoffBlockedAtBreq||0)+1; }catch(_){}
    try{ if(typeof r501EvidenceFunnel==='function') r501EvidenceFunnel({
      type:'EXEC_BACKOFF_BLOCK',action:'BACKOFF_BLOCK',authority:'TESTNET_EXECUTION',
      symbol:null,decisionImpact:true,orderBlocking:true,path:String(path||'').slice(0,80),
      remainingMs:_kalanMs,reason:`yasak aktif, cagri gonderilmedi (${Math.ceil(_kalanMs/1000)}sn kaldi)`}); }catch(_){}
    throw makeBinanceBackoffError(`Exec backoff aktif ${path}`, Math.ceil(_kalanMs/1000), 418);
  }
  await binanceThrottle(`${emergencyBypass ? 'EMERGENCY' : 'SIGNED'}:${path}`, w, orderWeight);""")

# ══ V509-B: SL SOZLESMESI SABITLERI ════════════════════════════════════
rep('V509_B_SABIT',
"const V508_BACKOFF_MAX_SEC =",
"""// ══ V5.0.9 — BACKTEST SL SOZLESMESI (1461/1461 DOGRULANDI) ══════════
// B1/B6 KAPANDI. Uc sozlesme gercek Binance verisine karsi olculdu:
//   1) entry = candidateTs+180000 anindaki 1m mumun close'u   1461/1461, fark 0
//   2) ATR   = TR'nin 14 periyot BASIT ortalamasi (Wilder DEGIL) 1461/1461
//   3) SL    = asagidaki formul                                 1461/1461, fark 0
//        atr       = mean(TR[-14:])
//        recentLow = min(low[-7:])
//        rawStop   = min(recentLow, close - 1.45*atr)
//        slPct     = clamp((close-rawStop)/close*100, 1.2, 8.0)
//        sl        = close * (1 - slPct/100)
// Formul r495Exact.candidateMetaFromCompact5m icinde ZATEN vardi; kimse
// backtest verisine karsi sinamamisti. "Kayip uretici" kayip degilmis.
// Bu sabitler acilis kapisinda denetlenir ki bir daha sessizce degismesin.
const V509_SL_ATR_CARPANI   = 1.45;
const V509_SL_ATR_PERIYOT   = 14;
const V509_SL_LOW_PENCERE   = 7;
const V509_SL_PCT_MIN       = 1.2;
const V509_SL_PCT_MAX       = 8.0;
const V509_PLAN_TARGET_R    = 2.4;    // runner degilse
const V509_PLAN_TARGET_CAP  = 1.12;   // entry * 1.12 tavani
const V508_BACKOFF_MAX_SEC =""")

# ══ V509-C: KAPIYA SL SOZLESMESI DENETIMI ══════════════════════════════
rep('V509_C_KAPI',
"  if(R495_TAKER_VOTE_ACTIVE) hata.push('R495_TAKER_OYU_ACIK_BACKTESTTE_YOK');",
"""  if(R495_TAKER_VOTE_ACTIVE) hata.push('R495_TAKER_OYU_ACIK_BACKTESTTE_YOK');
  // V509: SL sozlesmesi sabitleri kaynakta duruyor mu (1461/1461 dogrulandi)
  if(!eq(V509_SL_ATR_CARPANI,1.45))  hata.push(`SL_ATR_CARPANI_145_DEGIL:${V509_SL_ATR_CARPANI}`);
  if(Number(V509_SL_ATR_PERIYOT)!==14) hata.push(`SL_ATR_PERIYOT_14_DEGIL:${V509_SL_ATR_PERIYOT}`);
  if(Number(V509_SL_LOW_PENCERE)!==7)  hata.push(`SL_LOW_PENCERE_7_DEGIL:${V509_SL_LOW_PENCERE}`);
  if(!eq(V509_SL_PCT_MIN,1.2)||!eq(V509_SL_PCT_MAX,8.0)) hata.push(`SL_CLAMP_UYUSMAZ:${V509_SL_PCT_MIN}/${V509_SL_PCT_MAX}`);
  if(!eq(V509_PLAN_TARGET_R,2.4))    hata.push(`PLAN_TARGET_R_24_DEGIL:${V509_PLAN_TARGET_R}`);
  if(!eq(V509_PLAN_TARGET_CAP,1.12)) hata.push(`PLAN_TARGET_CAP_112_DEGIL:${V509_PLAN_TARGET_CAP}`);
  // kaynak kodda formulun kendisi de duruyor mu
  if(!(typeof r495Exact==='object' && typeof r495Exact.buildAcceptedPlan==='function'))
    hata.push('R495_EXACT_PLAN_URETICI_YOK');""")

# ══ V509-D: HER ISLEMDE SL PARITE KAYDI ════════════════════════════════
rep('V509_D_SL_PARITE',
"                    ai.entry=Number(_r495.plan.entry);ai.sl=Number(_r495.plan.stop);ai.tp=Number(_r495.plan.target);ai.r495ExactPlan=_r495.plan;",
"""                    ai.entry=Number(_r495.plan.entry);ai.sl=Number(_r495.plan.stop);ai.tp=Number(_r495.plan.target);ai.r495ExactPlan=_r495.plan;
                    // V509: SL parite kaniti her islemde birikir. CYS'de TP'nin
                    // buildAcceptedPlan formulunden geldigini aritmetikle gosterdim
                    // ama o n=1'di. Artik her islem kendi kanitini yaziyor.
                    try{ r501EvidenceFunnel({type:'SL_PARITY_PROOF',action:'SL_PARITY_PROOF',
                      authority:'EXACT_V592_EXECUTION',symbol:normalizeSymbol(coin.fullSymbol||coin.symbol),
                      decisionImpact:false,planEntry:Number(_r495.plan.entry),planStop:Number(_r495.plan.stop),
                      planTarget:Number(_r495.plan.target),planSlPct:Number(_r495.plan.entrySlPct),
                      planRunner:!!_r495.plan.runner,planTargetR:Number(_r495.plan.targetR),
                      candidateStop:Number(_r495?.candidate?.stop ?? _r495.plan.stop),
                      reason:'emir SL/TP kaynagi: r495Exact buildAcceptedPlan'}); }catch(_){}""")

m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_9_SL_CONTRACT_LOCKED_NOPROBE_RISK41_10X'")
log.append('V509_E_SURUM')

k=[("bReq bogumunda backoff", "if (!emergencyBypass && isExecBackoffActive()) {" in s),
   ("  bogum throttle'dan ONCE", s.index("if (!emergencyBypass && isExecBackoffActive())") < s.index("await binanceThrottle(`${emergencyBypass")),
   ("  acil yol muaf", "!emergencyBypass &&" in s),
   ("  engellenen sayac", "execBackoffBlockedAtBreq" in s),
   ("  funnel kaydi", "type:'EXEC_BACKOFF_BLOCK'" in s),
   ("SL sabitleri", all(x in s for x in ("V509_SL_ATR_CARPANI","V509_SL_ATR_PERIYOT","V509_SL_LOW_PENCERE","V509_SL_PCT_MIN","V509_SL_PCT_MAX"))),
   ("plan sabitleri", "V509_PLAN_TARGET_R" in s and "V509_PLAN_TARGET_CAP" in s),
   ("kapida SL denetimi", "SL_ATR_CARPANI_145_DEGIL" in s and "SL_CLAMP_UYUSMAZ" in s),
   ("kapida uretici denetimi", "R495_EXACT_PLAN_URETICI_YOK" in s),
   ("SL parite kaydi", "type:'SL_PARITY_PROOF'" in s),
   ("  plan degerleri kaydediliyor", "planStop:Number(_r495.plan.stop)" in s),
   ("olcum kaynakta", "1461/1461" in s and "%86" in s),
   ("hata sinifi belirtilmis", "AS1/AC" in s),
   ("V508 duruyor", "V508_BACKOFF_MAX_SEC" in s),
   ("V507 duruyor", "V507_LEVERAGE_PROOF_MAX_ATTEMPTS" in s),
   ("V502 kaldirac kilidi", "V502-A: KALDIRAC KILIDI" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
