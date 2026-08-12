# -*- coding: utf-8 -*-
# V5.0.8 — 418 YASAK SURESINE GERCEKTEN UY
import pathlib, hashlib, sys, re
p=pathlib.Path('server.js'); s=p.read_text(encoding='utf-8')
GIRDI='74374ae1d9ff5b6d23eeba42f2a4ac3e953f2800392fb088b1133e5da3581005'
h=hashlib.sha256(s.encode()).hexdigest()
if h!=GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et,eski,yeni,adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V508-A: TAVAN 120sn KALDIRILDI ═════════════════════════════════════
rep('V508_A_120_TAVAN',
"""  const retry = parseInt(retryHeader || (Number(status) === 418 ? '60' : '60'), 10);
  const sec = Math.max(Number(status) === 418 ? 60 : 30, Math.min(120, Number(retry)||60));""",
"""  // ══ V5.0.8 — YASAK SURESINE GERCEKTEN UYULUYOR ════════════════════
  // OLCULDU 12.08 (V507_72H_TEMIZ_1): 4,9 saatte 29 adet 418. 28 olayin 25'i
  // ONCEKI YASAK BITMEDEN yapilan cagriydi. Ornek:
  //   14:43:38  retryAfter 3441 sn (57 dk)
  //   14:45:45  -> 127 sn sonra tekrar arandi (tam 120sn tavani)
  // Zincirin nasil buyudugu:
  //   18:31:46 yasak   69 sn -> 33 sn erken cagri  -> yasak  879 sn
  //   18:44:38 yasak 1760 sn -> 142 sn erken cagri -> yasak 2077 sn
  // Binance tekrarlanan ihlalde yasagi UZATIR. Birlesik gercek yasak suresi
  // olcum penceresinin %45'i oldu (2,2 saat / 4,9 saat).
  //
  // KOK SEBEP: Math.min(120, retry). Binance 3441 dedi, kod 120'ye kirpti.
  // Eski yorum "180sn tum sistemi durduruyordu; 60sn yeterli" diyordu — ama
  // erken cagri sistemi durdurmuyor, YASAGI BUYUTUYOR. Beklemek daha ucuz.
  const retry = parseInt(retryHeader || (Number(status) === 418 ? '60' : '60'), 10);
  const sec = Math.max(Number(status) === 418 ? 60 : 30, Math.min(V508_BACKOFF_MAX_SEC, Number(retry)||60));""")

# ══ V508-B: TAVAN 180sn KALDIRILDI + KALICILIK ═════════════════════════
rep('V508_B_180_TAVAN',
"""function registerBinanceBackoff(reason='rate-limit', seconds=45, domain='PUBLIC') {
  const sec = Math.max(5, Math.min(180, Number(seconds)||45));""",
"""function registerBinanceBackoff(reason='rate-limit', seconds=45, domain='PUBLIC') {
  // V5.0.8: ikinci tavan da kaldirildi. Binance'in verdigi sureye uyulur.
  const sec = Math.max(5, Math.min(V508_BACKOFF_MAX_SEC, Number(seconds)||45));""")

rep('V508_B2_KALICI',
"""  binanceGov.last429At = Date.now();
  try { pushCritical('BINANCE_BACKOFF', `${reason}: ${sec}sn istek bekleme [${dom}]`, {seconds:sec, reason, domain:dom}, 'WARNING'); } catch(_) {}
}""",
"""  binanceGov.last429At = Date.now();
  v508SaveBackoff();   // V5.0.8: restart yasagi sifirlamasin
  try { pushCritical('BINANCE_BACKOFF', `${reason}: ${sec}sn istek bekleme [${dom}]`, {seconds:sec, reason, domain:dom}, 'WARNING'); } catch(_) {}
}""")

# ══ V508-C: SABIT + DISK KALICILIGI ════════════════════════════════════
rep('V508_C_KALICILIK',
"function registerBinanceBackoff(reason='rate-limit', seconds=45, domain='PUBLIC') {",
"""// ══ V5.0.8 — BACKOFF KALICILIGI ═════════════════════════════════════
// binanceGov.execBackoffUntil YALNIZ BELLEKTEYDI. Restart onu sifirliyordu;
// bot yasak ortasinda acilip hemen cagri yapiyor ve yasagi uzatiyordu.
// Bu, daha once katalogladigim "restart'ta olen state" hata sinifidir (BF).
const V508_BACKOFF_MAX_SEC = Math.max(60, Math.min(7200, Number(process.env.V508_BACKOFF_MAX_SEC || 3600)));
const V508_BACKOFF_PATH = path.join(String(process.env.TESTNET_STATE_DIR||'/data').trim()||'/data','lazarus_binance_backoff.json');
function v508SaveBackoff(){
  try{
    const tmp=V508_BACKOFF_PATH+'.tmp';
    fs.writeFileSync(tmp,JSON.stringify({v:1,
      execBackoffUntil:Number(binanceGov.execBackoffUntil||0),
      backoffUntil:Number(binanceGov.backoffUntil||0),
      savedAt:Date.now()}));
    fs.renameSync(tmp,V508_BACKOFF_PATH);   // atomik
  }catch(_){}
}
function v508LoadBackoff(){
  try{
    const j=JSON.parse(fs.readFileSync(V508_BACKOFF_PATH,'utf8'));
    const now=Date.now();
    const e=Number(j.execBackoffUntil||0), p2=Number(j.backoffUntil||0);
    if(e>now) binanceGov.execBackoffUntil=Math.max(binanceGov.execBackoffUntil||0,e);
    if(p2>now) binanceGov.backoffUntil=Math.max(binanceGov.backoffUntil||0,p2);
    if(e>now||p2>now) try{console.log(`[V508] onceki yasak surdurulyor: EXEC ${Math.max(0,Math.ceil((e-now)/1000))}sn · PUBLIC ${Math.max(0,Math.ceil((p2-now)/1000))}sn`);}catch(_){}
  }catch(_){}
}
try{ v508LoadBackoff(); }catch(_){}   // V5.0.8: acilista onceki yasagi surdur
function registerBinanceBackoff(reason='rate-limit', seconds=45, domain='PUBLIC') {""")

# ══ V508-D: ERKEN CAGRI SAYACI + FUNNEL ════════════════════════════════
rep('V508_D_SAYAC',
"""function isExecBackoffActive() {
  return Date.now() < Number(binanceGov.execBackoffUntil || 0);
}""",
"""function isExecBackoffActive() {
  const aktif = Date.now() < Number(binanceGov.execBackoffUntil || 0);
  // V5.0.8: yasak sirasinda engellenen cagri sayilir. Bu sayac dusuyorsa
  // duzeltme calisiyordur; yuksek kaliyorsa cagri yollari hala zorluyordur.
  if(aktif){
    try{ v592ParityStats.execBackoffBlocked=(v592ParityStats.execBackoffBlocked||0)+1; }catch(_){}
  }
  return aktif;
}""")

m=re.search(r"const LAZARUS_BUILD = '([^']+)'",s); assert m
s=s.replace(m.group(0),"const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_8_BACKOFF_HONORED_NOPROBE_RISK41_10X'")
log.append('V508_E_SURUM')

k=[("120sn tavani kalkti", "Math.min(120, Number(retry)||60)" not in s),
   ("180sn tavani kalkti", "Math.min(180, Number(seconds)||45)" not in s),
   ("yeni tavan sabiti", "V508_BACKOFF_MAX_SEC" in s),
   ("  varsayilan 3600", "V508_BACKOFF_MAX_SEC || 3600" in s),
   ("diske yazma", "function v508SaveBackoff()" in s and "fs.renameSync(tmp,V508_BACKOFF_PATH)" in s),
   ("diskten okuma", "function v508LoadBackoff()" in s),
   ("kayit yaziliyor", "v508SaveBackoff();   // V5.0.8" in s),
   ("engellenen cagri sayaci", "execBackoffBlocked" in s),
   ("acilista yukleniyor", s.count("v508LoadBackoff()")>=2),
   ("olcum kaynakta", "%45'i oldu" in s and "3441 sn" in s and "2077 sn" in s),
   ("kok sebep yazili", "Math.min(120, retry)" in s),
   ("restart sinifi belirtilmis", "restart'ta olen state" in s),
   ("V507 retry duruyor", "V507_LEVERAGE_PROOF_MAX_ATTEMPTS" in s),
   ("V506 telemetri duruyor", "type:'ENTRY_CANDLE_DRIFT_BLOCK'" in s),
   ("V505 oy duruyor", "!takerVoteActive" in s),
   ("V502 kaldirac kilidi", "V502-A: KALDIRAC KILIDI" in s),
   ("parite kapisi", "v592BootParityGate" in s),
   ("SONDA yok", "/api/probe" not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log)); print(f'  dogrulama {len(k)}/{len(k)} GECTI')
print(f'  SHA: {hashlib.sha256(s.encode()).hexdigest()}')
