# -*- coding: utf-8 -*-
# V5.0.3 — TESTNET SEMBOL EVRENI ON-FILTRESI
# Her donusum TAM BIR KEZ uygulanmazsa build COKER.
import pathlib, hashlib, sys
p = pathlib.Path('server.js'); s = p.read_text(encoding='utf-8')
GIRDI = '16b2baf3cba97a4f8d533cc47fa9f4429fbe2faf27dfe59585148b76ea24bc70'
h = hashlib.sha256(s.encode()).hexdigest()
if h != GIRDI: sys.exit(f'BUILD DURDU: girdi SHA {h} != {GIRDI}')
log=[]
def rep(et, eski, yeni, adet=1):
    global s
    n=s.count(eski)
    if n!=adet: sys.exit(f'BUILD DURDU [{et}]: beklenen {adet}, bulunan {n}')
    s=s.replace(eski,yeni); log.append(f'{et}: {adet} yer')

# ══ V503-A: BAYRAK ═════════════════════════════════════════════════════
rep('V503_A_BAYRAK',
"const V592_TESTNET_UNIVERSE_TTL_MS=",
"""// ══ V5.0.3 — TESTNET SEMBOL EVRENI ON-FILTRESI ═══════════════════════
// OLCULDU 11.08 (V502A_72H_TEMIZ_OLCUM_2): bot 5,7 saatte 2 gecerli sinyal
// uretti, IKISI DE emir aninda reddedildi:
//   21:30 CLOUSDT  TACTICAL · R495 2 oy · ilk engel R/R 1,68 · 24s +%18,6
//   21:33 CRWVUSDT LONG · lev 10 · marj 11,37
//   ikisi de -> SYMBOL_NOT_ON_TESTNET
// Kok sebep: bu kontrol emir yolunun ICINDE (v592SendMainOrder oncesi).
// Aday listesi kurulurken YOKTU. Bot mainnet top-gainer'i analiz ediyor,
// en iyisini seciyor, tum zinciri yurutuyor, son anda islemi TAMAMEN
// kaybediyor — sonraki adaya da gecmiyor.
// Olculen fark: mainnet uygun evren 683, testnet evreni 528 (%23) ve fark
// rastgele degil; top gainer'lar agirlikli olarak yeni listelenen coinler,
// testnette tam da onlar yok.
//
// SAPMA KAYDI: bu YALNIZ TESTNET icindir. Backtest mainnet sembolleriyle
// kostu ve orada her aday islem gorebiliyordu. Canlida mainnet=mainnet
// oldugu icin filtre devreye GIRMEZ. Bu bir parite duzeltmesi degil,
// testnet borsasinin eksik listesine karsi olcum kurtarma islemidir.
const V503_TESTNET_UNIVERSE_PREFILTER = BINANCE_EXECUTION_ENV==='TESTNET'
  && String(process.env.V503_TESTNET_UNIVERSE_PREFILTER ?? '1') !== '0';
const V592_TESTNET_UNIVERSE_TTL_MS=""")

# ══ V503-B: ON-FILTRE ══════════════════════════════════════════════════
rep('V503_B_FILTRE',
"""    for (const [scanIdx, coin] of scanList.entries()) {""",
"""    // ══ V503: testnet evren on-filtresi ════════════════════════════════
    // FAIL-OPEN: evren alinamazsa filtre uygulanmaz (islem durmasin), ama
    // SAYAC artar — sessiz erken donus yok.
    if (V503_TESTNET_UNIVERSE_PREFILTER) {
      try {
        const _uni = await v592RefreshTestnetUniverse();
        if (_uni && _uni.size > 0) {
          const _once = scanList.length, _elenen = [];
          scanList = scanList.filter(c => {
            const _s = normalizeSymbol(c.fullSymbol || c.symbol);
            if (_uni.has(_s)) return true;
            _elenen.push(_s.replace('USDT',''));
            return false;
          });
          v592ParityStats.testnetUniversePrefiltered =
            (v592ParityStats.testnetUniversePrefiltered||0) + _elenen.length;
          v592ParityStats.testnetUniversePrefilterRuns =
            (v592ParityStats.testnetUniversePrefilterRuns||0) + 1;
          if (_elenen.length) {
            logAuto(`\\u{1F9EA} V503 testnet evren on-filtresi: ${_once} aday \\u2192 ${scanList.length} · elenen: ${_elenen.slice(0,10).join(', ')}${_elenen.length>10?'\\u2026':''}`);
            try{ r501EvidenceFunnel({type:'TESTNET_UNIVERSE_PREFILTER',action:'PREFILTER',
              authority:'TESTNET_EXECUTION',symbol:null,decisionImpact:true,orderBlocking:false,
              before:_once,kept:scanList.length,removed:_elenen,
              reason:`testnet exchangeInfo listesinde olmayan ${_elenen.length} aday analiz oncesi elendi`}); }catch(_){}
          }
        } else {
          v592ParityStats.testnetUniversePrefilterSkipped =
            (v592ParityStats.testnetUniversePrefilterSkipped||0) + 1;
          logAuto('\\u26A0\\uFE0F V503 testnet evreni bos/alinamadi \\u2014 on-filtre bu taramada uygulanmadi (fail-open)');
        }
      } catch(_v503e) {
        v592ParityStats.testnetUniversePrefilterError =
          (v592ParityStats.testnetUniversePrefilterError||0) + 1;
        logAuto(`\\u26A0\\uFE0F V503 on-filtre hata: ${String(_v503e?.message||_v503e).slice(0,110)}`);
      }
    }

    for (const [scanIdx, coin] of scanList.entries()) {""")

# ══ V503-C: SURUM ADI ══════════════════════════════════════════════════
import re
m=re.search(r"const LAZARUS_BUILD = '([^']+)'", s); assert m
s=s.replace(m.group(0), "const LAZARUS_BUILD = 'R493_V5_9_2_CANLI_EXACT_CLOSED1M_R495_V5_0_3_TESTNET_UNIVERSE_PREFILTER_NOPROBE_RISK41_10X'")
log.append('V503_C_SURUM')

# ══ DOGRULAMALAR ═══════════════════════════════════════════════════════
k=[("bayrak testnet-only", "V503_TESTNET_UNIVERSE_PREFILTER = BINANCE_EXECUTION_ENV==='TESTNET'" in s),
   ("filtre dongu ONCESINDE", s.index('V503 testnet evren on-filtresi') < s.index('for (const [scanIdx, coin] of scanList.entries())')),
   ("sayac var (elenen)", 'testnetUniversePrefiltered' in s),
   ("sayac var (atlanan)", 'testnetUniversePrefilterSkipped' in s),
   ("sayac var (hata)", 'testnetUniversePrefilterError' in s),
   ("fail-open", 'fail-open' in s),
   ("funnel kaydi", 'TESTNET_UNIVERSE_PREFILTER' in s),
   ("sapma belgeli", 'SAPMA KAYDI' in s),
   ("emir yolu kontrolu DURUYOR", 'SYMBOL_NOT_ON_TESTNET' in s),
   ("kaldirac cift kilit DURUYOR", 'V502-A: KALDIRAC KILIDI' in s),
   ("parite kapisi DURUYOR", 'v592BootParityGate' in s),
   ("SONDA yok", '/api/probe' not in s)]
for ad,ok in k:
    if not ok: sys.exit(f'BUILD DURDU [DOGRULAMA]: {ad}')
p.write_text(s,encoding='utf-8')
print('\n'.join('  ✓ '+x for x in log))
print(f'  dogrulama: {len(k)}/{len(k)} GECTI')
print(f'  cikti SHA: {hashlib.sha256(s.encode()).hexdigest()}')
