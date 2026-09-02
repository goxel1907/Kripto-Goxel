# V6.8.2 — PIYASA VERISI ACILDI

Eskiden:

    const V592_POLICY_PARITY_MODE   = true;   // sabit, kodda gomulu
    const V592_RESEARCH_PASSIVE_ONLY = true;

Bu iki sabit karar aninda su verileri SIFIRLIYORDU:
CVD, aggTrades, taker orani, emir defteri dengesizligi, iceberg,
LIKIDASYONLAR, funding, Open Interest, 8 zaman diliminde Fibonacci,
ve kapanmamis 15m mum.

Sebep backtest paritesiydi. Kullanici karari: parite hedef degil.

Simdi:

    const V682_PIYASA_VERISI = String(process.env.V682_PIYASA_VERISI ?? '1') !== '0';
    const V592_POLICY_PARITY_MODE   = !V682_PIYASA_VERISI;
    const V592_RESEARCH_PASSIVE_ONLY = !V682_PIYASA_VERISI;

Varsayilan ACIK. Veri artik karara giriyor:

- Fibonacci / OTE  : 8 zaman diliminde gercek seviyeler
- CVD / taker akisi: gercek delta, notr 0 degil
- Emir defteri     : gercek dengesizlik + iceberg
- LIKIDASYONLAR    : canli !forceOrder akisi, long/short baskinlik, kaskad
- Open Interest    : 15m/1h/4h gercek degisim
- Funding          : gercek oran ve egim
- Canli 15m mum    : kapanmamis mum artik gorunuyor
- Akis yonu        : 'BALANCED_FLOW' sabiti yerine gercek yon

Geri almak icin tek satir: V682_PIYASA_VERISI=0

Panel gosterge listesi tek listeye dondu (artik hepsi karara giriyor).
Beyin karti etiketi "GOLGE" yerine "PIYASA VERISI - karara giriyor".

195/195 test geciyor.
