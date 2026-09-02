# V6.8.1 — PİYASA VERİSİ GÖLGEDE AÇILDI (karara girmiyor, ölçülüyor)

## Neden

`V592_POLICY_PARITY_MODE = true` karar anında şunları sıfırlıyor:
CVD, aggTrades, taker oranı, emir defteri dengesizliği, iceberg, **likidasyonlar**,
funding, Open Interest. Sebep: 725 işlemlik backtest'te bu veriler yoktu, canlıyı
onunla kıyaslanabilir tutmak içindi.

Kullanıcı kararı: backtest paritesi hedef değil, canlıda ölçeceğiz.

Ama veriyi doğrudan karara sokmak, hangi alanın ne yaptığını bilmeden canlı paraya
etki etmek olurdu. Onun yerine: **ham değerler karar anında kaydediliyor, karara
hiç girmiyor.** 30-50 kapanmış işlemden sonra ölçülecek — bu veri kazanan işlemle
kaybedeni gerçekten ayırıyor mu? Ayırıyorsa oy hakkı verilir, ayırmıyorsa kapalı
kalır ve elimizde "bakıldı, ayırmıyor" cevabı olur.

## Nasıl — yeni veri toplamaya gerek yoktu

Nötrleyicinin çağrıldığı yerde ham veri zaten duruyordu:

    const researchInput = data;  data = r592NeutralizeDecisionData(data);

V681 o `researchInput`'tan okuyor. Yani ekstra API çağrısı, ekstra ağırlık,
ekstra gecikme YOK. Kaydedilen alanlar:

    akis      : cvdRatio · cvdDelta · cvdValid · tickTrend · takerRatio · aggBuy/aggSell
    defter    : bookImb · icebergSignal
    pozisyon  : oi15m · oi1h · oi4h · oiState · funding · fundingSlope
    LIKIDASYON: likLong5m · likShort5m · likLong1m · likShort1m · likDominance · likCascade
    yavas trend: yavasSapma · yavasUstunde · sma50h

Likidasyonlar gerçek `!forceOrder@arr` akışından geliyor — bot zaten tüm Binance
futures likidasyonlarına bağlıydı, sadece karar anında sıfırlanıyordu.

## Yavaş trend terimi nereden çıktı

MarkitTick "Liquidity Entry Signals" script'i ölçüldü (879 grafik, 11.832 sinyal).
Script'in çekirdeği 10 mumluk süpürme+geri alma; likidasyon verisi içermiyor ve
bot bu dedektöre zaten sahip (`SSL_SWEEP_RECLAIM`).

Gönderildiği haliyle (filtreler kapalı) botun geometrisiyle **−0,202**;
LONG sinyalleri +0,047 — rastgele bir anın +0,146 tabanının altında.

Kendi geometrisiyle LONG tarafı pozitif (3R +0,263), SHORT sert negatif (−0,435).
Aynı sinyallere iki stop uygulandığında:

    TUMU (n=3986)      script stopu: 3R 0,263    BOT stopu: 3R 0,319
    HTF yukari (n=352) script stopu: 3R 0,594    BOT stopu: 3R 0,662

Botun R495 stopu script'inkini yeniyor — script'ten alınacak kod yok.
Ama ölçüm bir şey gösterdi: süpürme sinyali **yavaş trend ortalamasının
üstündeyken** çok daha güçlü (3R +0,662, iki yarıda tutarlı). Dikkat çekici olan:
daha önce ölçtüğüm 1h *pivot yapı etiketi* hiçbir şey vermemişti — ama
**yavaş bir ortalamanın üstünde/altında olmak** farklı bir şey ve veriyor.

n=352 ve yalnız süpürme mumlarında ölçüldü; doğrudan göndermek yerine
gölgeye eklendi. Tüm adaylar üzerinde canlıda ölçülecek.

## Karara etki YOK — testle sabitlendi

`v681Piyasa` yalnızca `researchPassive` bloğunun içinde (`decisionImpact:false`),
emir yolunda onu okuyan tek bir kapı yok. Test bunu sayıyla doğruluyor.

Kapanmış işlem CSV'sine 23 yeni kolon (`v681_*`) eklendi:
`/api/evidence/dataset.csv`

Beyin kartı ham veriyi **"[GÖLGE — karara girmiyor, ölçülüyor]"** etiketiyle
gösteriyor, böylece panelde yanlış anlaşılmaz.

## Testler

195/195. Yeni `tests/v681-piyasa-golgesi.test.js` (7 test).
