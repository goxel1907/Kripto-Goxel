# V6.9.3 — Kaldıraç 7x → 5x

Bu bir tercih değil, aritmetik.

```
risk$ = marj × kaldıraç × stop%
```

Marj (50$) ve risk (%23) **sözleşmeyle sabit**. Geriye stopun ne kadar geniş
olabileceğini belirleyen tek serbest değişken kalıyor: **kaldıraç**.

```
ödenebilir stop = risk% / ((marj/özsermaye) × kaldıraç)
```

| kaldıraç | ödenebilir stop | ATR×2,35 karşılanabilen **max ATR(1s)** |
|---|---|---|
| 7x | %6,57 | **%2,80** |
| **5x** | **%9,20** | **%3,91** |
| 3x | %15,3 | %6,52 |

EGLD'nin ATR'si **%5,30**'du — 7x sınırının çok dışında. Stop 0,58 × ATR'ye sıkıştı
ve gürültüye yenildi (−%26).

**Ölçüm** (879 grafik, örtüşmeyen 6.153 nokta, TP %9 sabit):

| stop | stop yeme |
|---|---|
| 0,5 × ATR | **%83,1** |
| 1 × ATR | %67,9 |
| 2 × ATR | %42,5 |
| 4 × ATR | **%14,6** |

**Kaldıraç düşürmek riski artırmaz** — risk$ aynı kalır, sadece stopa nefes alanı açılır.
Alternatifleri eledim: riski %30'a çıkarmak zararı büyütür, marj tabanını 30$'a indirmek
bileşik büyümeyi yavaşlatır. Kaldıraç tek bedelsiz kol.

**Bedeli:** aynı marjda nominal %29 küçük (350$ → 250$). Kazanan işlemde ROI daha düşük
görünür; **R katsayısı değişmez**, çünkü hedef de riskle birlikte ölçekleniyor.

---

## İki tuzak — ikisi de bu kodun kronik hastalığı

**1. `|| 5` fallback'i hiç çalışmıyordu.**
Sabitin varsayılanını 7'den 5'e çektim, açılış logu **hâlâ 7x** yazdı. Sebep: 326. satırdaki
`S('R486_MIN_LEVERAGE','7')` `process.env`'i önceden dolduruyor, fallback'e sıra hiç gelmiyor.
Gerçek varsayılan orası.

**2. Borsaya giden kaldıraç bambaşka bir değişken.**
Emir yolunda:

```js
const _lockLev = (V592_EXACT_BACKTEST_AUTHORITY && V592_LEVERAGE_LOCK > 0)
               ? V592_LEVERAGE_LOCK : Number(leverage);
```

`R486_MIN_LEVERAGE` **değil**, `V592_LEVERAGE_LOCK`. Sadece birincisini değiştirseydim:
açılış logu 5x der, **Binance'e 7x gider**, V691'in ATR tavanı yanlış kaldıraçtan
hesaplanırdı. İkisini de 5 yaptım.

**Ve bir daha sessizce ayrışmasın diye açılışa kontrol koydum:**

```
[V693 UYARI] KALDIRAC AYRISMASI: borsaya giden V592_LEVERAGE_LOCK=7x ama hesaplar
             R486_MIN_LEVERAGE=5x kullaniyor. V691 ATR tavani YANLIS kaldiractan
             hesaplanir. Ikisini esitle.
```

Ayrışma senaryosunu test ederek doğruladım — uyarı çıkıyor.

---

## Railway'de ne yapman gerekiyor

**İki satır.** Şu an env'inde `R486_MIN_LEVERAGE="7"` var ve ENV kodu ezer:

```
R486_MIN_LEVERAGE="5"
V592_LEVERAGE_LOCK="5"
```

İkincisi env'inde yoksa eklemene gerek yok — kod varsayılanı zaten 5. Ama varsa
mutlaka 5 yap, yoksa yukarıdaki uyarıyı görürsün.

Doğru yaptıysan açılışta şunu göreceksin:

```
[V634] RISK SOZLESMESI: 1 pozisyon x %23 · kaldirac 5x
[V691] STOP TABANI: ... 5x -> odenebilir stop %9.20 -> MAKSIMUM ATR(1s) = %3.91
```

`kaldirac 5x` ve `%3.91` görmüyorsan değişiklik geçmemiştir.

## Test

**246 test, 246 geçti** (6 yeni). Ayrıca gerçek açılış denemesi + ayrışma senaryosu.
