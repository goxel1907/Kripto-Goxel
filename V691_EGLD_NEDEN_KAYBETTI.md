# V6.9.1 — EGLD'yi ne öldürdü: bir kategori hatası

**Sonuç:** LONG 5.3370 → 5.1377, SL, **−26,0% ROI**, −12,99 USDT. Zirve +1,0%.

Suçlu tek bir satırdı — ve bir kısmı benim.

---

## Zincir, işlemin kendi sayılarıyla

| | değer |
|---|---|
| giriş | 5,3370 |
| ilk engel | 5,345857 → girişin **%0,166** üstünde |
| V662 engel tavanı | 0,166 / 0,42 = **%0,395** |
| ATR (1s) | %5,30 → gerekli stop = 5,30 × 2,35 = **%12,46** |
| likidasyon tavanı | (100/7) × 0,90 = %12,86 |

Kodda:

```js
minStopPct = Math.max(.80, Math.min(_v660LikTavan, _v660Gereken, _v662Tavan))
//                          min(12,86        ,  12,46      ,  0,395   ) = 0,395
//           max(0,80 , 0,395) = %0,80
```

**ATR tabanı %12,46'dan %0,80'e düştü.** Gerçek stop %3,07 bu sahte tabanı aştığı için
`tightStop = false` oldu ve genişletme hiç çalışmadı.

%3,07 / %5,30 = **0,58 × ATR**. Yani stop gürültünün içindeydi.

## Hata türü

`Math.min` içinde **taban ile tavan karıştırılmış**.

* ATR tabanı = **hayatta kalma** koşulu (stop gürültüden geniş olmalı)
* Engel tavanı = **cazibe** koşulu (stop hedefi diskalifiye edecek kadar geniş olmamalı)

Birinin diğerini ezmesi kategori hatası. Engel ATR gürültüsünden yakınsa doğru cevap
stopu kısmak değil, **market'e girmemek**. Zaten hikâyenin kendisi de bunu söylüyordu:

> `PUSU · R486 giriş sözleşmesi MARKET'i engelledi · bölgeye pusu kuruldu (5.194-5.223)`

Pusu 5,19-5,22'ye kurulmuştu. Giriş 5,3370'ten oldu — **%2,5 yukarıdan**.

## Benim payım

V6.8.7'de ilk-engel vetosuna muafiyet koymuştum (tam R/R ≥ 4 ise veto kalkar).
EGLD'nin ilk engel R/R'si **0,20**'ydi. Eski veto bunu keserdi. Muafiyet açtı,
sonra aynı yakın engel V662 üzerinden ATR tabanını da çökertti.

## Ölçüm — stop genişliği gerçekten önemli mi?

879 grafik, örtüşmeyen 6.153 nokta, TP %9 sabit, stop k × ATR:

| k × ATR | TP % | **STOP %** | beklenti (R) |
|---|---|---|---|
| 0,5 | 8,0 | **83,1** | 0,064 |
| 1,0 | 13,0 | 67,9 | 0,073 |
| 2,0 | 17,7 | 42,5 | 0,116 |
| 4,0 | 20,1 | **14,6** | 0,165 |

Monoton. **Bu tablo yeni bir kural için değil** — botun zaten sahip olduğu
`R486_MIN_STOP_ATR = 2.35` kuralının neden önemli olduğunu gösteriyor.
(Küme-sağlam eşleşmiş test t=1,37; keşif eşiğini geçmiyor, o yüzden yeni kural açmadım.)

## Ne değişti

**Tek şey:** taban artık engel tavanıyla ezilmiyor.

```js
const _v691Taban = Math.max(.80, Math.min(_v660LikTavan, _v660Gereken));
```

Engel tavanı hâlâ hesaplanıyor; ATR tabanından darsa artık **işaret** veriyor
(`_v691EngelDar`): *"ilk engel ATR gürültüsünden yakın"*. Bu bir MARKET işlemi değildir.

EGLD'de sonuç: `%3,07 < %12,46` → `tightStop = TRUE` → gerekçeye yazılır, erken riski
tetikler, retest uzaksa market bloklanır.

**Yeni kapı EKLEMEDİM.** İlk yazımda `marketAllowed`'a bir kapı koymuş ve ödenebilirliği
kaldıraca bölmüştüm — **marjı unutarak**, 2 kat fazla sıkı. Geri aldım. Doğrusu:

```
ödenebilir stop = risk% / ((marj/özsermaye) × kaldıraç)
                = 23 / (0,50 × 7) = %6,57   →   max ATR(1s) = %2,80
```

## Senin kararın gereken yer

**EGLD'nin ATR'si %5,30'du. Sözleşmenin sınırı %2,80.** Yani bu coin, bu sözleşmeyle
stopla korunarak alınamazdı. Bot artık bunu **açılışta tek satırda** yazdırıyor:

```
[V691] STOP TABANI: ATR x 2.35 · %23 risk · 50$/100$ marj · 7x
       -> ödenebilir stop %6.57 -> MAKSIMUM ATR(1s) = %2.80
```

Sınırı büyütmenin üç yolu var, üçü de senin tercihin:

| kaldıraç (%23 risk, 50$/100$) | max ATR |
|---|---|
| 7x | %2,80 |
| 5x | %3,91 |
| 3x | %6,52 |

| risk (7x sabit) | max ATR |
|---|---|
| %23 | %2,80 |
| %26 | %3,16 |
| %30 | %3,65 |

Bir de **marj tabanı**: 50$ yerine 30$ olsaydı max ATR %4,67 olurdu. Kod yorumunda
zaten yazıyordu: *"50$ sabit marjla stopu 0,4-0,5 ATR'ye sıkıştırıyordu."*

## ENV

Değişiklik gerekmiyor. Eski (hatalı) davranış: `V691_ATR_TABANI_KORUNUR=0`.

## Test

`node --test tests/*.test.js` → **240 test, 240 geçti** (6 yeni; EGLD'nin aritmetiği
teste birebir gömüldü).
