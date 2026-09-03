# V6.8.9 — İki order-flow kitabından ne çıktı, ne çıkmadı

Kaynaklar: *Introduction Into Order Flow Trading* (forexmentoronline) ve
**Trader Dale — *Order Flow Trading Setups*** (161 sayfa, 5 order-flow + 3 volume-profile
kurulumu).

Kitapların **her iddiasını** 879 grafik / 36.918 nokta üzerinde test ettim (15dk, 24 mum
ileri, ilk dokunuş, %3 SL / %9 TP). Kritik nokta: her ölçümü **dikey uzama (ret6) kontrollü**
yaptım — V6.8.8'de öğrendiğimiz gibi ham tablolar yanıltıyor.

---

## ✅ ŞUNU DOĞRULADILAR (zaten botta)

| Kitaptaki kavram | Bizdeki karşılığı |
|---|---|
| **Order splitting** — banka büyük emri parçalayınca **aynı fiyatta birden çok dip** oluşur (PDF 1, s.12) | **V6.8.8 EQL terimi** (+14). Kitap dün ölçtüğümüz şeyin *sebebini* anlatıyor. |
| **Low liquidity move** — tek yönlü dikey hareket biter, çünkü kurumlar oraya giremez | **V6.8.0 parabolik ceza** (ret6) |
| Heavy volume = kurumsal aktivite = S/R | `r49356VolumeProfile` / ghostPoc zaten var |

Yani bot son iki sürümde ölçümle bulduğu şeyleri kitaplar teoriyle anlatıyor. Bu iyi haber.

---

## 🔴 ŞU İDDİALARI ÖLÇTÜM, **TERSİ** ÇIKTI

### 1. "Sadece İLK dokunuşta işlem yap" (Setup #2, #3, #4'te üç kez tekrarlanıyor)

> *"Trade only the first hit (first test). Don't attempt to trade one trading level more
> than once, as the probability of a 2nd successful reaction to the same level is smaller."*

Ölçüm (bölgeye kaçıncı geliş):

| geliş | n | beklenti | kazanç |
|---|---|---|---|
| **1.** | 410 | **−0,090** | %37,3 |
| 2. | 936 | 0,301 | %50,6 |
| 3. | 1.569 | 0,385 | %52,9 |
| 4. | 2.295 | 0,650 | %60,8 |
| **5+** | 21.486 | **+0,882** | %68,2 |

ret6 kontrollü de aynı (0,098 → 1,037). **Tam ters.** 15dk kriptoda çok test edilmiş seviye
zayıflamıyor, güçleniyor — V6.8.8'in EQL bulgusuyla da tutarlı (3-4 dokunuş → 1,42-1,48).
Bu kural bota **konmadı**.

### 2. "Altında Unfinished Business varken LONG açma" (Setup #5, s.73)

Kitap: fitilsiz dip mıknatıstır, fiyat oraya çekilir, stop'un avlanır.

Ölçüm — **aynı mesafedeki** dipleri karşılaştırdım (ret6 kontrollü):

| dibe mesafe | fitilsiz dip | normal dip | fark |
|---|---|---|---|
| 0-1 ATR | n=894 · **1,047** | n=20.566 · 0,954 | **+4 puan** |
| 1-2 ATR | n=324 · **1,114** | n=6.408 · 0,891 | **+9 puan** |
| 2-3 ATR | n=115 · **1,043** | n=2.074 · 0,947 | **+4 puan** |
| 3-5 ATR | n=62 · **1,242** | n=978 · 1,055 | **+7 puan** |
| **toplam** | n=1.395 · **1,071** | n=30.026 · 0,943 | **+5,1 puan** |

Kararlılık: yarılar +5,1 / +4,8 · üçteler +9,5 / +4,7 / +1,2 — **hiç ters dönmüyor**
(V686'nın ilişkisi ilk yarıda dönüyordu, bu dönmüyor).

**Yorum:** 15dk kriptoda fitilsiz dip "bitmemiş müzayede" değil, düşüşün tek mumda
durdurulup geri alındığı **sert savunulmuş seviye**. Ceza değil **ödül** olarak kondu: **+5**.

---

## ⚪ ÖLÇTÜM, SİNYAL YOK — SKORA KOYMADIM

| İddia | Ölçüm | Karar |
|---|---|---|
| **Hacim kümesi (HVN) uzaklığı** (Setup #1, VP #1/2/3) | Ham veri monoton (0,875 → 0,489) ama ret6 kontrolünde düzleşiyor: 0,960 / 0,959 / 0,996 / 0,889 / 0,988 | Yine dikey uzamanın kılığı. **RED** |
| **Absorpsiyon** (yüksek hacim + küçük menzil) | n=73, kontrolde +2 puan, dipte −1 | Gerçek footprint (bid×ask) ister. **RED** |
| **Hacim tabanlı TP** (üstteki ilk HVN'den önce kapat) | Tüm bantlar 0,879-0,989 — düz | **RED** |
| **Üstteki fitilsiz tepe** (TP mıknatısı) | +2 / −2 / −2 / +1 | **RED** |

---

## ⚠️ İZLEMEYE ALDIM — HENÜZ SKORA KOYMADIM

**Delta / CVD teyidi** (Confirmation #3 ve #4). Kitap: destekte agresif alıcı görürsen LONG'a
gir. Bunu geçmiş veride test edemem (aggTrades arşivimiz yok), ama **97 canlı işlemde** test
ettim ve üç bağımsız ölçü de **aynı ters yöne** işaret ediyor:

* `entryCvdRatio` ↔ ROI: **r = −0,184**
* CVD trendi POZİTİF → ort ROI **−%5,03** · NEGATİF → **+%1,48**
* VPIN BUY_DOMINANT → **−%4,89** · SELL_DOMINANT → −%1,57 · NEUTRAL → **+%5,12**

Yani **görünür agresif alışın içine girmek kaybettiriyor** — kitabın naif okumasının tersi,
ama kitabın kendi mantığıyla (pasif tarafta ol, kovalama) ve V680 ile tutarlı.

**Neden şimdi koymadım:** n=79-97 ince, r=−0,18 zayıf, ve büyük ihtimalle V680'in zaten
cezalandırdığı dikey uzama ile karışık. 25-30 kapalı işlem daha birikince ayrıştırıp
karar vereceğim.

---

## Ne değişti (kod)

1. `r484Structure` artık en yakın **alttaki pivot dibin alt-fitil oranını** ölçüyor (`altDip`).
   Yeni veri/istek/gecikme yok — pivotlar ve mumlar zaten oradaydı.
2. `v678GrafikKalitesi`: fitil oranı ≤ %10 ve mesafe < 5 ATR ise **+5**.
3. Panel beyin kartı: "altta FİTİLSİZ dip 1.4xATR +5".

## ENV

Değişiklik gerekmiyor: `V689_FITILSIZ_DIP=1` · `V689_FITIL_ESIK=0.10` · `V689_MAX_ATR=5` · `V689_PUAN=5`

## Test

`node --test tests/*.test.js` → **228 test, 228 geçti** (6 yeni). Testler reddedilen
kuralların koda sızmadığını da doğruluyor.
