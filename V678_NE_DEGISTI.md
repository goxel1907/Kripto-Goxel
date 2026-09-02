# V6.7.7 — Grafik okuma gerçek grafiklerde ölçüldü, karar mekanizmasına bağlandı

## Yöntem

**879 gerçek 15m grafik** (Ağustos 2026, botun seçtiği coinlerin ta kendisi),
**36.918 değerlendirme noktası**. Her noktada `server.js`'in **kendi dedektörleri**
—`r484Structure` · `r484Trendline` · `r483Ob` · `r483Liquidity` · `r483Fib`—
ileriye **bakmadan** koşturuldu (yalnız o ana kadarki 118 mum). Sonra 6 saatlik
pencerede **ilk dokunuş** simülasyonu: TP mi SL mi önce geldi.

Simülasyon değil, botun kendi kodu. İleri bakış yok.

## Ölçüm botun üç varsayımını çürüttü

**1. İlk engel mesafesi — TAM TERS.** Botun R493 kapısı engelin UZAK olmasını ister:

| ilk engel mesafesi | n | WR | beklenti |
|---|---|---|---|
| %0–0,5 | 14.829 | %38,5 | **+0,561** |
| %0,5–1 | 6.500 | %32,2 | +0,378 |
| %1–2 | 5.972 | %29,4 | +0,289 |
| %2–4 | 3.975 | %27,9 | +0,234 |
| %4–8 | 2.006 | %24,0 | **−0,094** |
| %8+ | 1.078 | %24,5 | −0,058 |

Tek yönlü azalan. **Engel ne kadar yakınsa işlem o kadar iyi.** Mekanizma tutarlı:
yakın engel = fiyat bir kırılım seviyesinin hemen altında sıkışmış; uzak engel =
fiyat yapıdan kopmuş, boşlukta.

**2. ATR — yüksek ATR para kaybettiriyor.**

| ATR | n | WR | beklenti |
|---|---|---|---|
| %0–1 | 14.896 | %48,5 | +0,598 |
| %1–2 | 11.535 | %31,9 | +0,433 |
| %2–3 | 4.669 | %25,9 | +0,089 |
| %3–5 | 3.771 | %27,9 | +0,315 |
| %5+ | 2.047 | %23,1 | **−0,214** |

**3. CHOCH ve MSS taban altı.** Bot bunları güçlü boğa olayı sayıyor:
CHOCH_UP +0,293 · MSS_UP +0,248 · taban +0,408. En iyi olay **trend çizgisi
kırılım+retest** (+0,630) — dedektörü var, kapısı yoktu.

## Ne eklendi: `v678GrafikKalitesi()` — 0–100 skor

Kaynak yalnız botun kendi dedektörleri. Katsayılar ölçümden geldi:

```
taban 50
  + (aralıkPoz − 0,5) × 40
  + clamp((2,0 − ATR%) × 8, −20, +15)
  − 10   trend = DOWN_LH_LL
  + 12   trend çizgisi kırılım + RETEST     (yoksa +8 yalnız kırılım)
  +  6   arz OB içinde
  −  8   YANLIŞ kırılım
```

Gönderilen fonksiyonun aynı 36.918 nokta üzerinde ürettiği bantlar:

| skor | n | WR | beklenti |
|---|---|---|---|
| **0–40** | **8.155** | **%24,1** | **−0,071** ← tek negatif bant |
| 40–50 | 6.830 | %29,6 | +0,284 |
| 50–60 | 7.228 | %32,6 | +0,415 |
| 60–70 | 7.321 | %36,8 | +0,640 |
| 70+ | 7.384 | %43,2 | +0,815 |

Beş bantta da tek yönlü.

## Veto: yalnız negatif banda

`V678_MIN_SKOR` (varsayılan **40**) altındaki aday elenir. Ölçüm:
**8.155 nokta (%22,1) elenir, kalanın beklentisi +0,408 → +0,544 (+%33).**

Bu bir ATR vetosu ya da tekil kural değil — **bileşik ve ölçülmüş** bir kapı.
Senin "önemsiz bir kural yüzünden fırsat kaçmasın" şartına uyuyor: eleme yalnız
36.918 noktanın parayı kaybettiren tek bandına uygulanıyor.

Kapatmak için Railway'de `V678_GRAFIK_KALITE=0`, eşiği değiştirmek için
`V678_MIN_SKOR=30` gibi tek satır.

## Aşırı uydurma değil

Çekirdek kural (`aralıkPoz ≥0,6` + `ATR <%2` + `trend ≠ DOWN_LH_LL`):

| test | sonuç |
|---|---|
| SL%3/TP%9 | taban +0,408 → **+0,819 (2,01×)** |
| SL%2/TP%5 | taban +0,479 → +0,951 (1,98×) |
| SL%4/TP%12 | taban +0,290 → +0,574 (1,98×) |
| sembol bazlı | 18 sembolün **13'ünde** daha iyi |
| sembolleri yarıya böl | **2,00×** ve **2,03×** |

Üç geometri, iki yarı, çoğunluk sembol — hepsinde aynı.

## Doğrulama

- Gönderilen fonksiyon dosyadan **geri çıkarılıp** aynı 36.918 nokta üzerinde
  koşturuldu; bantlar birebir çıktı (−0,071 / +0,284 / +0,415 / +0,640 / +0,815).
- **166/166 test.** Yeni: `tests/v678-grafik-kalite-skoru.test.js` (6 test).
- Skor hesaplanamazsa aday **korunur** (fail-open); veto yalnız `ok===true` iken.
- Kapı R493 kalite-sizing'den **önce** — marj hesaplanmadan eler.
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`

## Sınırı

Ölçüm grafiğin rastgele noktalarında yapıldı; bot yalnız aday armlandığında
karar veriyor, yani taban oranlar farklı olacak. **Sıralama** taşınır, mutlak
sayılar taşınmaz. Canlı `V678_LOW_CHART_QUALITY` funnel kayıtları bunu gösterecek.
