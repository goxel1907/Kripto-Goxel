# V6.8.8 — "altımda ne var?"

Şikâyet: **"BOT BÖYLE FIRSATLARI GRAFİKTE OKUYAMIYOR"** — EGLD'de 4,79660 desteğinden
gelen %12,68'lik hareket okunamadı, bot 5,336'dan (tepeden) girdi.

Haklıydın. Sebebi tek satırdı ve skorun **en büyük terimiydi**.

## Hata

```js
s += (rp-0.5)*40;      // aralık pozisyonu
```

Bu terim **tepede olmaya +20, dipte olmaya −20** veriyordu. Yani senin çizdiğin
destek dönüşü kurulumu, tam olarak bu terim yüzünden eleniyordu.

## Ölçüm — 879 grafik / 36.918 nokta (15m, 24 mum ileri, ilk dokunuş, %3 SL / %9 TP)

Ham tablo terimin **ters** olduğunu söylüyor (genel beklenti 0,821):

| aralıkPoz | <15 | 15-30 | 30-45 | 45-60 | 60-75 | 75-85 | 85-95 | 95+ |
|---|---|---|---|---|---|---|---|---|
| beklenti | 0,804 | 0,939 | 0,951 | 0,896 | 0,832 | 0,751 | **0,586** | **0,540** |

Ama daha derine bakınca iş değişiyor. **Dikey uzama (ret6) kontrol edilince**
(n=32.385, alt-ortalama 0,943) tablo neredeyse **DÜMDÜZ**:

| aralıkPoz | <15 | 15-30 | 30-45 | 45-60 | 60-75 | 75-85 | 85-95 | 95+ |
|---|---|---|---|---|---|---|---|---|
| beklenti | 0,814 | 0,977 | 1,023 | 0,982 | 0,936 | 0,904 | 0,912 | **1,099** |

Yani "tepede olmak" tek başına kötü değil — kötü olan oraya **dikey gelmek**, onu da
zaten V680 ölçüyor. Skorun 40 puanlık en büyük terimi **gürültüydü**.

## Kalan tek gerçek işaret: test edilmiş yatay destek

ret6 kontrollü bile olsa duruyor:

| altımdaki en yakın seviye | n | beklenti | kazanç |
|---|---|---|---|
| **EQL (çok dokunuşlu yatay destek)** | 270 | **1,481** | %85,9 |
| tek dokunuşlu dip (SWING_LOW) | 31.332 | 0,945 | %69,7 |
| altta hiçbir şey yok | 783 | 0,654 | %59,9 |

**Bu 270 kurulumun 49'u eski skorla 40 eşiğinin ALTINDA kalıyordu** — senin
çizdiğin tipteki fırsat çöpe atılıyordu.

## Dürüstlük notu — ölçüp ATTIĞIM iki aday

Ham veride güçlü görünüyorlardı, ret6 kontrolünde sıfırlandılar. Skora **konmadı**:

* **desteğe uzaklık** — ham: 0-0,5 ATR 0,917 → 8+ ATR 0,051 (tertemiz monoton).
  ret6 kontrollü: 0,964 → 1,333. Yani tersine dönüyor. Dikey uzamanın kılık değiştirmiş hali.
* **dip süpürüp geri alma (sweepBull)** — ham +0,114, ret6 kontrollü +0,069/−0,046. Gürültü.

İkisi de panelde **raporlanıyor** ama karara girmiyor.

## Ne değişti

1. `(rp-0.5)*40` → `v688AralikPuani(rp)`: ölçülen bant tablosu, **−5 … +6**.
2. `v688DestekOkumasi(story, atrPct)`: altındaki en yakın seviye **EQL** ise **+14**
   (ham ölçüm +22 idi, n=270 küçük diye ihtiyatlı), altta **hiç seviye yoksa −8**,
   tek dokunuşlu dip ise **0**.
3. Panelde beyin kartı artık "altında TEST EDİLMİŞ destek (%0,9 aşağıda) +14" yazıyor.

Yeni veri yok, yeni istek yok, yeni gecikme yok — bu alanların **hepsi zaten**
`story.liquidity` içindeydi (`r483Liquidity` → below/belowType/belowDist), skor onları
hiç okumuyordu.

## Sonuç — 36.918 nokta üzerinde simülasyon (V680 dahil, eşik 40)

| | geçen | beklenti | en iyi %25 | en kötü %10 |
|---|---|---|---|---|
| eski | %76,4 | 0,996 | 1,182 (kaz %78,2) | −0,134 |
| **yeni** | **%82,9** | **1,035** | **1,436 (kaz %85,2)** | **−0,363** |

Daha **çok** aday geçiyor (fırsat kaybı yok), geçenlerin kalitesi yükseliyor, kötüler
daha net ayrışıyor.

## ENV

Değişiklik gerekmiyor — dördü de varsayılanla çalışıyor:
`V688_ARALIK_DUZELT=1` · `V688_DESTEK_OKUMA=1` · `V688_EQL_PUAN=14` · `V688_DESTEKSIZ_CEZA=8`

## Test

`node --test tests/*.test.js` → **222 test, 222 geçti** (7 yeni: `v688-destek-okumasi.test.js`).
