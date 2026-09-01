# V6.7.5 — Kendi export hatam · ve kâr geri vermenin ölçümü

## Hata benimdi

V6.7.4'ün 13 yörünge kolonundan CSV'ye yalnız **ikisi** çıktı
(`sampleCountUsed`, `sampleSpanMs`). Sebep:

```js
const cols = rows.length ? Object.keys(rows[0]) : [...]   // baslik TEK satirdan
```

CSV başlığı ilk satırdan üretiliyor. İndeksin ilk kaydı `USELESS_1788256810605`
**2 örnek** taşıyordu; V675'teki erken dönüş iki anahtarlı kısa şekil döndürdü;
başlık iki kolonla oluştu ve **diğer 88 satırın kolonları atıldı.**

Oysa veri duruyordu: 89 kaydın **86'sında tam seri** var — medyan 2.133 örnek,
en yüksek 17.810 (SKR: 8.862 örnek / 8.891 sn).

İki düzeltme:
- **A)** Yörünge bloğu artık **her zaman** aynı anahtar kümesini döndürür; eksik
  veride değerler `null`, şekil sabit.
- **B)** Asıl sebep: CSV başlığı artık **tüm satırların birleşimi**. Aynı tuzak
  değişken şekilli her kolon için geçerliydi; `passive.csv` de aynı şekilde
  düzeltildi. `Object.keys(rows[0])` dosyada sıfır kez geçiyor.

## Bu arada ölçülen: kâr geri verme

v3.43 veri seti, 85 kapanmış işlem, gerçek net **−147,63 $**:

| kâr tepesi | n | zararla kapanan | net | ort. geri verme |
|---|---|---|---|---|
| %0–3 | 30 | **28** | −147,92 $ | %15,0 |
| %3–6 | 9 | 6 | −37,67 $ | %17,9 |
| %6–10 | 13 | 9 | −56,48 $ | %19,3 |
| %10–20 | 18 | 4 | +15,48 $ | %12,6 |
| %20+ | 15 | 2 | +78,96 $ | %12,8 |

**69/85 işlem tepesinden düşük kapandı.** Tepesi ≥%5 görüp zararla kapanan
**17 işlem tek başına −115,85 $** — hesabın tüm net zararının %78'i.

En pahalıları:

| | tepe | kapanış | zarar | süre |
|---|---|---|---|---|
| SKRUSDT | +%8,2 | **−%66,2** | −33,11 $ | 148 dk |
| DEXEUSDT | +%17,4 | −%24,2 | −12,08 $ | 30 dk |
| COLLECTUSDT | +%5,0 | −%19,2 | −9,58 $ | 38 dk |
| BMTUSDT | +%18,9 | −%24,7 | −7,41 $ | 46 dk |
| CROSSUSDT | +%22,5 | −%19,1 | −5,72 $ | 50 dk |

Ve şu: tepesi **≥%3'e ulaşan 55 işlem net +0,3 $** — başabaş. Tüm zarar,
tepesi %3'ün altında kalan 30 işlemden geliyor.

## Parametreyi ŞİMDİ vermiyorum — simülasyon kendi kusurunu gösterdi

"Tepe ≥T görünce tepe×L'de kapat" taraması:

| T \ L | %70 | %80 | %85 | %90 | %95 | %99 |
|---|---|---|---|---|---|---|
| %1 | 151,7 | 169,5 | 179,2 | 189,5 | 200,5 | **209,9** |
| %3 | 76,8 | 93,8 | 103,0 | 112,9 | 123,5 | 132,4 |
| %5 | 41,7 | 57,9 | 66,8 | 76,3 | 86,5 | 95,2 |

L büyüdükçe net artmaya devam ediyor — yani "optimum" kural **"tam tepede çık"a**
yakınsıyor. Bu bir kenar çözümü ve simülasyonun kusurunu ele veriyor: `peakRoi`
**geriye dönük** biliniyor. Canlıda takip eden tepe her an nihai tepeden düşüktür,
çıkış da daha alçak bir seviyeden olur.

Yön ve büyüklük gerçek (+241 $ fark, ızgaranın tamamında tek yönlü). **Parametre
güvenilir değil.** Doğru tetik için `peakRoiAtMs` ve CVD yörüngesi gerekiyor —
tam da bu sürümün kurtardığı kolonlar.

## Doğrulama

- **154/154 test.** Yeni: `tests/v676-sabit-kolon-sekli.test.js` (3 test); biri
  `Object.keys(rows[0])`'in dosyada hiç kalmadığını doğruluyor.
- Sahte kimlikle boot: `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`
- Yalnız dışa aktarım; karar yolu ve parite değişmedi.

## Sırada

Deploy sonrası yeni `dataset.csv` ile: kâr tepesi ortalama kaçıncı dakikada
geliyor, ve ilk 1–5 dakikadaki CVD boşalması sonucu **önceden** haber veriyor mu.
İkisi birlikte tetiği tahmin değil ölçüm yapar.
