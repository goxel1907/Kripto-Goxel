# V6.8.0 — "TEPEDE OLMAK" ARTIK KOŞULLU

## Soru

Kullanıcı sordu: *botun beyni coinde neler olduğunun farkında mı?*

Canlı örnek — AKEUSDT, 02.09.2026 17:14. Beyin kartı şunu yazdı:

    AKE 53.8/100 geçti · sıra 3
    aralıkPoz %96 · ATR %3.81 · yapı COMPRESSION · zamanlama JOIN · ilk engel R/R 0.16
    • 15m yapı sıkışan (oynaklık daralıyor)
    • fiyat son 48 mumun aralığında %96 seviyesinde
    • altta ilk likidite 0.0087 (SWING_LOW) — %20.97 aşağıda

O anda fiyat 0,0110'du ve son 90 dakikada +%28 DİKEY çıkmıştı.
15 dakika sonra 0,0087'ye düştü — **-%21**.

## Cevap: hayır, farkında değildi. Sayılarla:

Skorun ayrıştırması (V678 formülü, elle doğrulandı, 53,8 birebir çıkıyor):

    taban                                    50,0
    + (aralıkPoz 0,96 − 0,5) × 40           +18,4   <-- EN BÜYÜK ARTI
    + clamp((2,0 − ATR 3,81) × 8, −20, 15)  −14,5
    = 53,9  ->  geçti (eşik 40)

Yani grafiğin en tehlikeli özelliği — *dikey bir hamlenin tam tepesinde,
altında 21% boşlukla* — skorun **en büyük artı terimi** olarak sayılıyordu.

Bot ham gerçekleri doğru görüyor (aralıkPoz %96 doğru, ATR %3,81 doğru,
destek %21 aşağıda doğru, ilk engel R/R 0,16 doğru). Göremediği şey bunların
**birlikte ne anlama geldiği**. Üstelik `ret6` (son 6 mumun getirisi) zaten
`r484Structure` içinde hesaplanıp story'ye konuyordu — skor fonksiyonu onu
hiç okumuyordu.

Bir de yapı etiketi yanıltıyordu: "COMPRESSION" diyor çünkü pivotlar iki
yandan onay ister; dikey hamlenin tepesi henüz onaylı pivot değil. Yani
etiket 40 dakika öncesini anlatıyor, `rangePos` ise şu anı. Beyin bu
çelişkiyi fark etmiyordu.

## Ölçüm

879 gerçek 15m grafik / 36.918 ilk-dokunuş noktası / ileri 24 mum /
üç geometri (SL-TP: %3-9, %2-5, %4-12). Beklenti R-katsayısı cinsinden.

Son 6 mum (90 dk) getirisi tek başına — temiz ve monoton:

    <%0      n=16.752   +0,086
    %0–3     n=15.633   +0,216
    %3–6     n= 2.773   +0,181
    %6–10    n= 1.074   +0,086
    %10–15   n=   406   −0,020
    %15–25   n=   193   −0,073
    >%25     n=    87   −0,174

Ve asıl bulgu — aralığın tepesiyle KESİŞİMİ:

    aralıkPoz>=%85 & son 90dk <%3     n=3.584   +0,314
    aralıkPoz>=%85 & son 90dk %3–8    n=1.618   +0,177
    aralıkPoz>=%85 & son 90dk %8–15   n=  365   −0,084
    aralıkPoz>=%85 & son 90dk >=%15   n=  191   −0,250

"Tepede olmak" oraya YAVAŞ gelindiyse ölçülen en iyi kurulum; DİKEY
gelindiyse ölçülen en kötüsü. V678 ikisini aynı sayıyordu.

AKE'nin tam profili (aralıkPoz>=%90 & ATR>=3 & son 90dk>=%15):
n=107, beklenti **−0,165** — ve V678 bunların **%96'sını geçiriyordu**.

## Sağlamlık

Kural: `aralıkPoz>=%85 & son 90dk>=%10`

    geometri 3/9   parabolik −0,134   diğerleri +0,137
    geometri 2/5   parabolik −0,234   diğerleri +0,238
    geometri 4/12  parabolik −0,144   diğerleri +0,073
    1. yarı        parabolik −0,211   diğerleri +0,148
    2. yarı        parabolik −0,133   diğerleri +0,150

Eşik taraması monoton: >=%6 −0,063 · >=%8 −0,141 · >=%10 −0,171 ·
>=%12 −0,220 · >=%15 −0,250 · >=%20 −0,274.

## Değişiklik

`v678GrafikKalitesi` artık `story.tf['15m'].ret6` okuyor:

    aralıkPoz>=0,70 -> ceza = clamp((ret6 − 4) × 1,6, 0, 28)
    aralıkPoz>=0,85 ve ret6>=10 -> ceza en az 25

Sonuç (eşik 40, tüm veri):

    geçen n=28.736 beklenti 0,1923  ->  n=28.211 beklenti 0,1985  (Δ +0,0062)
    1. yarı  Δ +0,0067 · 2. yarı Δ +0,0057
    3/9 Δ +0,0056 · 2/5 Δ +0,0094 · 4/12 Δ +0,0035
    elenen 525 aday (geçenlerin %1,83'ü) — beklentileri −0,141

Yani kaybedilen aday çok az ve elenen tam olarak zarar edenler.

**AKE anı: 53,8 -> 25,8 — ELENİR.**

ENV: `V680_PARABOLIK_CEZA=1`, `V680_RET6_TABAN=4`, `V680_CEZA_TAVAN=28`,
`V680_CEP_RET6=10`, `V680_CEP_CEZA=25`. Hepsi varsayılan; ENV'e yazmak şart
değil. `V680_PARABOLIK_CEZA=0` kuralı tümüyle kapatır.

Beyin kartı artık "son 90dk %X" ve "parabolik ceza −N" gösteriyor, ve
hikâyede yapı etiketinin geriden geldiğini açıkça söylüyor.

## Testler

188/188. Yeni `tests/v680-parabolik-ceza.test.js` (8 test) skor fonksiyonunu
kaynaktan çıkarıp GERÇEKTEN ÇALIŞTIRIYOR — AKE profilinin elendiğini,
yavaş gelinen tepenin cezalanmadığını, cezanın monoton ve tavanlı olduğunu,
aralık ortasında devreye girmediğini ve ret6 yokken fail-open kaldığını
sayıyla doğruluyor.
