# V6.8.6 — BOT YANLIS R/R'YE KAPI KOYUYORDU

## Soru
"En yuksek skor en kotu islemdi. Neden? Grafik okumada mi, verilerde mi hata?"

## Cevap: ikisi de degil

4USDT ucluSU:

    +8,64%   tam R/R 5,39   skor 71
    +17,18%  tam R/R 6,10   skor 62
    -55,85%  tam R/R 1,68   skor 78   <- EN YUKSEK SKOR, EN DUSUK R/R

Kaybeden islemin hikayesi tehlikeyi DOGRU okumustu ve yazmisti:

    tam R/R 1.68                      (digerleri 5,39 ve 6,10)
    FVG uzagi/chase                   (digerinde "FVG icinde")
    RSI-ratio zorlasma                (digerlerinde "kolaylasma")
    C20 ratio 4.38 -> 35.53           (digerlerinde 4,80 -> 1,82, yani sikisiyor)
    HTF:karsi 15m u:%0.3              (digerlerinde %7,46 ve %6,99 otede)
    mikroDurum MICRO_REVERSAL
    pusu YOK — dogrudan MARKET, risk x1,00

Yani grafik okumasi dogruydu, veri dogruydu. **Skor bunlari duymuyordu.**

## Olcum — 88 canli islem

    tam R/R       <-> ROI   r = +0,343   <- en guclu tekil isaret
    skor          <-> ROI   r = +0,179
    ILK ENGEL R/R <-> ROI   r = -0,016   <- SIFIR
    SKOR          <-> tam R/R  r = +0,146

Bot kapiyi ILK ENGEL R/R'sine koyuyor (R493_MIN_FIRST_OBSTACLE_RR,
R486_STORY_TACTICAL_MIN_FIRST_RR) — ROI ile iliskisi sifir olan sayiya.
TAM R/R ise hic kapiya girmiyordu.

    tam R/R < 4 : n=34  WR %35  ort ROI -10,21%  net -164,1$
    tam R/R >= 4: n=54  WR %50  ort ROI  +0,67%  net  +32,8$

Oturumun TUM zarari R/R<4 bandinda. En buyuk kayip cikarilinca da duruyor
(-131,0$). Esik taramasi monoton: 3,5 / 4 / 4,5 / 5 hepsinde ayni yon.

## SERT KAPI KOYMADIM — sebebi

Iliski verinin ILK YARISINDA tersine donuyor:

    1.yari: R/R<4 net  -8,6$ · R/R>=4 net -37,9$   <- TERS
    2.yari: R/R<4 net -155,5$ · R/R>=4 net +70,7$   <- cok guclu

n=88 ve etki son donemde yogun. Sert esik koymak, ilk yarinin reddettigi bir
kurali canli paraya sabitlemek olurdu. Onun yerine KADEMELI ceza:

    tam R/R < 4 ise ceza = (4 - R/R)/4 x 18 puan  (tavan 18)

R/R 1,68 -> -10,4 puan · R/R 3,0 -> -4,5 puan · R/R >= 4 -> ceza yok.
Ucurum yok, mevcut 40 esigi zaten devrede.

ENV: V686_TAM_RR_SKORA=1 · V686_RR_ESIK=4 · V686_RR_CEZA_TAVAN=18
Kapatmak icin V686_TAM_RR_SKORA=0.

## Ayrica olculdu (kapi konmadi)

    bolgeye PUSU kuruldu : n= 6  WR %83  net  +15,9$
    pusu yok (dogrudan)  : n=86  WR %41  net -179,4$

Pusu kuran girisler carpici sekilde iyi ama n=6 — kural yapmak icin az.
Kaydediliyor, buyuyunce bakilacak.

208/208 test geciyor.
