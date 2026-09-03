# V6.8.5 — EMIRLERIN COGU BORSAYA ULASMIYORDU

## Kullanicinin sorusu
"Sadece ayni isimli coine girdi, bir onceki versiyon hata olabilir mi, tesadufmu?"

## Cevap: ikisi de degil

Oturum hunisi (lifecycle, 184 kayit):

    ORDER_REQUEST_RECEIVED  38     <- bot 38 kez "ac" dedi
    MAIN_ORDER_SEND         38
    MAIN_ORDER_ACK           7     <- borsa yalniz 7'sini onayladi
    ORDER_ROUTE_ERROR       31
    MAIN_ORDER_SEND_UNCERTAIN 30
    MAIN_ORDER_UNRESOLVED_SYMBOL_LOCKED 30
    ORDER_REJECTED          62

Binance'in dondurdugu metin:

    /fapi/v1/order: Margin is insufficient. (-2019)

Tarama dar degildi: bySymbol 60 ayri coin gosteriyor, en cok bakilanlar
USELESSUSDT 833 / ACEUSDT 770 / UAIUSDT 768; 4USDT 211 ile 22. sirada.
Yani bot 4USDT'yi secmiyordu — digerlerinde emir BORSADA oluyordu.

## Kok neden

`r372GetBilesikMarjin` boyutu **totalWalletBalance**'tan hesapliyor. Kodun kendi
yorumu sebebini soyluyor: "Wallet balance kapanmis kar/zarari bilesik olarak
tasir; availableBalance ilk pozisyondan sonra kuculup ikinci pozu bozmaz."

Ama Binance emri **availableBalance**'tan keser. Acik pozisyon, komisyon veya
yetim emir ikisini ayirdiginda wallet 121$ gorunurken kullanilabilir 45$ olur;
bot 50$ marj ister, borsa -2019 der, emir olur, sembol kilitlenir.

## Duzeltme

Hedef marj artik kullanilabilir bakiyenin tasiyabilecegiyle kelepceleniyor:

    tasinabilir = kullanilabilir / (1,02 + 0,0005 x kaldirac)
    marj = min(wallet_hedefi, tasinabilir)

%2 kayma payi + gercek taker komisyonu dusuluyor. Kelepce yalniz TAVAN:
kucukse indirir, buyukse dokunmaz. Bilesik buyume bozulmaz — kullanilabilir
bakiye buyudukce kelepce kendiliginden acilir.

Kelepce devreye girdiginde panel loguna yaziyor:
`V685 marj kelepcesi: kullanilabilir 45.00$ -> marj 50.00$ yerine 43.96$`

Kapatmak icin: V685_KULLANILABILIR_KELEPCE=0

## 4USDT ucluSU (ayrica olculdu)

    02.09 23:40  giris 0.015912  +8,64%   +4,32$   TACTICAL (risk x0,60)
    03.09 00:14  giris 0.015976  +17,18%  +8,59$   TACTICAL (risk x0,60)
    03.09 01:29  giris 0.016294  -55,85%  -27,93$  MARKET   (risk x1,00)
                                          net -15,02$

Ucuncusu zirve +0,6 gorup -55,9'a gitti: ilk dakikadan yanlisti.
En YUKSEK skor (78) en kotu islemdi; kazanan 62 skorluydu.

Ayni coine tekrar giris kotu degil (n=14, WR %57 vs ilk giris %41) —
tekrar girisi yasaklamak icin sebep yok, olculdu.

203/203 test geciyor.
