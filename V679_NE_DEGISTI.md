# V6.7.8 — Botun beyni: canlı okuma dışarı açıldı, panele kart kondu

## Coğrafi blok nasıl aşıldı

Bu makine `fapi.binance.com`'a **451** alıyor (coğrafi blok). Bot almıyor —
Railway'den sürekli kline çekiyor. Çözüm: **veriyi bottan çekmek.**

Üstelik yeniden hesap yok, yeni Binance çağrısı yok: bot zaten her aday için
`r483ChartStory` üretiyor. Artık o okuma **saklanıyor ve dışa veriliyor**.
Gördüğün şey, botun karar anında gerçekten gördüğü şeyin ta kendisi.

## İkinci engel: HTTP önbelleği

Bu oturumda kanıtlandı — araya giren önbellek **her yolu ilk çekilişinde
donduruyor**: `r481-status` bir haftalık, `evidence/status` 7 saatlik veri
döndürdü; sorgu parametresi değiştirmek işe yaramadı.

Bu yüzden uç **değişken segment** alıyor:

```
GET /api/v679/beyin/<nonce>?limit=40[&symbol=ONG]
```

Her çağrı ayrı yol → her çağrı taze. `no-store` başlıkları da eklendi.

## Ne kaydediliyor

Her aday değerlendirmesinde (hem **geçen** hem **elenen**):
`symbol · skor · geçti/elendi · eşik · aralıkPoz · ATR · yapı · timing ·
ilk engel · ilk engel R/R · hedef likidite · kalite · fiyat · sıra` ve
**hikâye** — grafiğin insan cümlesine çevrilmiş hâli.

Halka tampon, son 200 kayıt (`V679_BEYIN_KAPASITE`). Karara etkisi **yok**.

## `v679Hikaye()` — hikâye, ölçümle birlikte

Cümleler yalnız mevcut dedektör çıktılarından üretiliyor ve **ölçülen beklenti
cümlenin içinde** — bot ne gördüğünü değil, gördüğünün ne anlama geldiğini de
söylüyor:

- *"düşen trend çizgisi kırıldı ve RETEST tuttu — ölçülen en iyi tekil sinyal
  (beklenti +0,630 / taban +0,408)"*
- *"YANLIŞ kırılım: kırdı ama üzerinde tutunamadı — ölçümde taban altı"*
- *"fiyat 15m ARZ order-block içinde — ölçümde bu iyi (+0,588), çünkü kırılım
  eşiğinin altında sıkışma demek"*
- *"aşağı likidite süpürüldü ve geri alındı (stop avı izi)"*
- *"grafik kalite skoru 34/100 — ÖLÇÜLEN TEK NEGATİF BANT (beklenti −0,071)"*

## Panel kartı

`index.html`'e sağ altta **🧠 Botun Beyni** düğmesi ve açılır panel eklendi.
Her aday için renkli kart: skor, geçti/elendi, kaç dakika önce, temel metrikler
ve hikâye maddeleri. Nonce ile çağırır, XSS kaçışı yapar.

Mevcut düzene dokunulmadı — `</body>` öncesine kendi kendine yeten blok olarak
eklendi. `<script>` 3/3, `<div>` 855/855 dengeli.

## Doğrulama

- **172/172 test.** Yeni: `tests/v679-botun-beyni.test.js` (6 test) — kaydedicinin
  emre dokunmadığını, kaydın veto dalından ÖNCE olduğunu (yoksa elenenler hiç
  kaydedilmez), panelin nonce kullandığını ve kaçış yaptığını pinliyor.
- Yerel boot + gerçek istek:
  `{"ok":true,"build":"V6_7_8_BOTUN_BEYNI","kapasite":200,"ozet":{"esik":40,"kapiAcik":true}}`
- Parite kapısı temiz.

## Henüz yapılmayan — dürüstçe

**MM / avlanma karşıtı özel bir dedektör göndermedim.** Elimizde olan: V678
skoru "yakın engel iyi / yüksek ATR kötü" ilişkisini taşıyor (yüksek ATR bandı
ölçümde −0,214 ile parayı kaybettiren tek bant) ve hikâye süpürmeleri ismiyle
söylüyor. Ama "bizi avlıyorlar" iddiasını **ölçmedim**.

Ölçmek için gereken: beyin tamponu birkaç saat dolduktan sonra elenen ve geçen
adayların sonraki hareketini karşılaştırmak — özellikle *girişten hemen sonra
dip yapıp dönen* işlemleri (ONG −%8,5 → +%17,4; CRV −%7,3 → +%5,7). Bu imza
sistematikse avlanma vardır ve ölçülebilir.
