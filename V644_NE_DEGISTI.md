# LAZARUS V6.4.4 — Pozisyon Odağı, 15m Aday Hafızası ve 50 USDT Taban

## Canlı işlem sözleşmesi

- Aynı anda en fazla **1 açık pozisyon**.
- Pozisyon başına başlangıç marjı en az **50 USDT**.
- Bileşik büyüme hesabı, özsermaye büyüdüğünde marjı artırabilir; kesin üst sınır **100 USDT**.
- Yaklaşık 166,67 USDT özsermayeye kadar yüzde hesabı 50 USDT'nin altında kaldığı için işlem 50 USDT ile açılır. Daha sonra yüzde hesabı 50–100 USDT arasında büyür; hiçbir işlem doğrudan 100 USDT ile başlamak zorunda değildir.

## Açık pozisyon odağı

Binance tarafından açık pozisyon doğrulandığı anda yeni aday üreten ana tarama ve R370/R385/R328/R366/R614 yardımcı taramaları durur. Bu sırada:

- açık pozisyon senkronu,
- koruyucu SL/TP denetimi,
- trailing ve pozisyon yönetimi,
- kapanış ve ledger doğrulaması

çalışmayı sürdürür. Pozisyonun gerçekten kapandığı teyit edilince aday taraması güvenli aralıktan sonra otomatik devam eder. Geçici boş `positionRisk` yanıtı taramayı yanlışlıkla yeniden başlatmaz.

## 15 dakikalık aday yolculuğu hafızası

Bot ekran görüntüsü piksellerini yorumlamaz. Bunun yerine daha güvenilir ve test edilebilir olan, karar anında kapanmış 15m mumlardan üretilmiş yapılandırılmış grafik kaydını saklar. Önceki taramalardaki:

- giriş/retest bölgesi,
- stop/geçersizlik seviyesi,
- ilk engel ve TP bölgesi,
- yön, yapı ve likidite hikâyesi

aynı coin sonraki taramada yeniden görüldüğünde hesaba katılır.

Hafıza yalnız iki açık zamanlama hatasında MARKET'i engeller:

1. Önceki stop seviyesi kırılmış ve reclaim oluşmamışsa `V644_PRIOR_STOP_WAIT_RECLAIM`.
2. Önceki hedef tüketilmiş ve giriş artık kovalamaya dönüşmüşse `V644_PRIOR_TARGET_NO_CHASE`.

Bunların dışındaki hafıza durumları botu boğmaz; kanıt ve sıralama bağlamı olarak kullanılır. Kayıt yalnız karar anından önce kapanmış mumlara dayanır (`CLOSED_15M_NO_LOOKAHEAD`). Veri yapısı backtestte üretilebilir; fakat bu iki yeni zamanlama vetosunun kârlılık etkisi ayrıca ölçülmeden eski backtest PF/WR değerinin değişmeden kaldığı iddia edilmez.

## Yeni durum uçları

- `/api/v644/candidate-memory`
- `/api/v644/candidate-memory/:symbol`
- `/api/auto/status` içinde `positionFocus` ve `candidateMemory`

## Bu sürümün ENV değerleri

```env
R486_MAX_POSITIONS="1"
R486_ABSOLUTE_MIN_MARGIN="50"
R497_SLOT_MARGIN_USDT="50"
V601_MARJ_TABAN="50"
V601_MARJ_TAVAN="100"
V644_POSITION_FOCUS_ACTIVE="1"
V644_15M_MEMORY_ACTIVE="1"
V644_15M_MEMORY_HOURS="12"
```

Geri dönüş için yalnız yeni davranışlar kapatılabilir:

```env
V644_POSITION_FOCUS_ACTIVE="0"
V644_15M_MEMORY_ACTIVE="0"
```

## Doğrulama

- Sunucu sözdizimi kontrolü: geçti.
- Otomatik regresyon testleri: **22/22 geçti**.
- Testler; 50/100 USDT marj sözleşmesini, tek pozisyonu, açık pozisyonda tüm aday yollarının durmasını, pozisyon yöneticilerinin açık kalmasını ve 15m hafızasının yalnız tanımlı iki durumda veto vermesini denetler.
