# V6.5.2 — PUSU SEVİYESİ BOĞAZDA ÇEKİLİR

**31 Ağustos 2026 · canlı log kanıtı · üç no-op'un dersi**

## Kanıt — aynı saniye

```
11:15:28  🎯 CLO V649 giriş bölgeye çekildi: 0.14103 → 0.13886 · sapma %1.56
11:15:28  ⚙️ CLO MEKANİK KARAR MOTORU: giriş:0.14103            ← ÇEKİLMEMİŞ
11:15:29  🪤 CLO R442 PUSU kuruldu: fiyat 0.14103 bölgesine     ← ÇEKİLMEMİŞ

11:15:26  🎯 AUCTION V649 giriş bölgeye çekildi: 3.558 → 3.529 · sapma %0.82
11:15:28  🪤 AUCTION R442 PUSU kuruldu: fiyat 3.558 bölgesine   ← ÇEKİLMEMİŞ
```

Çekme yapılıyor, ölçüm doğru, log düşüyor — **ama pusuya ulaşmıyor.**

## Sebep

V649 `entryTruth.plannedEntry`'yi çekiyor. Adayı üreten yol ise
`R486.3.9 PİYASA ANATOMİSİ` ve o yol `plannedEntry`'yi **okumuyor**; kendi
`ai.entry` değerini üretiyor.

`r447`'nin WAIT dönüşü okuyor (LA'da öyle olmuştu, orada çalışmıştı) — ama tek
üretici o değil. Yama **üretici-bağımlıydı.**

## Ders

Üç sürümde üçüncü kez aynı sınıf hata:

| sürüm | hata | nasıl bulundu |
|---|---|---|
| V6.4.7 | `recommendedSl` kapıya hiç geçmiyordu → dal ölü | kod okuması |
| V6.5.0 | `story.pivots` diye alan yoktu → açılsa no-op | göndermeden önce yakalandı |
| V6.5.1 | çekme yapılıyor ama tüketiciye ulaşmıyor | canlı log |

Ortak kök: **üreticiyi yamalamak.** Çözüm: **boğazda yamalamak.**

`r442PusuKur` pusu seviyesinin geçtiği **tek kapı** — tüm kod tabanında bir çağrı
yeri var ve `R442 PUSU TETİKLENDİ` bu haritadan okuyor. Çekme oraya taşındı;
hangi üretici gelirse gelsin seviye bölgeye oturur.

## Değişen

`r442PusuKur` içinde, haritaya yazmadan **önce**: 15m hafıza bölgesi varsa ve
sapma `%0,5–%4` bandındaysa `entry` bölge tavanına çekilir.

**Güvenlik:** bölge tavanı plan SL'inin üstünde değilse çekilmez (plan bozulmasın).
Ayrı bayrak yok — aynı `V649_BOLGEDEN_GIRIS` / `V649_BOLGE_TOLERANS` /
`V649_BOLGE_MAX_SAPMA` kullanılıyor.

V649'un `marketAllowed=false` kolu (B2) **yerinde kaldı** — o `entryTruth`
üzerinden çalışıyor ve doğru yerde.

## Doğrulama

98/98 test · `node --check` temiz · boot temiz (varsayılan + kapalı) ·
server.js 25 satır · CRLF korundu (30.344 → 30.365)

Testler artık *"çekilen değer haritaya yazılıyor mu"* ve *"r442PusuKur gerçekten
tek kapı mı"* sorularını da doğruluyor — no-op bir daha sessiz kalmasın.

## Deploy sonrası aranacak

```
🎯 SYM V652 pusu BÖLGEYE kuruldu: X → Y · bölge A-B · sapma %Z
🪤 SYM R442 PUSU kuruldu: fiyat Y bölgesine    ← Y, X değil
```

İki satırdaki fiyat **aynı** olmalı. Farklıysa hâlâ bir üretici atlıyor demektir.
