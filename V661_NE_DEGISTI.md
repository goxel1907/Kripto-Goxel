# V6.6.1 — BÜTÇE ARTIK VETO DEĞİL; TEK TAVAN LİKİDASYON

**31 Ağustos 2026 · kullanıcı kararı**

> "ATR ne olursa olsun 50 dolar 7x, yeter ki bot fırsat görsün mutabık kalsın, stop mesafesi uzasın"

## Bot neden hiç işlem açmıyordu

100$ bakiye · %8 bütçe · 50$ marj · 7x:

```
izin verilen stop = (100 × 8%) / (50 × 7) × 100 = %2,29
```

R372 stopu %5'e genişletiyor, R281 %1,64'e çekiyor, ATR tabanı %2+ istiyor. Sonuç
her seferinde bütçeyi aşıyor ve **iki ayrı veto** adayı öldürüyordu:

```
28788  V624                        → SL'i bütçe tavanına DARALTIYOR
28885  R495_RISK_FLOOR_CONFLICT    → risk > bütçe ise `continue` — aday ATLANIR
```

İkincisi sessizdi: log yazıyordu ama panelde "BEKLE" olarak görünüyordu.

## Değişen

**Bütçe artık veto değil, rapor.** `V661_BUTCE_VETO` varsayılan `'0'`:
- V624 SL'i daraltmıyor
- `R495_RISK_FLOOR_CONFLICT` adayı öldürmüyor

**ATR vetosu kapatıldı.** `V660_DURDURULAMAZ_VETO` varsayılan `'1'` → `'0'`: yüksek ATR
işlemi engellemiyor, stop likidasyon tavanında kelepçeleniyor.

**Tek sert tavan kaldı: LİKİDASYON.** Bu tercih değil fizik — 7x'te ~%14,3'te borsa
pozisyonu kapatır, ondan geniş stop hiçbir zaman çalışmaz.

```
 7x → güvenli tavan %12,86
10x → güvenli tavan  %9,00
```

**Her işlemde gerçek risk loglanıyor** — bütçe aşılsa da gizlenmiyor:

```
💥 SYM V661 RİSK: 50$ × 7x × SL %9,40 = 32,90$ (bakiyenin %32,9'u)
   · bütçe %23 AŞILDI (veto kapalı) · likidasyon tavanı %12,86
```

## BİLİNMESİ GEREKEN

100$ bakiye · 50$ marj · 7x:

```
ATR %2,0  → stop  %4,70 → risk 16,45$   (bakiyenin %16,5'i)
ATR %3,0  → stop  %7,05 → risk 24,68$   (%24,7)
ATR %4,0  → stop  %9,40 → risk 32,90$   (%32,9)
ATR %5,5+ → stop %12,86 → risk 45,00$   (%45,0)  ← TAVAN
```

**Tek kötü işlem bakiyenin %45'ini götürebilir.** Bu, talimatın doğal sonucu ve
kaldırılabilir bir şey değil: 50$ marj + 7x = 350$ pozisyon, 100$ hesapta 3,5 kat.
Üst üste iki stop hesabın çoğunu alır.

Likidasyon tavanı bu kaybı %45,1 ile sınırlıyor — o yüzden duruyor.

## Doğrulama

114/114 test · `node --check` temiz · boot temiz **iki konfigürasyonda**
(veto kapalı = yeni davranış, veto açık = eski davranış)

## Geri alma (deploy gerekmez)

```
V661_BUTCE_VETO="1"          bütçe yeniden veto olur (eski davranış)
V660_DURDURULAMAZ_VETO="1"   yüksek ATR yeniden engellenir
V660_LIK_PAYI="0.80"         likidasyon payı daralır → daha erken stop
```

## Deploy sonrası aranacak

```
💥 SYM V661 RİSK: 50$ × 7x × SL %X = Y$ (bakiyenin %Z'i)
```

Bu satır her aday için düşmeli. Düşmüyorsa emir yolu oraya hiç ulaşmıyordur.
