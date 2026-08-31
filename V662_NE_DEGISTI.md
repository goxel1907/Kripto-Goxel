# V6.6.2 — STOP ARTIK ATR'YE DEĞİL FIRSATA UYUYOR

**31 Ağustos 2026 · kullanıcı talimatı: "stop veya ATR yüzünden bariz fırsatlar asla kaçmayacak"**

## Kendi yamamın yan etkisi

V6.6.0'da stopu backtest paritesine (2,35 × ATR) genişlettim. Ama R493 kapısı şunu
hesaplıyor:

```
R/R = engel mesafesi / stop mesafesi
```

Stop payda. Genişletince R/R çöktü ve kapı aynı adayları elemeye başladı:

```
engel giriş üstünde +%2:
   stop %2,29 → R/R 0,873  GEÇER
   stop %9,40 → R/R 0,213  ELENİR
```

Canlıda ölçüm — bugünkü 10 aday, bölge tavanından giriş:

```
plan stopu (ort %2,23)     →  7/10 geçer
geniş stop (2,35×ATR / lik) →  2/10 geçer
```

## Kök sebep

```
backtest: ilk engel giriş üstünde ortanca 1,54 ATR
canlı   : bizim çözücümüz 0,2–0,9 ATR uzakta seviye buluyor
```

Backtestin oranı tutarlı: 1,54 / 2,35 = 0,655 (= firstObstacleRR ortancası 0,638).
Ben **yalnız stopu** kopyaladım, engel yakın kaldı, oran bozuldu.

## Çözüm

Stop artık ATR'nin değil **engelin** fonksiyonu:

```
stop ≤ engelMesafesi / V662_HEDEF_RR        (hedef 0,42 · R493 eşiği 0,35)
minStopPct = max(0,80 , min(likidasyon, ATR×kat, engel/0,42))
```

**Stop hiçbir zaman kendi işlemini diskalifiye edecek kadar genişlemez.**

Yer olan işlemde geniş, yer olmayanda dar — ama **her ikisi de işleme girer**:

```
sembol    engel    →  stop     R/R    sonuç
LA       +13,49%   → %12,86   1,049   GEÇER   (yer var, likidasyon tavanı bağlıyor)
UAI       +4,70%   → %11,19   0,420   GEÇER
AXL       +2,22%   →  %5,29   0,420   GEÇER
MINIMAX   +1,81%   →  %4,31   0,420   GEÇER
TNSR      +1,74%   →  %4,14   0,420   GEÇER
HEMI      +1,34%   →  %3,19   0,420   GEÇER
ZHIPU     +1,28%   →  %3,05   0,420   GEÇER
ZKC       +0,93%   →  %2,21   0,420   GEÇER   (önce 0,249 → elenirdi)
COLLECT   +0,88%   →  %2,10   0,420   GEÇER   (önce 0,312 → elenirdi)
NOT       +0,26%   →  %0,80   0,325   elenir  (gerçekten fırsat yok)

                                      9/10 GEÇER
```

## ATR artık hiçbir şeyi engellemiyor

```
V660_DURDURULAMAZ_VETO = '0'   yüksek ATR işlemi engellemez
V661_BUTCE_VETO        = '0'   bütçe adayı öldürmez
V662 tavanı                    ATR yalnız üç tavandan biri, en küçüğü bağlar
```

Kullanıcı sözleşmesi aynen: **50$ min · 100$ tavan · 7x min 10x max · 1 pozisyon.**
Tek sert sınır likidasyon — o da fizik.

## Doğrulama

121/121 test · `node --check` temiz · boot temiz **üç konfigürasyonda**
(varsayılan · engel-uyum kapalı · eski vetolar açık)

Sessiz no-op alarmı var: engel bulunamazsa `⚠️ V662: girişin üstünde engel bulunamadı`
loglanır, tavan sessizce Infinity kalmaz.

## Geri alma

```
V662_ENGELE_UYUMLU_STOP="0"   stop tekrar ATR/likidasyondan
V662_HEDEF_RR="0.60"          daha dar stop, daha yüksek R/R hedefi
```

## Deploy sonrası aranacak

```
📐 SYM V662 stop fırsata uyduruldu: ATR×2.35=%9.40 → %2.10 · engel +%0.88 · hedef R/R 0.42
```

Bu satır düşüyorsa stop artık fırsata göre ayarlanıyor demektir.
