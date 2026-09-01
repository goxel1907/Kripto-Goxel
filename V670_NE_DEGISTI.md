# V6.7.0 — İlk engel eşiği (foRR) 0,35 → 0,10

## Neden

SKR (-%59) denetimi sırasında botun **neden neredeyse hiç işlem açmadığı** ölçüldü.
Suçlu, kaybın kendisi değil: **ilk engel R/R kapısı 0,35**.

Ölçüm: `LAZARUS_HAZIRAN_2026_6TF_BACKTEST_PAKETI/all_scenario_trades.csv`,
senaryo `R495_3M_ACCEPT`. **İşlem sonuçları değişmiyor**, yalnız kapı değişiyor:

| foRR kapısı | n | WR | PF | net | işlem-başı | maxDD |
|---|---|---|---|---|---|---|
| yok | 555 | %65,8 | 1,83 | +1186,77$ | 2,14$ | 97,69$ |
| **≥ 0,10** | 289 | %68,5 | **2,44** | +911,56$ | **3,15$** | **53,29$** |
| ≥ 0,20 | 200 | %70,0 | 2,34 | +613,68$ | 3,07$ | 50,22$ |
| ≥ 0,35 *(canlıda buydu)* | 118 | %64,4 | 1,90 | +290,25$ | 2,46$ | 58,29$ |
| ≥ 0,50 | 84 | %65,5 | 1,48 | +120,91$ | 1,44$ | — |

Canlı gerçeğiyle (**max 1 pozisyon**, zaman sıralı, çakışan aday atlanır):

| foRR kapısı | n | WR | PF | net | işlem-başı | maxDD |
|---|---|---|---|---|---|---|
| yok | 305 | %66,9 | 1,91 | +716,31$ | 2,35$ | 73,86$ |
| **≥ 0,10** | 210 | %67,1 | **2,23** | +606,12$ | **2,89$** | **64,10$** |
| ≥ 0,20 | 160 | %68,1 | 2,04 | +405,18$ | 2,53$ | 63,06$ |
| ≥ 0,35 | 99 | %63,6 | 1,61 | +163,96$ | 1,66$ | 58,18$ |

0,35 kapısı **437 işlemi eliyordu**: bunların WR'ı %66,1, PF'i 1,81, neti **+896,52$**.
Yani kapı, PF'i de düşürüyordu — 0,35 tepe noktanın **ötesinde**.
`RAW_R447` senaryosu aynı şeyi söylüyor: 0,10→PF 1,99 · 0,20→2,16 · **0,35→1,80**.

Tek slotta önemli olan işlem-başı beklenti: **0,10'da 2,89$**, kapısızda 2,35$,
0,35'te 1,66$. Kapısız toplamda daha çok para yapıyor (+716$ vs +606$) ama
%15 daha derin dip (73,86$ vs 64,10$) ve daha düşük PF ile. 100$ hesapta dip önemli.

## Ne değişti

Üç ayrı yerde aynı 0,35 duruyordu; **üçü birden** inmezse hiçbir şey değişmezdi.

1. `V592_V45_FIRST_OBSTACLE_RR_MIN` 0,35 → **0,10**
   V4.5 seçicisi R495'ten **önce** eler. Yalnız R493 indirilseydi bu sessizce bloklamaya devam ederdi.
2. `R486_FIRST_OBSTACLE_MIN_RR` — `Math.max(0.35, …)` **TABANI** kaldırıldı → `Math.max(0.01, …)`, varsayılan 0,10.
   Eskiden env'e 0,10 yazılsa sessizce 0,35'e yükseltiliyordu. (Dosyadaki kendi yorumu
   zaten aynı hatayı bir kat yukarısı için not etmiş: *"old 0.70 clamp made ENV=0.35 ineffective"*.)
3. `R493_MIN_FIRST_OBSTACLE_RR` — taban 0,10 → **0,01**, varsayılan 0,35 → **0,10**.

Ve **boot parite kapısı** üç `eq(…,0.35)` şartından aralık kontrolüne çevrildi:

```
V45_FO_035_DEGIL   -> V45_FO_ARALIK_DISI    (0,01–1,00)
R493_FO_035_DEGIL  -> R493_FO_ARALIK_DISI   (0,01–1,00)
R486_FO_035_DEGIL  -> R486_FO_ARALIK_DISI   (0,01–1,00)
```

Bu üçü kalsaydı yeni değerle bot **fail-closed** açılır, `EMIR ACILMAYACAK` derdi.
Doğrulandı: sahte kimlikle boot → `✅ CANLI parite kapisi TEMIZ — emir yolu acik.`

## RAILWAY'DE YAPILMASI GEREKEN

Koddaki varsayılan artık 0,10 **ama Railway'de bu değişkenler açıkça 0.35 yazılıysa
env kazanır ve hiçbir şey değişmez.** Railway → Variables:

```
R493_MIN_FIRST_OBSTACLE_RR     = 0.10
V592_V45_FIRST_OBSTACLE_RR_MIN = 0.10
```

Kapıyı tamamen kaldırmak istersen ikisini de `0.01` yap (ölçüm: daha çok para,
daha derin dip). Geri almak için `0.35`. Kod değişikliği gerekmez.

## Değişmeyenler

Marj 50$ tabanı, 7x–10x kaldıraç, max 1 pozisyon, likidasyon tavanı (V660),
bütçe vetosunun kapalılığı (V661), stop üretimi (R495 clamp %1,2–8,0), hedef
tavanı (%12) — hepsi aynı. Bu sürüm **yalnız hangi adayların değerlendirmeye
alındığını** değiştirir.

## Test

`tests/v670-ilk-engel-esigi.test.js` — 6 test. Toplam **133/133**.
