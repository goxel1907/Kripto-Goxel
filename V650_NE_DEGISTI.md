# V6.5.0 — BÖLGEDEN GİRİŞ + GRAFİK MOTORUNA PİVOT GÖRÜŞÜ

**30 Ağustos 2026 · 78 işlemlik canlı ledger + 1060 backtest sinyali ile ölçüldü**

## Ölçüm

```
                    BACKTEST    CANLI      fark
kazanma oranı        %70,1      %42,3     -27,8 puan
kazanç/kayıp         1,40:1     0,89:1
işlem başı          +0,485R     net -90,88$ (78 işlem, PF 0,594)
```

Boşluk **kazanma oranında.** Çıkış merdiveni değil — V6.4.9'da onun backtest paritesinde
olduğunu kanıtladım ve oraya attığım hatalı yamayı geri aldım.

## Tek değişken açığın tamamını taşıyor

Backtest sinyallerini aldım, **sadece girişi yukarı kaydırdım** (çıkışlar aynı):

```
kayma %0,00 → kazanma %70,1   (taban)
kayma %0,50 → kazanma %66,6
kayma %0,84 → kazanma %50,6
kayma %1,30 → kazanma %46,9
kayma %1,50 → kazanma %40,8   ← canlı %42,3 TAM BURADA
kayma %2,24 → kazanma %33,4
```

Canlıda ölçülen bölge-üstü kayma:

```
KOMA +%0,84 · ZKP +%1,28 · TAC +%1,30 · COLLECT +%1,45 · PROM +%2,24   → ortanca ~%1,3
```

İki bağımsız ölçüm aynı sayıyı veriyor: **bot kendi ezberlediği 15m bölgenin ~%1,3 üstünden
giriyor ve kazanma oranı %70'ten %42'ye düşüyor.**

Bu bir **alt sınır**: giriş yukarı kayınca stop da yakınlaşıyor (%1,3 kaymada giriş-stop
mesafesi %68'e iniyor), yani gerçek hasar daha büyük.

## Yamalar

**B1 — giriş hafıza bölgesinin tavanına çekilir.**
Zincir uçtan uca doğrulandı: `entryTruth.plannedEntry` → `r447` WAIT dönüşü `entry`
→ `r442PusuKur` → pusu seviyesi. Pusu artık bölgeye kurulur, kovalanan fiyata değil.

**B2 — bölge üstündeyken market emri yok.** `marketAllowed` false olur, plan pusuya düşer.

Tolerans **%0,50** — backtest kırılma noktasından: %0,50 kaymada kayıp 3,5 puan,
%0,84'te 19,5 puan. Eşik ikisinin arasına konuldu.
Bölge %4'ten uzaksa hafıza bayat sayılır: ne çekilir ne bloklanır, sadece loglanır.

**C1 — grafik motoru: pivotlar karar vericiye açılır.**
`r483ChartStory` her zaman dilimi için `r484Structure` çağırıyor, o da `pivots` (swing
tepeler) üretiyor. **Zaten hesaplanıyordu, story'ye konulmuyordu.** Karar veren
`r486EntryTruthGuard` ilk engeli yalnız likidite + arz OB + bear FVG içinden seçebiliyordu;
swing tepeler görüş alanı dışındaydı. C1 veriyi açar — davranış değişmez, ölçülebilir olur.

**C2 — pivotlar ilk-engel adayı olur. VARSAYILAN KAPALI.**
ZKP kanıtı: tanı sistemi ilk engeli 0.05018'de (+%0,52) gördü, karar veren görmedi →
R/R 0,15 yerine 2,40 hesaplandı. Aynı sürümde B açılıyor; ikisi birlikte açılırsa
hangisinin etki ettiği ölçülemez. Açıkken pivot listesi boş gelirse **no-op alarmı** düşer.

## Gönderilmeyen

Pivot engelini ilk denememde `story.pivots` diye bir alan varsayarak yazmıştım —
**yoktu**, açılsaydı sessiz no-op olacaktı (V647'de tam bu hataya düşmüştüm).
Geri çektim, veri yolunu kanıtladım (`r483Pivots` → `r484Structure.pivots` →
`tf[k].pivots.H`), sonra yazdım.

## Doğrulama

83/83 test · `node --check` temiz · boot temiz **üç konfigürasyonda** (varsayılan,
hepsi açık, hepsi kapalı) · server.js 54 satır · CRLF korundu (30.289 → 30.333)

## Geri alma (deploy gerekmez)

```
V649_BOLGEDEN_GIRIS="0"     bölge çekmesi kapanır
V649_BOLGE_TOLERANS="1.5"   tolerans gevşer (varsayılan 0.5)
V649_BOLGE_MAX_SAPMA="8"    bayat sayma sınırı
V650_PIVOT_GORUS="0"        pivot verisi story'den çıkar
V650_PIVOT_ENGEL="1"        pivot engeli AÇILIR (varsayılan kapalı)
```

## İzlenecek

İşlem sayısı düşecek — fiyat bölgeye dönmezse pusu 75 dakikada boşa düşer. Beklenen
karşılık kazanma oranının yukarı gitmesi. **İki hafta veya ~30 işlem sonra ölçülecek:**
kazanma oranı %42,3'ten yukarı çıktı mı, ve giriş-bölge sapması ortancası %0,5'in altına
indi mi. İkincisi çıkmazsa yama fiilen çalışmıyordur.
