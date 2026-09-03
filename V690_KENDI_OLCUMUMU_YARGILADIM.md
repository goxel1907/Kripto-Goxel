# V6.9.0 — Kendi ölçümümü yargıladım, iki terimi geri aldım

**Kaynak:** Viaggi, *A Standardized R-Multiple Framework for the Statistical Validation of
Trading Edge in Retail Trading Systems* (SSRN 6653758).

Bu makale son iki sürümümü çürüttü. Sırasıyla.

---

## 1. Canlı kayıt: nerede olduğumuzun matematiği

Makalenin formülü: **N_min = z²·b / e²**

Bizim geometrimiz RR 1:3 (SL %3, TP %9) → b = 3.

**92 kapalı işlem:** net **−163,49 USDT** · kazanç **%43,5** · kümülatif **−19,40R** ·
işlem başı beklenti **−0,211R** · en uzun kayıp serisi **6** · maxDD **22,3R** ·
**MFE +0,0R** (özsermaye 92 işlem boyunca başlangıcın üstüne hiç çıkmadı).

| kanıt | RR 1:3, N=92 eşiği | bizim |
|---|---|---|
| zayıf | +21,3R | **−19,4R** |
| orta | +27,3R | −19,4R |
| güçlü | +38,6R | −19,4R |

Ve gerçek bir edge'imiz **olsaydı** (+0,10R), onu kanıtlamak için gereken işlem sayısı
(makale Tablo A.17):

| | tek-test | seçim sonrası (M=10) |
|---|---|---|
| RR 1:3, e=+0,10R | **812 işlem** | **1.731 işlem** |

**Sonuç:** canlı kayıttan edge kanıtlamak bizim işlem hızımızda ulaşılamaz. Grafik ölçümü
lüks değil, **tek kaldıraç**. O yüzden onu doğru yapmak zorundayım — ki yapmıyormuşum.

---

## 2. Kendi hatam: "36.918 nokta" şişmiş bir sayıydı

Taramam 4 mum adımla ilerliyordu ama ileri ufuk 24 mumdu → **her sonuç ~6 kez sayıldı**.
Üstelik aynı sembolün noktaları birbirinden bağımsız değil. Gerçek bağımsız birim **grafik**.

**Düzeltilmiş ölçüm:** örtüşmeyen örnek (24 mum adım) = **6.153 nokta / 879 grafik**.
Etkiyi her grafik *içinde* hesaplayıp grafikler *arası* t-testi yaptım (küme-sağlam).

Ve ~20 hipotez denediğim için eşik tek-test 1,645 değil, **seçim sonrası 2,28**
(aynı makale, Tablo A.16, korelasyonlu varyantlar).

| terim | küme n | ort fark | t | karar |
|---|---|---|---|---|
| **V680 dikey uzama cezası** | 50 | −1,330 | **−9,25** | ✅ **GEÇTİ** — dokunulmadı |
| V688 altta destek YOK (−8) | 120 | +0,222 | +1,96 | ❌ geçmedi **ve işaret ters** → **0** |
| V688 EQL destek (+14) | 43 | +0,167 | +1,06 | ❌ geçmedi → **+6**'ya çekildi |
| V689 fitilsiz dip (+5) | 205 | +0,041 | +0,49 | ❌ geçmedi → **0** |
| V688 aralıkPoz bandı | 30 | −0,000 | −0,00 | sıfır → zaten küçük, kaldı |

---

## 3. Ne yaptım

**Kaldı:** V680 dikey uzama cezası. Tek gerçek terim. t = −9,25, her eşiği geçiyor.

**Kaldı:** V6.8.8'in aralıkPoz düzeltmesi. Çünkü asıl iş **yanlış 40 puanlık terimi
kaldırmaktı** — ve küme-sağlam test rangePos'un t=0,00 olduğunu söylüyor, yani o terimin
orada olmaması gerektiğini doğruluyor. Yerine koyduğum ±6'lık tablo zararsız.

**+6'ya çekildi:** EQL desteği. Kanıt yetmiyor ama işaret **her kesitte** pozitif
(havuz 1,413 vs 0,976; aralıkPoz eşleşmiş üç bandın üçünde de yüksek). Küme-sağlam nokta
tahmini +6,7 → ihtiyatla **+6**. Silmedim, şişirmedim.

**Kapatıldı (0):** desteksiz cezası — işareti küme-sağlam testte **ters döndü**, savunulamaz.

**Kapatıldı (0):** fitilsiz dip — t=0,49, etki yok. Dün ekledim, bugün geri aldım.

Terimlerin hiçbiri **silinmedi**: ölçülmeye ve panelde raporlanmaya devam ediyor,
ENV ile geri açılabilir. Sadece **varsayılan ağırlık artık kanıtın taşıdığı kadar**.

---

## 4. EGLD örneği ne oldu

Senin çizdiğin kurulum (4,796 desteği) vs botun girişi (5,336 tepesi):

* **Eski terimle:** dip −15,2 / tepe +16,0 → **tepe 31 puan öndeydi**. Bot bu yüzden tepeden girdi.
* **V6.8.8 ile:** dip 30 puan öne geçti — ama bunun 22 puanı kanıtsızdı.
* **V6.9.0 ile:** dip hâlâ **önde**, fark ~5 puan + dikey uzama cezası.

Yön düzeldi ve kalıcı. Büyüklük artık uydurma değil.

---

## ENV

Değişiklik gerekmiyor. İstersen eski ağırlıkları geri açabilirsin:
`V688_EQL_PUAN=14` · `V688_DESTEKSIZ_CEZA=8` · `V689_PUAN=5` — ama arkalarında kanıt yok.

## Test

`node --test tests/*.test.js` → **234 test, 234 geçti** (6 yeni).
