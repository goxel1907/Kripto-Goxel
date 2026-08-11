# Bu 9 test dosyası canlı sürümde koşturulmaz

Hepsi **SONDA (probe)** modülünü test ediyor. Sonda, testnet'te parametre
dağılımını ölçmek için koşula bakmadan işlem açan bir **araştırma aracıydı**.
Canlı sürümde kodu tamamen silindi (bkz. `CANLI-C4`), çünkü gerçek parayla
koşulsuz işlem açan bir modül bulunmamalı.

Test dosyaları **silinmedi**: sonda testnet'te yeniden açıldığında bu testler
hâlâ geçerlidir. Canlı sürümün doğrulama bütçesine dahil edilmiyorlar, o kadar.

Sonda testlerinin *stratejiyi koruyan* kısımları — hayalet defter muhafızı,
yönetici çıkış izolasyonu, sanal özkaynak filtresi — `V47442` ve `V47443`
içinde ayrıca test ediliyor ve **onlar canlı sürümde koşuyor ve geçiyor**.
