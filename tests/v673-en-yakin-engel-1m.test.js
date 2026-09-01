'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// CANLI GRAFİK KANITI — ZORAUSDT, 31.08.2026 07:50:02Z, botun KENDİ kanıt paketi.
// Gerçek dedektörler (r483Liquidity/r483Ob/r483Fvg) karar anındaki ham mumlara
// koşturuldu. 1m'de bot şunları görüyordu:
//   • aralık pozisyonu %95 — anlık aralığın tepesinde
//   • arz order block 0.010158–0.010296 [MITIGATED] — GİRİŞ BU BÖLGENİN İÇİNDE
//   • en yakın likidite: SWING_HIGH 0.010217 → girişten %0,08 ötede
//   • Fibonacci: SHORT OTE 0.010223–0.010377 (girişin %0,14 üstünde başlıyor)
//   • 3m ve 5m yapı: DOWN_LH_LL, ikisinde de arz OB'nin İÇİNDE
// Botun kaydettiği ilk engel ise 0.0105935 (15m/1h swing) → R/R 0,85 → kapıyı geçti.
// Gerçek 1m engeliyle R/R = (0.010217-0.010209)/(0.010209-0.00985985) = 0,023.
//
// İki ölçüsüz mantık hatası, ikisi de aynı işlemde:
//   A) fallbackObstacles 1m'i HİÇ okumuyordu (3m/5m/15m/30m/1h/4h vardı, 1m yoktu)
//   B) kaydedilen ilkEngel 0.0105935 > kendi hedefLik 0.010277 — mantıken imkânsız

test('V673-A: ilk engel çözücüsü artık 1m de okuyor', () => {
  const i = server.indexOf('const fallbackObstacles=');
  assert.ok(i > 0);
  const blok = server.slice(i, i + 1600);
  for (const t of ["liquidity?.['1m']?.above", "orderBlock?.['1m']?.supply?.low", "fvg?.['1m']?.bear?.low"])
    assert.ok(blok.includes(t), `fallbackObstacles içinde ${t} olmalı`);
  // eskiler kaybolmamali
  for (const t of ["liquidity?.['3m']?.above", "liquidity?.['5m']?.above", "liquidity?.['15m']?.above",
                   "liquidity?.['30m']?.above", "liquidity?.['4h']?.above"])
    assert.ok(blok.includes(t), `${t} korunmalı`);
  // liste yine girisin USTUNDE filtrelenip siralanip [0] alinmali:
  // yeni TF ancak DAHA YAKIN bir seviye varsa baglayabilir, asla uzaklastiramaz.
  assert.ok(blok.includes('.filter(x=>x>entry*1.00005).sort((a,b)=>a-b)'), 'filtre+sıralama korunmalı');
});

test('V673-B: ilk engel kendi hedefinin ötesinde olamaz', () => {
  assert.match(server, /const V673_ENGEL_HEDEF_TUTARLILIK = String\(process\.env\.V673_ENGEL_HEDEF_TUTARLILIK \?\? '1'\) !== '0';/);
  const i = server.indexOf('V673_ENGEL_HEDEF_TUTARLILIK && first > 0');
  assert.ok(i > 0, 'tutarlılık kontrolü bulunmalı');
  const blok = server.slice(i, i + 400);
  assert.ok(blok.includes('first > target'), 'engel > hedef koşulu olmalı');
  assert.ok(blok.includes('first = target;'), 'hedef ilk engel sayılmalı');
  // fullRisk/firstRRAdjusted hesabindan ONCE calismali, yoksa gate eski degeri gorur
  assert.ok(i < server.indexOf('const fullRisk=entry>recommendedSl'), 'R/R hesabından önce olmalı');
});

test('V673: ZORA aritmetiği — 1m engeliyle bu işlem kapıdan geçemezdi', () => {
  const entry = 0.010209, sl = 0.00985985;
  const botunGorduğu = 0.0105935;   // 15m/1h swing — kaydedilen
  const gercek1m     = 0.010217;    // 1m swing high — dedektörün bulduğu
  const rr = o => (o - entry) / (entry - sl);
  assert.ok(rr(botunGorduğu) > 0.35, `kaydedilen R/R ${rr(botunGorduğu).toFixed(2)} eski kapıyı geçiyordu`);
  assert.ok(rr(gercek1m) < 0.10, `gerçek R/R ${rr(gercek1m).toFixed(4)} yeni kapının da altında`);
  // hedef 0.010277 idi; kaydedilen engel ondan OTEDE -> tutarlilik kontrolu de yakalardi
  assert.ok(botunGorduğu > 0.010277, 'kaydedilen engel kendi hedefinin ötesindeydi');
});

test('V673: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
