'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

// OLCUM — 879 grafik / 36.918 nokta (15m), 24 mum ileri, ilk dokunus, %3 SL / %9 TP.
// Genel beklenti 0,821.
//
// 1) ARALIK POZISYONU (eski terim (rangePos-0,5)*40 = tepeye +20, dibe -20)
//    HAM tablo terse benziyordu (tepe 0,540, dip-usti 0,939) ama bu DIKEY UZAMA idi.
//    ret6<3 kontrol edilince (n=32.385, alt-ort 0,943) tablo neredeyse DUZ:
//      <15:0,814  15-30:0,977  30-45:1,023  45-60:0,982
//      60-75:0,936  75-85:0,904  85-95:0,912  95+:1,099
//    Yani skorun EN BUYUK terimi (40 puan) gurultuydu. Yeni terim -5..+6.
//
// 2) TEST EDILMIS YATAY DESTEK (EQL) — ret6 kontrollu bile KALAN tek isaret:
//      EQL 1,481 (n=270, kaz %85,9) | tek dokunuslu dip 0,945 | altta hic yok 0,654
//    Bu 270 kurulumun 49'u ESKI skorla 40 esiginin ALTINDA kaliyordu.
//
// 3) ELENEN ADAYLAR (durustluk icin): destege UZAKLIK ve dip supurme ham veride
//    guclu, ret6 kontrolunde SIFIR -> skora konmadi.
//
// SIMULASYON (36.918 nokta, V680 dahil, esik 40):
//    eski: gecen %76,4 beklenti 0,996 | en iyi %25 -> 1,182 (kaz %78,2)
//    yeni: gecen %82,9 beklenti 1,035 | en iyi %25 -> 1,436 (kaz %85,2)

function yukle() {
  const isim = ['V680_PARABOLIK_CEZA','V680_RET6_TABAN','V680_CEZA_TAVAN','V680_CEP_RET6','V680_CEP_CEZA',
                'V686_TAM_RR_SKORA','V686_RR_ESIK','V686_RR_CEZA_TAVAN',
                'V688_ARALIK_DUZELT','V688_DESTEK_OKUMA','V688_EQL_PUAN','V688_DESTEKSIZ_CEZA'];
  const satir = isim.map(n => {
    const m = server.match(new RegExp('^const\\s+' + n + '\\s*=.*$', 'm'));
    assert.ok(m, n + ' sabiti olmali');
    return m[0];
  });
  const i = server.indexOf('function v688AralikPuani');
  assert.ok(i > 0, 'v688AralikPuani bulunmali');
  const son = server.indexOf('\nfunction r486EntryTruthGuard', i + 10);
  const ctx = { process: { env: {} }, Math, Number, String, Object };
  vm.createContext(ctx);
  vm.runInContext(satir.join('\n') + '\n' + server.slice(i, son) +
    '\nglobalThis.f = v678GrafikKalitesi; globalThis.rpf = v688AralikPuani;', ctx);
  return ctx;
}

const hik = (rp, opt = {}) => ({
  tf: { '15m': { rangePos: rp, ret6: opt.ret6 ?? 0, trend: opt.trend || 'RANGE_MIXED', trendline: {} } },
  ...(opt.liq === null ? {} : { liquidity: opt.liq || { below: 100, belowType: 'SWING_LOW', belowDist: 2.0 } })
});

test('V688: ters aralik terimi KODDAN kalkti', () => {
  const i = server.indexOf('function v678GrafikKalitesi(');
  const fn = server.slice(i, server.indexOf('function r486EntryTruthGuard(', i));
  assert.ok(!/s \+= \(rp-0\.5\)\*40;/.test(fn), 'duz (rp-0,5)*40 terimi kalmamali');
  assert.ok(fn.includes('v688AralikPuani(rp)'), 'olculen bant tablosu kullanilmali');
  assert.ok(fn.includes('v688DestekOkumasi(story,a)'), 'destek okunmali');
});

test('V688: aralik puani olculen bantlarda ve KUCUK', () => {
  const { rpf } = yukle();
  assert.strictEqual(rpf(0.05), -5);
  assert.strictEqual(rpf(0.20), 1);
  assert.strictEqual(rpf(0.35), 3);
  assert.strictEqual(rpf(0.50), 2);
  assert.strictEqual(rpf(0.65), 0);
  assert.strictEqual(rpf(0.80), -2);
  assert.strictEqual(rpf(0.90), -1);
  assert.strictEqual(rpf(0.99), 6);
  for (let x = 0; x <= 1.0001; x += 0.01) assert.ok(Math.abs(rpf(x)) <= 6, 'terim ±6 icinde kalmali');
});

test('V688: DIPTE test edilmis destek artik ELENMIYOR — kullanicinin cizdigi kurulum', () => {
  const { f } = yukle();
  // EGLD 26 Agu: 4,7966 destegi 3 kez test edildi, oradan %12,68 hareket.
  const dipte = f(hik(0.12, { liq: { below: 4.7966, belowType: 'EQL', belowDist: 0.9 } }), 2.0);
  const eskiTerim = (0.12 - 0.5) * 40;          // -15,2
  assert.ok(dipte.skor >= 55, `destek uzeri skor ${dipte.skor} — esigin cok ustunde olmali`);
  assert.strictEqual(dipte.destek.tip, 'EQL');
  assert.strictEqual(dipte.destek.puan, 14);
  assert.ok(dipte.aralikPuani > eskiTerim, 'eski terim bu adayi cezalandiriyordu');
});

test('V688: TEPEDE, altinda destek olmayan aday geriliyor (botun EGLD girisi)', () => {
  const { f } = yukle();
  const tepede = f(hik(0.90, { ret6: 6, liq: { below: null, belowType: null, belowDist: null } }), 2.0);
  const dipte  = f(hik(0.12, { liq: { below: 4.7966, belowType: 'EQL', belowDist: 0.9 } }), 2.0);
  assert.ok(dipte.skor > tepede.skor + 20,
    `dip ${dipte.skor} tepeden ${tepede.skor} en az 20 puan yukarida olmali`);
  assert.strictEqual(tepede.destek.puan, -8, 'altta destek yoksa -8');
});

test('V688: BAKMADIYSAK cezalandirmiyoruz (liquidity dugumu yok)', () => {
  const { f } = yukle();
  const k = f(hik(0.50, { liq: null }), 2.0);
  assert.strictEqual(k.destek, null, 'veri yoksa terim devre disi');
  assert.ok(k.ok, 'skor yine de uretilmeli');
});

test('V688: dikey uzama cezasi (V680) YERINDE — tepe riskini o tasiyor', () => {
  const { f } = yukle();
  const yavas = f(hik(0.957, { ret6: 1.5 }), 2.0);
  const hizli = f(hik(0.957, { ret6: 28 }), 3.81);
  assert.strictEqual(yavas.parabolikCeza, 0, 'yavas gelinen tepe cezalanmaz');
  assert.ok(hizli.parabolikCeza >= 25, 'dikey gelinen tepe sert cezalanir');
  assert.ok(hizli.skor < 40 && yavas.skor >= 50, `${hizli.skor} elenmeli, ${yavas.skor} kalmali`);
});

test('V688: elenen adaylar skorda YOK (durustluk kontrolu)', () => {
  const i = server.indexOf('function v688DestekOkumasi');
  const fn = server.slice(i, server.indexOf('function v678GrafikKalitesi(', i));
  assert.ok(!/puan\s*[+\-]=.*dATR/.test(fn), 'uzaklik puana girmemeli (ret6 kontrolunde sifirlandi)');
  assert.ok(!/puan\s*[+\-]=.*sweep/.test(fn), 'sweep puana girmemeli');
  assert.ok(fn.includes('uzaklikAtr'), 'yine de RAPORLANMALI');
});
