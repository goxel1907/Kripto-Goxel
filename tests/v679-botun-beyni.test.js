'use strict';
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');
const panel  = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');

// NEDEN: bu makine fapi.binance.com'a 451 (coğrafi blok) alıyor; bot almıyor.
// Çözüm canlı veriyi BOTTAN çekmek — üstelik yeniden hesap yok, botun karar
// anında GÖRDÜĞÜ okuma saklanıp dışa veriliyor.
//
// İKİNCİ SORUN: araya giren HTTP önbelleği her yolu ilk çekilişinde donduruyor
// (r481-status bir haftalık, evidence/status 7 saatlik veri döndürdü; sorgu
// parametresi değiştirmek işe yaramadı). Bu yüzden uç DEĞİŞKEN SEGMENT alıyor:
// /api/v679/beyin/:nonce — her çağrı ayrı yol, her çağrı taze.

test('V679: halka tampon karara etki etmiyor', () => {
  assert.match(server, /const V679_BEYIN_KAPASITE = Math\.max\(20, Math\.min\(400, Number\(process\.env\.V679_BEYIN_KAPASITE \|\| 200\)\)\);/);
  const i = server.indexOf('function v679Kaydet(');
  assert.ok(i > 0);
  const fn = server.slice(i, i + 420);
  assert.ok(fn.includes('v679Beyin.push'), 'kayıt eklemeli');
  assert.ok(fn.includes('splice(0, v679Beyin.length-V679_BEYIN_KAPASITE)'), 'kapasiteyi aşmamalı');
  assert.ok(fn.includes('catch(_){}'), 'fail-open olmalı');
  for (const yasak of ['markAutoSkip','continue;','closePosition','sendOrder'])
    assert.ok(!fn.includes(yasak), `kaydedici ${yasak} içermemeli`);
});

test('V679: uç DEĞİŞKEN segment alıyor (önbelleği devre dışı bırakır)', () => {
  assert.match(server, /app\.get\('\/api\/v679\/beyin\/:nonce'/);
  const i = server.indexOf("app.get('/api/v679/beyin/:nonce'");
  const blok = server.slice(i, i + 1100);
  assert.ok(blok.includes("no-store"), 'no-store başlığı olmalı');
  assert.ok(blok.includes('ozet:{gecen'), 'özet dönmeli');
  assert.ok(blok.includes('kayitlar:rows'), 'kayıtları dönmeli');
});

test('V679: hikâye yalnız mevcut dedektör çıktılarından üretiliyor', () => {
  const i = server.indexOf('function v679Hikaye(');
  assert.ok(i > 0);
  const fn = server.slice(i, server.indexOf('// ═══ V678 ═══ Grafik kalite kapisi', i));
  for (const yol of ["story?.tf?.['15m']", "t.trendline", "story?.liquidity?.['15m']",
                     "story?.orderBlock?.['15m']", "tl.retestUp", "tl.falseBreakUp"])
    assert.ok(fn.includes(yol), `${yol} okunmalı`);
  // olculen sayilar cumlelere gomulu olmali ki kullanici neyin neden onemli oldugunu gorsun
  assert.ok(fn.includes('+0,630'), 'kırılım+retest beklentisi yazılmalı');
  assert.ok(fn.includes('-0,071'), 'negatif bant beklentisi yazılmalı');
  assert.ok(fn.includes('catch(_){}'), 'fail-open olmalı');
});

test('V679: hem geçen hem ELENEN aday kaydediliyor', () => {
  const i = server.indexOf('v679Kaydet({symbol:coin.symbol');
  assert.ok(i > 0, 'kayıt çağrısı bulunmalı');
  const blok = server.slice(i, i + 900);
  assert.ok(blok.includes('gecti:!(_k678&&_k678.ok&&_k678.skor<V678_MIN_SKOR)'), 'geçti/elendi ayrımı');
  // cagri, veto if'inden ONCE olmali; yoksa elenenler hic kaydedilmez
  assert.ok(i < server.indexOf('if (_k678 && _k678.ok && _k678.skor < V678_MIN_SKOR) {'),
    'kayıt, veto dalından önce olmalı');
});

test('V679: panel kartı nonce kullanıyor ve XSS kaçışı yapıyor', () => {
  assert.ok(panel.includes("railGet('/api/v679/beyin/'+Date.now()"), 'nonce ile çağırmalı');
  assert.ok(panel.includes('function v679Esc('), 'kaçış fonksiyonu olmalı');
  assert.ok(/v679Esc\(k\.symbol\)/.test(panel), 'sembol kaçırılmalı');
  assert.ok(/v679Esc\(c\)/.test(panel), 'hikâye satırları kaçırılmalı');
  assert.strictEqual((panel.match(/<script/g)||[]).length, (panel.match(/<\/script>/g)||[]).length);
  assert.strictEqual((panel.match(/<div/g)||[]).length, (panel.match(/<\/div>/g)||[]).length);
});

test('V679: build etiketi package.json sürümüyle uyuşuyor', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
  const onek = 'V' + pkg.version.split('.').join('_');
  const build = server.match(/const LAZARUS_BUILD = '([^']+)'/);
  assert.ok(build[1].startsWith(onek), `${build[1]} != ${onek}*`);
});
