const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const serverPath = path.join(__dirname, '..', 'server.js');
const source = fs.readFileSync(serverPath, 'utf8');

test('R495 sizing reads the persisted V4.5 snapshot, not a block-scoped variable', () => {
  assert.doesNotMatch(source, /\?\?\s*_v45\?\.features\?\.entryDriftAtr/);
  assert.match(source, /const _v45Sizing = decisionChain\?\.v45MultiSource/);
  assert.match(source, /_v45Sizing\?\.features\?\.entryDriftAtr/);
});

test('R495 final sizing never reads the block-scoped ai variable', () => {
  const start = source.indexOf('// ═══ R495 FINAL RISK AUTHORITY');
  const end = source.indexOf('// ═══ R437 GİRİŞ KOVALAMA KAPISI', start);
  assert.ok(start >= 0 && end > start, 'R495 final sizing block must exist');
  const sizingBlock = source.slice(start, end);
  assert.match(sizingBlock, /const _r495Ai = decisionChain\?\.aiBrain \|\| \{\};/);
  assert.doesNotMatch(sizingBlock, /(^|[^\w])ai\?\./m);
  assert.match(sizingBlock, /_r495Ai\?\.story\?\.distanceFromBreakoutAtr/);
});

test('R495 sizing failures preserve the actual error in evidence', () => {
  assert.match(source, /R495 final sizing hata: \$\{_r495SizeErr\}/);
});
