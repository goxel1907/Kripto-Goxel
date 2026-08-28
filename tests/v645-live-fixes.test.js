const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const server = fs.readFileSync(path.join(__dirname, '..', 'server.js'), 'utf8');

function extractFunction(name, nextName) {
  const start = server.indexOf(`function ${name}(`);
  const end = server.indexOf(`function ${nextName}(`, start);
  assert.ok(start >= 0 && end > start, `${name} kaynakta bulunmalı`);
  return server.slice(start, end);
}

test('late exhaustion guard blocks only the composite DEXE-like late entry', () => {
  const context = {};
  vm.createContext(context);
  vm.runInContext(`${extractFunction('v645LateExhaustionGate','r48633StoryAuthority')}\nthis.gate=v645LateExhaustionGate;`, context);

  const late = context.gate({
    stage:'EXHAUSTION', mmScenario:'POST_IMPULSE_REJECTION_WAIT_RECLAIM', topRisk:9.9,
    structuralTopEvidence:3, sourceRsi4h:82, targetRejection:false, parabolicChase:true,
    microConsensus:{postImpulseReject:true,confirmedPullback:false,confirmedContinuation:false}
  }, {tier:'WAIT',brainConfidence:4,score:9}, {side:'LONG',proof:false,retestProof:false});
  assert.equal(late.active, true);
  assert.equal(late.code, 'V645_LATE_EXHAUSTION_RECLAIM');
  assert.equal(late.lateSignals, 4);

  const cleanContinuation = context.gate({
    stage:'MARKUP', mmScenario:'MICRO_CONSENSUS_MARKUP', topRisk:3.4,
    structuralTopEvidence:1, sourceRsi4h:65,
    microConsensus:{postImpulseReject:false,confirmedContinuation:true}
  }, {tier:'B+',brainConfidence:67}, {side:'LONG',proof:true,retestProof:false});
  assert.equal(cleanContinuation.active, false);

  const loneOverbought = context.gate({
    stage:'MARKUP', mmScenario:'MARKUP_WITH_NEW_LONGS', topRisk:5.2,
    structuralTopEvidence:1, sourceRsi4h:81,
    microConsensus:{postImpulseReject:false}
  }, {tier:'WAIT',brainConfidence:20}, {side:'LONG',proof:false,retestProof:false});
  assert.equal(loneOverbought.active, false, 'tek RSI/WAIT sinyali botu boğmamalı');
});

test('hard late-timing PUSU cannot be delegated back to MARKET by V637', () => {
  assert.match(server, /r447Authority\.hardTimingVeto===true/);
  assert.match(server, /authority\.hardTimingVeto===true/);
  assert.match(server, /code:lateExhaustion\.code/);
});

test('R428 entry photo uses the real trailing state and no undefined positionState', () => {
  assert.doesNotMatch(server, /positionState\[full\]/);
  assert.match(server, /let r428GirisFoto=null;/);
  assert.match(server, /girisFoto:\s*r428GirisFoto/);
});
