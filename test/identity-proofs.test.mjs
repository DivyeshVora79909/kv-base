import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID, hkdfSync, createHmac } from 'node:crypto';
import { fork } from 'node:child_process';
import { proofDefinition } from './fixtures/proof-domain.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { prepareCatalog } from '../src/catalog.mjs';
import { createEngine } from '../src/lifecycle.mjs';
import { createKeyring } from '../src/keyring.mjs';
import { createIdentity } from '../src/identity.mjs';
import { createIdentityProofs } from '../src/identity-proofs.mjs';
import { canonical } from '../src/values.mjs';
import { sourcePrefix } from '../src/keys.mjs';
import { createCredentialCustody, createEffectWorker } from '../src/effects.mjs';
import { createFakeProviderServer, createFakeEmailClient, createFakeSmsClient } from '../src/providers.mjs';

const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE, skip = clusterFile ? false : 'KV_BASE_FDB_CLUSTER_FILE must select a real test cluster';
const password = 'long reader password value', adminPassword = 'long administrator password value';
function graph() {
  const node = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  return { tenant: 't', principals: [node('t', 'tenant', []), node('admin', 'user', ['t'], { Credential: ['use'] }, true), node('visible', 'group', ['admin'], { Document: ['read', 'create', 'update', 'delete'] }), node('reader', 'user', ['visible'])] };
}
async function fixture(t, { afterAccepted, proofPolicy = {} } = {}) {
  const prefix = `proof-${randomUUID()}`, providerPrefix = `proof-provider-${randomUUID()}`;
  const definition = proofDefinition();
  const f = { prefix, time: 1000, fence: 'incarnation-A', activeKey: 'old', keys: { old: Buffer.alloc(32, 31), next: Buffer.alloc(32, 32) }, contacts: { reader: { email: 'reader@example.test', phone: '+919876543210' } }, catalog: prepareCatalog(definition) };
  const clock = () => f.time, deliveryContext = { tenant: 't', actor: 'admin' }, providerStore = await openFoundationDbStore({ clusterFile, prefix: providerPrefix });
  const emailServer = await createFakeProviderServer({ protocol: 'resend', store: providerStore, secret: 'email-provider-secret', afterAccepted }), smsServer = await createFakeProviderServer({ protocol: 'brevo', store: providerStore, secret: 'sms-provider-secret', afterAccepted });
  f.providers = { email: createFakeEmailClient({ protocol: 'resend', baseUrl: emailServer.baseUrl }), sms: createFakeSmsClient({ protocol: 'brevo', baseUrl: smsServer.baseUrl }) };
  f.channels = { email: { provider: 'email', credentialId: 'email-credential', credentialRevision: '1', from: 'signin@example.test' }, phone: { provider: 'sms', credentialId: 'sms-credential', credentialRevision: '1' } };
  f.openProofs = () => { f.proofs = createIdentityProofs({ engine: f.engine, identity: f.identity, keyring: f.keyring, getRestoreFence: () => f.fence, custody: f.custody, providers: f.providers, deliveryContext, contacts: f.contacts, channels: f.channels, clock, proofPolicy: { expiryMs: 1000, maxAttempts: 3, maxStarts: 3, startWindowMs: 100, ...proofPolicy } }); f.worker = createEffectWorker({ engine: f.engine, custody: f.custody, providers: f.providers, clock, leaseMs: 10, apply: f.proofs.applyDelivery }); };
  f.open = async () => {
    f.store = await openFoundationDbStore({ clusterFile, prefix }); f.engine = createEngine({ store: f.store, catalog: f.catalog, tenant: 't' }); f.keyring = createKeyring({ keys: f.keys, activeKey: f.activeKey });
    f.identity = createIdentity({ engine: f.engine, keyring: f.keyring, getRestoreFence: () => f.fence, clock, passwordPolicy: { N: 1024, sessionMs: 100000 } });
    f.custody = createCredentialCustody({ catalog: f.catalog, keyring: f.keyring, getAuthority: tx => f.identity.getAuthorityInTransaction(tx), clock }); f.openProofs();
  };
  await f.open(); await f.identity.initialize({ policy: graph(), credentials: [{ actor: 'admin', password: adminPassword }, { actor: 'reader', password }] });
  await f.engine.transact(async tx => { await f.custody.storeRevision(tx, deliveryContext, { id: 'email-credential', provider: 'email', secret: 'email-provider-secret' }); await f.custody.storeRevision(tx, deliveryContext, { id: 'sms-credential', provider: 'sms', secret: 'sms-provider-secret' }); });
  const { default: fdb } = await import('foundationdb'), db = fdb.open(clusterFile), root = fdb.tuple.pack(['kv-base', prefix]), end = Buffer.from(root); end[end.length - 1]++;
  f.capture = async () => { const tx = db.rawCreateTransaction({ timeout: 4500 }); try { return (await tx.getRangeAll(root, end)).map(([k, v]) => [Buffer.from(k), Buffer.from(v)]); } finally { tx.rawCancel(); } };
  f.restore = async snapshot => { const tx = db.rawCreateTransaction({ timeout: 4500 }); try { tx.clearRange(root, end); for (const [key, value] of snapshot) tx.set(key, value); await tx.rawCommit(); } finally { tx.rawCancel(); } };
  f.rows = () => f.engine.read(tx => tx.scan()); f.sourceRows = () => f.engine.read(tx => tx.scan({ prefix: sourcePrefix('t') }));
  f.proof = proofId => f.engine.read(tx => tx.get(`identity-proof/v1/${canonical('t')}/proof/${canonical(proofId)}`));
  f.delivery = async begun => {
    const proof = await f.proof(begun.proofId); await f.worker.runOne(proof.effectId);
    const ledger = proof.channel === 'email' ? await emailServer.ledger() : await smsServer.ledger(), entry = ledger.find(row => row.value.operationId === proof.providerOperationId);
    assert.ok(entry, 'delivery must be independently present at external provider'); const match = /code is (\d{8})\./.exec(entry.value.request.text); assert.ok(match); return match[1];
  };
  f.stats = () => ({ email: emailServer.stats(), sms: smsServer.stats() });
  t.after(async () => { await emailServer.close(); await smsServer.close(); await f.store.close(); await providerStore.close(); const tx = db.rawCreateTransaction({ timeout: 4500 }); try { tx.clearRange(root, end); const providerRoot = fdb.tuple.pack(['kv-base', providerPrefix]), providerEnd = Buffer.from(providerRoot); providerEnd[providerEnd.length - 1]++; tx.clearRange(providerRoot, providerEnd); await tx.rawCommit(); } finally { tx.rawCancel(); db.close(); } });
  return f;
}
const begin = (f, operationId = 'begin', channel = 'email') => f.proofs.begin({ actor: 'reader', channel, operationId });
const complete = (f, begun, code, operationId = 'complete', hooks) => f.proofs.complete({ proofId: begun.proofId, code, operationId }, hooks);
const sessions = async f => (await f.rows()).filter(row => row.key.includes('/session/'));
function barrier() { let enter, release; return { entered: new Promise(resolve => { enter = resolve; }), blocked: new Promise(resolve => { release = resolve; }), enter: () => enter(), release: () => release() }; }

test('primary FoundationDB D2b: email and E164 phone proof are independently delivered and never returned in admission or durable plaintext', { skip }, async t => {
  const f = await fixture(t);
  for (const channel of ['email', 'phone']) {
    const begun = await begin(f, channel, channel); assert.deepEqual(Object.keys(begun), ['proofId', 'expiresAt']);
    assert.equal((await f.proof(begun.proofId)).delivery, 'pending'); await assert.rejects(complete(f, begun, '00000000'), { code: 'UNAUTHENTICATED' });
    const code = await f.delivery(begun), privateProof = await f.proof(begun.proofId);
    const verifierBinding = { tenant: 't', purpose: 'proof-verifier', subject: begun.proofId, revision: '1', fence: f.fence };
    const verifierKey = Buffer.from(hkdfSync('sha256', f.keys.old, Buffer.from('kv-base custody v1'), Buffer.from(canonical(['t', 'proof-verifier'])), 32));
    const expectedVerifier = 'old:' + createHmac('sha256', verifierKey).update(canonical([verifierBinding, code])).digest('base64url');
    assert.equal(privateProof.digest, expectedVerifier); verifierKey.fill(0);
    const issued = await complete(f, begun, code, `complete-${channel}`); assert.equal((await f.proof(begun.proofId)).state, 'consumed');
    assert.equal((await f.identity.read(issued.token, (_tx, context) => context)).actor, 'reader');
    const logical = canonical(await f.rows()), physical = Buffer.concat((await f.capture()).flat());
    for (const secret of [code, issued.token, password, 'email-provider-secret', 'sms-provider-secret']) { assert.equal(logical.includes(secret), false); assert.equal(physical.includes(Buffer.from(secret)), false); }
  }
  assert.equal(f.stats().email.starts, 1); assert.equal(f.stats().sms.starts, 1); await f.engine.check();
});

test('primary FoundationDB D2b: acceptance and original receipt roll back together and lost admission response never creates another effect', { skip }, async t => {
  const f = await fixture(t), before = await f.rows();
  await assert.rejects(f.proofs.begin({ actor: 'reader', channel: 'email', operationId: 'aborted' }, { beforeCommit() { throw Error('abort'); } }), /abort/); assert.deepEqual(await f.rows(), before);
  await assert.rejects(f.proofs.begin({ actor: 'reader', channel: 'email', operationId: 'lost-begin' }, { afterCommit() { throw Error('lost reply'); } }), { code: 'COMMIT_UNKNOWN' });
  const begun = await begin(f, 'lost-begin'); await f.store.close(); await f.open(); assert.deepEqual(await begin(f, 'lost-begin'), begun);
  assert.equal((await f.sourceRows()).filter(row => row.value.model === 'Effect').length, 1); assert.equal((await f.rows()).find(row => row.key.includes('/rate/')).value.count, 1);
  await assert.rejects(begin(f, 'lost-begin', 'phone'), { code: 'RECEIPT_MISMATCH' }); await f.engine.check();
});

test('primary FoundationDB D2b: absent contacts unknown actors malformed controls and durable admission limits allocate no extra proofs', { skip }, async t => {
  const f = await fixture(t), before = await f.rows();
  await assert.rejects(f.proofs.begin({ tenant: 'other', actor: 'reader', channel: 'email', operationId: 'wire-tenant' }), { code: 'INVALID_PROOF' });
  for (const actor of ['unknown', 'admin']) await assert.rejects(f.proofs.begin({ actor, channel: 'email', operationId: actor }), { code: 'UNAUTHENTICATED' });
  let touched = false; const input = { actor: 'reader', channel: 'email' }; Object.defineProperty(input, 'operationId', { enumerable: true, get() { touched = true; return 'getter'; } }); await assert.rejects(f.proofs.begin(input), { code: 'INVALID_PROOF' }); assert.equal(touched, false); assert.deepEqual(await f.rows(), before);
  for (const operationId of ['one', 'two', 'three']) await begin(f, operationId); await f.store.close(); await f.open(); await assert.rejects(begin(f, 'four'), { code: 'AUTH_WORK_LIMIT' });
  assert.equal((await f.sourceRows()).filter(row => row.value.model === 'Effect').length, 3); f.time += 101; assert.ok((await begin(f, 'after-window')).proofId);
});

test('primary FoundationDB D2b: each wrong delivered proof operation consumes one bounded attempt and locks before new receipt admission', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), wrong = code === '00000000' ? '11111111' : '00000000';
  for (let n = 0; n < 2; n++) await assert.rejects(complete(f, begun, wrong, 'wrong-one'), { code: 'UNAUTHENTICATED' }); assert.equal((await f.proof(begun.proofId)).attempts, 1);
  for (const operationId of ['wrong-two', 'wrong-three']) await assert.rejects(complete(f, begun, wrong, operationId), { code: 'UNAUTHENTICATED' });
  const before = await f.capture(); await f.store.close(); await f.open(); await assert.rejects(complete(f, begun, code, 'correct-after-lock'), { code: 'UNAUTHENTICATED' }); assert.deepEqual(await f.capture(), before);
  assert.equal((await f.proof(begun.proofId)).state, 'locked'); assert.equal((await sessions(f)).length, 0);
});

test('primary FoundationDB D2b: consumed proof yields one original encrypted session result after lost reply reopen and proof deadline', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun);
  await assert.rejects(complete(f, begun, code, 'lost-completion', { afterCommit() { throw Error('lost reply'); } }), { code: 'COMMIT_UNKNOWN' });
  await f.store.close(); await f.open(); f.time = Number(begun.expiresAt) + 1;
  const original = await complete(f, begun, code, 'lost-completion'); assert.equal((await sessions(f)).length, 1); assert.equal((await f.identity.read(original.token, (_tx, context) => context)).actor, 'reader');
  await assert.rejects(complete(f, begun, code, 'different-operation'), { code: 'UNAUTHENTICATED' }); await assert.rejects(complete(f, begun, code === '00000000' ? '11111111' : '00000000', 'lost-completion'), { code: 'RECEIPT_MISMATCH' });
});

test('primary FoundationDB D2b: expiry current actor credential and contact changes each invalidate delivered proof', { skip }, async t => {
  const f = await fixture(t), expired = await begin(f, 'expired'), expiredCode = await f.delivery(expired); f.time = Number(expired.expiresAt); await assert.rejects(complete(f, expired, expiredCode), { code: 'UNAUTHENTICATED' });
  f.time = 3000; const changed = await begin(f, 'changed'), changedCode = await f.delivery(changed), session = await f.identity.login({ actor: 'reader', password, operationId: 'password-session' });
  await f.identity.changePassword(session.token, { oldPassword: password, newPassword: 'new reader password value', operationId: 'rotate-reader' }); await assert.rejects(complete(f, changed, changedCode), { code: 'UNAUTHENTICATED' });
  const moved = await begin(f, 'moved'), movedCode = await f.delivery(moved); f.contacts.reader.email = 'changed@example.test'; f.openProofs(); await assert.rejects(complete(f, moved, movedCode), { code: 'UNAUTHENTICATED' });
  assert.equal((await sessions(f)).length, 1);
});

test('primary FoundationDB D2b: revocation and external restore fence reject old proofs and bearer results while fresh delivery remains usable', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f, 'restored'), code = await f.delivery(begun), backup = await f.capture();
  const revoked = graph(); revoked.principals.find(node => node.id === 'reader').active = false; await f.identity.replacePolicy(revoked); await assert.rejects(complete(f, begun, code), { code: 'UNAUTHENTICATED' });
  await f.store.close(); await f.restore(backup); f.fence = 'incarnation-B'; await f.open(); await assert.rejects(complete(f, begun, code), { code: 'UNAUTHENTICATED' });
  const fresh = await begin(f, 'after-restore'), freshCode = await f.delivery(fresh), issued = await complete(f, fresh, freshCode); assert.equal((await f.identity.read(issued.token, (_tx, context) => context)).actor, 'reader'); await f.engine.check();
});

test('primary FoundationDB D2b: retained custody rotation validates pinned request proof verifier and exact original receipt', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun); await f.store.close(); f.activeKey = 'next'; await f.open();
  assert.deepEqual(await begin(f), begun); const issued = await complete(f, begun, code); assert.ok(issued.token.startsWith('s1.next.')); assert.deepEqual(await complete(f, begun, code), issued); await f.engine.check();
});

test('primary FoundationDB D2b: accepted external delivery with lost response stays unknown until original-key reconciliation', { skip }, async t => {
  let dropped = false; const f = await fixture(t, { afterAccepted: ({ response }) => { if (!dropped) { dropped = true; response.destroy(); } } }), begun = await begin(f), proof = await f.proof(begun.proofId);
  await f.worker.runOne(proof.effectId); assert.equal((await f.proof(begun.proofId)).delivery, 'pending'); await assert.rejects(complete(f, begun, '00000000'), { code: 'UNAUTHENTICATED' });
  await f.worker.reconcile(proof.effectId); assert.equal((await f.proof(begun.proofId)).delivery, 'delivered'); assert.equal(f.stats().email.starts, 1);
  const code = await f.delivery(begun); assert.ok((await complete(f, begun, code)).token); assert.equal(f.stats().email.starts, 1); await f.engine.check();
});

test('primary FoundationDB D2b: concurrent correct completions commit one session and recover the original winner', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), gate = barrier();
  const loser = complete(f, begun, code, 'same-completion', { beforeCommit: async () => { gate.enter(); await gate.blocked; } }), failed = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const winner = await complete(f, begun, code, 'same-completion'); gate.release(); await failed;
  assert.deepEqual(await complete(f, begun, code, 'same-completion'), winner); assert.equal((await sessions(f)).length, 1); await f.engine.check();
});

test('primary FoundationDB D2b: concurrent final revocation and incarnation rotation roll back proof consumption session and receipt', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), gate = barrier();
  const loser = complete(f, begun, code, 'revoke-completion', { beforeCommit: async () => { gate.enter(); await gate.blocked; } }), failed = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const revoked = graph(); revoked.principals.find(node => node.id === 'reader').active = false; await f.identity.replacePolicy(revoked); gate.release(); await failed;
  assert.equal((await f.proof(begun.proofId)).state, 'open'); assert.equal((await sessions(f)).length, 0); await f.identity.replacePolicy(graph());
  await assert.rejects(complete(f, begun, code, 'fence-completion', { beforeCommit: () => { f.fence = 'incarnation-B'; } }), { code: 'RESTORE_FENCED' });
  assert.equal((await f.proof(begun.proofId)).state, 'open'); assert.equal((await sessions(f)).length, 0);
});

test('primary FoundationDB D2b: different concurrent operation identities cannot both consume one delivered proof', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), gate = barrier();
  const loser = complete(f, begun, code, 'loser-identity', { beforeCommit: async () => { gate.enter(); await gate.blocked; } }), failed = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const winner = await complete(f, begun, code, 'winner-identity'); gate.release(); await failed;
  await assert.rejects(complete(f, begun, code, 'loser-identity'), { code: 'UNAUTHENTICATED' }); assert.deepEqual(await complete(f, begun, code, 'winner-identity'), winner); assert.equal((await sessions(f)).length, 1);
  await f.engine.check();
});

test('primary FoundationDB D2b: restored successful proof receipts cannot release a pre-restore bearer', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), issued = await complete(f, begun, code), backup = await f.capture();
  await f.identity.logout(issued.token, { operationId: 'before-restore' }); await f.store.close(); await f.restore(backup); f.fence = 'incarnation-B'; await f.open();
  await assert.rejects(f.identity.read(issued.token, () => true), { code: 'UNAUTHENTICATED' }); await assert.rejects(complete(f, begun, code), { code: 'UNAUTHENTICATED' });
  const fresh = await f.identity.login({ actor: 'reader', password, operationId: 'fresh-password-after-restore' }); assert.equal((await f.identity.read(fresh.token, (_tx, context) => context)).actor, 'reader'); await f.engine.check();
});

test('primary FoundationDB D2b: guesses against an original successful receipt consume bounded durable recovery attempts once', { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun), issued = await complete(f, begun, code), candidates = ['00000000', '11111111', '22222222', '33333333'].filter(value => value !== code);
  for (let n = 0; n < 2; n++) await assert.rejects(complete(f, begun, candidates[0]), { code: 'RECEIPT_MISMATCH' }); assert.equal((await f.proof(begun.proofId)).attempts, 1);
  for (const guess of candidates.slice(1, 3)) await assert.rejects(complete(f, begun, guess), { code: 'RECEIPT_MISMATCH' });
  await f.store.close(); await f.open(); const before = await f.capture();
  await assert.rejects(complete(f, begun, candidates[3] ?? '99999999'), { code: 'UNAUTHENTICATED' }); await assert.rejects(complete(f, begun, code), { code: 'UNAUTHENTICATED' }); assert.deepEqual(await f.capture(), before);
  assert.equal((await sessions(f)).length, 1); assert.equal((await f.identity.read(issued.token, (_tx, context) => context)).actor, 'reader');
});

for (const phase of ['beforeCommit', 'afterCommit']) test(`primary FoundationDB D2b: real child SIGKILL ${phase} retains atomic one-use proof session and original receipt`, { skip }, async t => {
  const f = await fixture(t), begun = await begin(f), code = await f.delivery(begun);
  const child = fork(new URL('./fixtures/proof-process.mjs', import.meta.url), [clusterFile, f.prefix, begun.proofId, code, phase, String(f.time), f.fence], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); });
  await new Promise((resolve, reject) => { const timer = setTimeout(() => reject(Error('Proof child hook timeout')), 15000); child.once('error', reject); child.once('message', message => { clearTimeout(timer); if (message.error) reject(Error(message.error)); else { assert.equal(message.phase, phase); resolve(); } }); child.once('exit', status => { clearTimeout(timer); reject(Error(`Proof child exited early ${status}`)); }); });
  const stopped = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGKILL'); await stopped;
  await f.store.close(); await f.open(); assert.equal((await f.proof(begun.proofId)).state, phase === 'beforeCommit' ? 'open' : 'consumed'); assert.equal((await sessions(f)).length, phase === 'beforeCommit' ? 0 : 1);
  const issued = await complete(f, begun, code, 'crash-' + phase); assert.equal((await sessions(f)).length, 1); assert.deepEqual(await complete(f, begun, code, 'crash-' + phase), issued); assert.equal(f.stats().email.starts, 1); await f.engine.check();
});
