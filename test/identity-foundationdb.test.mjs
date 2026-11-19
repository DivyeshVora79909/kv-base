import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { mutateAuthorizedInTransaction } from '../src/authorized-mutate.mjs';
import { prepareIdentityFixture, registerIdentityConformance, login, rows, rawSources, graph, sources, password, adminPassword } from './helpers/identity-conformance.mjs';

const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE;
const skip = clusterFile ? false : 'KV_BASE_FDB_CLUSTER_FILE must select a real test cluster';
async function fixture(t, options = {}) {
  const prefix = `identity-${randomUUID()}`;
  let db, root, end;
  async function native() {
    if (!db) {
      const { default: fdb } = await import('foundationdb'); db = fdb.open(clusterFile); root = fdb.tuple.pack(['kv-base', prefix]);
      end = Buffer.from(root); for (let i = end.length - 1; i >= 0; i--) if (end[i] < 255) { end[i]++; end = end.subarray(0, i + 1); break; }
    }
    return db.rawCreateTransaction({ timeout: 4500 });
  }
  async function capture() {
    const tx = await native(); try { return (await tx.getRangeAll(root, end)).map(([key, value]) => [Buffer.from(key), Buffer.from(value)]); } finally { tx.rawCancel(); }
  }
  return prepareIdentityFixture(t, options, {
    prefix,
    open: () => openFoundationDbStore({ clusterFile, prefix }),
    physical: async () => Buffer.concat((await capture()).flat()),
    capture,
    restore: async snapshot => { const tx = await native(); try { tx.clearRange(root, end); for (const [key, value] of snapshot) tx.set(key, value); await tx.rawCommit(); } finally { tx.rawCancel(); } },
    cleanup: async () => { const tx = await native(); try { tx.clearRange(root, end); await tx.rawCommit(); } finally { tx.rawCancel(); db.close(); } },
  });
}
if (clusterFile) registerIdentityConformance('primary FoundationDB', fixture);
else test('primary FoundationDB identity conformance requires selected real cluster', { skip }, () => {});
function barrier() { let enter, release; return { entered: new Promise(resolve => { enter = resolve; }), blocked: new Promise(resolve => { release = resolve; }), enter: () => enter(), release: () => release() }; }

test('primary FoundationDB identity: simultaneous original login identity commits one session and recovers the exact winner', { skip }, async t => {
  const f = await fixture(t), gate = barrier(); let calls = 0;
  const loser = loginWithHook();
  const failed = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const winner = await login(f, 'same-operation'); gate.release(); await failed;
  assert.deepEqual(await login(f, 'same-operation'), winner);
  assert.equal((await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length, 1);
  assert.equal(calls, 1); assert.deepEqual((await rows(f, winner.token)).records.map(row => row.id), ['a']);
  function loginWithHook() { return f.identity.login({ actor: 'reader', password, operationId: 'same-operation' }, { beforeCommit: async () => { calls++; gate.enter(); await gate.blocked; } }); }
});

test('primary FoundationDB identity: overlapping failed password identities count only the committed original result', { skip }, async t => {
  const f = await fixture(t), gate = barrier(), request = { actor: 'reader', password: 'wrong password value', operationId: 'same-failure' };
  const loser = f.identity.login(request, { beforeCommit: async () => { gate.enter(); await gate.blocked; } });
  const failed = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  await assert.rejects(f.identity.login(request), { code: 'UNAUTHENTICATED' }); gate.release(); await failed;
  await assert.rejects(f.identity.login(request), { code: 'UNAUTHENTICATED' });
  assert.equal((await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/failure/')).value.count, 1);
  assert.equal((await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length, 0);
});

test('primary FoundationDB identity: concurrent current policy revocation rejects staged source mutation and leaves no receipt', { skip }, async t => {
  const f = await fixture(t), issued = await login(f), gate = barrier(), before = await rawSources(f), changed = sources()[1]; changed.data.amount = '200'; let calls = 0;
  const operation = f.identity.transact(issued.token, { operationId: 'overlapping-policy', request: { amount: '200' } }, async (tx, context, getAuthority) => {
    calls++; const result = await mutateAuthorizedInTransaction(tx, f.catalog, context, [{ type: 'put', record: changed }], { getAuthority }); gate.enter(); await gate.blocked; return result;
  });
  const failed = assert.rejects(operation, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const revoked = graph(); revoked.principals.find(node => node.id === 'reader').active = false; await f.identity.replacePolicy(revoked); gate.release(); await failed;
  assert.equal(calls, 1); assert.deepEqual(await rawSources(f), before);
  await assert.rejects(rows(f, issued.token), { code: 'UNAUTHENTICATED' });
  await f.identity.replacePolicy(graph()); let replayCalls = 0;
  await f.identity.transact(issued.token, { operationId: 'overlapping-policy', request: { amount: '200' } }, () => { replayCalls++; return { applied: 0 }; });
  assert.equal(replayCalls, 1); await f.engine.check();
});

test('primary FoundationDB identity: concurrent password revision prevents old-session source commit and duplicate original login issuance', { skip }, async t => {
  const f = await fixture(t), issued = await login(f), gate = barrier(), before = await rawSources(f), changed = sources()[1]; changed.data.amount = '200';
  const operation = f.identity.mutate(issued.token, [{ type: 'put', record: changed }], { operationId: 'overlapping-password', beforeCommit: async () => { gate.enter(); await gate.blocked; } });
  const failed = assert.rejects(operation, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  await f.identity.changePassword(issued.token, { oldPassword: password, newPassword: 'a different replacement password', operationId: 'winner-password' }); gate.release(); await failed;
  assert.deepEqual(await rawSources(f), before); await assert.rejects(rows(f, issued.token), { code: 'UNAUTHENTICATED' });
  await assert.rejects(f.identity.login({ actor: 'reader', password, operationId: 'login-reader' }), { code: 'UNAUTHENTICATED' }); await f.engine.check();
});

test('primary FoundationDB identity: both-owner policy is reloaded after a concurrent transfer wins', { skip }, async t => {
  const f = await fixture(t), issued = await login(f), admin = await f.identity.login({ actor: 'admin', password: adminPassword, operationId: 'admin' }), gate = barrier(), changed = sources()[1]; changed.data.amount = '200';
  const operation = f.identity.mutate(issued.token, [{ type: 'put', record: changed }], { operationId: 'overlapping-owner', beforeCommit: async () => { gate.enter(); await gate.blocked; } });
  const failed = assert.rejects(operation, { code: 'STORAGE_CONFLICT' }); await gate.entered;
  const moved = sources()[1]; moved.owner = 'hidden'; await f.identity.mutate(admin.token, [{ type: 'put', record: moved }], { operationId: 'owner-winner' }); gate.release(); await failed;
  await assert.rejects(f.identity.mutate(issued.token, [{ type: 'put', record: changed }], { operationId: 'overlapping-owner' }), { code: 'FORBIDDEN' });
  const actual = (await rawSources(f)).map(row => row.value).find(row => row.id === 'a'); assert.equal(actual.owner, 'hidden'); assert.equal(actual.data.amount, '100');
  assert.deepEqual((await rows(f, issued.token)).records, []); await f.engine.check();
});
