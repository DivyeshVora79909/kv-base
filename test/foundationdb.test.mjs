import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { fork } from 'node:child_process';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { registerKvConformance, registerOptimisticConformance } from './helpers/kv-conformance.mjs';
import { registerDurableEngineConformance } from './helpers/durable-engine.mjs';

const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE;
const enabled = Boolean(clusterFile), skip = enabled ? false : 'KV_BASE_FDB_CLUSTER_FILE must select a real test cluster';
const namespace = () => `conformance-${randomUUID()}`;
async function open(t, options = {}) { const store = await openFoundationDbStore({ clusterFile, prefix: namespace(), ...options }); t.after(() => store.close()); return store; }
if (enabled) {
  registerKvConformance('FoundationDB', open, { receipts: true });
  registerOptimisticConformance('FoundationDB', open);
  registerDurableEngineConformance('FoundationDB', t => {
    const prefix = namespace(); return async () => { const store = await openFoundationDbStore({ clusterFile, prefix }); t.after(() => store.close()); return store; };
  });
} else test('FoundationDB conformance requires selected real cluster', { skip }, () => {});

test('FoundationDB independent physical key value transaction and time limits reject atomically', { skip }, async t => {
  const store = await open(t); assert.equal(store.capabilities.serializable, true); assert.equal(store.capabilities.serialized, false);
  for (const operation of [tx => tx.set('x'.repeat(10_001), 1), tx => tx.set('large', new Uint8Array(100_001))]) {
    await assert.rejects(store.transact(async tx => { await tx.set('source/partial', true); await operation(tx); }, { receipt: { id: 'too-large', digest: 'too-large' } }), { code: 'STORAGE_LIMIT' });
    assert.deepEqual(await store.read(tx => tx.scan()), []); assert.equal(await store.getReceipt('too-large'), undefined);
  }
  const limited = await open(t, { transactionBytes: 1024 });
  await assert.rejects(limited.transact(async tx => { for (let i = 0; i < 10; i++) await tx.set(`source/${i}`, new Uint8Array(400)); }, { receipt: { id: 'budget', digest: 'budget' } }), { code: 'STORAGE_LIMIT' });
  assert.deepEqual(await limited.read(tx => tx.scan()), []); assert.equal(await limited.getReceipt('budget'), undefined);
  const timed = await open(t, { timeoutMs: 100 }); let calls = 0;
  await assert.rejects(timed.transact(async tx => { calls++; await tx.set('source/partial', true); await new Promise(resolve => setTimeout(resolve, 150)); }, { receipt: { id: 'timeout', digest: 'timeout' } }), { code: 'STORAGE_LIMIT' });
  assert.equal(calls, 1); assert.equal(await timed.getReceipt('timeout'), undefined); assert.equal(await timed.read(tx => tx.get('source/partial')), undefined);
});

test('FoundationDB independent zero-limit range retains its pre-winner snapshot', { skip }, async t => {
  const store = await open(t); let ready, release;
  const entered = new Promise(resolve => { ready = resolve; }), blocked = new Promise(resolve => { release = resolve; });
  const loser = store.transact(async tx => { assert.deepEqual(await tx.scan({ prefix: 'protected/', limit: 0 }), []); ready(); await blocked; await tx.set('source/stale', true); });
  const rejection = assert.rejects(loser, { code: 'STORAGE_CONFLICT' }); await entered;
  await store.transact(tx => tx.set('protected/new', 1)); release(); await rejection;
  assert.deepEqual(await store.read(tx => tx.scan()), [{ key: 'protected/new', value: 1 }]);
});

test('FoundationDB independent binary namespaces isolate ambiguous Unicode and NUL prefixes', { skip }, async t => {
  const base = namespace(), a = await open(t, { prefix: base }), b = await open(t, { prefix: base + '\0child' });
  await a.transact(tx => tx.set('source/key', 1), { receipt: { id: 'operation', digest: 'a' } });
  await b.transact(tx => tx.set('source/key', 2), { receipt: { id: 'operation', digest: 'b' } });
  assert.equal(await a.read(tx => tx.get('source/key')), 1); assert.equal(await b.read(tx => tx.get('source/key')), 2);
  assert.equal((await a.getReceipt('operation')).digest, 'a'); assert.equal((await b.getReceipt('operation')).digest, 'b');
  const readonly = await open(t, { prefix: base, readOnly: true }); assert.equal(await readonly.read(tx => tx.get('source/key')), 1);
  await assert.rejects(readonly.transact(tx => tx.set('x', 1)), { code: 'READ_ONLY' });
});

test('FoundationDB independent incompatible metadata and unrecognized persisted state reject opening', { skip }, async t => {
  const { default: fdb } = await import('foundationdb');
  const prefix = namespace(), store = await open(t, { prefix }); await store.close();
  const db = fdb.open(clusterFile); t.after(() => db.close());
  const root = fdb.tuple.pack(['kv-base', prefix]), metadata = Buffer.concat([root, Buffer.from([2]), Buffer.from('format')]);
  const tamper = db.rawCreateTransaction({ timeout: 4500 }); tamper.set(metadata, Buffer.from('unsupported-v99')); await tamper.rawCommit();
  await assert.rejects(openFoundationDbStore({ clusterFile, prefix }), { code: 'STORAGE_FORMAT_MISMATCH' });
  const unknown = namespace(), unknownKey = Buffer.concat([fdb.tuple.pack(['kv-base', unknown]), Buffer.from([0]), Buffer.from('source/unknown')]);
  const write = db.rawCreateTransaction({ timeout: 4500 }); write.set(unknownKey, Buffer.from('unrecognized')); await write.rawCommit();
  await assert.rejects(openFoundationDbStore({ clusterFile, prefix: unknown }), { code: 'STORAGE_FORMAT_MISMATCH' });
});

test('FoundationDB independent closing drains concurrent work and rejects new callbacks', { skip }, async t => {
  const store = await open(t); let enter, release; const entered = new Promise(resolve => { enter = resolve; }), wait = new Promise(resolve => { release = resolve; });
  const operation = store.transact(async tx => { await tx.set('source/drained', true); enter(); await wait; }); await entered;
  const closing = store.close(); let calls = 0;
  await assert.rejects(store.transact(() => { calls++; }), { code: 'STORAGE_CLOSED' }); release(); await operation; await closing;
  assert.equal(calls, 0); await assert.rejects(store.read(() => {}), { code: 'STORAGE_CLOSED' });
});

for (const phase of ['beforeCommit', 'afterCommit']) test(`FoundationDB independent real child SIGKILL ${phase} exposes atomic data and original receipt`, { skip }, async t => {
  const prefix = namespace();
  const child = fork(new URL('./fixtures/foundationdb-process.mjs', import.meta.url), [clusterFile, prefix, phase], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); });
  await new Promise((resolve, reject) => { const timeout = setTimeout(() => reject(Error('Child crash phase timeout')), 15_000); child.once('error', reject); child.once('message', message => { clearTimeout(timeout); assert.equal(message.phase, phase); resolve(); }); child.once('exit', code => { clearTimeout(timeout); reject(Error(`Child exited early ${code}`)); }); });
  const stopped = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGKILL'); await stopped;
  const store = await open(t, { prefix }), rows = await store.read(tx => tx.scan()), receipt = await store.getReceipt('crash-operation');
  if (phase === 'beforeCommit') { assert.deepEqual(rows, []); assert.equal(receipt, undefined); }
  else {
    assert.deepEqual(rows, ['application/', 'authority/', 'derived/', 'reference/', 'route/', 'source/'].map(namespace => ({ key: namespace + 'committed', value: { n: 42n } })));
    assert.deepEqual(receipt, { id: 'crash-operation', digest: 'same-request', result: { n: 42n } });
    assert.deepEqual(await store.transact(() => { throw Error('Must not replay'); }, { receipt: { id: 'crash-operation', digest: 'same-request' } }), { n: 42n });
  }
});
