import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { conflictSchedules } from './helpers/kv-conformance.mjs';

const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE;
const expected = {
  'present-point-read-write': [['source/protected', 2]],
  'batch-existing-key': [['source/protected', 2]],
  'batch-absent-key': [['source/protected', 1], ['source/missing', 2]],
  'absent-key-uniqueness': [['source/missing', 2]],
  'empty-range-insertion-phantom': [['range/new', 2]],
  'populated-range-deletion-phantom': [],
  'limited-page-protects-far-later-edit': [['range/001', 1], ['range/999', 10]],
  'no-op-retains-point-dependency': [['source/protected', 2]],
  'reverse-reference-empty-range': [['reference/parent/child', 'child']],
  'authority-revocation': [['authority/actor', { read: false }]],
};
async function host(t, prefix) {
  const child = fork(fileURLToPath(new URL('./fixtures/foundationdb-service.mjs', import.meta.url)), [clusterFile, prefix], { stdio: ['ignore', 'pipe', 'pipe', 'ipc'] });
  let stderr = ''; child.stderr.on('data', data => { stderr += data; });
  const exit = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await exit; } });
  const ready = await new Promise((resolve, reject) => {
    child.once('error', reject); child.once('exit', code => reject(Error(`Conformance host exited ${code}: ${stderr}`)));
    child.once('message', resolve);
  });
  assert.equal(ready.event, 'ready');
  const base = `http://127.0.0.1:${ready.port}`;
  const request = async (path, name) => {
    const result = await fetch(base + path + (name ? `?name=${encodeURIComponent(name)}` : ''), { signal: AbortSignal.timeout(8000) });
    return { status: result.status, body: await result.json() };
  };
  const entered = async names => {
    const deadline = Date.now() + 4000;
    while (Date.now() < deadline) {
      const health = await request('/health'); assert.equal(health.body.pid, ready.pid);
      if (names.every(name => health.body.progress.includes(name))) return;
      await delay(10);
    }
    throw Error('Concurrent transaction did not enter through the single service');
  };
  return { child, exit, request, entered, pid: ready.pid };
}

test('FoundationDB through one writable Node service', { skip: !clusterFile && 'Set KV_BASE_FDB_CLUSTER_FILE for the real primary target', timeout: 60000 }, async t => {
  const prefix = `kv-base-service-conformance/${randomUUID()}/`; let service = await host(t, prefix);
  await t.test('disjoint writes overlap and both commit in the same process', async () => {
    const a = service.request('/disjoint', 'a'), b = service.request('/disjoint', 'b');
    await service.entered(['disjoint-a', 'disjoint-b']); await service.request('/release', 'disjoint-a'); await service.request('/release', 'disjoint-b');
    assert.deepEqual(await a, { status: 200, body: { committed: 'a' } });
    assert.deepEqual(await b, { status: 200, body: { committed: 'b' } });
    const state = (await service.request('/engine-state')).body;
    assert.deepEqual(state.sources.map(row => row.value).sort((a, b) => a.id.localeCompare(b.id)), ['a', 'b'].map(name => ({ id: `disjoint-${name}`, tenant: 'conformance', model: 'Datum', owner: 'operator', createdAt: '0', data: {} })));
    assert.deepEqual(state.check, { sources: 2, trees: 0, occurrences: 0, references: 0, claims: 0 });
  });
  for (const schedule of conflictSchedules) await t.test(`shared schedule ${schedule.name}`, async () => {
    assert.equal((await service.request('/prepare', schedule.name)).status, 200);
    const loser = service.request('/loser', schedule.name); await service.entered([schedule.name]);
    assert.equal((await service.request('/winner', schedule.name)).status, 200);
    await service.request('/release', schedule.name);
    assert.deepEqual(await loser, { status: 409, body: { error: 'STORAGE_CONFLICT' } });
    const state = await service.request('/state', schedule.name);
    const rows = expected[schedule.name].map(([key, value]) => ({ key, value })).sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)));
    assert.deepEqual(state.body, { rows, loserReceipt: null });
  });
  await t.test('lost commit response recovers the original receipt without applying twice', async () => {
    await assert.rejects(service.request('/lost'));
    assert.deepEqual(await service.request('/recover'), { status: 200, body: { applied: '1' } });
    let state = (await service.request('/once-state')).body;
    assert.equal(state.counter, '1'); assert.equal(state.applied, 1);
    assert.deepEqual(state.receipt, { id: 'test/once/original', digest: 'exact-once-request', result: { applied: '1' } });
    service.child.kill('SIGKILL'); assert.equal((await service.exit).signal, 'SIGKILL');
    service = await host(t, prefix);
    assert.deepEqual(await service.request('/recover'), { status: 200, body: { applied: '1' } });
    state = (await service.request('/once-state')).body; assert.equal(state.counter, '1'); assert.equal(state.applied, 0);
  });
  service.child.send('close'); assert.equal((await service.exit).code, 0);
});
