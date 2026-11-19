import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { prepareCatalog } from '../src/catalog.mjs';
import { createEngine } from '../src/lifecycle.mjs';
import { prepareAuthority } from '../src/authority.mjs';
import { pipelineDefinition, pipelinePolicy, pipelineSources, ownerIndex } from '../examples/pipeline.mjs';
const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE, skip = clusterFile ? false : 'Selected real FDB cluster required';
function deferred() { let resolve; return { promise: new Promise(yes => { resolve = yes; }), resolve: value => resolve(value) }; }
async function fixture(t) {
  const raw = await openFoundationDbStore({ clusterFile, prefix: 'cursor-race-' + randomUUID() });
  let hold = false, arrivals = 0, failAfterRead = false; const first = deferred(), second = deferred(), release = deferred();
  t.after(async () => { release.resolve(); await raw.close(); });
  const store = { ...raw, read: callback => raw.read(async tx => {
    let paused = false;
    async function pause(keys) { if (hold && !paused && keys.some(key => key.startsWith('root/'))) { paused = true; arrivals++; (arrivals === 1 ? first : second).resolve(); await release.promise; } }
    const result = await callback({ ...tx, async get(key) { const value = await tx.get(key); await pause([key]); return value; }, async getMany(keys) { const value = await tx.getMany(keys); await pause(keys); return value; } });
    if (failAfterRead) throw Object.assign(Error('Read completed but its storage boundary failed'), { code: 'READ_FAILED' }); return result;
  }) };
  const catalog = prepareCatalog(pipelineDefinition()), engine = createEngine({ store, catalog, tenant: 't' });
  const records = pipelineSources(); for (const [id, at] of [['second-entry', '2'], ['third-entry', '3']]) records.push({ id, tenant: 't', owner: 'visible', createdAt: '0', model: 'Entry', data: { ledgerId: 'ledger', at, sequence: '1', amount: '0' } });
  await engine.mutate(records.map(record => ({ type: 'put', record })));
  const options = { getAuthority: async () => prepareAuthority(catalog, pipelinePolicy()) };
  return { engine, options, first, second, release, hold: () => { hold = true; }, fail: value => { failAfterRead = value; } };
}
const rawQuery = { index: ownerIndex, partition: ['ledger', 'visible'], limit: 1 }, authorizedQuery = { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 1 };
function request(f, kind, cursor, extra = {}) { return kind === 'raw' ? f.engine.window(rawQuery, { ...(cursor === undefined ? {} : { cursor }), ...extra }) : f.engine.authorizedPage({ tenant: 't', actor: 'reader' }, authorizedQuery, { ...f.options, ...(cursor === undefined ? {} : { cursor }), ...extra }); }
function ids(page, kind) { return kind === 'raw' ? page.occurrences.map(row => row.sourceId) : page.records.map(row => row.id); }

for (const kind of ['raw', 'authorized']) test(`primary FDB ${kind} concurrent cursor resume publishes exactly one successor`, { skip }, async t => {
  const f = await fixture(t), initial = await request(f, kind); assert.deepEqual(ids(initial, kind), ['visible-entry']); f.hold();
  const one = request(f, kind, initial.cursor); await f.first.promise; assert.equal(f.engine.releaseCursor(initial.cursor), false);
  const two = request(f, kind, initial.cursor); void two.catch(() => {});
  await Promise.race([two.catch(() => undefined), f.second.promise]); f.release.resolve(); const results = await Promise.allSettled([one, two]);
  assert.equal(results.filter(row => row.status === 'fulfilled').length, 1); assert.equal(results.filter(row => row.status === 'rejected').length, 1); assert.equal(results.find(row => row.status === 'rejected').reason.code, 'INVALID_CURSOR');
  const winner = results.find(row => row.status === 'fulfilled').value; assert.deepEqual(ids(winner, kind), ['second-entry']); assert.equal(typeof winner.cursor, 'string');
  assert.deepEqual(ids(await request(f, kind, winner.cursor), kind), ['third-entry']); await assert.rejects(request(f, kind, initial.cursor), { code: 'INVALID_CURSOR' }); assert.equal((await f.engine.inspect()).cursors, 0);
});

for (const kind of ['raw', 'authorized']) test(`primary FDB ${kind} failed storage return leaves original cursor retryable without leaked successor`, { skip }, async t => {
  const f = await fixture(t); f.fail(true); await assert.rejects(request(f, kind), { code: 'READ_FAILED' }); f.fail(false); assert.equal((await f.engine.inspect()).cursors, 0);
  const initial = await request(f, kind); f.fail(true);
  await assert.rejects(request(f, kind, initial.cursor), { code: 'READ_FAILED' }); f.fail(false); assert.equal((await f.engine.inspect()).cursors, 1);
  const resumed = await request(f, kind, initial.cursor); assert.deepEqual(ids(resumed, kind), ['second-entry']); assert.deepEqual(ids(await request(f, kind, resumed.cursor), kind), ['third-entry']); assert.equal((await f.engine.inspect()).cursors, 0);
});

test('primary FDB authorized reader callback failure releases cursor reservation for original retry', { skip }, async t => {
  const f = await fixture(t), initial = await request(f, 'authorized');
  await assert.rejects(request(f, 'authorized', initial.cursor, { onMetrics() { throw Error('Reader observer failed'); } }), /Reader observer failed/);
  assert.equal((await f.engine.inspect()).cursors, 1); assert.deepEqual(ids(await request(f, 'authorized', initial.cursor), 'authorized'), ['second-entry']);
});
