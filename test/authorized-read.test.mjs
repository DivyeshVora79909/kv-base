import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog } from '../src/catalog.mjs';
import { prepareAuthority } from '../src/authority.mjs';
import { createMemoryStore } from '../src/memory.mjs';
import { mutate } from '../src/mutate.mjs';
import { checkStore } from '../src/check.mjs';
import { readAuthorized, readIncomingAuthorized } from '../src/authorized-read.mjs';
import { sourceKey, incomingPrefix, nodeKey, rootKey } from '../src/keys.mjs';
import { treeAggregate } from '../src/tree.mjs';

const integer = { type: 'string', pattern: '^(0|-?[1-9][0-9]*)$' };
const named = { type: 'object', properties: { name: { type: 'string' } }, required: ['name'], additionalProperties: false };
function definition() {
  return {
    name: 'authorized-read-witness', version: '1',
    contracts: { ordered: { version: '1', partition: ['string', 'id', 'boolean'], order: 'integer', inputs: { amount: 'integer' }, aggregates: { total: { monoid: 'sum', input: 'amount' } } } },
    views: { ownerEffective: { contract: 'ordered' } },
    models: {
      Entry: {
        schema: { type: 'object', additionalProperties: false, properties: {
          account: { type: 'string' }, active: { type: 'boolean' }, effective: integer,
          amount: integer, label: { type: 'string' }, target: { type: 'string' },
          links: { type: 'array', items: { type: 'string' } },
        }, required: ['account', 'active', 'effective', 'amount', 'label'] },
        memberships: [{ slot: 'main', view: 'ownerEffective', partition: [{ field: 'account' }, { header: 'owner' }, { field: 'active' }], order: { field: 'effective' }, inputs: { amount: { field: 'amount' } }, onMissing: 'reject' }],
        references: [
          { slot: 'target', field: 'target', targets: ['Anchor', 'Secret'], many: false, onDelete: 'reject' },
          { slot: 'links', field: 'links', targets: ['Anchor'], many: true, onDelete: 'reject' },
        ],
        access: { orders: { effective: { view: 'ownerEffective', slot: 'main', ownerPartition: 1 } } },
      },
      Anchor: { schema: named, access: { orders: {} } },
      Secret: { schema: named },
    },
  };
}
function graph(tenant = 't') {
  const principal = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  return { tenant, principals: [
    principal(tenant, 'tenant', []), principal('administrator', 'user', [tenant], {}, true),
    principal('g', 'group', [tenant], { Entry: ['read'], Anchor: ['read'] }),
    principal('u', 'user', ['g']), principal('d', 'user', ['u']),
    principal('x', 'user', ['g']), principal('without-grants', 'user', [tenant]),
  ] };
}
const context = { tenant: 't', actor: 'u' };
const query = { model: 'Entry', order: 'effective', partition: ['book', true], limit: 100 };
const record = (id, owner, effective, data = {}, tenant = 't') => ({ id, tenant, owner, model: 'Entry', createdAt: '0', data: { account: 'book', active: true, effective: String(effective), amount: '1', label: id, target: 'anchor', ...data } });
const anchor = (id = 'anchor', owner = 'u', model = 'Anchor', tenant = 't') => ({ id, tenant, owner, model, createdAt: '0', data: { name: id } });
function records(tenant = 't') {
  return [anchor('anchor', 'u', 'Anchor', tenant), anchor('hidden-anchor', 'x', 'Anchor', tenant), anchor('secret-anchor', 'u', 'Secret', tenant),
    record('A', 'u', -3, { links: ['anchor', 'anchor'] }, tenant),
    record('z', 'g', 2, { links: ['anchor'] }, tenant),
    record('é', 'd', 2, {}, tenant), record('😀', 'u', 2, {}, tenant),
    record('late', 'g', '9007199254740993123456789', {}, tenant),
    record('later', 'd', '9007199254740993123456790', {}, tenant),
    record('hidden-first', 'x', -100, { amount: '900', links: ['anchor'] }, tenant),
    record('hidden-last', tenant, '99999999999999999999999999999', {}, tenant),
    record('other-book', 'u', -200, { account: 'elsewhere' }, tenant),
    record('inactive-partition', 'u', -300, { active: false }, tenant),
  ];
}
async function fixture(input = records(), declaration = definition()) {
  const catalog = prepareCatalog(declaration), store = createMemoryStore();
  await mutate(store, catalog, 't', input.map(record => ({ type: 'put', record })));
  const prepared = prepareAuthority(catalog, graph());
  return { catalog, store, input, options: { getAuthority: async () => prepared } };
}
const textCompare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
const compare = (a, b) => {
  const x = BigInt(a.data.effective), y = BigInt(b.data.effective);
  return x < y ? -1 : x > y ? 1 : textCompare(a.id, b.id);
};
// This witness uses declared fixture facts directly. It deliberately does not use
// permits, projectRecord, sourceOracle, range helpers, or any stored index state.
function expected(input, request = query) {
  return input.filter(row => row.tenant === 't' && row.model === 'Entry'
    && ['u', 'd', 'g'].includes(row.owner)
    && row.data.account === request.partition[0] && row.data.active === request.partition[1])
    .sort(compare).filter(row => {
      for (const side of ['lower', 'upper']) {
        const bound = request.range?.[side];
        if (!bound) continue;
        const x = BigInt(row.data.effective), y = BigInt(bound.value);
        let c = x < y ? -1 : x > y ? 1 : 0;
        if (!c && Object.hasOwn(bound, 'sourceId')) c = textCompare(row.id, bound.sourceId) || textCompare('main', bound.slot);
        if (side === 'lower' ? c < 0 || (c === 0 && bound.inclusive === false) : c > 0 || (c === 0 && bound.inclusive === false)) return false;
      }
      return true;
    }).slice(0, request.limit);
}
const read = (f, request = query, options = {}) => readAuthorized(f.store, f.catalog, context, request, { ...f.options, ...options });
const incoming = (f, request = {}, options = {}) => readIncomingAuthorized(f.store, f.catalog, context,
  { targetId: 'anchor', model: 'Entry', slot: 'target', limit: 100, ...request }, { ...f.options, ...options });

test('authorized owner merge matches independent source filtering, exact ties and composite cuts', async () => {
  const f = await fixture();
  const requests = [query, { ...query, limit: 0 }, { ...query, limit: 1 }, { ...query, limit: 4 },
    { ...query, partition: ['elsewhere', true] }, { ...query, partition: ['book', false] },
    { ...query, partition: ['absent', true] },
    { ...query, range: { lower: { value: '2' }, upper: { value: '2' } } },
    { ...query, range: { lower: { value: '2', sourceId: 'z', slot: 'main', inclusive: false }, upper: { value: '9007199254740993123456789' } } },
    { ...query, range: { lower: { value: '3' }, upper: { value: '-1' } } },
  ];
  for (const request of requests) for (const batchSize of [1, 2, 32]) {
    assert.deepEqual(await read(f, request, { batchSize }), { records: expected(f.input, request) });
  }
  await checkStore(f.store, f.catalog, 't');
});

test('authorized pages track independently modeled payload, order, owner and lifetime history', async () => {
  const rows = [anchor(), ...Array.from({ length: 150 }, (_, index) => record(
    `history-${String(index).padStart(3, '0')}`, ['u', 'd', 'g', 'x', 't'][index % 5],
    index % 29 === 0 ? '9007199254740993123456789' : String(index % 17 - 8),
    { account: index % 7 === 0 ? 'elsewhere' : 'book', active: index % 11 !== 0, amount: String(index - 50) },
  ))];
  const f = await fixture(rows), facts = new Map(rows.map(row => [row.id, structuredClone(row)]));
  const requests = [
    ...[0, 1, 7, 35, 200].map(limit => ({ ...query, limit })),
    { ...query, range: { lower: { value: '-3' }, upper: { value: '4', inclusive: false } } },
    { ...query, limit: 9, range: { lower: { value: '2', sourceId: 'history-025', slot: 'main', inclusive: false } } },
    { ...query, range: { lower: { value: '9007199254740993123456788' } } },
    { ...query, partition: ['elsewhere', true], limit: 19 },
  ];
  async function compareSnapshot() {
    for (const request of requests) assert.deepEqual((await read(f, request, { batchSize: 3 })).records, expected([...facts.values()], request));
  }
  async function apply(commands) {
    await mutate(f.store, f.catalog, 't', commands);
    for (const command of commands) {
      if (command.type === 'delete') facts.delete(command.id);
      else facts.set(command.record.id, structuredClone(command.record));
    }
    await compareSnapshot();
  }
  await compareSnapshot();
  await apply(Array.from({ length: 35 }, (_, index) => {
    const row = structuredClone(facts.get(`history-${String(index).padStart(3, '0')}`));
    row.data.label = `payload revision ${index}`;
    if (index % 2 === 0) row.data.effective = String(BigInt(row.data.effective) + 11n);
    return { type: 'put', record: row };
  }));
  await apply(Array.from({ length: 40 }, (_, offset) => {
    const row = structuredClone(facts.get(`history-${String(offset + 40).padStart(3, '0')}`));
    row.owner = ['u', 'd', 'g'].includes(row.owner) ? 'x' : 'd';
    return { type: 'put', record: row };
  }));
  await apply([
    ...Array.from({ length: 15 }, (_, index) => ({ type: 'delete', id: `history-${String(index).padStart(3, '0')}` })),
    ...Array.from({ length: 20 }, (_, index) => ({ type: 'put', record: record(`new-${index}`, ['g', 'x', 'u'][index % 3], String(index % 5 - 2), { label: `new body ${index}` }) })),
  ]);
  await checkStore(f.store, f.catalog, 't');
});

test('shared business summaries retain hidden contributions without disclosing hidden rows or summary values', async () => {
  const declaration = definition();
  declaration.contracts.shared = { version: '1', partition: ['string'], order: 'integer', inputs: { amount: 'integer' }, aggregates: { total: { monoid: 'sum', input: 'amount' } } };
  declaration.views.sharedEffective = { contract: 'shared' };
  declaration.models.Entry.memberships.push({ slot: 'shared', view: 'sharedEffective', partition: [{ field: 'account' }], order: { field: 'effective' }, inputs: { amount: { field: 'amount' } }, onMissing: 'reject' });
  const rows = [anchor(), record('visible', 'u', 1, { amount: '7' }), record('hidden-contributor', 'x', 2, { amount: '900000000000000000000', label: 'hidden body' })];
  const f = await fixture(rows, declaration);
  const shared = await f.store.read(tx => treeAggregate(tx, f.catalog, { tenant: 't', view: 'sharedEffective', partition: ['book'] }));
  assert.equal(shared.total, 900000000000000000007n);
  const page = await read(f);
  assert.deepEqual(page, { records: [rows[1]] });
  assert.deepEqual(await incoming(f), { records: [rows[1]] });
  assert.deepEqual(Object.keys(page), ['records']);
  assert.equal(JSON.stringify(page).includes('hidden'), false);
  assert.equal(JSON.stringify(page).includes('900000000000000000000'), false);
  await checkStore(f.store, f.catalog, 't');
});

test('private models, missing grants, hidden and private relation anchors fail closed', async () => {
  const f = await fixture();
  await assert.rejects(readAuthorized(f.store, f.catalog, { ...context, actor: 'without-grants' }, query, f.options), { code: 'FORBIDDEN' });
  await assert.rejects(read(f, { ...query, model: 'Secret' }), { code: 'FORBIDDEN' });
  await assert.rejects(incoming(f, { targetId: 'hidden-anchor' }), { code: 'FORBIDDEN' });
  await assert.rejects(incoming(f, { targetId: 'secret-anchor' }), { code: 'FORBIDDEN' });
  await assert.rejects(incoming(f, { targetId: 'missing-anchor' }), { code: 'FORBIDDEN' });
  assert.deepEqual(Object.keys(await read(f)), ['records']);
  assert.deepEqual(Object.keys(await incoming(f)), ['records']);
});

test('tenant data and catalog authority never cross request boundaries', async () => {
  const f = await fixture();
  const elsewhere = records('other').map(row => row.model === 'Entry' ? { ...row, data: { ...row.data, label: 'OTHER TENANT' } } : row);
  await mutate(f.store, f.catalog, 'other', elsewhere.map(record => ({ type: 'put', record })));
  assert.deepEqual((await read(f)).records, expected(f.input));
  await assert.rejects(readAuthorized(f.store, f.catalog, { tenant: 'other', actor: 'u' }, query, f.options), { code: 'FORBIDDEN' });
  const changed = definition(); changed.version = '2';
  const otherCatalog = prepareCatalog(changed), wrong = prepareAuthority(otherCatalog, graph());
  await assert.rejects(read(f, query, { getAuthority: async () => wrong }));
});

test('each request reloads fixture authority inside its protected snapshot', async () => {
  const f = await fixture(), key = 'fixture-authority/t'; let calls = 0;
  await f.store.transact(tx => tx.set(key, graph()));
  const getAuthority = async tx => { calls++; return prepareAuthority(f.catalog, await tx.get(key)); };
  assert.deepEqual((await read(f, query, { getAuthority })).records, expected(f.input));
  await f.store.transact(async tx => {
    const current = await tx.get(key); current.principals.find(row => row.id === 'g').grants.resources.Entry = [];
    await tx.set(key, current);
  });
  await assert.rejects(read(f, query, { getAuthority }), { code: 'FORBIDDEN' });
  assert.equal(calls, 2);
});

test('cached stream heads avoid repeatedly hydrating unpopped candidates', async () => {
  const rows = [anchor(), ...Array.from({ length: 93 }, (_, index) => record(`s${String(index).padStart(3, '0')}`, ['u', 'd', 'g'][index % 3], index))];
  const f = await fixture(rows), fetched = [];
  const wrapped = { read: callback => f.store.read(tx => callback({ ...tx,
    async getMany(keys) { fetched.push(...keys.filter(key => key.startsWith('source/'))); return tx.getMany(keys); },
    async get(key) { if (key.startsWith('source/')) fetched.push(key); return tx.get(key); },
  })) };
  let metrics;
  const request = { ...query, limit: 17 };
  const result = await readAuthorized(wrapped, f.catalog, context, request, { ...f.options, batchSize: 2, onMetrics: value => { metrics = value; } });
  assert.deepEqual(result.records, expected(rows, request));
  assert.equal(fetched.length, new Set(fetched).size, 'cached heads must not be rehydrated on every merge comparison');
  assert.ok(fetched.length < rows.length - 1, 'bounded page should not hydrate the entire source population');
  assert.equal(metrics.streams, 3);
  assert.ok(metrics.refills > 0);
});

test('finite discovery, stream, candidate, refill, node, batch and byte budgets reject without a partial page', async () => {
  const f = await fixture(); let metrics;
  const result = await read(f, query, { batchSize: 1, onMetrics: value => { metrics = value; } });
  const caps = { maxDiscovery: metrics.discovery, maxStreams: metrics.streams, maxCandidates: metrics.candidates,
    maxRefills: metrics.refills, maxNodeVisits: metrics.nodes, maxBatchCalls: metrics.batches,
    maxReadBytes: metrics.readBytes, maxOutputBytes: metrics.outputBytes };
  assert.deepEqual(await read(f, query, { batchSize: 1, ...caps }), result);
  for (const [key, value] of Object.entries(caps)) {
    assert.ok(value > 0, `${key} must account for work`);
    await assert.rejects(read(f, query, { batchSize: 1, ...caps, [key]: value - 1 }), { code: 'WORK_LIMIT' });
  }
});

test('malformed query and budget controls reject without evaluating getters', async () => {
  const f = await fixture();
  for (const options of [{ maxDiscovery: Infinity }, { maxStreams: -1 }, { maxCandidates: 1.5 },
    { batchSize: 0 }, { maxReadBytes: '5' }, { maxRefills: undefined }, { unknown: true }]) {
    await assert.rejects(read(f, query, options));
  }
  for (const request of [{ ...query, limit: -1 }, { ...query, limit: Infinity }, { ...query, limit: 1.5 },
    { ...query, partition: ['book'] }, { ...query, partition: ['book', 'true'] }, { ...query, range: null }, { ...query, unexpected: true }]) {
    await assert.rejects(read(f, request));
  }
  let calls = 0;
  const options = Object.defineProperty({ ...f.options }, 'maxCandidates', { enumerable: true, get() { calls++; return 10; } });
  await assert.rejects(readAuthorized(f.store, f.catalog, context, query, options));
  assert.equal(calls, 0);
});

test('hydration rejects missing, wrong-key, malformed and mismatched source occurrences', async () => {
  for (const damage of ['missing', 'id', 'tenant', 'model', 'owner', 'order', 'partition', 'contribution', 'shape']) {
    const f = await fixture([anchor(), record('A', 'u', 1)]);
    await f.store.transact(async tx => {
      const key = sourceKey('t', 'A'), source = await tx.get(key);
      if (damage === 'missing') return tx.delete(key);
      if (damage === 'id') source.id = 'impostor';
      if (damage === 'tenant') source.tenant = 'other';
      if (damage === 'model') { source.model = 'Anchor'; source.data = { name: 'impostor' }; }
      if (damage === 'owner') source.owner = 'd';
      if (damage === 'order') source.data.effective = '2';
      if (damage === 'partition') source.data.account = 'different';
      if (damage === 'contribution') source.data.amount = '2';
      if (damage === 'shape') source.data.amount = false;
      await tx.set(key, source);
    });
    await assert.rejects(read(f), { code: 'CORRUPT_STATE' }, damage);
  }
});

test('occurrence slot and source identity corruption cannot disclose an unrelated row', async () => {
  for (const field of ['sourceId', 'slot']) {
    const f = await fixture([anchor(), record('A', 'u', 1)]);
    const scope = { tenant: 't', view: 'ownerEffective', partition: ['book', 'u', true] };
    await f.store.transact(async tx => {
      const root = await tx.get(rootKey(f.catalog, scope)), key = nodeKey(f.catalog, scope, root.head), node = await tx.get(key);
      node[field] = 'anchor'; await tx.set(key, node);
    });
    await assert.rejects(read(f), { code: 'CORRUPT_STATE' });
  }
});

test('reverse relation discovery applies disclosure and deduplicates repeated source slots', async () => {
  const f = await fixture();
  const visible = f.input.filter(row => row.model === 'Entry' && ['u', 'd', 'g'].includes(row.owner) && row.data.target === 'anchor').sort((a, b) => textCompare(a.id, b.id));
  assert.deepEqual(await incoming(f), { records: visible });
  assert.deepEqual(await incoming(f, { limit: 2 }), { records: visible.slice(0, 2) });
  assert.deepEqual(await incoming(f, { slot: 'links' }), { records: visible.filter(row => row.data.links?.includes('anchor')) });
  const empty = anchor('empty');
  await mutate(f.store, f.catalog, 't', [{ type: 'put', record: empty }]);
  assert.deepEqual(await incoming(f, { targetId: 'empty' }), { records: [] });
});

test('reverse discovery budgets include hidden candidates and physical memory scan work', async () => {
  const f = await fixture(); let metrics;
  const result = await incoming(f, {}, { onMetrics: value => { metrics = value; } });
  assert.ok(metrics.candidates > result.records.length, 'hidden source candidates still consume work');
  assert.ok(metrics.scannedKeys > metrics.candidates, 'memory prefix scan enumerates unrelated store keys');
  const caps = { maxDiscovery: metrics.discovery, maxCandidates: metrics.candidates,
    maxScannedKeys: metrics.scannedKeys, maxReadBytes: metrics.readBytes, maxOutputBytes: metrics.outputBytes };
  assert.deepEqual(await incoming(f, {}, caps), result);
  for (const [key, value] of Object.entries(caps)) {
    assert.ok(value > 0);
    await assert.rejects(incoming(f, {}, { ...caps, [key]: value - 1 }), { code: 'WORK_LIMIT' });
  }
});

test('reverse metadata must agree with the authoritative source reference slot and position', async () => {
  for (const damage of ['target', 'position', 'source', 'missing-source-id', 'tenant']) {
    const f = await fixture([anchor(), anchor('elsewhere'), record('A', 'u', 1)]);
    await f.store.transact(async tx => {
      const [{ key, value }] = await tx.scan({ prefix: incomingPrefix(f.catalog, 't', 'anchor') });
      if (damage === 'target') value.targetId = 'elsewhere';
      if (damage === 'position') value.position = 42;
      if (damage === 'source') value.sourceId = 'missing';
      if (damage === 'missing-source-id') delete value.sourceId;
      if (damage === 'tenant') value.tenant = 'other';
      await tx.set(key, value);
    });
    await assert.rejects(incoming(f), { code: 'CORRUPT_STATE' });
  }
});

test('authority and hydration share one snapshot while a payload update waits', { timeout: 10000 }, async () => {
  const f = await fixture(); let entered, release, writerFinished = false, calls = 0, authorityLoads = 0;
  const authorityKey = 'fixture-authority/t';
  await f.store.transact(tx => tx.set(authorityKey, graph()));
  const reached = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const wrapped = { read: callback => f.store.read(tx => callback({ ...tx,
    async getMany(keys) {
      const values = await tx.getMany(keys);
      if (keys.some(key => key.startsWith('source/')) && calls++ === 0) { entered(); await gate; }
      assert.equal(writerFinished, false);
      return values;
    },
  })) };
  const currentAuthority = async tx => {
    authorityLoads++;
    return prepareAuthority(f.catalog, await tx.get(authorityKey));
  };
  const reading = readAuthorized(wrapped, f.catalog, context, query, { getAuthority: currentAuthority, batchSize: 1 });
  await reached;
  const changed = structuredClone(f.input.find(row => row.id === 'A')); changed.data.label = 'changed after reader';
  const writing = mutate(f.store, f.catalog, 't', [{ type: 'put', record: changed }]).then(() => { writerFinished = true; });
  release();
  assert.deepEqual((await reading).records, expected(f.input));
  assert.equal(authorityLoads, 1);
  await writing;
  assert.equal((await read(f)).records.find(row => row.id === 'A').data.label, 'changed after reader');
});

test('protected empty owner roots and empty incoming ranges remain in the read evidence', async () => {
  const f = await fixture([anchor()]); let ownerGuards, incomingGuards;
  const capture = save => ({ read: callback => f.store.read(async tx => { const result = await callback(tx); save(tx.inspect().guards); return result; }) });
  assert.deepEqual(await readAuthorized(capture(guards => { ownerGuards = guards; }), f.catalog, context, query, f.options), { records: [] });
  for (const owner of ['u', 'd', 'g']) {
    const key = rootKey(f.catalog, { tenant: 't', view: 'ownerEffective', partition: ['book', owner, true] });
    assert.ok(ownerGuards.some(guard => guard.type === 'point' && guard.key === key && guard.exists === false), owner);
  }
  assert.deepEqual(await readIncomingAuthorized(capture(guards => { incomingGuards = guards; }), f.catalog, context,
    { targetId: 'anchor', model: 'Entry', slot: 'target', limit: 10 }, f.options), { records: [] });
  assert.ok(incomingGuards.some(guard => guard.type === 'range' && guard.prefix === incomingPrefix(f.catalog, 't', 'anchor')));
});
