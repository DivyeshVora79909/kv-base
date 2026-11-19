import test from 'node:test';
import assert from 'node:assert/strict';
import {
  prepareCatalog, prepareAuthority, createMemoryStore, mutate, mutateAuthorized,
  readAuthorized, readIncomingAuthorized, readPointAuthorized, readAnchorSummaryAuthorized,
  readAuthorizedInTransaction, readAnchorSummaryAuthorizedInTransaction,
  checkStore,
} from '../src/index.mjs';
import { sourceKey, sourcePrefix } from '../src/keys.mjs';

const integer = { type: 'string', pattern: '^(0|-?[1-9][0-9]*)$' };
const field = field => ({ field });
const schema = properties => ({ type: 'object', additionalProperties: false, properties, required: Object.keys(properties) });
const business = ['Invoice', ['anchor'], ['day', 'sequence']];
const owners = ['Invoice', ['anchor', 'owner'], ['day', 'sequence']];
const declare = (partitionFields, publication) => ({
  version: '1', partitionFields, orderFields: ['day', 'sequence'],
  roles: { anchor: 'id', ...(partitionFields.includes('owner') ? { owner: 'id' } : {}), day: 'integer', sequence: 'integer' },
  inputs: { amount: 'integer' }, aggregates: { total: { monoid: 'sum', input: 'amount' }, prefix: { monoid: 'prefix', input: 'amount' } },
  ...(publication ? { publication } : {}),
});
const membership = (slot, index, partition, at = ['day', 'sequence'], amount = 'amount') => ({ slot, index, partition, order: at.map(field), inputs: { amount: field(amount) }, onMissing: 'reject' });
function definition() {
  return {
    name: 'structural-authority', version: '1',
    models: {
      Invoice: { schema: schema({ name: { type: 'string' } }), access: { orders: {} }, indexes: [
        declare(['anchor'], { anchorPartition: 0, aggregates: ['total'] }), declare(['anchor', 'owner']),
      ] },
      Entry: {
        schema: schema({ invoice: { type: 'string' }, day: integer, sequence: integer, amount: integer, label: { type: 'string' } }),
        references: [{ slot: 'invoice', field: 'invoice', targets: ['Invoice'], many: false, onDelete: 'reject' }],
        access: { orders: {} }, memberships: [
          membership('business', business, [field('invoice')]),
          membership('owner-a', owners, [field('invoice'), { header: 'owner' }]),
          membership('owner-b', owners, [field('invoice'), { header: 'owner' }]),
        ],
      },
      Credit: {
        schema: schema({ book: { type: 'string' }, effective: integer, serial: integer, units: integer }),
        references: [{ slot: 'invoice', field: 'book', targets: ['Invoice'], many: false, onDelete: 'reject' }],
        access: { orders: {} }, memberships: [membership('credit', owners, [field('book'), { header: 'owner' }], ['effective', 'serial'], 'units')],
      },
      Private: {
        schema: schema({ invoice: { type: 'string' }, day: integer, sequence: integer, amount: integer }),
        references: [{ slot: 'invoice', field: 'invoice', targets: ['Invoice'], many: false, onDelete: 'reject' }],
        memberships: [membership('private', owners, [field('invoice'), { header: 'owner' }])],
      },
    },
  };
}
function policy(revoked = false) {
  const principal = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  return { tenant: 't', principals: [
    principal('t', 'tenant', []), principal('admin', 'user', ['t'], { Invoice: ['use'] }, true),
    principal('visible', 'group', ['admin'], revoked ? {} : { Invoice: ['read'], Entry: ['read', 'update'], Credit: ['read'] }),
    principal('hidden', 'group', ['admin']),
    principal('reader', 'user', ['visible']), principal('other-reader', 'user', ['hidden']),
  ] };
}
const record = (id, model, owner, data, tenant = 't') => ({ id, model, owner, data, tenant, createdAt: '0' });
const entry = (id, owner, amount, invoice = 'invoice', sequence = '2') => record(id, 'Entry', owner, { invoice, day: '1', sequence, amount, label: id });
function sources() {
  return [
    record('invoice', 'Invoice', 'visible', { name: 'Public balance anchor' }),
    record('hidden-invoice', 'Invoice', 'hidden', { name: 'Unreadable traversal anchor' }),
    entry('visible-row', 'visible', '100'), entry('hidden-row', 'hidden', '-80'),
    entry('through-hidden', 'visible', '1', 'hidden-invoice'),
    record('credit', 'Credit', 'visible', { book: 'invoice', effective: '1', serial: '2', units: '7' }),
    record('private', 'Private', 'visible', { invoice: 'invoice', day: '1', sequence: '2', amount: '3' }),
  ];
}
const context = { tenant: 't', actor: 'reader' };
const query = { model: 'Entry', index: owners, partition: ['invoice'], limit: 20 };
const put = record => ({ type: 'put', record });

test('queued structural reads and publications detach the request before acquiring their snapshot', async () => {
  const f = await fixture();
  let enter, release;
  const entered = new Promise(resolve => { enter = resolve; });
  const blocked = new Promise(resolve => { release = resolve; });
  const holding = f.store.read(async () => { enter(); await blocked; });
  await entered;
  const actor = structuredClone(context), rows = structuredClone(query);
  const aggregate = { index: structuredClone(business), partition: ['invoice'] };
  const pendingRows = readAuthorized(f.store, f.catalog, actor, rows, f.options);
  const pendingSummary = readAnchorSummaryAuthorized(f.store, f.catalog, actor, aggregate, f.options);
  actor.actor = 'other-reader'; rows.partition[0] = 'hidden-invoice';
  aggregate.partition[0] = 'hidden-invoice'; aggregate.index[0] = 'Private';
  release(); await holding;
  assert.deepEqual((await pendingRows).records, expected(f.input));
  assert.deepEqual(await pendingSummary, { summary: { total: 20n } });
});
async function fixture(input = sources(), d = definition()) {
  const catalog = prepareCatalog(d), store = createMemoryStore();
  await mutate(store, catalog, 't', input.map(put));
  await store.transact(tx => tx.set('fixture-policy', policy()));
  const options = { getAuthority: async tx => prepareAuthority(catalog, await tx.get('fixture-policy')) };
  return { catalog, store, input, options };
}
const read = (f, request = query, options = {}) => readAuthorized(f.store, f.catalog, context, request, { ...f.options, ...options });
const incoming = (f, targetId = 'invoice') => readIncomingAuthorized(f.store, f.catalog, context, { targetId, model: 'Entry', slot: 'invoice', limit: 20 }, f.options);
const summary = (f, request = { index: business, partition: ['invoice'] }, options = {}) => readAnchorSummaryAuthorized(f.store, f.catalog, context, request, { ...f.options, ...options });

// Visibility, multiplicity and ordering derive directly from source facts.
// No writer projection, permits, index traversal or range helper is used.
function expected(input, request = query, visibleOwners = ['reader', 'visible']) {
  const rows = [];
  for (const source of input) if (source.tenant === 't' && source.model === request.model && visibleOwners.includes(source.owner) && source.data.invoice === request.partition[0]) {
    for (const slot of ['owner-a', 'owner-b']) rows.push({ source, slot });
  }
  const text = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
  rows.sort((a, b) => BigInt(a.source.data.day) < BigInt(b.source.data.day) ? -1 : BigInt(a.source.data.day) > BigInt(b.source.data.day) ? 1 : BigInt(a.source.data.sequence) < BigInt(b.source.data.sequence) ? -1 : BigInt(a.source.data.sequence) > BigInt(b.source.data.sequence) ? 1 : text(a.source.id, b.source.id) || text(a.slot, b.slot));
  return rows.filter(({ source, slot }) => {
    for (const side of ['lower', 'upper']) {
      const bound = request.range?.[side]; if (!bound) continue;
      const tuple = [BigInt(source.data.day), BigInt(source.data.sequence)], cut = bound.value.map(BigInt);
      let cmp = tuple[0] < cut[0] ? -1 : tuple[0] > cut[0] ? 1 : tuple[1] < cut[1] ? -1 : tuple[1] > cut[1] ? 1 : 0;
      if (!cmp && bound.sourceId !== undefined) cmp = text(source.id, bound.sourceId) || text(slot, bound.slot);
      if (side === 'lower' ? cmp < 0 || (!cmp && bound.inclusive === false) : cmp > 0 || (!cmp && bound.inclusive === false)) return false;
    }
    return true;
  }).slice(0, request.limit).map(({ source }) => source);
}

test('shared structural streams disclose only permitted model rows and preserve tied repeated slots', async () => {
  const f = await fixture();
  for (const request of [query, { ...query, limit: 0 }, { ...query, limit: 1 }, { ...query, range: { lower: { value: ['1', '2'], sourceId: 'visible-row', slot: 'owner-a', inclusive: false } } }, { ...query, partition: ['empty'] }]) {
    assert.deepEqual(await read(f, request), { records: expected(f.input, request) });
  }
  const page = await read(f);
  assert.equal(JSON.stringify(page).includes('hidden-row'), false);
  assert.equal(JSON.stringify(page).includes('private'), false);
  assert.equal(JSON.stringify(page).includes('credit'), false);
  assert.deepEqual(Object.keys(page), ['records']);
  await checkStore(f.store, f.catalog, 't');
});

test('explicit full-anchor publication reveals 20 while rows, point and incoming use record policy', async () => {
  const f = await fixture();
  const directTotal = f.input.filter(row => row.model === 'Entry' && row.data.invoice === 'invoice').reduce((sum, row) => sum + BigInt(row.data.amount), 0n);
  assert.equal(directTotal, 20n);
  assert.deepEqual(await summary(f), { summary: { total: directTotal } });
  assert.deepEqual(await read(f), { records: expected(f.input) });
  assert.deepEqual(await readPointAuthorized(f.store, f.catalog, context, { id: 'visible-row' }, f.options), { records: [f.input.find(row => row.id === 'visible-row')] });
  for (const id of ['hidden-row', 'private', 'absent']) assert.deepEqual(await readPointAuthorized(f.store, f.catalog, context, { id }, f.options), { records: [] });
  assert.deepEqual(await incoming(f), { records: [f.input.find(row => row.id === 'visible-row')] });
  assert.deepEqual(Object.keys((await summary(f)).summary), ['total']);
});

test('reverse traversal requires no Read grant on its target and still validates source references', async () => {
  const f = await fixture();
  assert.deepEqual(await incoming(f, 'hidden-invoice'), { records: [f.input.find(row => row.id === 'through-hidden')] });
  await assert.rejects(summary(f, { index: business, partition: ['hidden-invoice'] }), { code: 'FORBIDDEN' });
  await f.store.transact(async tx => {
    const source = await tx.get(sourceKey('t', 'through-hidden')); source.data.invoice = 'invoice'; await tx.set(sourceKey('t', source.id), source);
  });
  await assert.rejects(incoming(f, 'hidden-invoice'), { code: 'CORRUPT_STATE' });
});

test('owner transfers check both owners, then atomically alter visibility while preserving full summary', async () => {
  const f = await fixture(), moved = structuredClone(f.input.find(row => row.id === 'visible-row'));
  moved.owner = 'hidden';
  const before = await f.store.read(tx => tx.scan());
  await assert.rejects(mutateAuthorized(f.store, f.catalog, context, [put(moved)], f.options), { code: 'FORBIDDEN' });
  assert.deepEqual(await f.store.read(tx => tx.scan()), before);
  assert.deepEqual(await mutateAuthorized(f.store, f.catalog, { tenant: 't', actor: 'admin' }, [put(moved)], f.options), { applied: 1 });
  assert.deepEqual(await read(f), { records: [] });
  assert.deepEqual(await incoming(f), { records: [] });
  assert.deepEqual(await summary(f), { summary: { total: 20n } });
  await checkStore(f.store, f.catalog, 't');
});

test('revoking group access changes reads without rewriting sources or business aggregates', async () => {
  const f = await fixture(), before = await f.store.read(tx => tx.scan({ prefix: 'source/' }));
  assert.equal((await read(f)).records.length, 2);
  await f.store.transact(tx => tx.set('fixture-policy', policy(true)));
  await assert.rejects(read(f), { code: 'FORBIDDEN' });
  await assert.rejects(summary(f), { code: 'FORBIDDEN' });
  assert.deepEqual(await readPointAuthorized(f.store, f.catalog, context, { id: 'visible-row' }, f.options), { records: [] });
  assert.deepEqual(await f.store.read(tx => tx.scan({ prefix: 'source/' })), before);
  await checkStore(f.store, f.catalog, 't');
});

test('tenant boundaries and finite setup, candidate, refill, node and byte budgets return no partial page', async () => {
  const f = await fixture();
  await assert.rejects(readAuthorized(f.store, f.catalog, { tenant: 'other', actor: 'reader' }, query, f.options), { code: 'FORBIDDEN' });
  for (const key of ['maxDiscovery', 'maxStreams', 'maxCandidates', 'maxRefills', 'maxNodeVisits', 'maxBatchCalls', 'maxReadBytes', 'maxOutputBytes']) {
    await assert.rejects(read(f, query, { [key]: 0 }), { code: 'WORK_LIMIT' }, key);
  }
  await assert.rejects(summary(f, undefined, { maxOutputBytes: 0 }), { code: 'WORK_LIMIT' });
  let metrics;
  await read(f, query, { onMetrics: m => { metrics = m; } });
  assert.ok(metrics.candidates > 2, 'shared/private candidates are charged');
  assert.ok(metrics.nodes > 0); assert.ok(metrics.readBytes > 0); assert.equal(metrics.outputs, 2);
});

test('only the contributing record trusted owner header qualifies an owner partition', async () => {
  const d = definition();
  d.models.Entry.schema.properties.relatedOwner = { type: 'string' };
  for (const m of d.models.Entry.memberships) if (m.slot.startsWith('owner')) m.partition[1] = field('relatedOwner');
  const input = sources().map(row => row.model === 'Entry' ? { ...row, data: { ...row.data, relatedOwner: 'visible' } } : row);
  const f = await fixture(input, d);
  await assert.rejects(read(f), { code: 'INVALID_READ' });
});

test('publication declarations reject undeclared aggregates, nonrecord anchors and ambiguous signatures', () => {
  for (const change of [
    d => { d.models.Invoice.indexes[0].publication.aggregates = ['absent']; },
    d => { d.models.Invoice.indexes[0].publication.aggregates = ['total', 'total']; },
    d => { d.models.Invoice.indexes[0].publication.anchorPartition = 1; },
    d => { d.models.Invoice.indexes[0].roles.anchor = 'string'; },
    d => { d.models.Invoice.indexes[0].publication.extra = true; },
  ]) { const d = definition(); change(d); assert.throws(() => prepareCatalog(d)); }
});

test('tied tuple keys merge multiple permitted owner streams in independent total order', async () => {
  const input = [...sources(), entry('reader-row', 'reader', '0')], f = await fixture(input);
  assert.deepEqual(await read(f), { records: expected(input) });
  assert.deepEqual((await read(f)).records.map(row => row.id), ['reader-row', 'reader-row', 'visible-row', 'visible-row']);
});

test('private traversal targets add no grant requirement to readable incoming sources', async () => {
  const d = definition(); delete d.models.Invoice.access;
  const f = await fixture(sources(), d);
  assert.deepEqual(await incoming(f), { records: [f.input.find(row => row.id === 'visible-row')] });
  assert.deepEqual(await readPointAuthorized(f.store, f.catalog, context, { id: 'invoice' }, f.options), { records: [] });
  await assert.rejects(summary(f), { code: 'FORBIDDEN' });
});

test('public rows, declared full summary and independently checked inventory compose in one snapshot', async () => {
  const f = await fixture();
  await f.store.read(async tx => {
    const snapshotStore = { read: callback => callback(tx) };
    const sourceFacts = (await tx.scan({ prefix: sourcePrefix('t') })).map(row => row.value);
    assert.deepEqual(await readAuthorizedInTransaction(tx, f.catalog, context, query, f.options), { records: expected(sourceFacts) });
    assert.deepEqual(await readAnchorSummaryAuthorizedInTransaction(tx, f.catalog, context, { index: business, partition: ['invoice'] }, f.options), { summary: { total: 20n } });
    assert.equal((await checkStore(snapshotStore, f.catalog, 't')).sources, f.input.length);
  });
  await assert.rejects(summary(f, { index: business, partition: ['invoice'], anchorId: 'hidden-invoice' }), { code: 'INVALID_READ' });
  await assert.rejects(summary(f, { index: owners, partition: ['invoice', 'visible'] }), { code: 'INVALID_READ' });
});

test('R3/R4 ancestor retarget and contributor owner transfer settle tied inventory, public rows and full summary together', async () => {
  const hops = [{ reference: 'issue', target: 'Issue' }, { reference: 'invoice', target: 'Invoice' }];
  const d = definition();
  delete d.models.Entry; delete d.models.Credit; delete d.models.Private;
  d.models.Issue = {
    schema: schema({ invoiceId: { type: 'string' }, day: integer }), access: { orders: {} },
    references: [{ slot: 'invoice', field: 'invoiceId', targets: ['Invoice'], many: false, onDelete: 'reject' }],
  };
  d.models.Tax = {
    schema: schema({ issueId: { type: 'string' }, sequence: integer, amount: integer }), access: { orders: {} },
    references: [{ slot: 'issue', field: 'issueId', targets: ['Issue'], many: false, onDelete: 'reject' }],
    memberships: [
      { ...membership('business', business, [{ path: hops, header: 'id' }]), order: [{ path: [hops[0]], field: 'day' }, field('sequence')] },
      ...['owner-a', 'owner-b'].map(slot => ({ ...membership(slot, owners, [{ path: hops, header: 'id' }, { header: 'owner' }]), order: [{ path: [hops[0]], field: 'day' }, field('sequence')] })),
    ],
  };
  const input = [
    record('A', 'Invoice', 'visible', { name: 'A' }), record('B', 'Invoice', 'visible', { name: 'B' }),
    record('issue', 'Issue', 'visible', { invoiceId: 'A', day: '1' }),
    record('tax-visible', 'Tax', 'visible', { issueId: 'issue', sequence: '2', amount: '100' }),
    record('tax-hidden', 'Tax', 'hidden', { issueId: 'issue', sequence: '2', amount: '-80' }),
    record('tax-reader', 'Tax', 'reader', { issueId: 'issue', sequence: '2', amount: '0' }),
  ];
  const catalog = prepareCatalog(d), store = createMemoryStore();
  await mutate(store, catalog, 't', input.map(put));
  const p = policy();
  for (const node of p.principals) {
    delete node.grants.resources.Entry; delete node.grants.resources.Credit;
    if (node.id === 'visible') node.grants.resources.Tax = ['read'];
  }
  await store.transact(tx => tx.set('fixture-policy', p));
  const options = { getAuthority: async tx => prepareAuthority(catalog, await tx.get('fixture-policy')) };
  const issue = structuredClone(input[2]), tax = structuredClone(input[3]);
  issue.data.invoiceId = 'B'; tax.owner = 'hidden';
  await mutateAuthorized(store, catalog, { tenant: 't', actor: 'admin' }, [put(issue), put(tax)], options);
  await store.read(async tx => {
    const facts = (await tx.scan({ prefix: sourcePrefix('t') })).map(row => row.value), byId = new Map(facts.map(row => [row.id, row]));
    const inventory = facts.filter(row => row.model === 'Tax').map(row => ({ row, invoice: byId.get(byId.get(row.data.issueId).data.invoiceId).id }));
    assert.ok(inventory.every(item => item.invoice === 'B'));
    const visible = inventory.filter(item => ['reader', 'visible'].includes(item.row.owner)).sort((a, b) => Buffer.compare(Buffer.from(a.row.id), Buffer.from(b.row.id))).flatMap(item => [item.row, item.row]);
    const page = await readAuthorizedInTransaction(tx, catalog, context, { model: 'Tax', index: owners, partition: ['B'], limit: 20 }, options);
    assert.deepEqual(page, { records: visible });
    assert.deepEqual(page.records.map(row => row.id), ['tax-reader', 'tax-reader']);
    assert.deepEqual(await readAuthorizedInTransaction(tx, catalog, context, { model: 'Tax', index: owners, partition: ['A'], limit: 20 }, options), { records: [] });
    const total = inventory.reduce((sum, item) => sum + BigInt(item.row.data.amount), 0n);
    assert.deepEqual(await readAnchorSummaryAuthorizedInTransaction(tx, catalog, context, { index: business, partition: ['B'] }, options), { summary: { total } });
    assert.deepEqual(await readAnchorSummaryAuthorizedInTransaction(tx, catalog, context, { index: business, partition: ['A'] }, options), { summary: { total: 0n } });
    assert.equal(JSON.stringify(page).includes('tax-hidden'), false);
    const snapshotStore = { read: callback => callback(tx) };
    assert.equal((await checkStore(snapshotStore, catalog, 't')).occurrences, 9);
  });
});

test('a related record owner path cannot stand in for the contributing record owner role', async () => {
  const d = definition();
  for (const m of d.models.Entry.memberships) if (m.slot.startsWith('owner')) m.partition[1] = { path: [{ reference: 'invoice', target: 'Invoice' }], header: 'owner' };
  const f = await fixture(sources(), d);
  await assert.rejects(read(f), { code: 'INVALID_READ' });
});
