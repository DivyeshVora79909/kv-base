import test from 'node:test';
import assert from 'node:assert/strict';
import {
  prepareCatalog, resolveIndex, indexId, projectRecord, createMemoryStore,
  mutate, readRoot, treeSelect, treeAggregate, checkStore,
} from '../src/index.mjs';

const field = name => ({ field: name });
const integer = { type: 'string', pattern: '^(0|-?[1-9][0-9]*)$' };
const schema = (properties, required = Object.keys(properties)) => ({ type: 'object', additionalProperties: false, properties, required });
const signature = (partitionFields = ['country', 'year'], orderFields = ['effective']) => ['ledger', partitionFields, orderFields];
function index(partitionFields = ['country', 'year'], orderFields = ['effective']) {
  return {
    version: '1', partitionFields, orderFields,
    roles: Object.fromEntries([...partitionFields, ...orderFields].map(role => [role, ({ country: 'string', year: 'integer', effective: 'integer', anchor: 'id', sourceOwner: 'id' })[role]])),
    inputs: { amount: 'integer' },
    aggregates: { total: { monoid: 'sum', input: 'amount' }, prefix: { monoid: 'prefix', input: 'amount' } },
  };
}
const binding = (slot, index, partition, order, amount) => ({ slot, index, partition, order: [order], inputs: { amount: field(amount) }, onMissing: 'reject' });
function definition() {
  return {
    name: 'structural-witness', version: '1',
    models: {
      ledger: { schema: schema({ name: { type: 'string' }, opening: integer }), indexes: [index(), index(['country'], ['year'])] },
      issue: {
        schema: schema({ nation: { type: 'string' }, period: integer, at: integer, debit: integer, ledgerId: { type: 'string' } }),
        references: [{ slot: 'ledger', field: 'ledgerId', targets: ['ledger'], many: false, onDelete: 'reject' }],
        memberships: [binding('ordinary', signature(), [field('nation'), field('period')], field('at'), 'debit')],
      },
      credit: {
        schema: schema({ jurisdiction: { type: 'string' }, fiscalYear: integer, posted: integer, units: integer, ledgerId: { type: 'string' } }),
        references: [{ slot: 'ledger', field: 'ledgerId', targets: ['ledger'], many: false, onDelete: 'reject' }],
        memberships: [binding('ordinary', signature(), [field('jurisdiction'), field('fiscalYear')], field('posted'), 'units')],
      },
    },
  };
}
const record = (id, model, data, owner = 'one', tenant = 'tenant') => ({ id, model, data, owner, tenant, createdAt: '0' });
function records() {
  return [
    record('ledger-record', 'ledger', { name: 'Ledger', opening: '50' }),
    record('a', 'issue', { nation: 'IN', period: '2026', at: '3', debit: '9007199254740993', ledgerId: 'ledger-record' }),
    record('b', 'credit', { jurisdiction: 'IN', fiscalYear: '2026', posted: '1', units: '-2', ledgerId: 'ledger-record' }, 'two'),
    record('c', 'issue', { nation: 'IN', period: '2027', at: '2', debit: '7', ledgerId: 'ledger-record' }, 'two'),
  ];
}
const put = record => ({ type: 'put', record });
const same = (a, b) => assert.deepEqual(JSON.parse(JSON.stringify(a, (_, value) => typeof value === 'bigint' ? value.toString() : value)), JSON.parse(JSON.stringify(b, (_, value) => typeof value === 'bigint' ? value.toString() : value)));
async function members(tx, catalog, scope) {
  const root = await readRoot(tx, catalog, scope), result = [];
  for (let rank = 0; rank < (root?.size ?? 0); rank++) result.push(await treeSelect(tx, catalog, scope, rank));
  assert.equal(await treeSelect(tx, catalog, scope, result.length), undefined);
  return result;
}

// Expected occurrences are enumerated from fixture facts, with no projection,
// writer, route or tree helper involved in their construction or ordering.
function independentlyEnumerate(sourceRecords, { owner, opening = false } = {}) {
  const rows = [];
  for (const r of sourceRecords) {
    if (owner !== undefined && r.owner !== owner) continue;
    if (r.model === 'ledger') {
      if (opening) rows.push({ sourceId: r.id, slot: 'opening', order: 0n, amount: BigInt(r.data.opening) });
    } else {
      const amount = BigInt(r.model === 'issue' ? r.data.debit : r.data.units);
      const order = BigInt(r.model === 'issue' ? r.data.at : r.data.posted);
      rows.push({ sourceId: r.id, slot: 'owner', order, amount });
      if (r.model === 'issue') rows.push({ sourceId: r.id, slot: 'repeated', order, amount });
    }
  }
  return rows.sort((a, b) => a.order < b.order ? -1 : a.order > b.order ? 1 : a.sourceId < b.sourceId ? -1 : a.sourceId > b.sourceId ? 1 : a.slot < b.slot ? -1 : a.slot > b.slot ? 1 : 0);
}

function ownerDefinition(opening) {
  const d = definition(), ownerIndex = ['ledger', ['anchor', 'sourceOwner'], ['effective']];
  d.models.ledger.indexes.push(index(['anchor', 'sourceOwner']));
  if (opening) d.models.ledger.memberships = [binding('opening', ownerIndex, [{ header: 'id' }, { header: 'owner' }], { constant: '0' }, 'opening')];
  for (const [modelId, amount, at] of [['issue', 'debit', 'at'], ['credit', 'units', 'posted']]) {
    const membership = binding('owner', ownerIndex, [field('ledgerId'), { header: 'owner' }], field(at), amount);
    d.models[modelId].memberships = [membership];
    if (modelId === 'issue') d.models[modelId].memberships.push({ ...membership, slot: 'repeated' });
  }
  return { definition: d, ownerIndex };
}

test('structural identity keeps the declaring model, array order and partition/order boundary', () => {
  const d = definition();
  d.models.ledger.indexes.push(index(['year', 'country']));
  d.models.ledger.indexes.push(index(['country', 'effective'], ['year']));
  d.models.ledger.indexes.push(index(['country'], ['year', 'effective']));
  d.models.ledger.indexes.push(index(['country'], ['effective', 'year']));
  const p = prepareCatalog(d), ordinary = resolveIndex(p, signature());
  assert.notEqual(p.views[ordinary.view].identity, p.views[resolveIndex(p, signature(['year', 'country'])).view].identity);
  assert.notEqual(p.views[ordinary.view].identity, p.views[resolveIndex(p, signature(['country', 'effective'], ['year'])).view].identity);
  assert.notEqual(p.views[resolveIndex(p, signature(['country'], ['year', 'effective'])).view].identity, p.views[resolveIndex(p, signature(['country'], ['effective', 'year'])).view].identity);
  assert.notEqual(indexId(signature()), indexId(signature(['country', 'effective'], ['year'])));
  assert.equal(resolveIndex(p, ['ledger', ['country', 'year'], ['effective']]), ordinary);
  d.models.ledger.indexes[0].partitionFields.reverse();
  assert.deepEqual(ordinary.partitionFields, ['country', 'year']);
  assert.ok(Object.isFrozen(p.definition.models.ledger.indexes[0]));
  assert.throws(() => resolveIndex(p, ['ledger-record', ['country', 'year'], ['effective']]));
});

test('two models bind different local names to one structural declaration with exact arithmetic', () => {
  const p = prepareCatalog(definition()), sources = records();
  const a = projectRecord(p, sources[1]), b = projectRecord(p, sources[2]);
  assert.equal(a.length, 1); assert.equal(b.length, 1);
  assert.equal(a[0].view, b[0].view);
  assert.deepEqual(a[0].partition, ['IN', 2026n]);
  assert.deepEqual(b[0].partition, ['IN', 2026n]);
  assert.equal(a[0].contributions.total, 9007199254740993n);
  assert.equal(b[0].contributions.total, -2n);
  assert.equal(projectRecord(p, sources[0]).length, 0, 'schema ownership creates no implicit opening occurrence');
});

test('preparation rejects incompatible domains, arity, unknown models and conflicting definitions', () => {
  for (const change of [
    d => d.models.issue.memberships[0].partition.pop(),
    d => d.models.issue.memberships[0].order.push(field('at')),
    d => d.models.issue.schema.properties.nation = { type: 'boolean' },
    d => d.models.issue.memberships[0].order = [field('absent')],
    d => d.models.issue.memberships[0].index[0] = 'absent',
    d => d.models.ledger.indexes.push({ ...index(), version: '2' }),
    d => d.models.ledger.indexes[0].roles.effective = 'floating',
    d => d.models.ledger.indexes[0].partitionFields = ['country', 'country'],
    d => d.models.issue.memberships.push(d.models.issue.memberships[0]),
    d => d.models.issue.memberships[0].inputs = {},
  ]) {
    const d = definition(); change(d);
    assert.throws(() => prepareCatalog(d));
  }
});

test('unanchored typed country/year partitions are looked up without a record ID', async () => {
  const catalog = prepareCatalog(definition()), store = createMemoryStore(), sources = records();
  await mutate(store, catalog, 'tenant', sources.toReversed().map(put));
  await store.read(async tx => {
    const scope = { tenant: 'tenant', index: signature(), partition: ['IN', '2026'] };
    const rows = await members(tx, catalog, scope);
    assert.deepEqual(rows.map(r => [r.sourceId, r.slot]), [['b', 'ordinary'], ['a', 'ordinary']]);
    same(await treeAggregate(tx, catalog, scope), { total: 9007199254740991n, prefix: { sum: 9007199254740991n, minPrefix: -2n } });
    assert.equal((await readRoot(tx, catalog, { ...scope, partition: ['IN', '2027'] })).size, 1);
    assert.equal(await readRoot(tx, catalog, { ...scope, partition: ['US', '2026'] }), undefined);
    same(await treeAggregate(tx, catalog, { ...scope, partition: ['US', '2026'] }), { total: 0n, prefix: { sum: 0n, minPrefix: 0n } });
    assert.equal(await readRoot(tx, catalog, { ...scope, tenant: 'other' }), undefined);
  });
  assert.equal((await checkStore(store, catalog, 'tenant')).occurrences, 3);
});

for (const opening of [false, true]) test(`owner partitions preserve repeated stable slots and ${opening ? 'explicit' : 'no implicit'} opening`, async () => {
  const fixture = ownerDefinition(opening), catalog = prepareCatalog(fixture.definition), store = createMemoryStore(), sources = records();
  await mutate(store, catalog, 'tenant', sources.toReversed().map(put));
  await store.read(async tx => {
    const heads = [];
    for (const owner of ['one', 'two']) {
      const scope = { tenant: 'tenant', index: fixture.ownerIndex, partition: ['ledger-record', owner] };
      const expected = independentlyEnumerate(sources, { owner, opening });
      const actual = await members(tx, catalog, scope), root = await readRoot(tx, catalog, scope);
      heads.push(root);
      assert.deepEqual(actual.map(r => [r.sourceId, r.slot]), expected.map(r => [r.sourceId, r.slot]));
      assert.equal(root.size, expected.length);
      assert.equal((await treeAggregate(tx, catalog, scope)).total, expected.reduce((sum, r) => sum + r.amount, 0n));
      assert.equal(actual.filter(r => r.slot === 'opening').length, opening && owner === 'one' ? 1 : 0);
    }
    assert.notEqual(heads[0].sourceId, heads[1].sourceId, 'populated partitions own distinct heads');
  });
  assert.equal((await checkStore(store, catalog, 'tenant')).occurrences, opening ? 6 : 5);
});

test('declaring model owns validators and a rejected final fold rolls back the whole mutation', async () => {
  const d = definition();
  d.models.ledger.indexes[0].validators = [{ scope: 'root', use: 'nonnegative' }];
  const catalog = prepareCatalog(d, { validators: { nonnegative: { version: '1', validate: summary => summary.total >= 0n } } });
  const store = createMemoryStore(), sources = records();
  await mutate(store, catalog, 'tenant', sources.map(put));
  const before = await store.read(tx => tx.scan());
  const negative = structuredClone(sources[1]); negative.data.debit = '-10';
  await assert.rejects(mutate(store, catalog, 'tenant', [put(negative)]));
  assert.deepEqual(await store.read(tx => tx.scan()), before);
  assert.equal((await checkStore(store, catalog, 'tenant')).sources, sources.length);
});
