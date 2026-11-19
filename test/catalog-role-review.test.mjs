import test from 'node:test';
import assert from 'node:assert/strict';
import {
  prepareCatalog, createMemoryStore, mutate, readRoot, treeSelect, treeAggregate,
} from '../src/index.mjs';

const exactInteger = { type: 'string', pattern: '^(0|-?[1-9][0-9]*)$' };
const signature = ['Ledger', ['group'], ['effective']];
const field = name => ({ field: name });

function definition(at) {
  return {
    name: 'catalog-role-review', version: '1',
    models: {
      Ledger: {
        schema: { type: 'object', properties: {}, additionalProperties: false },
        indexes: [{
          version: '1', partitionFields: ['group'], orderFields: ['effective'],
          roles: { group: 'string', effective: 'integer' }, inputs: { amount: 'integer' },
          aggregates: { total: { monoid: 'sum', input: 'amount' } },
        }],
      },
      Entry: {
        schema: {
          type: 'object', additionalProperties: false,
          properties: { group: { type: 'string' }, at, amount: exactInteger },
          required: ['group', 'at', 'amount'],
          $defs: { exactInteger, flag: { type: 'boolean' } },
        },
        memberships: [{
          slot: 'ordinary', index: signature, partition: [field('group')],
          order: [field('at')], inputs: { amount: field('amount') }, onMissing: 'reject',
        }],
      },
    },
  };
}

function record(id, at, amount) {
  return { id, tenant: 'tenant', owner: 'owner', model: 'Entry', createdAt: '0', data: { group: 'IN', at, amount } };
}

test('an inline boolean field cannot bind to an exact integer role', () => {
  assert.throws(() => prepareCatalog(definition({ type: 'boolean' })), { code: 'INVALID_CATALOG' });
});

test('a local exact integer reference participates with independent exact order and fold', async () => {
  const catalog = prepareCatalog(definition({ $ref: '#/$defs/exactInteger' }));
  const store = createMemoryStore();
  // Expected order and sum come directly from these source facts, without catalog projection.
  const sources = [record('later', '9007199254740993', '9007199254740993'), record('earlier', '-2', '-7')];
  await mutate(store, catalog, 'tenant', sources.map(record => ({ type: 'put', record })));
  await store.read(async tx => {
    const scope = { tenant: 'tenant', index: signature, partition: ['IN'] };
    assert.equal((await readRoot(tx, catalog, scope)).size, 2);
    assert.deepEqual([await treeSelect(tx, catalog, scope, 0), await treeSelect(tx, catalog, scope, 1)].map(row => [row.sourceId, row.order]), [
      ['earlier', [-2n]], ['later', [9007199254740993n]],
    ]);
    assert.equal((await treeAggregate(tx, catalog, scope)).total, 9007199254740986n);
  });
  await assert.rejects(mutate(store, catalog, 'tenant', [{ type: 'put', record: record('noncanonical', '01', '5') }]), { code: 'INVALID_RECORD' });
  await assert.rejects(mutate(store, catalog, 'tenant', [{ type: 'put', record: record('boolean', true, '5') }]), { code: 'INVALID_RECORD' });
});

test('a local boolean reference cannot bind to an exact integer role at preparation', () => {
  assert.throws(() => prepareCatalog(definition({ $ref: '#/$defs/flag' })), { code: 'INVALID_CATALOG' });
});

test('local reference chains and escaped JSON pointers preserve incompatible terminal types', () => {
  const d = definition({ $ref: '#/$defs/first' });
  d.models.Entry.schema.$defs.first = { $ref: '#/$defs/flag~1alias~0' };
  d.models.Entry.schema.$defs['flag/alias~'] = { $ref: '#/$defs/flag' };
  assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  d.models.Entry.schema.$defs['flag/alias~'] = { $ref: '#/$defs/exactInteger' };
  assert.ok(prepareCatalog(d));
});

test('allOf intersections, anyOf alternatives and reference siblings reject impossible role types', () => {
  for (const at of [
    { allOf: [{ $ref: '#/$defs/flag' }] },
    { allOf: [{ $ref: '#/$defs/exactInteger' }, { type: 'boolean' }] },
    { anyOf: [{ $ref: '#/$defs/flag' }, { type: 'integer' }] },
    { oneOf: [{ $ref: '#/$defs/flag' }, { type: 'array' }] },
    { $ref: '#/$defs/flag', type: 'string' },
    { $ref: '#/$defs/impossible' },
  ]) {
    const d = definition(at); d.models.Entry.schema.$defs.impossible = false;
    assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  }
});

test('referenced compositions permit canonical strings and leave value predicates to Ajv', async () => {
  for (const at of [
    { allOf: [{ $ref: '#/$defs/exactInteger' }, { type: 'string', pattern: '^[1-9][0-9]*$' }] },
    { anyOf: [{ $ref: '#/$defs/flag' }, { $ref: '#/$defs/exactInteger' }] },
    { oneOf: [{ $ref: '#/$defs/flag' }, { $ref: '#/$defs/exactInteger' }] },
  ]) {
    const d = definition({ $ref: '#/$defs/composed' }); d.models.Entry.schema.$defs.composed = at;
    const catalog = prepareCatalog(d), store = createMemoryStore();
    await mutate(store, catalog, 'tenant', [{ type: 'put', record: record('entry', '9007199254740993', '7') }]);
    await store.read(async tx => {
      const scope = { tenant: 'tenant', index: signature, partition: ['IN'] };
      assert.deepEqual((await treeSelect(tx, catalog, scope, 0)).order, [9007199254740993n]);
      assert.equal((await treeAggregate(tx, catalog, scope)).total, 7n);
    });
    await assert.rejects(mutate(store, catalog, 'tenant', [{ type: 'put', record: record('bad', '01', '7') }]), { code: 'INVALID_RECORD' });
  }
});

test('referenced incompatibilities are checked in partition, order and input role positions', () => {
  for (const position of ['partition', 'order', 'input']) {
    const d = definition({ $ref: '#/$defs/exactInteger' });
    if (position === 'partition') d.models.Entry.schema.properties.group = { $ref: '#/$defs/flag' };
    if (position === 'order') d.models.Entry.schema.properties.at = { $ref: '#/$defs/flag' };
    if (position === 'input') d.models.Entry.schema.properties.amount = { $ref: '#/$defs/flag' };
    assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  }
  const d = definition({ $ref: '#/$defs/flag' });
  d.models.Ledger.indexes[0].roles.effective = 'boolean';
  assert.ok(prepareCatalog(d), 'the referenced boolean family is compatible with a boolean role');
});

test('nested $id resources preserve their own local reference base', () => {
  const d = definition({ $ref: '#/$defs/local' });
  d.models.Entry.schema.$id = 'https://schemas.example.test/entry';
  d.models.Entry.schema.$defs.local = {
    $id: 'nested', allOf: [{ $ref: '#/$defs/value' }], $defs: { value: { type: 'boolean' } },
  };
  assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  d.models.Entry.schema.$defs.local.$defs.value = exactInteger;
  assert.ok(prepareCatalog(d));
});

test('relative root and anonymous nested $id resources preserve standard reference resolution', () => {
  for (const rootId of [undefined, 'schemas/entry']) {
    const d = definition({ $ref: '#/$defs/local' });
    if (rootId !== undefined) d.models.Entry.schema.$id = rootId;
    d.models.Entry.schema.$defs.local = {
      $id: 'nested', allOf: [{ $ref: '#/$defs/value' }], $defs: { value: { type: 'boolean' } },
    };
    assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
    d.models.Entry.schema.$defs.local.$defs.value = exactInteger;
    assert.ok(prepareCatalog(d));
  }
});

test('registered model schemas resolve references independently of declaration order', () => {
  const d = definition({ $ref: 'https://schemas.example.test/shared#/$defs/value' });
  d.models.Shared = {
    schema: {
      $id: 'https://schemas.example.test/shared', type: 'object', properties: {},
      additionalProperties: false, $defs: { value: { type: 'boolean' } },
    },
  };
  assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  d.models.Shared.schema.$defs.value = exactInteger;
  assert.ok(prepareCatalog(d));
});

test('a late-declared reference-path terminal is checked through its own schema references', async () => {
  const d = definition({ type: 'boolean' });
  d.models.Entry.schema.properties.targetId = { type: 'string' };
  d.models.Entry.schema.required.push('targetId');
  d.models.Entry.references = [{ slot: 'target', field: 'targetId', targets: ['Later'], many: false, onDelete: 'reject' }];
  d.models.Entry.memberships[0].order = [{ path: [{ reference: 'target', target: 'Later' }], field: 'at' }];
  d.models.Later = {
    schema: {
      type: 'object', properties: { at: { $ref: '#/$defs/value' } }, required: ['at'],
      additionalProperties: false, $defs: { value: { type: 'boolean' } },
    },
  };
  assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
  d.models.Later.schema.$defs.value = exactInteger;
  const catalog = prepareCatalog(d), store = createMemoryStore();
  const entry = record('entry', true, '11'); entry.data.targetId = 'target';
  const target = { id: 'target', tenant: 'tenant', owner: 'owner', model: 'Later', createdAt: '0', data: { at: '9007199254740993' } };
  await mutate(store, catalog, 'tenant', [{ type: 'put', record: entry }, { type: 'put', record: target }]);
  await store.read(async tx => {
    const scope = { tenant: 'tenant', index: signature, partition: ['IN'] };
    assert.deepEqual((await treeSelect(tx, catalog, scope, 0)).order, [9007199254740993n]);
    assert.equal((await treeAggregate(tx, catalog, scope)).total, 11n);
  });
});

test('typed reference declarations reject incompatible schemas behind local references', () => {
  for (const many of [false, true]) {
    const d = definition({ $ref: '#/$defs/exactInteger' });
    d.models.Entry.schema.properties.targetId = { $ref: '#/$defs/flag' };
    d.models.Entry.references = [{ slot: 'target', field: 'targetId', targets: ['Ledger'], many, onDelete: 'reject' }];
    assert.throws(() => prepareCatalog(d), { code: 'INVALID_CATALOG' });
    d.models.Entry.schema.$defs.flag = many ? { type: 'array', items: { type: 'string' } } : { type: 'string' };
    assert.ok(prepareCatalog(d));
  }
});
