import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog } from '../src/catalog.mjs';
import { prepareAuthority } from '../src/authority.mjs';
import { createMemoryStore } from '../src/memory.mjs';
import { mutate } from '../src/mutate.mjs';
import { mutateAuthorized } from '../src/authorized-mutate.mjs';
import { checkStore } from '../src/check.mjs';
import { sourceKey, incomingPrefix } from '../src/keys.mjs';
import { causalDefinition, causalCalculations, causalDiamond, causalOutput, causalAnchor } from '../examples/causal.mjs';

const schema = { type: 'object', additionalProperties: false,
  properties: { label: { type: 'string' }, target: { type: 'string' }, links: { type: 'array', items: { type: 'string' } } }, required: ['label'] };
const definition = { name: 'authorized-mutation', version: '1', contracts: {}, views: {}, models: {
  item: { schema, access: { orders: {} }, references: [
    { slot: 'target', field: 'target', many: false, targets: ['target', 'secret'], onDelete: 'reject' },
    { slot: 'links', field: 'links', many: true, targets: ['target', 'secret'], onDelete: 'reject' },
  ], uniqueness: [{ slot: 'label', fields: [{ field: 'label' }] }] },
  target: { schema, access: { orders: {} } }, secret: { schema },
  policy: { schema: { type: 'object', additionalProperties: false, properties: { revoked: { type: 'boolean' } }, required: ['revoked'] }, access: { orders: {} } },
} };
const all = ['read', 'create', 'update', 'delete', 'use'];
const principal = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
function graph(actions = all) {
  return { tenant: 't', principals: [
    principal('t', 'tenant', []), principal('admin', 'user', ['t'], {}, true),
    principal('group', 'group', ['t']), principal('u', 'user', ['group'], { item: actions, target: actions, policy: actions }),
    principal('child', 'user', ['u']), principal('peer', 'user', ['group']),
  ] };
}
const context = { tenant: 't', actor: 'u' };
const record = (id, owner = 'u', model = 'item', data = { label: id }) => ({ id, tenant: 't', owner, model, createdAt: '0', data });
const put = record => ({ type: 'put', record }), del = id => ({ type: 'delete', id });
const snapshot = store => store.read(tx => tx.scan());
async function setup(records = [], authority = graph()) {
  const catalog = prepareCatalog(definition), store = createMemoryStore();
  if (records.length) await mutate(store, catalog, 't', records.map(put));
  await store.transact(tx => tx.set('fixture/authority', authority));
  const options = { getAuthority: async tx => prepareAuthority(catalog, await tx.get('fixture/authority')) };
  const write = (changes, extra = {}) => mutateAuthorized(store, catalog, context, changes, { ...options, ...extra });
  return { catalog, store, options, write };
}
async function rejectsAtomic(store, action, code = 'FORBIDDEN') {
  const before = await snapshot(store);
  await assert.rejects(action, { code });
  assert.deepEqual(await snapshot(store), before);
}

test('authorized command matrix uses hand-drawn owners and per-action grants', async () => {
  for (const action of ['create', 'update', 'delete']) for (const owner of ['u', 'child', 'group', 't', 'peer']) {
    const existing = record('existing', owner);
    const { store, write, catalog } = await setup(action === 'create' ? [] : [existing], graph([action]));
    const changes = action === 'create' ? [put(existing)] : action === 'delete' ? [del(existing.id)] : [put({ ...existing, data: { label: 'edited' } })];
    const expected = owner === 'u' || owner === 'child' || (owner === 'group' && action !== 'delete');
    if (expected) { assert.deepEqual(await write(changes), { applied: 1 }); await checkStore(store, catalog, 't'); }
    else await rejectsAtomic(store, () => write(changes));
  }
});

test('missing action grants and private models deny even an administrator', async () => {
  const { store, write } = await setup([record('existing')], graph(['read']));
  for (const changes of [[put(record('new'))], [put(record('existing'))], [del('existing')]]) await rejectsAtomic(store, () => write(changes));
  const adminGraph = graph(); adminGraph.principals.find(p => p.id === 'u').grants = { admin: true, resources: {} };
  const admin = await setup([record('private', 'u', 'secret')], adminGraph);
  await rejectsAtomic(admin.store, () => admin.write([put(record('new-private', 'u', 'secret'))]));
  await rejectsAtomic(admin.store, () => admin.write([del('private')]));
  await rejectsAtomic(admin.store, () => admin.write([put(record('outside', 'peer'))]));
});

test('owner transfers require both owners before immutable-header diagnostics', async () => {
  for (const [from, to, allowed] of [['u', 'child', true], ['child', 'group', true], ['u', 'peer', false], ['peer', 'u', false]]) {
    const existing = record('existing', from), { store, write, catalog } = await setup([existing]);
    const next = { ...existing, owner: to };
    if (allowed) { await write([put(next)]); await checkStore(store, catalog, 't'); }
    else {
      next.createdAt = '9';
      await rejectsAtomic(store, () => write([put(next)]));
    }
  }
});

test('missing and inaccessible deletes reject alike and cross-tenant puts deny', async () => {
  const { store, write } = await setup([record('hidden', 'peer')]);
  for (const changes of [[del('missing')], [del('hidden')], [put({ ...record('new'), tenant: 'other' })]]) await rejectsAtomic(store, () => write(changes));
});

test('forward reference creation and valid retargets check final state in either command order', async () => {
  for (const reverse of [false, true]) {
    const { store, write, catalog } = await setup();
    const target = record('target-a', 'child', 'target'), child = record('child-a', 'u', 'item', { label: 'first', target: target.id });
    await write((reverse ? [target, child] : [child, target]).map(put));
    const second = record('target-b', 'group', 'target');
    const next = { ...child, data: { ...child.data, target: second.id } };
    await write((reverse ? [second, next] : [next, second]).map(put));
    assert.equal((await store.read(tx => tx.scan({ prefix: incomingPrefix(catalog, 't', 'target-b') })))[0].value.sourceId, child.id);
    await checkStore(store, catalog, 't');
  }
});

test('added references require explicit target Use, including for admin', async () => {
  for (const admin of [false, true]) {
    const authority = graph();
    const actor = authority.principals.find(p => p.id === 'u');
    actor.grants.admin = admin; actor.grants.resources.target = ['read', 'update'];
    const { store, write } = await setup([record('target', 'u', 'target')], authority);
    await rejectsAtomic(store, () => write([put(record('linked', 'u', 'item', { label: 'linked', target: 'target' }))]));
  }
});

test('retargeting rejects hidden, private and missing targets without publishing earlier commands', async () => {
  const parent = record('visible', 'u', 'target'), item = record('item', 'u', 'item', { label: 'item', target: 'visible' });
  const { store, write } = await setup([parent, item, record('hidden', 'peer', 'target'), record('private', 'u', 'secret')]);
  for (const target of ['hidden', 'private', 'missing']) await rejectsAtomic(store, () => write([
    put(record('otherwise-valid')), put({ ...item, data: { ...item.data, target } }),
  ]));
});

test('reference removal and unchanged positions do not require newly acquiring Use', async () => {
  const item = record('item', 'u', 'item', { label: 'item', target: 'hidden', links: ['hidden'] });
  const { store, write, catalog } = await setup([record('hidden', 'peer', 'target'), item], graph(['update']));
  await write([put({ ...item, data: { ...item.data, label: 'changed' } })]);
  await write([put({ ...item, data: { label: 'removed' } })]);
  await checkStore(store, catalog, 't');
});

test('array references authorize newly occupied positions independently', async () => {
  const item = record('item', 'u', 'item', { label: 'item', links: ['visible', 'hidden'] });
  const { store, write } = await setup([record('visible', 'u', 'target'), record('hidden', 'peer', 'target'), item]);
  await rejectsAtomic(store, () => write([put({ ...item, data: { ...item.data, links: ['hidden', 'visible'] } })]));
  await write([put({ ...item, data: { ...item.data, links: ['visible'] } })]);
});

test('final authority revocation rolls back authored changes and protected policy reads', async () => {
  const policy = record('policy', 'u', 'policy', { revoked: false }), existing = record('existing');
  const { store, catalog } = await setup([policy, existing]);
  const seen = [];
  const getAuthority = async tx => {
    seen.push(tx);
    const state = await tx.get(sourceKey('t', 'policy')), authority = graph();
    if (state.data.revoked) authority.principals.find(p => p.id === 'u').grants.resources.item = ['read'];
    return prepareAuthority(catalog, authority);
  };
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [
    put({ ...existing, data: { label: 'edited' } }), put({ ...policy, data: { revoked: true } }),
  ], { getAuthority }));
  assert.equal(seen.length, 2); assert.equal(seen[0], seen[1]);
  await checkStore(store, catalog, 't');
});

test('final scope loss rejects both original and proposed owner checks', async () => {
  const existing = record('existing', 'child'), { store, catalog } = await setup([existing]);
  let calls = 0;
  const getAuthority = async () => {
    const authority = graph();
    if (++calls === 2) authority.principals.find(p => p.id === 'child').parents = ['t'];
    return prepareAuthority(catalog, authority);
  };
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [put({ ...existing, owner: 'u' })], { getAuthority }));
  assert.equal(calls, 2);
});

test('final Use loss rejects after successful staged reference and uniqueness repairs', async () => {
  const target = record('target', 'u', 'target'), { store, catalog } = await setup([target]);
  let calls = 0;
  const getAuthority = async () => {
    const authority = graph();
    if (++calls === 2) authority.principals.find(p => p.id === 'u').grants.resources.target = ['read'];
    return prepareAuthority(catalog, authority);
  };
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [put(record('item', 'u', 'item', { label: 'item', target: 'target' }))], { getAuthority }));
});

test('authorized joint uniqueness swaps and deletes preserve final-state semantics', async () => {
  const a = record('a'), b = record('b'), parent = record('parent', 'u', 'target');
  const child = record('child', 'u', 'item', { label: 'child', target: 'parent' });
  const { store, write, catalog } = await setup([a, b, parent, child]);
  await write([put({ ...a, data: b.data }), put({ ...b, data: a.data })]);
  await rejectsAtomic(store, () => write([del('parent')]), 'DELETE_RESTRICTED');
  await write([del('parent'), del('child')]);
  await rejectsAtomic(store, () => write([put({ ...a, data: { label: 'b' } }), put({ ...b, data: { label: 'b' } })]), 'DUPLICATE_CLAIM');
  await checkStore(store, catalog, 't');
});

test('authorized no-op preserves guards, returns no derived metadata and writes nothing', async () => {
  const existing = record('existing'), { store, catalog, options } = await setup([existing]);
  let evidence, writes;
  const getAuthority = async tx => { const authority = await options.getAuthority(tx); evidence = tx.inspect(); return authority; };
  const result = await mutateAuthorized(store, catalog, context, [put(existing)], { getAuthority, beforeCommit: event => { writes = event.changes; } });
  assert.deepEqual(result, { applied: 1 }); assert.deepEqual(writes, []);
  for (const key of ['fixture/authority', sourceKey('t', 'existing')]) assert.ok(evidence.guards.some(guard => guard.key === key));
});

test('input data is detached before awaiting authority and malformed controls fail closed', async () => {
  const { store, catalog, options } = await setup();
  const input = record('new');
  await mutateAuthorized(store, catalog, context, [put(input)], { getAuthority: async tx => {
    input.owner = 'peer'; input.data.label = 'substituted'; return options.getAuthority(tx);
  } });
  assert.deepEqual(await store.read(tx => tx.get(sourceKey('t', 'new'))), record('new'));
  await assert.rejects(mutateAuthorized(store, catalog, context, [], {}), { code: 'INVALID_AUTHORITY' });
  const poisoned = Object.defineProperty({}, 'getAuthority', { enumerable: true, get() { throw new Error('accessor executed'); } });
  await assert.rejects(mutateAuthorized(store, catalog, context, [], poisoned), { code: 'INVALID_MUTATION' });
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [put(record('new')), del('new')], options), 'DUPLICATE_SOURCE');
});

test('privileged hidden dependent settlement stays atomic and result exposes only authored count', async () => {
  const declared = structuredClone(causalDefinition);
  declared.models.fact.access = { orders: {} };
  const catalog = prepareCatalog(declared, { calculations: causalCalculations() }), store = createMemoryStore();
  const records = causalDiamond(); records[0].owner = 'u';
  await mutate(store, catalog, 't', records.map(put));
  const authority = graph();
  for (const principal of authority.principals) principal.grants.resources = {};
  authority.principals.find(p => p.id === 'u').grants.resources.fact = ['update'];
  const prepared = prepareAuthority(catalog, authority), getAuthority = async () => prepared;
  const edited = { ...records[0], data: { ...records[0].data, amount: '5' } };
  assert.deepEqual(await mutateAuthorized(store, catalog, context, [put(edited)], { getAuthority }), { applied: 1 });
  assert.equal((await store.read(tx => tx.get(sourceKey('t', causalOutput('D'))))).data.amount, '25');
  await checkStore(store, catalog, 't');
  let calls = 0;
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [put(records[0])], { getAuthority: async () => {
    if (++calls === 1) return prepared;
    const revoked = structuredClone(authority); revoked.principals.find(p => p.id === 'u').grants.resources.fact = [];
    return prepareAuthority(catalog, revoked);
  } }));
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [del(causalOutput('D'))], { getAuthority }));
  await checkStore(store, catalog, 't');
});

test('commit faults discard the authorized source, references and claim changes together', async () => {
  const { store, write } = await setup([record('target', 'u', 'target')]);
  const before = await snapshot(store);
  await assert.rejects(write([put(record('new', 'u', 'item', { label: 'new', target: 'target' }))], { beforeCommit: () => { throw new Error('commit fault'); } }), /commit fault/);
  assert.deepEqual(await snapshot(store), before);
});

function generatedReferenceFixture(changeOwner = false) {
  const declared = structuredClone(causalDefinition), calculations = causalCalculations();
  for (const model of ['fact', 'anchor', 'result']) declared.models[model].access = { orders: {} };
  declared.models.item = { schema, access: { orders: {} }, references: [
    { slot: 'target', field: 'target', targets: ['result'], many: false, onDelete: 'reject' },
  ] };
  if (changeOwner) {
    const resolve = calculations.derive.resolve;
    calculations.derive.resolve = async args => {
      const result = await resolve(args);
      if (args.record.data.factor === '3') result.outputs[0].owner = 'peer';
      return result;
    };
  }
  const catalog = prepareCatalog(declared, { calculations }), store = createMemoryStore(), authority = graph();
  for (const node of authority.principals) node.grants.resources = {};
  authority.principals.find(p => p.id === 'u').grants.resources = { fact: all, anchor: all, item: all, result: ['use'] };
  const getAuthority = async () => prepareAuthority(catalog, authority);
  return { catalog, store, authority, getAuthority };
}

test('references to newly generated targets are authorized on settled rows in either order', async () => {
  for (const reverse of [false, true]) {
    const { catalog, store, authority, getAuthority } = generatedReferenceFixture();
    const anchor = { ...causalAnchor('A', []), owner: 'u' };
    const item = record('item', 'u', 'item', { label: 'item', target: causalOutput('A') });
    const changes = (reverse ? [anchor, item] : [item, anchor]).map(put);
    authority.principals.find(p => p.id === 'u').grants.resources.result = [];
    await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, changes, { getAuthority }));
    authority.principals.find(p => p.id === 'u').grants.resources.result = ['use'];
    await mutateAuthorized(store, catalog, context, changes, { getAuthority });
    await checkStore(store, catalog, 't');
    authority.principals.find(p => p.id === 'u').grants.resources.result = all;
    await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [put(record(causalOutput('B'), 'u', 'result', { amount: '0', group: 'B', at: '1' }))], { getAuthority }), 'INVALID_CALCULATION');
    await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [del(causalOutput('A'))], { getAuthority }), 'INVALID_CALCULATION');
  }
});

test('retarget authorization inspects settled generated target owner and aborts its transfer', async () => {
  const { catalog, store, getAuthority } = generatedReferenceFixture(true);
  const anchor = { ...causalAnchor('A', [], '2'), owner: 'u' };
  await mutateAuthorized(store, catalog, context, [put(anchor)], { getAuthority });
  const item = record('item', 'u', 'item', { label: 'item', target: causalOutput('A') });
  await rejectsAtomic(store, () => mutateAuthorized(store, catalog, context, [
    put(item), put({ ...anchor, data: { ...anchor.data, factor: '3' } }),
  ], { getAuthority }));
  assert.equal((await store.read(tx => tx.get(sourceKey('t', causalOutput('A'))))).owner, 'u');
  await checkStore(store, catalog, 't');
});
