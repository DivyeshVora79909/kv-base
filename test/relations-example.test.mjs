import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, prepareCatalog, mutate, checkStore } from '../src/index.mjs';
import { incomingPrefix } from '../src/keys.mjs';
import { relationsDefinition, relationsRecords } from '../examples/relations.mjs';

const put = record => ({ type: 'put', record });
const remove = id => ({ type: 'delete', id });
const snapshot = store => store.read(tx => tx.scan());
// Internal metadata inspection for verification; this is not a client read API.
const incoming = (store, catalog, id) => store.read(tx => tx.scan({ prefix: incomingPrefix(catalog, 'tenant', id) }));
async function fixture() {
  const catalog = prepareCatalog(relationsDefinition), store = createMemoryStore(), records = relationsRecords();
  await mutate(store, catalog, 'tenant', records.map(put));
  return { catalog, store, records };
}

test('relation fixture keeps one junction fact and derives both incoming directions', async () => {
  const { catalog, store, records } = await fixture();
  const junction = records.find(record => record.id === 'alice-project');
  assert.deepEqual(junction.data, { person: 'alice', project: 'project' });
  for (const id of Object.values(junction.data)) {
    assert.ok((await incoming(store, catalog, id)).some(({ value }) => value.sourceId === junction.id));
  }
  assert.deepEqual((await incoming(store, catalog, 'team-a')).map(({ value }) => value.sourceId).sort(), ['alice', 'bob']);
  assert.deepEqual(await checkStore(store, catalog, 'tenant'), { sources: 8, trees: 0, occurrences: 0, references: 7, claims: 3 });
});

test('pair and profile uniqueness reject duplicates atomically', async () => {
  const { catalog, store, records } = await fixture();
  const before = await snapshot(store);
  for (const id of ['alice-project', 'alice-profile']) {
    const original = records.find(record => record.id === id);
    await assert.rejects(mutate(store, catalog, 'tenant', [put({ ...original, id: `${id}-duplicate` })]), { code: 'DUPLICATE_CLAIM' });
    assert.deepEqual(await snapshot(store), before);
  }
  // Bob has no profile: scalar uniqueness promises at most one, not universal existence.
  assert.equal((await checkStore(store, catalog, 'tenant')).sources, 8);
});

test('retarget and joint deletion validate the final graph and roll back rejected guards', async () => {
  const { catalog, store, records } = await fixture();
  const alice = records.find(record => record.id === 'alice');
  const bob = records.find(record => record.id === 'bob');
  await mutate(store, catalog, 'tenant', [put({ ...alice, data: { ...alice.data, team: 'team-b' } })]);
  assert.deepEqual((await incoming(store, catalog, 'team-b')).map(({ value }) => value.sourceId), ['alice']);
  const before = await snapshot(store);
  await assert.rejects(mutate(store, catalog, 'tenant', [remove('team-a')]), { code: 'DELETE_RESTRICTED' });
  assert.deepEqual(await snapshot(store), before);
  // The deletion may appear first; Bob's final reference is repaired in this transaction.
  await mutate(store, catalog, 'tenant', [remove('team-a'), put({ ...bob, data: { ...bob.data, team: 'team-b' } })]);
  assert.equal((await incoming(store, catalog, 'team-a')).length, 0);
  await mutate(store, catalog, 'tenant', [remove('project'), remove('alice-project'), remove('bob-project')]);
  assert.deepEqual(await checkStore(store, catalog, 'tenant'), { sources: 4, trees: 0, occurrences: 0, references: 3, claims: 1 });
});
