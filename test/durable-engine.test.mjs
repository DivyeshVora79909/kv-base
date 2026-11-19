import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openSqliteStore } from '../src/sqlite.mjs';
import { prepareCatalog, createEngine, settlementOracle, treeAggregate } from '../src/index.mjs';
import { mutateInTransaction } from '../src/mutate.mjs';
import { sourcePrefix, sourceKey } from '../src/keys.mjs';
import { referencePathDefinition, referencePathRecords, invoiceIndex } from '../examples/reference-path.mjs';
import { causalDefinition, causalCalculations, causalDiamond, causalOutput } from '../examples/causal.mjs';

async function database(t) {
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-durable-engine-'));
  const filename = join(directory, 'engine.sqlite');
  const handles = [];
  t.after(async () => { for (const store of handles) await store.close(); await rm(directory, { recursive: true, force: true }); });
  return async () => { const store = await openSqliteStore(filename); handles.push(store); return store; };
}
const put = record => ({ type: 'put', record });

test('durable structural path memberships and rotating generations survive reopen without source rewriting', async t => {
  const open = await database(t), catalog = prepareCatalog(referencePathDefinition), records = referencePathRecords();
  let store = await open(), engine = createEngine({ store, catalog, tenant: 'tenant' });
  await engine.mutate(records.map(put));
  const tax = await engine.read(tx => tx.get(sourceKey('tenant', 'tax')));
  records[2].data.invoice = 'B';
  await engine.mutate([put(records[2])]);
  const rebuilt = await engine.rebuild();
  assert.equal(rebuilt.generation, 1); await store.close();
  store = await open(); engine = createEngine({ store, catalog, tenant: 'tenant' });
  assert.equal((await engine.inspect()).generation, 1);
  assert.deepEqual(await engine.read(tx => tx.get(sourceKey('tenant', 'tax'))), tax);
  await engine.read(async tx => {
    // Direct expected amounts come from the untouched tax and final issue facts.
    assert.equal((await treeAggregate(tx, catalog, { tenant: 'tenant', index: invoiceIndex, partition: ['A'] })).balance, 0n);
    assert.equal((await treeAggregate(tx, catalog, { tenant: 'tenant', index: invoiceIndex, partition: ['B'] })).balance, BigInt(tax.data.amount));
  });
  assert.deepEqual(await engine.check(), { sources: 4, trees: 1, occurrences: 1, references: 2, claims: 0 });
  const before = await engine.read(tx => tx.scan());
  await assert.rejects(engine.rebuild({ beforeCommit() { throw Error('reject activation'); } }), /reject activation/);
  assert.deepEqual(await engine.read(tx => tx.scan()), before);
});

test('durable reactive settlement matches full source recomputation after reopen and subsequent edits', async t => {
  const open = await database(t), catalog = prepareCatalog(causalDefinition, { calculations: causalCalculations() }), records = causalDiamond();
  let store = await open(), engine = createEngine({ store, catalog, tenant: 't' });
  await engine.mutate(records.map(put)); await store.close();
  store = await open(); engine = createEngine({ store, catalog, tenant: 't' });
  records[0].data.amount = '9007199254740993'; await engine.mutate([put(records[0])]);
  const expected = await settlementOracle(catalog, 't', records);
  const actual = await engine.read(async tx => (await tx.scan({ prefix: sourcePrefix('t') })).map(row => row.value));
  assert.deepEqual(actual.filter(record => record.model === 'result').sort((a, b) => a.id < b.id ? -1 : 1), [...expected.generated].sort((a, b) => a.id < b.id ? -1 : 1));
  assert.equal((await engine.read(tx => tx.get(sourceKey('t', causalOutput('D'))))).data.amount, String(BigInt(records[0].data.amount) * 5n));
  await engine.check(); await engine.rebuild(); await store.close();
  store = await open(); engine = createEngine({ store, catalog, tenant: 't' }); await engine.check();
});

test('original durable receipt recovers committed engine mutations after acknowledgement loss and rejects changed requests', async t => {
  const open = await database(t), catalog = prepareCatalog(referencePathDefinition), records = referencePathRecords();
  let store = await open(); let calls = 0;
  const receipt = { id: 'tenant/operator/create-invoice-path', digest: 'source-request-v1' };
  await assert.rejects(store.transact(async tx => {
    calls++; return mutateInTransaction(tx, catalog, 'tenant', records.map(put));
  }, { receipt, afterCommit() { throw Error('lost acknowledgement'); } }), { code: 'COMMIT_UNKNOWN' });
  await store.close(); store = await open();
  const original = await store.getReceipt(receipt.id); assert.ok(original);
  const replay = await store.transact(() => { calls++; throw Error('must not execute again'); }, { receipt });
  assert.deepEqual(replay, original.result); assert.equal(calls, 1);
  await assert.rejects(store.transact(() => { calls++; }, { receipt: { ...receipt, digest: 'different-request' } }), { code: 'RECEIPT_MISMATCH' });
  assert.equal(calls, 1);
  await assert.rejects(store.transact(() => {}, { receipt, verifyReplay() { throw Object.assign(Error('revoked caller'), { code: 'FORBIDDEN' }); } }), { code: 'FORBIDDEN' });
  assert.equal((await createEngine({ store, catalog, tenant: 'tenant' }).check()).sources, 4);
});

test('engine receipts forward hooks and replay checking reads the active derived generation', async t => {
  const open = await database(t), catalog = prepareCatalog(referencePathDefinition), records = referencePathRecords();
  const store = await open(), engine = createEngine({ store, catalog, tenant: 'tenant' });
  await engine.mutate(records.map(put)); await engine.rebuild();
  const receipt = { id: 'engine/active-generation', digest: 'exact-request' };
  let calls = 0;
  await assert.rejects(engine.transact(async tx => {
    calls++; return treeAggregate(tx, catalog, { tenant: 'tenant', index: invoiceIndex, partition: ['A'] });
  }, { receipt, afterCommit() { throw Error('acknowledgement lost'); } }), { code: 'COMMIT_UNKNOWN' });
  const expected = BigInt(records.find(record => record.model === 'Tax').data.amount);
  const original = await engine.getReceipt(receipt.id); assert.equal(original.result.balance, expected);
  const recovered = await engine.transact(() => { calls++; throw Error('duplicate'); }, { receipt,
    verifyReplay: async tx => {
      assert.equal((await treeAggregate(tx, catalog, { tenant: 'tenant', index: invoiceIndex, partition: ['A'] })).balance, expected);
      await assert.rejects(tx.set('forbidden', true), { code: 'READ_ONLY' });
    },
  });
  assert.deepEqual(recovered, original.result); assert.equal(calls, 1);
});
