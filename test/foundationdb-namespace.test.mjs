import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { decodeKvData } from '../src/values.mjs';
const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE, skip = clusterFile ? false : 'Selected real FDB cluster required';
const suffix = (kind, key) => Buffer.concat([Buffer.from([kind]), Buffer.from(key)]);

for (const order of ['parent-first', 'nul-extension-first']) test(`primary FDB exact namespaces survive ${order} initialization and checkpoint restoration`, { skip }, async t => {
  const base = 'exact-namespace-' + randomUUID() + '😀', prefixes = [base, base + '\0', base + '\0\0'], stores = new Map(), originals = new Map();
  const open = async prefix => { const store = await openFoundationDbStore({ clusterFile, prefix }); t.after(() => store.close()); return store; };
  for (const prefix of order === 'parent-first' ? prefixes : prefixes.toReversed()) {
    const store = await open(prefix), expected = { namespace: prefix, exact: 90071992547409931234567n }; stores.set(prefix, store);
    await store.transact(async tx => { await tx.set('source/value', expected); return { original: prefix }; }, { receipt: { id: 'original', digest: prefix } });
  }
  for (const prefix of prefixes) {
    const store = stores.get(prefix), checkpoint = await store.captureCheckpoint(); originals.set(prefix, checkpoint);
    assert.deepEqual(checkpoint.rows.map(([key]) => key), [suffix(0, 'source/value'), suffix(1, 'original'), suffix(2, 'format')]);
    assert.deepEqual(decodeKvData(checkpoint.rows[0][1]), { namespace: prefix, exact: 90071992547409931234567n });
    assert.deepEqual(decodeKvData(checkpoint.rows[1][1]), { id: 'original', digest: prefix, result: { original: prefix } });
    assert.equal(checkpoint.rows[2][1].toString(), 'kv-base-node-v8-1');
  }
  await stores.get(prefixes[2]).transact(tx => tx.set('source/later', 'only deepest namespace'));
  assert.deepEqual(await stores.get(prefixes[0]).captureCheckpoint(), originals.get(prefixes[0])); assert.deepEqual(await stores.get(prefixes[1]).captureCheckpoint(), originals.get(prefixes[1]));
  const targetPrefix = 'exact-restore-' + randomUUID(), child = await open(targetPrefix + '\0'); await child.transact(tx => tx.set('source/child', 'retained child'));
  const childBefore = await child.captureCheckpoint(), target = await open(targetPrefix); await target.restoreCheckpoint(originals.get(prefixes[0]));
  assert.deepEqual(await target.captureCheckpoint(), originals.get(prefixes[0])); assert.deepEqual(await child.captureCheckpoint(), childBefore);
  assert.deepEqual(await target.read(tx => tx.get('source/value')), { namespace: prefixes[0], exact: 90071992547409931234567n }); assert.equal(await target.read(tx => tx.get('source/child')), undefined);
  await target.close(); const reopened = await open(targetPrefix); assert.deepEqual(await reopened.captureCheckpoint(), originals.get(prefixes[0]));
});
