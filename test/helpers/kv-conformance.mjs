import test from 'node:test';
import assert from 'node:assert/strict';

// Semantic cases carried from memory/SQLite witnesses and the predecessor's
// storage/conformance.rs. Expectations come from literal source facts and byte
// sorting, never adapter guard inventories or engine-derived expected state.
const namespaces = ['source/', 'derived/', 'reference/', 'route/', 'authority/', 'application/'];
const compare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
const image = rows => [...rows].map(([key, value]) => ({ key, value })).sort((a, b) => compare(a.key, b.key));
const snapshot = store => store.read(tx => tx.scan());
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const seed = (store, rows) => store.transact(async tx => { for (const [key, value] of rows) await tx.set(key, value); });

export function registerKvConformance(name, open, { receipts = false } = {}) {
  const run = (label, callback) => test(`${name} shared conformance: ${label}`, async t => callback(await open(t), t));
  run('exact detached values and ordered duplicate/missing batch reads', async store => {
    const cycle = { n: 90071992547409931234567n }; cycle.self = cycle;
    const source = { cycle, bytes: Uint8Array.from([0, 255, 1]), map: new Map([['x', 8n]]) };
    await store.transact(async tx => {
      await tx.set('source/a', source); source.bytes[0] = 9;
      const [a, missing, duplicate] = await tx.getMany(['source/a', 'source/absent', 'source/a']);
      assert.equal(missing, undefined); assert.deepEqual(a, duplicate); a.bytes[1] = 0;
      assert.equal((await tx.get('source/a')).bytes[1], 255);
    });
    const actual = await store.read(tx => tx.get('source/a'));
    assert.equal(actual.cycle.self, actual.cycle); assert.equal(actual.cycle.n, 90071992547409931234567n);
    assert.deepEqual([...actual.bytes], [0, 255, 1]); assert.deepEqual(actual.map, new Map([['x', 8n]]));
  });
  run('callback and hook abort every application namespace atomically', async store => {
    const expected = namespaces.map(prefix => [prefix + 'base', { n: 1n }]); await seed(store, expected);
    for (const phase of ['callback', 'hook']) {
      await assert.rejects(store.transact(async tx => {
        for (const prefix of namespaces) { await tx.delete(prefix + 'base'); await tx.set(prefix + 'new', 9n); }
        if (phase === 'callback') throw Error('abort');
      }, { beforeCommit() { throw Error('abort'); } }), /abort/);
      assert.deepEqual(await snapshot(store), image(expected));
    }
    await seed(store, [['application/progress', true]]); assert.equal(await store.read(tx => tx.get('application/progress')), true);
  });
  run('paged staged insert replace delete and recreated keys match a source image', async store => {
    const expected = new Map(Array.from({ length: 150 }, (_, i) => [`page/${String(i).padStart(3, '0')}`, i]));
    await seed(store, expected);
    await store.transact(async tx => {
      for (let i = 0; i < 80; i++) { const key = `page/${String(i).padStart(3, '0')}`; await tx.delete(key); expected.delete(key); }
      for (let i = 130; i < 220; i++) { const key = `page/${String(i).padStart(3, '0')}`; await tx.set(key, i * 2); expected.set(key, i * 2); }
      await tx.set('page/001', -1); expected.set('page/001', -1);
      for (const limit of [0, 1, 64, 1000]) assert.deepEqual(await tx.scan({ prefix: 'page/', limit }), image(expected).slice(0, limit));
      assert.deepEqual(await tx.getMany(['page/001', 'page/002', 'page/149', 'page/001']), [-1, undefined, 298, -1]);
    });
    assert.deepEqual(await snapshot(store), image(expected));
  });
  run('no-op writes retain present absent and empty range guards', async store => {
    await seed(store, [['source/a', { n: 1n }]]); let published;
    await store.transact(async tx => {
      await tx.set('source/a', { n: 1n }); await tx.delete('source/missing');
      assert.equal(await tx.get('source/missing'), undefined); assert.deepEqual(await tx.scan({ prefix: 'empty/' }), []);
      const witness = tx.inspect();
      assert.ok(witness.guards.some(g => g.type === 'point' && g.key === 'source/a' && g.exists));
      assert.ok(witness.guards.some(g => g.type === 'point' && g.key === 'source/missing' && !g.exists));
      assert.ok(witness.guards.some(g => g.type === 'range' && g.prefix === 'empty/'));
    }, { beforeCommit: ({ changes }) => { published = changes; } });
    assert.deepEqual(published, []); assert.deepEqual(await snapshot(store), [{ key: 'source/a', value: { n: 1n } }]);
  });
  run('UTF8 NUL and bounded ordering follows independent byte comparisons', async store => {
    const keys = ['a', 'a\0', 'a\0x', 'a1', 'a2', 'z', '\uE000', '😀']; await seed(store, keys.map(key => [key, key]));
    for (const range of [{}, { prefix: 'a\0' }, { prefix: 'a', gt: 'a', lte: 'a2', limit: 2 }, { gte: 'a\0', lt: 'z' }, { gt: 'z', limit: 1 }]) {
      const expected = keys.filter(key => (range.prefix === undefined || key.startsWith(range.prefix))
        && (range.gt === undefined || compare(key, range.gt) > 0) && (range.gte === undefined || compare(key, range.gte) >= 0)
        && (range.lt === undefined || compare(key, range.lt) < 0) && (range.lte === undefined || compare(key, range.lte) <= 0)).sort(compare).slice(0, range.limit ?? Infinity);
      assert.deepEqual((await store.read(tx => tx.scan(range))).map(row => row.key), expected);
    }
  });
  run('invalid values abort and readonly nested closed facades reject', async store => {
    for (const value of [new SharedArrayBuffer(4), new Map([['x', new SharedArrayBuffer(4)]]), () => {}]) {
      await assert.rejects(store.transact(async tx => { await tx.set('source/partial', true); await tx.set('source/invalid', value); }));
      assert.equal(await store.read(tx => tx.get('source/partial')), undefined);
    }
    let saved;
    await store.read(async tx => { saved = tx; await assert.rejects(tx.set('x', 1), { code: 'READ_ONLY' }); await assert.rejects(store.read(() => {}), { code: 'NESTED_TRANSACTION' }); });
    await assert.rejects(saved.get('x'), { code: 'TRANSACTION_CLOSED' });
    for (const key of ['', '\uD800', 1]) await assert.rejects(store.read(tx => tx.get(key)), { code: 'INVALID_KEY' });
    await assert.rejects(store.read(tx => tx.scan({ limit: -1 })), { code: 'INVALID_RANGE' });
  });
  if (receipts) run('original receipt digest replay authorization and unknown acknowledgement', async store => {
    const receipt = { id: 'original', digest: 'request-one' }; let calls = 0;
    await assert.rejects(store.transact(async tx => {
      calls++; for (const prefix of namespaces) await tx.set(prefix + 'once', 1n); return { applied: 1n };
    }, { receipt, afterCommit() { throw Error('lost acknowledgement'); } }), { code: 'COMMIT_UNKNOWN' });
    const original = await store.getReceipt(receipt.id); assert.deepEqual(original, { ...receipt, result: { applied: 1n } });
    assert.deepEqual(await store.transact(() => { calls++; throw Error('duplicate'); }, { receipt }), { applied: 1n });
    assert.equal(calls, 1);
    await assert.rejects(store.transact(() => {}, { receipt: { ...receipt, digest: 'changed' } }), { code: 'RECEIPT_MISMATCH' });
    await assert.rejects(store.transact(() => {}, { receipt, verifyReplay() { throw Object.assign(Error('revoked'), { code: 'FORBIDDEN' }); } }), { code: 'FORBIDDEN' });
    assert.deepEqual(await snapshot(store), image(namespaces.map(prefix => [prefix + 'once', 1n])));
  });
}

export const conflictSchedules = [
  { name: 'present-point-read-write', rows: [['source/protected', 1]], read: tx => tx.get('source/protected'), write: tx => tx.set('source/protected', 2) },
  { name: 'batch-existing-key', rows: [['source/protected', 1]], read: tx => tx.getMany(['source/protected', 'source/missing', 'source/protected']), write: tx => tx.set('source/protected', 2) },
  { name: 'batch-absent-key', rows: [['source/protected', 1]], read: tx => tx.getMany(['source/protected', 'source/missing', 'source/protected']), write: tx => tx.set('source/missing', 2) },
  { name: 'absent-key-uniqueness', rows: [], read: tx => tx.get('source/missing'), write: tx => tx.set('source/missing', 2) },
  { name: 'empty-range-insertion-phantom', rows: [], read: tx => tx.scan({ prefix: 'range/' }), write: tx => tx.set('range/new', 2) },
  { name: 'populated-range-deletion-phantom', rows: [['range/member', 1]], read: tx => tx.scan({ gte: 'range/', lt: 'range0' }), write: tx => tx.delete('range/member') },
  { name: 'limited-page-protects-far-later-edit', rows: [['range/001', 1], ['range/999', 9]], read: tx => tx.scan({ prefix: 'range/', limit: 1 }), write: tx => tx.set('range/999', 10) },
  { name: 'no-op-retains-point-dependency', rows: [['source/protected', 1]], read: tx => tx.set('source/protected', 1), write: tx => tx.set('source/protected', 2) },
  { name: 'reverse-reference-empty-range', rows: [], read: tx => tx.scan({ prefix: 'reference/parent/' }), write: tx => tx.set('reference/parent/child', 'child') },
  { name: 'authority-revocation', rows: [['authority/actor', { read: true }]], read: tx => tx.get('authority/actor'), write: tx => tx.set('authority/actor', { read: false }) },
];

export function registerOptimisticConformance(name, open) {
  test(`${name} shared optimistic conformance: disjoint points both commit`, async t => {
    const store = await open(t), first = deferred(), second = deferred(), release = deferred(); let calls = 0;
    const operation = (key, ready) => store.transact(async tx => { calls++; assert.equal(await tx.get(key), undefined); ready.resolve(); await release.promise; await tx.set(key, key); }, { receipt: { id: key, digest: key } });
    const a = operation('source/disjoint-a', first), b = operation('source/disjoint-b', second);
    await Promise.all([first.promise, second.promise]); release.resolve(); await Promise.all([a, b]);
    assert.equal(calls, 2); assert.deepEqual(await snapshot(store), image([['source/disjoint-a', 'source/disjoint-a'], ['source/disjoint-b', 'source/disjoint-b']]));
  });
  for (const schedule of conflictSchedules) test(`${name} shared optimistic conformance: ${schedule.name}`, async t => {
    const store = await open(t); await seed(store, schedule.rows); const entered = deferred(), release = deferred(); let calls = 0;
    const loser = store.transact(async tx => {
      calls++; await schedule.read(tx); entered.resolve(); await release.promise;
      for (const prefix of namespaces) await tx.set(prefix + 'loser', true); return 'stale-result';
    }, { receipt: { id: 'loser', digest: schedule.name } });
    const rejected = assert.rejects(loser, { code: 'STORAGE_CONFLICT' });
    await entered.promise; await store.transact(schedule.write, { receipt: { id: 'winner', digest: schedule.name } });
    const winner = await snapshot(store); release.resolve(); await rejected;
    assert.equal(calls, 1); assert.equal(await store.getReceipt('loser'), undefined);
    assert.deepEqual(await snapshot(store), winner); assert.ok(await store.getReceipt('winner'));
  });
  test(`${name} shared optimistic conformance: colliding original receipt applies once`, async t => {
    const store = await open(t), entered = deferred(), release = deferred(), receipt = { id: 'same', digest: 'same-request' }; let calls = 0;
    const a = store.transact(async tx => { calls++; await tx.set('source/once', 1); entered.resolve(); await release.promise; return 1; }, { receipt });
    const rejected = assert.rejects(a, { code: 'STORAGE_CONFLICT' }); await entered.promise;
    assert.equal(await store.transact(async tx => { calls++; await tx.set('source/once', 1); return 1; }, { receipt }), 1);
    release.resolve(); await rejected;
    assert.equal(await store.transact(() => { calls++; return 2; }, { receipt }), 1); assert.equal(calls, 2);
    assert.deepEqual(await snapshot(store), [{ key: 'source/once', value: 1 }]);
  });
}
