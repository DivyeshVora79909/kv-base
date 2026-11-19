import test from 'node:test';
import assert from 'node:assert/strict';
import { serialize } from 'node:v8';
import { DatabaseSync } from 'node:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { encodeKvData, decodeKvData } from '../src/values.mjs';
import { openSqliteStore } from '../src/sqlite.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE, skip = clusterFile ? false : 'Selected real FDB cluster required';
const names = ['Int8Array', 'Uint8Array', 'Uint8ClampedArray', 'Int16Array', 'Uint16Array', 'Int32Array', 'Uint32Array', 'Float16Array', 'Float32Array', 'Float64Array', 'BigInt64Array', 'BigUint64Array'];
function graph(viewFirst = false) {
  const buffer = new ArrayBuffer(128); new Uint8Array(buffer).set(Array.from({ length: 128 }, (_, i) => i));
  const views = names.filter(name => typeof globalThis[name] === 'function').map(name => new globalThis[name](buffer, 32, 2)), data = new DataView(buffer, 35, 17);
  const result = viewFirst ? { views, data, buffer } : { buffer, views, data }; result.duplicate = views[0]; result.map = new Map([[buffer, views], [data, buffer]]); result.self = result; return result;
}
function check(actual) {
  assert.equal(actual.self, actual); assert.equal(actual.duplicate, actual.views[0]); assert.equal(actual.map.get(actual.buffer), actual.views); assert.equal(actual.map.get(actual.data), actual.buffer);
  assert.deepEqual([...new Uint8Array(actual.buffer)], Array.from({ length: 128 }, (_, i) => i));
  for (const view of actual.views) { assert.equal(view.buffer, actual.buffer); assert.equal(view.byteOffset, 32); assert.equal(view.length, 2); }
  assert.equal(actual.data.buffer, actual.buffer); assert.equal(actual.data.byteOffset, 35); assert.equal(actual.data.byteLength, 17);
}
function legacy() { const cycle = { exact: 900719925474099312345678901n }; cycle.self = cycle; return { cycle, bytes: Uint8Array.from([0, 255, 128, 7]), map: new Map([['exact', 6n]]) }; }
function checkLegacy(value) { assert.equal(value.cycle.self, value.cycle); assert.equal(value.cycle.exact, 900719925474099312345678901n); assert.deepEqual([...value.bytes], [0, 255, 128, 7]); assert.deepEqual(value.map, new Map([['exact', 6n]])); }

test('value codec preserves every available fixed typed view and graph identity in either traversal order', () => {
  for (const viewFirst of [false, true]) {
    const source = graph(viewFirst), encoded = encodeKvData(source); new Uint8Array(source.buffer).fill(255); const restored = decodeKvData(encoded); check(restored); assert.notEqual(restored.buffer, source.buffer);
    restored.data.setUint8(0, 222); assert.equal(new Uint8Array(restored.views[0].buffer)[35], 222); check(decodeKvData(encoded));
  }
});

test('versioned value codec retains ordinary exact values and reads immutable legacy V8 bytes', () => {
  const value = { ...legacy(), date: new Date(1700000000000), expression: /source+/giu, set: new Set([9n, 'literal']), marker: { protocol: 'kv-base-value-2', type: 'DataView' } };
  for (const encoded of [serialize(value), encodeKvData(value)]) { const read = decodeKvData(encoded); checkLegacy(read); assert.equal(read.date.getTime(), 1700000000000); assert.equal(read.expression.source, 'source+'); assert.equal(read.expression.flags, 'giu'); assert.deepEqual(read.set, new Set([9n, 'literal'])); assert.deepEqual(read.marker, value.marker); }
  assert.throws(() => decodeKvData(encodeKvData(graph()).subarray(0, 20)));
});

test('optional SQLite legacy values and original receipts coexist with new view graphs without rewriting stored bytes', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'kv-value-legacy-')); t.after(() => rm(directory, { recursive: true, force: true })); const filename = join(directory, 'state.sqlite'); await openSqliteStore(filename).close();
  const value = serialize(legacy()), receipt = serialize({ id: 'legacy-original', digest: 'legacy-request', result: legacy() }); let native = new DatabaseSync(filename);
  native.prepare('INSERT INTO kv(k,v) VALUES(?,?)').run(Buffer.from('legacy-value'), value); native.prepare('INSERT INTO receipts(k,v) VALUES(?,?)').run(Buffer.from('legacy-original'), receipt); native.close();
  let store = openSqliteStore(filename); checkLegacy(await store.read(tx => tx.get('legacy-value'))); checkLegacy((await store.getReceipt('legacy-original')).result); checkLegacy(await store.transact(() => { throw Error('Original must recover'); }, { receipt: { id: 'legacy-original', digest: 'legacy-request' } }));
  await store.transact(tx => tx.set('new-value', graph(true))); await store.close(); store = openSqliteStore(filename); check(await store.read(tx => tx.get('new-value'))); await store.close();
  native = new DatabaseSync(filename); assert.deepEqual(Buffer.from(native.prepare('SELECT v FROM kv WHERE k=?').get(Buffer.from('legacy-value')).v), value); assert.deepEqual(Buffer.from(native.prepare('SELECT v FROM receipts WHERE k=?').get(Buffer.from('legacy-original')).v), receipt); native.close();
});

test('primary FDB legacy and view-graph values retain exact mixed checkpoint bytes through fresh restore', { skip }, async t => {
  const prefix = 'legacy-value-' + randomUUID(), store = await openFoundationDbStore({ clusterFile, prefix }); t.after(() => store.close()); const { default: fdb } = await import('foundationdb'), native = fdb.open(clusterFile), root = fdb.tuple.pack(['kv-base', prefix]);
  const value = serialize(legacy()), receipt = serialize({ id: 'legacy-original', digest: 'legacy-request', result: legacy() }), tx = native.rawCreateTransaction({ timeout: 4500 });
  try { tx.set(Buffer.concat([root, Buffer.from([0]), Buffer.from('legacy-value')]), value); tx.set(Buffer.concat([root, Buffer.from([1]), Buffer.from('legacy-original')]), receipt); await tx.rawCommit(); } finally { tx.rawCancel(); native.close(); }
  checkLegacy(await store.read(tx => tx.get('legacy-value'))); checkLegacy((await store.getReceipt('legacy-original')).result); checkLegacy(await store.transact(() => { throw Error('Original must recover'); }, { receipt: { id: 'legacy-original', digest: 'legacy-request' } }));
  await store.transact(tx => tx.set('new-value', graph(true))); const checkpoint = await store.captureCheckpoint(); assert.deepEqual(checkpoint.rows.find(([key]) => key.equals(Buffer.concat([Buffer.from([0]), Buffer.from('legacy-value')])))[1], value); assert.deepEqual(checkpoint.rows.find(([key]) => key.equals(Buffer.concat([Buffer.from([1]), Buffer.from('legacy-original')])))[1], receipt);
  const restored = await openFoundationDbStore({ clusterFile, prefix: 'restored-values-' + randomUUID() }); t.after(() => restored.close()); await restored.restoreCheckpoint(checkpoint); assert.deepEqual(await restored.captureCheckpoint(), checkpoint); checkLegacy(await restored.read(tx => tx.get('legacy-value'))); check(await restored.read(tx => tx.get('new-value'))); checkLegacy((await restored.getReceipt('legacy-original')).result);
});
