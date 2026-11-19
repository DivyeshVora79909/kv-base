import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, writeFile, unlink, symlink } from 'node:fs/promises';
import { join } from 'node:path';
import { fork } from 'node:child_process';
import { sourceKey } from '../src/keys.mjs';
import { fixture, graph, definition, digest, chunks, collect } from './helpers/objects-fixture.mjs';
import { prepareCatalog, validateRecord } from '../src/catalog.mjs';
import { objectModel, openObjectStore } from '../src/objects.mjs';
const skip = !process.env.KV_BASE_FDB_CLUSTER_FILE && 'Select a real FoundationDB cluster with KV_BASE_FDB_CLUSTER_FILE';

test('object declaration independently checks exact byte boundary states and extra fields', () => {
  const catalog = prepareCatalog({ name: 'object-contract', version: '1', models: { Object: objectModel() } });
  const record = { id: 'blob', tenant: 't', owner: 'reader', model: 'Object', createdAt: '0', data: { state: 'ready', bytes: 268435456, sha256: 'a'.repeat(64) } };
  assert.deepEqual(validateRecord(catalog, record), record);
  for (const data of [{ ...record.data, bytes: 268435457 }, { ...record.data, state: 'other' }, { ...record.data, secret: 'extra' }]) assert.throws(() => validateRecord(catalog, { ...record, data }), { code: 'INVALID_RECORD' });
});

test('objects primary FDB reject identity/engine tenant mismatch before filesystem work', { skip }, async t => {
  const f = await fixture(t); let touched = false;
  await assert.rejects(openObjectStore({ engine: f.engine, identity: { ...f.identity, tenant: 'other' }, keyring: f.keyring, getRestoreFence: () => { touched = true; return f.fence; }, directory: join(f.directory, 'must-not-create'), model: 'Object' }), { code: 'INVALID_OBJECT' });
  assert.equal(touched, false); assert.equal(f.objects.tenant, 't'); assert.equal(f.objects.model, 'Object');
});

test('objects primary FDB independent exact bytes and source metadata roundtrip with encrypted original grant', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from([0, 255, 1, 3, 9, 128]), input = { operationId: 'begin-once', id: 'blob', owner: 'visible', bytes: bytes.length, sha256: digest(bytes) };
  const grant = await f.objects.begin(f.token, input); assert.deepEqual(await f.objects.begin(f.token, input), grant);
  const inventory = JSON.stringify(await f.engine.read(tx => tx.scan())); for (const secret of [f.token, grant.uploadToken, f.directory]) assert.equal(inventory.includes(secret), false);
  assert.deepEqual(await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)), { staged: true, bytes: bytes.length, sha256: digest(bytes) });
  const ready = await f.objects.finalize(f.token, { operationId: 'finalize-once', id: 'blob', uploadToken: grant.uploadToken });
  assert.deepEqual(ready, { ready: true, bytes: bytes.length, sha256: digest(bytes) });
  await f.store.close(); await f.open();
  assert.deepEqual(await f.objects.finalize(f.token, { operationId: 'finalize-once', id: 'blob', uploadToken: grant.uploadToken }), ready);
  assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
  const files = await f.objects.inspectFiles(); assert.deepEqual(files.stages, []); assert.equal(files.objects.length, 1); assert.deepEqual(await readFile(join(f.directory, 'objects', files.objects[0])), bytes);
  assert.deepEqual((await f.engine.read(tx => tx.get(sourceKey('t', 'blob')))).data, { state: 'ready', bytes: bytes.length, sha256: digest(bytes) }); await f.engine.check();
});

test('objects primary FDB independent oversize mismatched and unsupported chunks clean attempts and preserve pending source', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('hello'), grant = await f.begin(bytes);
  for (const [data, code] of [[Buffer.from('hello!'), 'OBJECT_SIZE'], [Buffer.from('wrong'), 'OBJECT_DIGEST'], [Buffer.from('hi'), 'OBJECT_DIGEST']]) {
    await assert.rejects(f.objects.upload(f.token, grant.uploadToken, chunks(data)), { code }); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
    assert.equal((await f.objects.status(f.token, 'blob')).state, 'pending');
  }
  await assert.rejects(f.objects.upload(f.token, grant.uploadToken, (async function* () { yield 'text'; })()), { code: 'INVALID_OBJECT' });
  await assert.rejects(f.objects.download(f.token, 'blob'), { code: 'OBJECT_NOT_READY' });
  await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.finalize(f.token, { operationId: 'finish', id: 'blob', uploadToken: grant.uploadToken }); assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
});

test('objects primary FDB independent grants bind exact session owner size digest and strict public input', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('bounded'), grant = await f.begin(bytes), second = await f.login(), other = await f.login('other');
  for (const token of [second, other]) await assert.rejects(f.objects.upload(token, grant.uploadToken, chunks(bytes)), { code: 'INVALID_OBJECT_GRANT' });
  const at = grant.uploadToken.lastIndexOf('.') + 1, changed = grant.uploadToken.slice(0, at) + (grant.uploadToken[at] === 'A' ? 'B' : 'A') + grant.uploadToken.slice(at + 1);
  await assert.rejects(f.objects.upload(f.token, changed, chunks(bytes)), { code: 'INVALID_OBJECT_GRANT' });
  await assert.rejects(f.objects.begin(f.token, { operationId: 'hidden', id: 'hidden', owner: 'hidden', bytes: bytes.length, sha256: digest(bytes) }), { code: 'FORBIDDEN' });
  await assert.rejects(f.objects.begin(f.token, { operationId: 'injection', id: 'injection', owner: 'visible', bytes: 0, sha256: digest(Buffer.alloc(0)), actor: 'admin' }), { code: 'INVALID_OBJECT' });
  await assert.rejects(f.objects.begin(f.token, { operationId: 'changed', id: 'blob', owner: 'visible', bytes: 3, sha256: digest(bytes) }), { code: 'OBJECT_EXISTS' });
  assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('objects primary FDB independent upload admission and stalled stream timeout release physical work', { skip }, async t => {
  const f = await fixture(t, { limits: { maxUploads: 1, uploadMs: 50 } }), bytes = Buffer.from('stall'), grant = await f.begin(bytes); let entered;
  const ready = new Promise(resolve => { entered = resolve; });
  const stalled = f.objects.upload(f.token, grant.uploadToken, { [Symbol.asyncIterator]() { return { next() { entered(); return new Promise(() => {}); } }; } });
  const rejection = assert.rejects(stalled, { code: 'OBJECT_TIMEOUT' }); await ready;
  await assert.rejects(f.objects.upload(f.token, grant.uploadToken, chunks(bytes)), { code: 'WORK_LIMIT' }); await rejection;
  assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] }); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
});

test('objects primary FDB independent pending owner transfer invalidates original grant without touching replacement authority', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('transfer'), grant = await f.begin(bytes), admin = await f.login('admin');
  await assert.rejects(f.objects.transfer(f.token, { operationId: 'denied-transfer', id: 'blob', owner: 'hidden' }), { code: 'FORBIDDEN' });
  await f.objects.transfer(admin, { operationId: 'transfer', id: 'blob', owner: 'hidden' });
  await assert.rejects(f.objects.upload(f.token, grant.uploadToken, chunks(bytes)), { code: 'INVALID_OBJECT_GRANT' });
  await assert.rejects(f.objects.download(f.token, 'blob'), { code: 'FORBIDDEN' }); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('objects primary FDB independent ready transfers and policy revocation recheck current Read', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('policy'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.finalize(f.token, { operationId: 'ready', id: 'blob', uploadToken: grant.uploadToken });
  const policy = graph(); policy.principals.find(n => n.id === 'visible').grants.resources.Object = ['create', 'update', 'delete']; await f.identity.replacePolicy(policy);
  await assert.rejects(f.objects.download(f.token, 'blob'), { code: 'FORBIDDEN' }); const admin = await f.login('admin'); assert.deepEqual(await collect((await f.objects.download(admin, 'blob')).stream), bytes);
  await f.objects.transfer(admin, { operationId: 'transfer-ready', id: 'blob', owner: 'hidden' });
  const other = await f.login('other'); assert.deepEqual(await collect((await f.objects.download(other, 'blob')).stream), bytes); await assert.rejects(f.objects.download(f.token, 'blob'), { code: 'FORBIDDEN' });
});

test('objects primary FDB independent expiry restore fencing tombstone and stale cleanup never delete replacement bytes', { skip }, async t => {
  const f = await fixture(t), oldBytes = Buffer.from('old'), oldGrant = await f.begin(oldBytes); await f.objects.upload(f.token, oldGrant.uploadToken, chunks(oldBytes));
  f.time = 2000; await assert.rejects(f.objects.finalize(f.token, { operationId: 'too-late', id: 'blob', uploadToken: oldGrant.uploadToken }), { code: 'INVALID_OBJECT_GRANT' }); await f.objects.reconcile(); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
  const admin = await f.login('admin'); await f.objects.remove(admin, { operationId: 'remove-old', id: 'blob' });
  const bytes = Buffer.from('replacement'), grant = await f.begin(bytes, 'blob', 'visible', 'replacement'); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.finalize(f.token, { operationId: 'replacement-ready', id: 'blob', uploadToken: grant.uploadToken });
  await f.objects.reconcile(); await f.objects.reconcile(); assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
  await assert.rejects(f.objects.upload(f.token, oldGrant.uploadToken, chunks(oldBytes)), { code: 'INVALID_OBJECT_GRANT' });
  f.fence = 'incarnation-B'; await assert.rejects(f.objects.download(f.token, 'blob'), { code: 'UNAUTHENTICATED' });
  const fresh = await f.login(); assert.deepEqual(await collect((await f.objects.download(fresh, 'blob')).stream), bytes); const freshAdmin = await f.login('admin'); await f.objects.remove(freshAdmin, { operationId: 'remove-new', id: 'blob' }); await f.objects.reconcile(); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('objects primary FDB independent interrupted accepted finalize activates on private recovery and recovers original receipt', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('intent'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
  await f.store.close(); await f.open({ onPhase(name) { if (name === 'finalize:intent') throw Error('crash intent'); } });
  await assert.rejects(f.objects.finalize(f.token, { operationId: 'accepted', id: 'blob', uploadToken: grant.uploadToken }), /crash intent/);
  await f.store.close(); await f.open(); assert.equal((await f.objects.reconcile()).activated, 1); assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
  assert.deepEqual(await f.objects.finalize(f.token, { operationId: 'accepted', id: 'blob', uploadToken: grant.uploadToken }), { ready: true, bytes: bytes.length, sha256: digest(bytes) });
});

test('objects primary FDB independent stale session during accepted finalize aborts publication and cleans promoted bytes', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('revoke'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
  await f.store.close(); await f.open({ onPhase(name) { if (name === 'finalize:promoted') throw Error('stop promotion'); } }); await assert.rejects(f.objects.finalize(f.token, { operationId: 'accepted', id: 'blob', uploadToken: grant.uploadToken }), /stop promotion/);
  await f.identity.logout(f.token, { operationId: 'revoke' }); await f.store.close(); await f.open(); const result = await f.objects.reconcile(); assert.equal(result.activated, 0); assert.equal(result.cleaned, 1);
  assert.equal((await f.engine.read(tx => tx.get(sourceKey('t', 'blob')))).data.state, 'pending'); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('objects primary FDB independent paginated cleanup handles more rows than a recovery budget', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('page'), admin = await f.login('admin'); for (let n = 0; n < 7; n++) { const grant = await f.begin(bytes, `blob-${n}`); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.remove(admin, { operationId: `remove-${n}`, id: `blob-${n}` }); }
  let cursor, examined = 0, cleaned = 0; do { const result = await f.objects.reconcile(cursor ? { after: cursor } : {}); examined += result.examined; cleaned += result.cleaned; cursor = result.cursor; } while (cursor);
  assert.equal(examined, 7); assert.equal(cleaned, 7); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] }); assert.equal((await f.engine.check()).sources, 0);
});

test('objects primary FDB independent lost activation acknowledgement keeps original receipt and never promotes twice', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('unknown'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
  await f.store.close(); await f.open({ onPhase(name) { if (name === 'finalize:ready') throw Error('lost reply'); } });
  await assert.rejects(f.objects.finalize(f.token, { operationId: 'same', id: 'blob', uploadToken: grant.uploadToken }), { code: 'COMMIT_UNKNOWN' });
  await f.store.close(); await f.open(); assert.deepEqual(await f.objects.finalize(f.token, { operationId: 'same', id: 'blob', uploadToken: grant.uploadToken }), { ready: true, bytes: bytes.length, sha256: digest(bytes) });
  assert.equal((await f.objects.inspectFiles()).objects.length, 1); assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
});

test('objects primary FDB independent disjoint uploads overlap while the same grant excludes a second writer', { skip }, async t => {
  const f = await fixture(t), a = Buffer.from('first'), b = Buffer.from('second'), ga = await f.begin(a, 'a'), gb = await f.begin(b, 'b'); let enteredA, enteredB, release;
  const readyA = new Promise(resolve => { enteredA = resolve; }), readyB = new Promise(resolve => { enteredB = resolve; }), blocked = new Promise(resolve => { release = resolve; });
  const stream = (bytes, entered) => (async function* () { entered(); await blocked; yield bytes; })();
  const first = f.objects.upload(f.token, ga.uploadToken, stream(a, enteredA)), second = f.objects.upload(f.token, gb.uploadToken, stream(b, enteredB)); await Promise.all([readyA, readyB]);
  await assert.rejects(f.objects.upload(f.token, ga.uploadToken, chunks(a)), { code: 'OBJECT_BUSY' }); release(); await Promise.all([first, second]);
  await Promise.all([f.objects.finalize(f.token, { operationId: 'fa', id: 'a', uploadToken: ga.uploadToken }), f.objects.finalize(f.token, { operationId: 'fb', id: 'b', uploadToken: gb.uploadToken })]);
  assert.deepEqual(await collect((await f.objects.download(f.token, 'a')).stream), a); assert.deepEqual(await collect((await f.objects.download(f.token, 'b')).stream), b);
});

test('objects primary FDB independent mid-upload transfer fences stale completion and preserves cleanup intent', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('in flight'), grant = await f.begin(bytes), admin = await f.login('admin'); let entered, release;
  const ready = new Promise(resolve => { entered = resolve; }), blocked = new Promise(resolve => { release = resolve; });
  const operation = f.objects.upload(f.token, grant.uploadToken, (async function* () { entered(); await blocked; yield bytes; })());
  const rejection = assert.rejects(operation, { code: 'INVALID_OBJECT_GRANT' }); await ready;
  assert.deepEqual(await f.objects.transfer(admin, { operationId: 'transfer-mid-flight', id: 'blob', owner: 'hidden' }), { transferred: true }); release(); await rejection;
  await f.objects.reconcile(); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
  const source = await f.engine.read(tx => tx.get(sourceKey('t', 'blob'))); assert.equal(source.owner, 'hidden'); assert.deepEqual(source.data, { state: 'pending', bytes: bytes.length, sha256: digest(bytes) });
});

test('objects primary FDB independent original grant recovery enforces issuing session and current update policy', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('recover'), input = { operationId: 'begin', id: 'blob', owner: 'visible', bytes: bytes.length, sha256: digest(bytes) }, grant = await f.objects.begin(f.token, input);
  await assert.rejects(f.objects.begin(await f.login(), input), { code: 'INVALID_OBJECT_GRANT' });
  const policy = graph(); policy.principals.find(n => n.id === 'visible').grants.resources.Object = ['read', 'create']; await f.identity.replacePolicy(policy);
  await assert.rejects(f.objects.begin(f.token, input), { code: 'FORBIDDEN' });
  assert.equal(JSON.stringify(await f.engine.read(tx => tx.scan())).includes(grant.uploadToken), false);
});

test('objects primary FDB independent hidden occupied IDs and absent denied owners expose the same forbidden result', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('private'), other = await f.login('other');
  await f.objects.begin(other, { operationId: 'hidden-begin', id: 'hidden-id', owner: 'hidden', bytes: bytes.length, sha256: digest(bytes) });
  const before = await f.engine.read(tx => tx.scan());
  for (const [id, owner] of [['hidden-id', 'visible'], ['hidden-id', 'hidden'], ['absent-id', 'hidden']]) {
    await assert.rejects(f.objects.begin(f.token, { operationId: `probe-${id}-${owner}`, id, owner, bytes: bytes.length, sha256: digest(bytes) }), { code: 'FORBIDDEN', message: 'Operation is not permitted' });
  }
  assert.deepEqual(await f.engine.read(tx => tx.scan()), before); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('objects primary FDB independent an own grant cannot reserve or alter a hidden staged version', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('private version'), other = await f.login('other');
  const hidden = await f.objects.begin(other, { operationId: 'hidden', id: 'hidden', owner: 'hidden', bytes: bytes.length, sha256: digest(bytes) }); await f.objects.upload(other, hidden.uploadToken, chunks(bytes));
  const own = await f.begin(bytes); await f.objects.upload(f.token, own.uploadToken, chunks(bytes)); const before = await f.engine.read(tx => tx.scan());
  await assert.rejects(f.objects.finalize(f.token, { operationId: 'sabotage', id: 'hidden', uploadToken: own.uploadToken }), { code: 'INVALID_OBJECT_GRANT' });
  assert.deepEqual(await f.engine.read(tx => tx.scan()), before); await f.objects.finalize(other, { operationId: 'legitimate', id: 'hidden', uploadToken: hidden.uploadToken }); assert.deepEqual(await collect((await f.objects.download(other, 'hidden')).stream), bytes);
});

test('objects primary FDB independent file checker hashes actual ready and staged bytes from source facts under explicit budgets', { skip }, async t => {
  const f = await fixture(t), ready = Buffer.from('ready'), staged = Buffer.from('staged!'), a = await f.begin(ready, 'ready'), b = await f.begin(staged, 'staged');
  await f.objects.upload(f.token, a.uploadToken, chunks(ready)); await f.objects.finalize(f.token, { operationId: 'ready', id: 'ready', uploadToken: a.uploadToken }); await f.objects.upload(f.token, b.uploadToken, chunks(staged));
  const report = await f.objects.checkFiles({ maxFiles: 2, maxBytes: ready.length + staged.length }); assert.equal(report.sources, 2); assert.equal(report.ready, 1); assert.equal(report.totalBytes, ready.length + staged.length); assert.equal(report.files.length, 2); assert.ok(report.scannedKeys >= 8);
  for (const file of report.files) { const expected = file.kind === 'objects' ? ready : staged; assert.equal(file.sha256, digest(expected)); assert.equal(file.bytes, expected.length); assert.deepEqual(await readFile(join(f.directory, file.kind, file.name)), expected); }
  await assert.rejects(f.objects.checkFiles({ maxFiles: 1 }), { code: 'WORK_LIMIT' }); await assert.rejects(f.objects.checkFiles({ maxBytes: 1 }), { code: 'WORK_LIMIT' });
});

test('objects primary FDB independent file checker rejects missing ready files symlinks unknown files and orphan source metadata', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('checked'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.finalize(f.token, { operationId: 'ready', id: 'blob', uploadToken: grant.uploadToken });
  const file = (await f.objects.inspectFiles()).objects[0], target = join(f.directory, 'objects', file); await unlink(target); await assert.rejects(f.objects.checkFiles(), { code: 'OBJECT_FILE_INTEGRITY' });
  const outside = join(f.directory, 'outside'); await writeFile(outside, bytes); await symlink(outside, target); await assert.rejects(f.objects.checkFiles(), { code: 'OBJECT_FILE_INTEGRITY' }); await unlink(target); await writeFile(target, bytes, { mode: 0o400 });
  const unknown = join(f.directory, 'objects', '00000000-0000-0000-0000-000000000000'); await writeFile(unknown, bytes); await assert.rejects(f.objects.checkFiles(), { code: 'OBJECT_FILE_INTEGRITY' }); await unlink(unknown);
  await f.engine.mutate([{ type: 'put', record: { id: 'orphan', tenant: 't', owner: 'reader', model: 'Object', createdAt: '0', data: { state: 'ready', bytes: bytes.length, sha256: digest(bytes) } } }]);
  await assert.rejects(f.objects.checkFiles(), { code: 'OBJECT_FILE_INTEGRITY' });
});

test('objects primary FDB independent file checker rejects malformed durable finalize metadata before backup admission', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('metadata'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); await f.objects.finalize(f.token, { operationId: 'ready', id: 'blob', uploadToken: grant.uploadToken });
  const stored = (await f.engine.read(tx => tx.scan({ prefix: 'objects/' }))).find(entry => entry.value.protocol === 'local-object-1');
  await f.engine.transact(async tx => { const corrupted = structuredClone(stored.value); delete corrupted.finalize.receiptId; await tx.set(stored.key, corrupted); });
  await assert.rejects(f.objects.checkFiles(), { code: 'CORRUPT_OBJECT' });
});

test('objects primary FDB independent corrupted accepted bytes isolate recovery and preserve unrelated activation', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('good bytes'), bad = await f.begin(bytes, 'bad'), good = await f.begin(bytes, 'good'); await f.objects.upload(f.token, bad.uploadToken, chunks(bytes)); await f.objects.upload(f.token, good.uploadToken, chunks(bytes));
  await f.store.close(); await f.open({ onPhase(name) { if (name === 'finalize:intent') throw Error('reserve only'); } });
  for (const [id, grant] of [['bad', bad], ['good', good]]) await assert.rejects(f.objects.finalize(f.token, { operationId: `finish-${id}`, id, uploadToken: grant.uploadToken }), /reserve only/);
  const damaged = (await f.engine.read(tx => tx.scan({ prefix: 'objects/' }))).find(row => row.value.id === 'bad' && row.value.protocol === 'local-object-1').value;
  await writeFile(join(f.directory, 'stages', damaged.stage), Buffer.from('bad bytes!'));
  await f.store.close(); await f.open(); const result = await f.objects.reconcile(); assert.equal(result.activated, 1); assert.equal(result.cleaned, 1); assert.deepEqual(result.errors, [{ code: 'OBJECT_DIGEST' }]);
  assert.equal((await f.engine.read(tx => tx.get(sourceKey('t', 'bad')))).data.state, 'pending'); assert.deepEqual(await collect((await f.objects.download(f.token, 'good')).stream), bytes); await f.engine.check();
});

test('objects primary FDB independent late caller recovers receipt after a worker already activated its intent', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('late caller'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes)); let entered, release, activations = 0;
  const ready = new Promise(resolve => { entered = resolve; }), blocked = new Promise(resolve => { release = resolve; });
  await f.store.close(); await f.open({ onPhase: async name => { if (name === 'finalize:intent') { entered(); await blocked; } if (name === 'finalize:ready') activations++; } });
  const operation = f.objects.finalize(f.token, { operationId: 'late', id: 'blob', uploadToken: grant.uploadToken }); await ready;
  assert.equal((await f.objects.reconcile()).activated, 1); release(); assert.deepEqual(await operation, { ready: true, bytes: bytes.length, sha256: digest(bytes) }); assert.equal(activations, 1);
});

test('objects primary FDB independent rejected final validator preserves source trees and absent activation receipt', { skip }, async t => {
  const d = structuredClone(definition), index = ['Object', ['owner', 'state'], ['at']]; let readyAllowed = false;
  d.models.Object.indexes = [{ version: '1', partitionFields: index[1], orderFields: index[2], roles: { owner: 'id', state: 'string', at: 'integer' }, inputs: { n: 'integer' }, aggregates: { count: { monoid: 'sum', input: 'n' } }, validators: [{ scope: 'root', use: 'ready' }] }];
  d.models.Object.memberships = [{ slot: 'object-state', index, partition: [{ header: 'owner' }, { field: 'state' }], order: [{ header: 'createdAt' }], inputs: { n: { constant: '1' } }, onMissing: 'reject' }];
  const catalog = prepareCatalog(d, { validators: { ready: { version: '1', validate: (_summary, { scope }) => scope.partition[1] !== 'ready' || readyAllowed } } });
  const f = await fixture(t, { catalog }), bytes = Buffer.from('atomic'), grant = await f.begin(bytes); await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
  const maintained = async () => (await f.engine.read(tx => tx.scan())).filter(row => ['source/', 'root/', 'node/', 'incoming/', 'claim/', 'reactive/'].some(prefix => row.key.startsWith(prefix)));
  const before = await maintained(); await assert.rejects(f.objects.finalize(f.token, { operationId: 'validate', id: 'blob', uploadToken: grant.uploadToken }), { code: 'VALIDATION_FAILED' });
  assert.deepEqual(await maintained(), before); assert.equal((await f.engine.read(tx => tx.get(sourceKey('t', 'blob')))).data.state, 'pending'); await f.engine.check();
  readyAllowed = true; assert.equal((await f.objects.reconcile()).activated, 1); assert.equal((await f.engine.read(tx => tx.get(sourceKey('t', 'blob')))).data.state, 'ready'); await f.engine.check();
});

for (const phase of ['upload:staged-file', 'finalize:intent', 'finalize:promoted', 'finalize:ready', 'cleanup:deleted']) test(`objects primary FDB independent real SIGKILL at ${phase} reconciles bytes and source facts`, { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('physical crash bytes'), grant = await f.begin(bytes);
  if (phase !== 'upload:staged-file') await f.objects.upload(f.token, grant.uploadToken, chunks(bytes));
  if (phase === 'cleanup:deleted') { await f.objects.finalize(f.token, { operationId: 'before-delete', id: 'blob', uploadToken: grant.uploadToken }); await f.objects.remove(await f.login('admin'), { operationId: 'delete', id: 'blob' }); }
  await f.store.close();
  const child = fork(new URL('./fixtures/object-process.mjs', import.meta.url), [JSON.stringify({ phase, time: f.time, directory: f.directory, prefix: f.prefix, token: f.token, uploadToken: grant.uploadToken, bytes: bytes.toString('base64') })], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let stderr = ''; child.stderr.on('data', chunk => { stderr += chunk; }); t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); });
  await new Promise((resolve, reject) => { const timeout = setTimeout(() => reject(Error(`Child phase timeout ${stderr}`)), 15_000); child.once('error', reject); child.once('message', value => { clearTimeout(timeout); assert.equal(value.phase, phase); resolve(); }); child.once('exit', code => { clearTimeout(timeout); reject(Error(`Child exited before phase ${code} ${stderr}`)); }); });
  const stopped = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGKILL'); await stopped;
  if (phase === 'upload:staged-file') f.time += 501;
  await f.open(); const result = await f.objects.reconcile();
  if (['upload:staged-file', 'cleanup:deleted'].includes(phase)) {
    assert.equal(result.cleaned, 1); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
    assert.equal((await f.engine.read(tx => tx.get(sourceKey('t', 'blob'))))?.data.state, phase === 'upload:staged-file' ? 'pending' : undefined);
  } else {
    assert.equal(result.activated, phase === 'finalize:ready' ? 0 : 1); assert.deepEqual(await collect((await f.objects.download(f.token, 'blob')).stream), bytes);
    assert.deepEqual(await f.objects.finalize(f.token, { operationId: 'crash-finalize', id: 'blob', uploadToken: grant.uploadToken }), { ready: true, bytes: bytes.length, sha256: digest(bytes) });
  }
  await f.engine.check();
});
