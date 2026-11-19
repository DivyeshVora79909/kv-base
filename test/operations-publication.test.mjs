import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { dirname, join } from 'node:path';
import { fixture } from './helpers/operations-fixture.mjs';
import { writeBackup, readBackup, activateRestoration } from '../src/operations.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { sourcePrefix } from '../src/keys.mjs';

const skip = process.env.KV_BASE_FDB_CLUSTER_FILE ? false : 'Selected real FDB cluster required';
const readJson = async path => JSON.parse(await fs.readFile(path, 'utf8'));

async function withOpenFault({ matches, phase, fault, verify }, callback) {
  const originalOpen = fs.open; let hits = 0;
  fs.open = async (path, flags, ...args) => {
    if (!matches(path, flags) || hits) return originalOpen(path, flags, ...args);
    if (phase === 'open') { hits++; await verify?.(); throw fault; }
    const file = await originalOpen(path, flags, ...args);
    file.sync = async () => { hits++; await verify?.(); throw fault; };
    return file;
  };
  syncBuiltinESMExports();
  try { await callback(); assert.equal(hits, 1, 'the controlled filesystem boundary was reached exactly once'); }
  finally { fs.open = originalOpen; syncBuiltinESMExports(); }
}

for (const phase of ['open', 'sync']) test(`backup parent-directory ${phase} failure after rename preserves an unknown original publication`, { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('authenticated surviving backup bytes');
  await f.addObject('ready', bytes);
  const target = join(f.directory, 'published-backup'), backupId = 'original-backup', fault = Object.assign(Error('injected post-rename backup failure'), { code: 'EIO' });
  const expected = await f.runtime.storage.captureCheckpoint(); let unknown;
  await withOpenFault({
    matches: (path, flags) => path === dirname(target) && (flags & constants.O_DIRECTORY) !== 0,
    phase, fault, verify: async () => assert.equal((await readJson(join(target, 'manifest.json'))).manifest.backupId, backupId),
  }, async () => {
    await assert.rejects(writeBackup({
      runtime: f.runtime, objectRoot: f.objectRoot, directory: target, backupId,
      keyring: f.keyring, incarnation: f.fence, sourceIdentity: f.sourceIdentity, quiesce: callback => callback(),
    }), error => {
      unknown = error;
      assert.equal(error.code, 'COMMIT_UNKNOWN'); assert.equal(error.cause, fault);
      assert.equal(error.backupId, backupId); assert.equal(error.directory, target); return true;
    });
  });
  const recovered = await readBackup({ directory: unknown.directory, keyring: f.keyring });
  assert.equal(recovered.manifest.backupId, unknown.backupId); assert.deepEqual(recovered.snapshot, expected);
  assert.equal(recovered.manifest.files.length, 1);
  const file = recovered.manifest.files[0];
  assert.deepEqual(await fs.readFile(join(target, 'files', file.kind, file.name)), bytes);
  assert.equal((await fs.readdir(f.directory)).some(name => name.startsWith('published-backup.staging-')), false);
});

for (const phase of ['open', 'sync']) test(`restoration parent-directory ${phase} failure after rename preserves and replays the original descriptor`, { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('selected restored bytes'); await f.addObject('ready', bytes);
  const expected = await f.runtime.engine.read(tx => tx.scan({ prefix: sourcePrefix('t') }));
  const backup = await f.backup(), prepared = await f.prepare(backup), deploymentFile = join(f.directory, 'deployment.json');
  const original = { version: '1', prefix: f.prefix, objectRoot: f.objectRoot, incarnation: f.fence };
  await fs.writeFile(deploymentFile, JSON.stringify(original), { mode: 0o600 });
  const operationId = 'original-restoration', fault = Object.assign(Error('injected post-rename activation failure'), { code: 'EIO' }); let unknown;
  await withOpenFault({
    matches: (path, flags) => path === dirname(deploymentFile) && (flags & constants.O_DIRECTORY) !== 0,
    phase, fault, verify: async () => {
      const selected = await readJson(deploymentFile);
      assert.equal(selected.restoreOperationId, operationId); assert.equal(selected.prefix, prepared.descriptor.prefix);
    },
  }, async () => {
    await assert.rejects(activateRestoration({ handle: prepared, deploymentFile, operationId, stopPrevious: () => f.runtime.close() }), error => {
      unknown = error; assert.equal(error.code, 'COMMIT_UNKNOWN'); assert.equal(error.cause, fault); assert.equal(error.operationId, operationId); return true;
    });
  });
  const selected = await readJson(deploymentFile); let callbacks = 0;
  assert.deepEqual(await activateRestoration({ handle: prepared, deploymentFile, operationId: unknown.operationId, stopPrevious() { callbacks++; }, after() { callbacks++; } }), selected);
  assert.equal(callbacks, 0); assert.equal(selected.backupId, backup.backupId);
  assert.equal(selected.objectRoot, prepared.descriptor.objectRoot); assert.equal(selected.incarnation, prepared.descriptor.incarnation);
  assert.equal((await fs.readdir(f.directory)).some(name => name.startsWith('deployment.json.staging-')), false);
  const store = await openFoundationDbStore({ clusterFile: selected.clusterFile, prefix: selected.prefix }); f.stores.push(store);
  const reopened = await f.openRuntime({ store, objectRoot: selected.objectRoot, incarnation: selected.incarnation, previousIncarnations: selected.previousIncarnations });
  try {
    assert.deepEqual(await reopened.engine.read(tx => tx.scan({ prefix: sourcePrefix('t') })), expected);
    assert.equal((await reopened.objects.checkFiles()).ready, 1); await reopened.engine.check();
    const loaded = await readBackup({ directory: backup.directory, keyring: f.keyring }), file = loaded.manifest.files[0];
    assert.deepEqual(await fs.readFile(join(selected.objectRoot, file.kind, file.name)), bytes);
  } finally { await reopened.close(); }
});

test('a pre-rename backup directory sync failure remains definite and removes only its staging tree', { skip }, async t => {
  const f = await fixture(t), target = join(f.directory, 'unpublished-backup'), fault = Object.assign(Error('injected staging sync failure'), { code: 'EIO' });
  await withOpenFault({
    matches: (path, flags) => dirname(path) === f.directory && path.startsWith(`${target}.staging-`) && (flags & constants.O_DIRECTORY) !== 0,
    phase: 'sync', fault,
  }, async () => {
    await assert.rejects(writeBackup({ runtime: f.runtime, objectRoot: f.objectRoot, directory: target, backupId: 'unpublished', keyring: f.keyring, incarnation: f.fence, sourceIdentity: f.sourceIdentity, quiesce: callback => callback() }), error => error === fault);
  });
  await assert.rejects(fs.lstat(target), { code: 'ENOENT' });
  assert.equal((await fs.readdir(f.directory)).some(name => name.startsWith('unpublished-backup.staging-')), false);
});

test('a pre-rename restoration staging-open failure remains definite and preserves the prior descriptor', { skip }, async t => {
  const f = await fixture(t), backup = await f.backup(), prepared = await f.prepare(backup), deploymentFile = join(f.directory, 'deployment.json');
  const original = { version: '1', prefix: f.prefix, objectRoot: f.objectRoot, incarnation: f.fence };
  await fs.writeFile(deploymentFile, JSON.stringify(original), { mode: 0o600 });
  const fault = Object.assign(Error('injected staging open failure'), { code: 'EIO' });
  await withOpenFault({ matches: path => path.startsWith(`${deploymentFile}.staging-`), phase: 'open', fault }, async () => {
    await assert.rejects(activateRestoration({ handle: prepared, deploymentFile, operationId: 'unpublished', stopPrevious: () => f.runtime.close() }), error => error === fault);
  });
  assert.deepEqual(await readJson(deploymentFile), original);
  assert.equal((await fs.readdir(f.directory)).some(name => name.startsWith('deployment.json.staging-')), false);
});
