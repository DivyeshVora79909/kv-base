import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile, link } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import http from 'node:http';
import { syncBuiltinESMExports } from 'node:module';
import { createHash } from 'node:crypto';
import { writeDeployment, readDeployment, readKeyringFile } from '../src/self-hosted.mjs';
import { runtimeFixture, barrier, clusterFile } from './helpers/runtime-fixture.mjs';
import { sourceKey } from '../src/keys.mjs';
const skip = !clusterFile && 'Selected real FDB cluster required';
const digest = value => createHash('sha256').update(value).digest('hex');
async function until(callback, timeout = 1500) {
  const deadline = Date.now() + timeout;
  while (!await callback()) { if (Date.now() >= deadline) assert.fail('Controlled schedule did not progress'); await new Promise(resolve => setTimeout(resolve, 10)); }
}
function send(url, headers, first) {
  let request;
  const result = new Promise((resolve, reject) => {
    request = http.request(url, { method: headers['x-upload-grant'] ? 'PUT' : 'POST', headers }, response => {
      const chunks = []; response.on('data', part => chunks.push(part)); response.on('error', reject);
      response.on('end', () => resolve({ status: response.statusCode, body: JSON.parse(Buffer.concat(chunks)) }));
    }); request.on('error', reject); request.write(first);
  }); void result.catch(() => {}); return { request, result };
}

test('review concurrent initializers publish exactly one complete selected descriptor', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-initializer-review-')); t.after(() => rm(directory, { recursive: true, force: true }));
  const deploymentFile = join(directory, 'deployment.json'), base = { version: '1', clusterFile: '/operator/fdb.cluster', objectRoot: join(directory, 'files'), tenant: 't', catalogIdentity: 'a'.repeat(64) };
  const descriptors = ['A', 'B'].map(prefix => ({ ...base, prefix, incarnation: prefix }));
  const results = await Promise.allSettled(descriptors.map(descriptor => writeDeployment({ deploymentFile, descriptor })));
  assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
  const selected = await readDeployment(deploymentFile), winner = results.find(result => result.status === 'fulfilled'); assert.deepEqual(selected, winner.value);
  assert.equal(results.find(result => result.status === 'rejected').reason.code, 'DEPLOYMENT_EXISTS');
  assert.deepEqual(JSON.parse(await readFile(deploymentFile)), selected);
});

test('review a fully published descriptor survives interrupted staging-link cleanup while custody still rejects hard links', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-link-review-')); t.after(() => rm(directory, { recursive: true, force: true }));
  const descriptor = { version: '1', clusterFile: '/operator/fdb.cluster', objectRoot: join(directory, 'files'), tenant: 't', catalogIdentity: 'a'.repeat(64), prefix: 'A', incarnation: 'A' }, stage = join(directory, 'deployment.json.staging-interrupted'), deploymentFile = join(directory, 'deployment.json');
  await writeFile(stage, JSON.stringify(descriptor), { mode: 0o600 }); await link(stage, deploymentFile);
  assert.deepEqual(await readDeployment(deploymentFile), descriptor);
  await assert.rejects(writeDeployment({ deploymentFile, descriptor: { ...descriptor, prefix: 'B' } }), { code: 'DEPLOYMENT_EXISTS' });
  const keyring = join(directory, 'keyring.json'); await writeFile(keyring, JSON.stringify({ version: '1', activeKey: 'k', keys: { k: Buffer.alloc(32, 1).toString('base64') } }), { mode: 0o600 }); await link(keyring, join(directory, 'keyring-alias.json'));
  await assert.rejects(readKeyringFile(keyring), { code: 'INVALID_DEPLOYMENT' });
});

test('review a scheduled worker progresses while an independent HTTP upload remains admitted', { skip }, async t => {
  const f = await runtimeFixture(t); await f.runtime.close(); await f.open({ objectLimits: { grantMs: 1000, uploadMs: 5000 }, serviceLimits: { requestTimeoutMs: 5000 } });
  await f.runtime.bootstrap(f.bootstrap); await f.start({ workerIntervalMs: 10 }); const token = await f.login(), bytes = Buffer.from('a deliberately held upload');
  const begun = await f.call('/v1/objects/begin', { operationId: 'held-begin', id: 'held', owner: 'visible', bytes: bytes.length, sha256: digest(bytes) }, token);
  const upload = send(f.url + '/v1/objects/upload', { authorization: 'Bearer ' + token, 'content-type': 'application/octet-stream', 'content-length': bytes.length, 'x-upload-grant': begun.body.uploadToken }, bytes.subarray(0, 1)); t.after(() => upload.request.destroy());
  await until(async () => (await f.runtime.objects.status(token, 'held')).state === 'uploading');
  const proof = await f.call('/v1/proof/begin', { username: 'reader', channel: 'email', operationId: 'during-upload' }); assert.equal(proof.status, 200);
  const effect = await f.runtime.engine.read(tx => tx.get(sourceKey('t', 'identity-proof:' + proof.body.proofId)));
  await until(async () => (await f.email.ledger()).some(row => row.value.operationId === effect.data.providerOperationId));
  assert.equal((await f.runtime.objects.status(token, 'held')).state, 'uploading');
  upload.request.end(bytes.subarray(1)); assert.equal((await upload.result).status, 200);
  assert.equal((await f.email.ledger()).filter(row => row.value.operationId === effect.data.providerOperationId).length, 1);
});

test('review an HTTP mutation admitted before quiescence finishes its JSON and commits before maintenance', { skip }, async t => {
  const f = await runtimeFixture(t); await f.runtime.bootstrap(f.bootstrap); const entered = barrier(), original = http.createServer;
  http.createServer = function(options, callback) { return original.call(this, options, (request, response) => { const result = callback(request, response); if (request.url === '/v1/mutate') entered.enter(); return result; }); }; syncBuiltinESMExports();
  t.after(() => { http.createServer = original; syncBuiltinESMExports(); }); await f.start(); const token = await f.login(), visible = await f.runtime.engine.read(tx => tx.get(sourceKey('t', 'visible-entry')));
  const body = Buffer.from(JSON.stringify({ operationId: 'prepause-write', changes: [{ type: 'put', record: { id: visible.id, model: visible.model, owner: visible.owner, data: { ...visible.data, amount: '101' } } }] }));
  const pending = send(f.url + '/v1/mutate', { authorization: 'Bearer ' + token, 'content-type': 'application/json', 'content-length': body.length }, body.subarray(0, 1)); t.after(() => pending.request.destroy()); await entered.entered;
  let checked = false; const pause = f.runtime.quiesce(async () => { assert.equal((await f.runtime.engine.read(tx => tx.get(sourceKey('t', 'visible-entry')))).data.amount, '101'); checked = true; }); void pause.catch(() => {});
  await new Promise(resolve => setImmediate(resolve)); assert.equal(checked, false); pending.request.end(body.subarray(1));
  assert.equal((await pending.result).status, 200); await pause; assert.equal(checked, true);
});
