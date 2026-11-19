import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { pipelineFixture, ledgerIndex, ownerIndex } from './helpers/pipeline-fixture.mjs';
const skip = !process.env.KV_BASE_FDB_CLUSTER_FILE && 'Select a real FoundationDB cluster with KV_BASE_FDB_CLUSTER_FILE';

test('one prepared pipeline delivers proof admits session preserves hidden balance and closes typed object lifecycle', { skip }, async t => {
  const f = await pipelineFixture(t); await f.startService();
  const request = async (path, input, token) => {
    const response = await fetch(f.baseUrl + path, { method: 'POST', headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) }, body: JSON.stringify(input) });
    return { status: response.status, body: await response.json() };
  };
  const begun = await request('/v1/proof/begin', { username: 'reader', channel: 'email', operationId: 'begin' }); assert.equal(begun.status, 200); assert.deepEqual(Object.keys(begun.body).sort(), ['expiresAt', 'proofId']);
  const code = await f.delivery(begun.body), issued = await request('/v1/proof/complete', { proofId: begun.body.proofId, code, operationId: 'complete' }); assert.equal(issued.status, 200); const token = issued.body.token;
  const page = await request('/v1/query', { kind: 'index', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 10 } }, token); assert.equal(page.status, 200); assert.deepEqual(page.body.records.map(record => record.id), ['visible-entry']);
  const summary = await request('/v1/query', { kind: 'summary', query: { index: ledgerIndex, partition: ['ledger'] } }, token); assert.equal(summary.status, 200); assert.deepEqual(summary.body, { summary: { balance: '20' } });
  const bytes = Buffer.from([0, 255, 1, 7]), sha256 = createHash('sha256').update(bytes).digest('hex');
  const grant = await request('/v1/objects/begin', { operationId: 'object-begin', id: 'blob', owner: 'reader', bytes: bytes.length, sha256 }, token); assert.equal(grant.status, 200);
  const upload = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${token}`, 'x-upload-grant': grant.body.uploadToken, 'content-type': 'application/octet-stream' }, body: bytes }); assert.equal(upload.status, 200);
  assert.equal((await request('/v1/objects/finalize', { operationId: 'object-ready', id: 'blob', uploadToken: grant.body.uploadToken }, token)).status, 200);
  const record = { id: 'attached', model: 'Entry', owner: 'reader', data: { ledgerId: 'ledger', at: '2', sequence: '1', amount: '5', attachment: 'blob' } };
  assert.equal((await request('/v1/mutate', { operationId: 'attach', changes: [{ type: 'put', record }] }, token)).status, 200);
  const download = await fetch(f.baseUrl + '/v1/objects/blob/data', { headers: { authorization: `Bearer ${token}` } }); assert.equal(download.status, 200); assert.deepEqual(Buffer.from(await download.arrayBuffer()), bytes);
  const blocked = await request('/v1/objects/remove', { operationId: 'blocked-delete', id: 'blob' }, token); assert.notEqual(blocked.status, 200); assert.deepEqual(Buffer.from(await (await fetch(f.baseUrl + '/v1/objects/blob/data', { headers: { authorization: `Bearer ${token}` } })).arrayBuffer()), bytes);
  assert.equal((await request('/v1/mutate', { operationId: 'detach', changes: [{ type: 'delete', id: 'attached' }] }, token)).status, 200);
  assert.equal((await request('/v1/objects/remove', { operationId: 'delete', id: 'blob' }, token)).status, 200); await f.objects.reconcile(); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
  assert.equal((await f.objects.checkFiles()).ready, 0); await f.engine.check(); assert.equal(f.providerStats().email.starts, 1);
});
