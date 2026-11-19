import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { connect } from 'node:net';
import { fork } from 'node:child_process';
import { createService } from '../src/service.mjs';
import { sourceKey } from '../src/keys.mjs';
import { pipelineFixture, pipelinePolicy, ownerIndex, ledgerIndex } from './helpers/pipeline-fixture.mjs';

const skip = process.env.KV_BASE_FDB_CLUSTER_FILE ? false : 'KV_BASE_FDB_CLUSTER_FILE must select a real test cluster';
async function fixture(t, options = {}) {
  const f = await pipelineFixture(t, options);
  f.echoCalls = 0; f.operations['/v1/echo'] = async (_token, input) => { f.echoCalls++; if (!input || Object.keys(input).length !== 1 || typeof input.echo !== 'string') throw Object.assign(Error('Invalid echo'), { code: 'BAD_REQUEST' }); return { echo: input.echo }; };
  await f.startService(options.service);
  f.call = async (path, input, token, configuration = {}) => {
    const response = await fetch(f.baseUrl + path, { method: configuration.method ?? 'POST', headers: { 'content-type': 'application/json', ...(token === undefined ? {} : { authorization: `Bearer ${token}` }), ...configuration.headers }, ...(input === undefined ? {} : { body: typeof input === 'string' ? input : JSON.stringify(input) }) });
    const text = await response.text(); let body; try { body = JSON.parse(text); } catch {}
    return { status: response.status, body, text, headers: response.headers };
  };
  f.httpLogin = async (username = 'reader', operationId = `http-${randomUUID()}`) => { const response = await f.call('/v1/login', { username, password: f.password, operationId }); assert.equal(response.status, 200); return response.body.token; };
  f.token = await f.httpLogin(); return f;
}
const mutate = (f, operationId, changes, token = f.token, extras = {}) => f.call('/v1/mutate', { operationId, changes, ...extras }, token);
const put = (id, owner = 'visible', amount = '7') => ({ type: 'put', record: { id, model: 'Entry', owner, data: { ledgerId: 'ledger', at: '2', sequence: '1', amount } } });
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
async function expectError(promise, status, error) { const response = await promise; assert.equal(response.status, status, response.text); assert.deepEqual(response.body, { error }); return response; }
async function object(f, id = 'attachment', bytes = Buffer.from('attachment bytes')) {
  const begun = await f.call('/v1/objects/begin', { operationId: `begin-${id}`, id, owner: 'visible', bytes: bytes.length, sha256: digest(bytes) }, f.token); assert.equal(begun.status, 200, begun.text);
  const uploaded = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${f.token}`, 'content-type': 'application/octet-stream', 'x-upload-grant': begun.body.uploadToken }, body: bytes }); assert.equal(uploaded.status, 200, await uploaded.text());
  const finalized = await f.call('/v1/objects/finalize', { operationId: `finalize-${id}`, id, uploadToken: begun.body.uploadToken }, f.token); assert.equal(finalized.status, 200, finalized.text);
  return { begun: begun.body, bytes };
}

test('primary FoundationDB HTTP: fixed routes expose health and deny raw diagnostic policy and caller context endpoints', { skip }, async t => {
  const f = await fixture(t), before = await f.rows();
  const health = await f.call('/health', undefined, undefined, { method: 'GET' }); assert.deepEqual(health.body, { status: 'ok' }); assert.equal(health.status, 200);
  for (const path of ['/v1/raw', '/v1/receipt', '/v1/check', '/v1/rebuild', '/v1/retire', '/v1/authority', '/v1/keys', '/v1/effects/drain', '/v1/objects/reconcile']) await expectError(f.call(path, {}, f.token), 404, 'NOT_FOUND');
  await expectError(f.call('/v1/login', { actor: 'admin', password: f.password, operationId: 'actor' }), 400, 'BAD_REQUEST');
  await expectError(f.call('/v1/login', { username: 'reader', password: f.password, operationId: 'context', tenant: 'other' }), 400, 'BAD_REQUEST');
  await expectError(mutate(f, 'context', [put('new')], f.token, { context: { tenant: 't', actor: 'admin' } }), 400, 'BAD_REQUEST');
  await expectError(f.call('/v1/query?raw=true', {}, f.token), 400, 'BAD_REQUEST'); await expectError(f.call('/v1/query', {}, f.token, { headers: { origin: 'https://other.example' } }), 400, 'BAD_REQUEST');
  assert.deepEqual(await f.rows(), before);
});

test('primary FoundationDB HTTP: bearer validation fences every authenticated fixed operation before handlers and admission exposes no tokens', { skip }, async t => {
  const f = await fixture(t), before = await f.rows();
  for (const token of [undefined, 'invalid', 's1.old.' + Buffer.alloc(32, 1).toString('base64url')]) { await expectError(f.call('/v1/echo', { echo: 'safe' }, token), 401, 'UNAUTHENTICATED'); await expectError(f.call('/v1/mutate', { operationId: 'invalid', changes: [put('bad')] }, token), 401, 'UNAUTHENTICATED'); }
  assert.equal(f.echoCalls, 0); assert.deepEqual(await f.rows(), before);
  const echo = await f.call('/v1/echo', { echo: 'safe' }, f.token); assert.deepEqual(echo.body, { echo: 'safe' }); assert.equal(f.echoCalls, 1);
  const unknown = await f.call('/v1/login', { username: 'unknown', password: f.password, operationId: 'unknown' }); assert.equal(unknown.status, 401); assert.equal(unknown.text.includes('reader'), false); assert.equal(unknown.text.includes(f.token), false);
});

test('primary FoundationDB HTTP: authored receipt identity binds canonical route commands while exact replay preserves source headers', { skip }, async t => {
  const f = await fixture(t), command = put('created');
  const accepted = await mutate(f, 'original-create', [command]); assert.deepEqual(accepted.body, { applied: 1 }); const before = await f.sourceRows(); f.time += 10;
  assert.deepEqual((await mutate(f, 'original-create', [command])).body, accepted.body); assert.deepEqual(await f.sourceRows(), before);
  await expectError(mutate(f, 'original-create', [put('created', 'visible', '8')]), 409, 'RECEIPT_MISMATCH');
  await expectError(f.call('/v1/objects/begin', { operationId: 'original-create', id: 'cross-route', owner: 'visible', bytes: 0, sha256: digest(Buffer.alloc(0)) }, f.token), 409, 'RECEIPT_MISMATCH'); assert.deepEqual(await f.sourceRows(), before);
  await expectError(mutate(f, 'wire-tenant', [{ type: 'put', record: { ...command.record, tenant: 'other' } }]), 400, 'BAD_REQUEST');
  await expectError(mutate(f, 'wire-created', [{ type: 'put', record: { ...command.record, createdAt: '0' } }]), 400, 'BAD_REQUEST');
  await expectError(mutate(f, 'null', [null]), 400, 'BAD_REQUEST');
});

test('primary FoundationDB HTTP: point ordered incoming and published reads disclose independently selected visible facts only', { skip }, async t => {
  const f = await fixture(t);
  const queries = [
    { kind: 'point', model: 'Entry', query: { id: 'visible-entry' } },
    { kind: 'index', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 10 } },
    { kind: 'incoming', query: { model: 'Entry', targetId: 'ledger', slot: 'ledger', limit: 10 } },
  ];
  for (const query of queries) { const response = await f.call('/v1/query', query, f.token); assert.equal(response.status, 200, response.text); assert.deepEqual(response.body.records.map(row => row.id), ['visible-entry']); assert.equal(response.text.includes('hidden-entry'), false); assert.equal(response.text.includes('-80'), false); assert.deepEqual(Object.keys(response.body), ['records']); }
  const hidden = await f.call('/v1/query', { kind: 'point', model: 'Entry', query: { id: 'hidden-entry' } }, f.token); assert.deepEqual(hidden.body, { records: [] });
  const summary = await f.call('/v1/query', { kind: 'summary', query: { index: ledgerIndex, partition: ['ledger'] } }, f.token); assert.equal(summary.status, 200, summary.text); assert.deepEqual(summary.body, { summary: { balance: '20' } });
  for (const response of [hidden, summary]) { assert.equal(response.text.includes('hidden-entry'), false); assert.equal(response.text.includes('count'), false); assert.equal(response.text.includes('metrics'), false); assert.equal(response.text.includes('node'), false); }
  await expectError(f.call('/v1/query', { kind: 'point', model: 'Credential', query: { id: 'email-credential' } }, f.token), 403, 'FORBIDDEN');
  await expectError(f.call('/v1/query', { kind: 'index', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 10, getAuthority: 'wire' } }, f.token), 400, 'INVALID_READ');
});

test('primary FoundationDB HTTP: transfers require both owners and fresh queries enforce current revocation without disclosing hidden sources', { skip }, async t => {
  const f = await fixture(t), before = await f.sourceRows(), moved = { type: 'put', record: { id: 'visible-entry', model: 'Entry', owner: 'hidden', data: { ledgerId: 'ledger', at: '1', sequence: '1', amount: '100' } } };
  await expectError(mutate(f, 'forbidden-transfer', [moved]), 403, 'FORBIDDEN'); assert.deepEqual(await f.sourceRows(), before);
  const admin = await f.httpLogin('admin'); assert.deepEqual((await mutate(f, 'admin-transfer', [moved], admin)).body, { applied: 1 });
  const response = await f.call('/v1/query', { kind: 'index', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 10 } }, f.token); assert.deepEqual(response.body, { records: [] }); assert.equal(response.text.includes('hidden-entry'), false);
  const revoked = pipelinePolicy(); revoked.principals.find(node => node.id === 'reader').active = false; await f.identity.replacePolicy(revoked); await expectError(f.call('/v1/echo', { echo: 'safe' }, f.token), 401, 'UNAUTHENTICATED');
});

test('primary FoundationDB HTTP: public Object queries remain available while generic writes cannot bypass file lifecycle', { skip }, async t => {
  const f = await fixture(t), active = await object(f), before = await f.sourceRows();
  await expectError(mutate(f, 'delete-object', [{ type: 'delete', id: 'attachment' }]), 403, 'FORBIDDEN');
  await expectError(mutate(f, 'fake-object', [{ type: 'put', record: { id: 'fake-ready', model: 'Object', owner: 'visible', data: { state: 'ready', bytes: 0, sha256: digest(Buffer.alloc(0)) } } }]), 403, 'FORBIDDEN'); assert.deepEqual(await f.sourceRows(), before);
  const query = await f.call('/v1/query', { kind: 'point', model: 'Object', query: { id: 'attachment' } }, f.token); assert.equal(query.status, 200); assert.equal(query.body.records[0].data.state, 'ready');
  const response = await fetch(f.baseUrl + '/v1/objects/attachment/data', { headers: { authorization: `Bearer ${f.token}` } }); assert.equal(response.status, 200); assert.deepEqual(Buffer.from(await response.arrayBuffer()), active.bytes); await f.objects.checkFiles();
});

test('primary FoundationDB HTTP: upload grants bind owner session bytes digest and server-selected filename', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.from('verified object data'), input = { operationId: 'upload-begin', id: 'byte-object', owner: 'visible', bytes: bytes.length, sha256: digest(bytes) };
  await expectError(f.call('/v1/objects/begin', { ...input, filename: '../escape' }, f.token), 400, 'INVALID_OBJECT');
  const begun = await f.call('/v1/objects/begin', input, f.token); assert.equal(begun.status, 200); const replacement = await f.httpLogin('reader');
  const denied = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${replacement}`, 'x-upload-grant': begun.body.uploadToken, 'content-type': 'application/octet-stream' }, body: bytes }); assert.equal(denied.status, 403); assert.deepEqual(await denied.json(), { error: 'INVALID_OBJECT_GRANT' });
  const corrupted = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${f.token}`, 'x-upload-grant': begun.body.uploadToken, 'content-type': 'application/octet-stream' }, body: Buffer.alloc(bytes.length, 1) }); assert.equal(corrupted.status, 400); assert.deepEqual(await corrupted.json(), { error: 'OBJECT_DIGEST' });
  const oversized = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${f.token}`, 'x-upload-grant': begun.body.uploadToken, 'content-type': 'application/octet-stream' }, body: Buffer.alloc(bytes.length + 1) }); assert.equal(oversized.status, 413); assert.deepEqual(await oversized.json(), { error: 'OBJECT_SIZE' });
  assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
});

test('primary FoundationDB HTTP: delivered proof routes map only configured usernames and expose original one-use sanitized results', { skip }, async t => {
  const f = await fixture(t);
  await expectError(f.call('/v1/proof/begin', { actor: 'admin', channel: 'email', operationId: 'actor' }), 400, 'BAD_REQUEST');
  const begun = await f.call('/v1/proof/begin', { username: 'reader', channel: 'phone', operationId: 'phone-proof' }); assert.equal(begun.status, 200); assert.deepEqual(Object.keys(begun.body), ['proofId', 'expiresAt']); assert.equal(begun.text.includes('effect'), false); assert.equal(begun.text.includes('phone'), false);
  const code = await f.delivery(begun.body), request = { proofId: begun.body.proofId, code, operationId: 'phone-completion' }, issued = await f.call('/v1/proof/complete', request); assert.equal(issued.status, 200); assert.deepEqual(Object.keys(issued.body), ['token', 'expiresAt']); assert.deepEqual((await f.call('/v1/proof/complete', request)).body, issued.body);
  await expectError(f.call('/v1/proof/complete', { ...request, operationId: 'other-completion' }), 401, 'UNAUTHENTICATED'); assert.equal(f.providerStats().sms.starts, 1);
});

test('primary FoundationDB HTTP: body output page limits and malformed UTF8 fail with bounded sanitized responses', { skip }, async t => {
  const f = await fixture(t, { service: { maxBodyBytes: 512, maxResponseBytes: 160, maxPageSize: 2 } });
  await expectError(f.call('/v1/echo', { echo: 'x'.repeat(600) }, f.token), 413, 'REQUEST_LIMIT');
  const large = await expectError(f.call('/v1/echo', { echo: 'x'.repeat(155) }, f.token), 429, 'WORK_LIMIT'); assert.ok(Buffer.byteLength(large.text) <= 160); assert.equal(large.text.includes('xxx'), false);
  await expectError(f.call('/v1/query', { kind: 'index', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 3 } }, f.token), 400, 'BAD_REQUEST');
  await expectError(f.call('/v1/echo', '{"echo":', f.token), 400, 'BAD_REQUEST');
  const invalid = await fetch(f.baseUrl + '/v1/echo', { method: 'POST', headers: { authorization: `Bearer ${f.token}`, 'content-type': 'application/json' }, body: Buffer.from([123, 34, 120, 34, 58, 34, 255, 34, 125]) }); assert.equal(invalid.status, 400); assert.deepEqual(await invalid.json(), { error: 'BAD_REQUEST' });
});

test('primary FoundationDB HTTP: invalid writable lifecycle and cross-tenant host compositions reject before listening', { skip }, async t => {
  const f = await pipelineFixture(t), configuration = { engine: f.engine, identity: f.identity, objects: f.objects, logins: f.logins, publicModels: ['Ledger', 'Entry', 'Object'] };
  for (const patch of [{ writableModels: ['Object'] }, { writableModels: ['Effect'] }, { writableModels: 'Entry' }, { identity: { ...f.identity, tenant: 'other' } }, { objects: { ...f.objects, tenant: 'other' } }, { requestTimeoutMs: 2 ** 40 }]) assert.throws(() => createService({ ...configuration, ...patch }), { code: 'INVALID_SERVICE' });
});

async function stalledRequest(url, { method = 'POST', headers = {}, first = Buffer.from('{'), continuous = false } = {}) {
  let request, interval; const start = performance.now();
  const result = new Promise(resolve => {
    request = httpRequest(url, { method, headers }, response => { const chunks = []; response.on('data', chunk => chunks.push(chunk)); response.on('end', () => resolve({ status: response.statusCode, text: Buffer.concat(chunks).toString(), elapsed: performance.now() - start })); });
    request.on('error', error => resolve({ error: error.code, elapsed: performance.now() - start })); request.write(first);
    if (continuous) interval = setInterval(() => request.write(' '), 10);
  });
  return { request, result, stop: () => { clearInterval(interval); request.destroy(); } };
}

test('primary FoundationDB HTTP: continuous incomplete JSON has an absolute deadline and releases its admission slot', { skip }, async t => {
  const f = await fixture(t, { service: { requestTimeoutMs: 100, maxActive: 1 } }), before = await f.rows();
  const stalled = await stalledRequest(f.baseUrl + '/v1/login', { headers: { 'content-type': 'application/json', 'content-length': 1000 }, continuous: true }); t.after(stalled.stop);
  await new Promise(resolve => setTimeout(resolve, 20)); await expectError(f.call('/health', undefined, undefined, { method: 'GET' }), 429, 'WORK_LIMIT');
  const response = await stalled.result; stalled.stop(); assert.equal(response.status, 408, response.error); assert.deepEqual(JSON.parse(response.text), { error: 'REQUEST_TIMEOUT' }); assert.ok(response.elapsed < 750);
  assert.equal((await f.call('/health', undefined, undefined, { method: 'GET' })).status, 200); assert.deepEqual(await f.rows(), before);
});

test('primary FoundationDB HTTP: a stalled upload discards partial bytes before the same private grant retries successfully', { skip }, async t => {
  const f = await fixture(t, { service: { requestTimeoutMs: 120 } }), bytes = Buffer.from('object content');
  const begun = await f.call('/v1/objects/begin', { operationId: 'stall-begin', id: 'stalled-upload', owner: 'visible', bytes: bytes.length, sha256: digest(bytes) }, f.token); assert.equal(begun.status, 200);
  const stalled = await stalledRequest(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${f.token}`, 'content-type': 'application/octet-stream', 'content-length': bytes.length, 'x-upload-grant': begun.body.uploadToken }, first: bytes.subarray(0, 1) }); t.after(stalled.stop);
  const response = await stalled.result; stalled.stop(); assert.equal(response.status, 408, response.error); assert.ok(response.elapsed < 1000); assert.deepEqual(JSON.parse(response.text), { error: 'REQUEST_TIMEOUT' }); assert.deepEqual(await f.objects.inspectFiles(), { stages: [], objects: [] });
  const retry = await fetch(f.baseUrl + '/v1/objects/upload', { method: 'PUT', headers: { authorization: `Bearer ${f.token}`, 'content-type': 'application/octet-stream', 'x-upload-grant': begun.body.uploadToken }, body: bytes }); assert.equal(retry.status, 200, await retry.text());
  const finalized = await f.call('/v1/objects/finalize', { operationId: 'stall-finalize', id: 'stalled-upload', uploadToken: begun.body.uploadToken }, f.token); assert.equal(finalized.status, 200); await f.objects.checkFiles();
});

test('primary FoundationDB HTTP: paused large download expires and service shutdown drains without waiting for the reader', { skip }, async t => {
  const f = await fixture(t), bytes = Buffer.alloc(8 * 1024 * 1024, 13); await object(f, 'large-file', bytes);
  await f.service.close(); const errors = []; await f.startService({ requestTimeoutMs: 120, onError: event => errors.push(event.code) });
  const url = new URL(f.baseUrl), socket = connect(Number(url.port), '127.0.0.1'); t.after(() => socket.destroy());
  await new Promise((resolve, reject) => { socket.once('error', reject); socket.once('connect', () => socket.write(`GET /v1/objects/large-file/data HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ${f.token}\r\n\r\n`)); socket.once('data', chunk => { assert.ok(chunk.toString('latin1').startsWith('HTTP/1.1 200')); socket.pause(); resolve(); }); });
  const start = performance.now(); await f.service.close(); assert.ok(performance.now() - start < 1000); await new Promise(resolve => setTimeout(resolve, 20)); assert.ok(errors.includes('REQUEST_TIMEOUT'), JSON.stringify(errors)); socket.destroy();
});

test('primary FoundationDB HTTP: explicit download byte cap returns only a bounded status before streaming private files', { skip }, async t => {
  const f = await fixture(t); await object(f); await f.service.close(); await f.startService({ maxDownloadBytes: 1 });
  const response = await fetch(f.baseUrl + '/v1/objects/attachment/data', { headers: { authorization: `Bearer ${f.token}` } }); assert.equal(response.status, 429); assert.deepEqual(await response.json(), { error: 'WORK_LIMIT' }); await f.objects.checkFiles();
});

test('primary FoundationDB HTTP: opaque pages fence replacement sessions password revisions and live incarnation changes', { skip }, async t => {
  const f = await fixture(t); assert.equal((await mutate(f, 'second', [put('second')])).status, 200);
  const query = { kind: 'page', query: { model: 'Entry', index: ownerIndex, partition: ['ledger'], limit: 1 } }, first = await f.call('/v1/query', query, f.token); assert.equal(typeof first.body.cursor, 'string'); assert.deepEqual(first.body.records.map(row => row.id), ['visible-entry']);
  const next = await f.httpLogin(); await expectError(f.call('/v1/query', { ...query, cursor: first.body.cursor }, next), 409, 'STALE_CURSOR');
  assert.equal((await mutate(f, 'unrelated', [{ type: 'put', record: { id: 'other-ledger', model: 'Ledger', owner: 'visible', data: { name: 'Unrelated' } } }])).status, 200);
  assert.deepEqual((await f.call('/v1/query', { ...query, cursor: first.body.cursor }, f.token)).body.records.map(row => row.id), ['second']);
  const beforePassword = await f.call('/v1/query', query, next), password = 'a new pipeline password value'; assert.equal((await f.call('/v1/password', { oldPassword: f.password, newPassword: password, operationId: 'http-password' }, next)).status, 200); f.password = password;
  const updated = await f.httpLogin(); await expectError(f.call('/v1/query', { ...query, cursor: beforePassword.body.cursor }, updated), 409, 'STALE_CURSOR');
  const beforeFence = await f.call('/v1/query', query, updated); f.fence = 'incarnation-B'; await expectError(f.call('/v1/query', query, updated), 401, 'UNAUTHENTICATED');
  const refreshed = await f.httpLogin(); await expectError(f.call('/v1/query', { ...query, cursor: beforeFence.body.cursor }, refreshed), 409, 'STALE_CURSOR');
});

for (const phase of ['beforeCommit', 'afterCommit']) test(`primary FoundationDB HTTP: real service child SIGKILL ${phase} recovers the exact authorized mutation identity`, { skip }, async t => {
  const f = await fixture(t), command = put('process-entry'), input = { operationId: 'crash-' + phase, changes: [command] };
  await f.service.close(); f.service = undefined;
  const child = fork(new URL('./fixtures/service-process.mjs', import.meta.url), [process.env.KV_BASE_FDB_CLUSTER_FILE, f.prefix, f.directory, phase, String(f.time), f.fence], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] }); t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM'); });
  const port = await new Promise((resolve, reject) => { const timer = setTimeout(() => reject(Error('Service child readiness timeout')), 15000); child.once('error', reject); child.once('message', message => { clearTimeout(timer); if (message.error) reject(Error(message.error)); else { assert.equal(message.ready, true); resolve(message.port); } }); });
  const reached = new Promise((resolve, reject) => { const timer = setTimeout(() => reject(Error('Service child commit hook timeout')), 15000); child.once('message', message => { clearTimeout(timer); if (message.error) reject(Error(message.error)); else { assert.equal(message.phase, phase); resolve(); } }); });
  const lostResponse = fetch(`http://127.0.0.1:${port}/v1/mutate`, { method: 'POST', headers: { authorization: `Bearer ${f.token}`, 'content-type': 'application/json' }, body: JSON.stringify(input) }).then(() => { throw Error('Killed service should not return acknowledgement'); }, () => true);
  await reached; const stopped = new Promise(resolve => child.once('exit', resolve)); child.kill('SIGKILL'); await stopped; await lostResponse;
  await f.reopen(); await f.startService();
  const initial = await f.engine.read(tx => tx.get(sourceKey('t', 'process-entry'))); assert.equal(initial === undefined, phase === 'beforeCommit');
  const replay = await f.call('/v1/mutate', input, f.token); assert.equal(replay.status, 200, replay.text); assert.deepEqual(replay.body, { applied: 1 }); const sources = await f.sourceRows(); f.time += 1;
  assert.deepEqual((await f.call('/v1/mutate', input, f.token)).body, replay.body); assert.deepEqual(await f.sourceRows(), sources); await f.engine.check();
});
