import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { readAuthorizedInTransaction, readPointAuthorized, readIncomingAuthorized, readAnchorSummaryAuthorizedInTransaction } from './authorized-read.mjs';
import { mutateAuthorizedInTransaction } from './authorized-mutate.mjs';
import { sourceKey } from './keys.mjs';
import { assertJson, fail } from './values.mjs';

function fields(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))
    || Object.keys(value).some(key => !allowed.includes(key)) || required.some(key => !Object.hasOwn(value, key))) fail('BAD_REQUEST', 'Invalid request fields');
}
const finite = value => Number.isSafeInteger(value) && value > 0;
const denied = () => fail('FORBIDDEN', 'Request is not permitted');
const statuses = { BAD_REQUEST: 400, INVALID_READ: 400, INVALID_MUTATION: 400, INVALID_RECORD: 400,
  INVALID_IDENTITY: 400, INVALID_PROOF: 400, INVALID_VALUE: 400, REQUEST_TIMEOUT: 408, RECEIPT_MISMATCH: 409, UNAUTHENTICATED: 401,
  FORBIDDEN: 403, WORK_LIMIT: 429, AUTH_WORK_LIMIT: 429, STORAGE_CONFLICT: 409,
  COMMIT_UNKNOWN: 503, RESTORE_FENCED: 401, AUTH_CHANGED: 409, REQUEST_LIMIT: 413 };
Object.assign(statuses, { STORAGE_LIMIT: 429, INVALID_OBJECT: 400, INVALID_OBJECT_GRANT: 403,
  OBJECT_EXISTS: 409, OBJECT_BUSY: 409, OBJECT_SIZE: 413, OBJECT_DIGEST: 400,
  OBJECT_TIMEOUT: 408, OBJECT_NOT_READY: 409, INVALID_CURSOR: 400, STALE_CURSOR: 409 });

/** Fixed host routes. Caller input never supplies context, policy providers,
 * transport callbacks, secret keys or diagnostic APIs. TLS belongs to the host. */
export function createService({ engine, identity, logins, publicModels, writableModels, operations = {}, objects,
  maxBodyBytes = 262144, maxResponseBytes = 4194304, maxActive = 32, maxPageSize = 100,
  requestTimeoutMs = 30000, maxUploadBytes = 16777216, maxDownloadBytes = maxUploadBytes, clock = Date.now, onError = () => {}, getHealth = () => ({ status: 'ok' }) }) {
  if (!engine?.catalog || !identity || identity.tenant !== engine.tenant || !logins || !Array.isArray(publicModels) || writableModels !== undefined && !Array.isArray(writableModels)
    || typeof clock !== 'function' || typeof getHealth !== 'function' || ![maxBodyBytes, maxResponseBytes, maxActive, maxPageSize, requestTimeoutMs, maxUploadBytes, maxDownloadBytes].every(finite)) fail('INVALID_SERVICE', 'Invalid service configuration');
  if (objects && (objects.tenant !== engine.tenant || !engine.catalog.models[objects.model]?.access)) fail('INVALID_SERVICE', 'Objects must use the fixed service tenant and catalog');
  if (requestTimeoutMs > 2147483647) fail('INVALID_SERVICE', 'Request timeout exceeds native timer range');
  const models = new Set(publicModels), writes = new Set(writableModels ?? publicModels.filter(model => model !== objects?.model)), accounts = new Map(Object.entries(logins));
  for (const model of writes) if (!models.has(model) || model === objects?.model) fail('INVALID_SERVICE', 'Writable models must be public ordinary authored models');
  if (maxResponseBytes < 128) fail('INVALID_SERVICE', 'Response bound must fit a status');
  for (const model of models) if (!engine.catalog.models[model]?.access || engine.catalog.models[model]?.managed) fail('INVALID_SERVICE', 'Only public authored models may be exposed');
  const extras = new Map(Object.entries(operations));
  for (const [path, operation] of extras) {
    if (!/^\/v1\/[a-z][a-z0-9/-]+$/.test(path)) fail('INVALID_SERVICE', 'Invalid fixed operation');
    if (typeof operation === 'function') extras.set(path, { authenticated: true, handle: operation });
    else if (!operation || typeof operation.handle !== 'function' || typeof operation.authenticated !== 'boolean') fail('INVALID_SERVICE', 'Invalid fixed operation');
  }
  const unknownActor = `unmapped-${randomBytes(32).toString('base64url')}`;
  let active = 0, closing = false;
  function token(request) {
    const match = /^Bearer ([A-Za-z0-9._-]+)$/.exec(request.headers.authorization ?? '');
    if (!match) fail('UNAUTHENTICATED', 'Authentication required'); return match[1];
  }
  async function* body(request, maximum) {
    const iterator = request.iterator({ destroyOnReturn: false })[Symbol.asyncIterator](); let timer, bytes = 0;
    const deadline = new Promise((_, reject) => { timer = setTimeout(() => reject(Object.assign(Error('Request body deadline exceeded'), { code: 'REQUEST_TIMEOUT' })), requestTimeoutMs); });
    try {
      while (true) { const next = await Promise.race([iterator.next(), deadline]); if (next.done) break; bytes += next.value.length; if (bytes > maximum) fail('REQUEST_LIMIT', 'Body too large'); yield next.value; }
    } finally { clearTimeout(timer); void iterator.return?.().catch?.(() => {}); }
  }
  async function json(request) {
    if (request.headers['content-type']?.split(';')[0] !== 'application/json') fail('BAD_REQUEST', 'JSON required');
    const length = request.headers['content-length'];
    if (length !== undefined && (!/^\d+$/.test(length) || Number(length) > maxBodyBytes)) fail('REQUEST_LIMIT', 'Body too large');
    const parts = []; let size = 0;
    for await (const part of body(request, maxBodyBytes)) {
      size += part.length; if (size > maxBodyBytes) fail('REQUEST_LIMIT', 'Body too large'); parts.push(part);
    }
    try { const result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(parts))); assertJson(result); return result; }
    catch { fail('BAD_REQUEST', 'Malformed JSON'); }
  }
  function send(response, status, result) {
    const body = Buffer.from(JSON.stringify(result, (_key, value) => typeof value === 'bigint' ? String(value) : value));
    if (body.length > maxResponseBytes) fail('WORK_LIMIT', 'Response too large');
    response.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length,
      'cache-control': 'no-store', 'x-content-type-options': 'nosniff', ...(status >= 400 ? { connection: 'close' } : {}) }); response.end(body);
  }
  function allowed(model) { if (!models.has(model)) denied(); }
  function writable(model) { if (!writes.has(model)) denied(); }
  async function mutations(bearer, input) {
    fields(input, ['operationId', 'changes']);
    if (!Array.isArray(input.changes) || input.changes.length > maxPageSize) fail('BAD_REQUEST', 'Invalid change batch');
    const changes = input.changes.map(change => {
      if (!change || typeof change !== 'object' || Array.isArray(change)) fail('BAD_REQUEST', 'Invalid change');
      if (change.type === 'put') {
        fields(change, ['type', 'record']); fields(change.record, ['id', 'model', 'owner', 'data'], ['id', 'model', 'data']); writable(change.record.model);
        return { type: 'put', record: { ...change.record, tenant: engine.tenant } };
      }
      fields(change, ['type', 'id']); if (change.type !== 'delete') fail('BAD_REQUEST', 'Invalid change'); return change;
    });
    return identity.transact(bearer, { operationId: input.operationId, request: { route: 'mutate', changes } }, async (tx, context, getAuthority) => {
      const commands = [];
      for (const change of changes) {
        const old = await tx.get(sourceKey(context.tenant, change.type === 'put' ? change.record.id : change.id));
        if (old) writable(old.model);
        if (change.type === 'put') {
          const time = clock(); if (!Number.isSafeInteger(time) || time < 0) fail('INVALID_SERVICE', 'Invalid trusted clock');
          commands.push({ ...change, record: { ...change.record, owner: change.record.owner ?? old?.owner ?? context.actor, createdAt: old?.createdAt ?? String(time) } });
        } else commands.push(change);
      }
      return mutateAuthorizedInTransaction(tx, engine.catalog, context, commands, { getAuthority });
    });
  }
  async function query(bearer, input) {
    fields(input, ['kind', 'query', 'model', 'cursor'], ['kind', 'query']);
    if (!['point', 'index', 'page', 'incoming', 'summary'].includes(input.kind)) fail('BAD_REQUEST', 'Invalid query');
    if (input.kind === 'point') allowed(input.model);
    else if (input.kind === 'summary') allowed(input.query?.index?.[0]);
    else allowed(input.query?.model);
    if (input.query?.limit !== undefined && (!Number.isSafeInteger(input.query.limit) || input.query.limit < 0 || input.query.limit > maxPageSize)) fail('BAD_REQUEST', 'Page too large');
    if (input.kind === 'page') return identity.authorizedPage(bearer, input.query, input.cursor === undefined ? {} : { cursor: input.cursor });
    if (input.cursor !== undefined) fail('BAD_REQUEST', 'Cursor requires a page query');
    return identity.read(bearer, async (tx, context, getAuthority) => {
      const store = { read: callback => callback(tx) }, options = { getAuthority };
      if (input.kind === 'point') {
        const record = await tx.get(sourceKey(context.tenant, input.query?.id));
        if (record && record.model !== input.model) return { records: [] };
        return readPointAuthorized(store, engine.catalog, context, input.query, options);
      }
      if (input.kind === 'incoming') return readIncomingAuthorized(store, engine.catalog, context, input.query, options);
      if (input.kind === 'summary') return readAnchorSummaryAuthorizedInTransaction(tx, engine.catalog, context, input.query, options);
      return readAuthorizedInTransaction(tx, engine.catalog, context, input.query, options);
    });
  }
  const server = createServer({ maxHeaderSize: 16384, requestTimeout: requestTimeoutMs, headersTimeout: requestTimeoutMs }, async (request, response) => {
    let admitted = false;
    try {
      if (closing || active >= maxActive) fail('WORK_LIMIT', 'Service admission exhausted');
      active++; admitted = true;
      const url = new URL(request.url, 'http://localhost');
      if (url.search || request.headers.origin !== undefined) fail('BAD_REQUEST', 'Unsupported request');
      if (request.method === 'GET' && url.pathname === '/health') {
        const health = await getHealth(), status = health?.status;
        if (!['ok', 'ready', 'uninitialized', 'unavailable', 'stopped'].includes(status)) fail('INVALID_SERVICE', 'Invalid trusted health status');
        send(response, ['ok', 'ready'].includes(status) ? 200 : 503, { status }); return;
      }
      if (objects && request.method === 'PUT' && url.pathname === '/v1/objects/upload') {
        const bearer = token(request), grant = request.headers['x-upload-grant'];
        if (typeof grant !== 'string' || request.headers['content-type'] !== 'application/octet-stream') fail('BAD_REQUEST', 'Upload grant and byte body required');
        const declared = request.headers['content-length'];
        if (declared !== undefined && (!/^\d+$/.test(declared) || Number(declared) > maxUploadBytes)) fail('REQUEST_LIMIT', 'Upload too large');
        const result = await objects.upload(bearer, grant, body(request, maxUploadBytes)); send(response, 200, result); return;
      }
      const objectPath = objects && /^\/v1\/objects\/([^/]+)(\/data)?$/.exec(url.pathname);
      if (objectPath && request.method === 'GET') {
        const bearer = token(request); let objectId;
        try { objectId = decodeURIComponent(objectPath[1]); } catch { fail('BAD_REQUEST', 'Invalid object path'); }
        if (!objectPath[2]) { send(response, 200, await objects.status(bearer, objectId)); return; }
        const result = await objects.download(bearer, objectId);
        if (!finite(result.bytes) && result.bytes !== 0 || result.bytes > maxDownloadBytes) { result.stream.destroy(); fail('WORK_LIMIT', 'Download too large'); }
        response.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': result.bytes,
          'cache-control': 'no-store', 'x-content-type-options': 'nosniff', 'etag': '"' + result.sha256 + '"' });
        const signal = AbortSignal.timeout(requestTimeoutMs);
        try { await pipeline(result.stream, response, { signal }); } catch (cause) { if (signal.aborted) fail('REQUEST_TIMEOUT', 'Download deadline exceeded'); throw cause; } return;
      }
      if (request.method !== 'POST') { send(response, 404, { error: 'NOT_FOUND' }); return; }
      const objectMethod = objects && { '/v1/objects/begin': 'begin', '/v1/objects/finalize': 'finalize', '/v1/objects/remove': 'remove', '/v1/objects/transfer': 'transfer' }[url.pathname];
      if (!['/v1/login', '/v1/logout', '/v1/password', '/v1/mutate', '/v1/query'].includes(url.pathname) && !extras.has(url.pathname) && !objectMethod) { send(response, 404, { error: 'NOT_FOUND' }); return; }
      const anonymous = url.pathname === '/v1/login' || extras.get(url.pathname)?.authenticated === false;
      const bearer = anonymous ? undefined : token(request), input = await json(request);
      if (extras.has(url.pathname) && !anonymous) await identity.read(bearer, () => null);
      let result;
      switch (url.pathname) {
        case '/v1/login': {
          fields(input, ['username', 'password', 'operationId']);
          const actor = accounts.get(input.username);
          // A fixed unknown identity still runs the password KDF without creating
          // attacker-selected principal/failure namespaces.
          result = await identity.login({ actor: actor ?? unknownActor, password: input.password, operationId: input.operationId }); break;
        }
        case '/v1/logout': result = await identity.logout(bearer, input); break;
        case '/v1/password': result = await identity.changePassword(bearer, input); break;
        case '/v1/mutate': result = await mutations(bearer, input); break;
        case '/v1/query': result = await query(bearer, input); break;
        default: result = objectMethod ? await objects[objectMethod](bearer, input) : await extras.get(url.pathname).handle(bearer, input);
      }
      send(response, 200, result);
    } catch (error) {
      request.resume();
      const status = statuses[error.code] ?? 500, code = statuses[error.code] ? error.code : 'INTERNAL_ERROR';
      try { onError({ code }); } catch {}
      if (!response.headersSent) send(response, status, { error: code }); else response.destroy();
    } finally { if (admitted) active--; }
  });
  server.setTimeout(Math.min(2147483647, requestTimeoutMs * 2), socket => socket.destroy());
  server.keepAliveTimeout = 5000;
  return Object.freeze({ server,
    async listen({ host = '127.0.0.1', port = 0 } = {}) {
      if (closing) fail('SERVICE_CLOSED', 'Service is closed');
      await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, () => { server.removeListener('error', reject); resolve(); }); });
      const address = server.address(); return { host: address.address, port: address.port };
    },
    async close() {
      closing = true; if (!server.listening) return;
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    },
  });
}
