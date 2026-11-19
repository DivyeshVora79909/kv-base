import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { mkdir, lstat, chmod, open, rename, unlink, readdir } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { resolveRequestAuthority, permits } from './authority.mjs';
import { mutateAuthorizedInTransaction } from './authorized-mutate.mjs';
import { validateRecord } from './catalog.mjs';
import { sourceKey, sourcePrefix } from './keys.mjs';
import { canonical, cloneData, decodeDomain, fingerprint, fail } from './values.mjs';

const natural = n => Number.isSafeInteger(n) && n >= 0;
const maxObjectBytes = 256 * 1024 * 1024;

/** Ordinary source declaration, merged once into the application's catalog. */
export function objectModel() {
  return { schema: { type: 'object', additionalProperties: false, required: ['state', 'bytes', 'sha256'], properties: {
    state: { enum: ['pending', 'ready'] }, bytes: { type: 'integer', minimum: 0, maximum: maxObjectBytes }, sha256: { type: 'string', pattern: '^[a-f0-9]{64}$' },
  } }, access: { orders: {} } };
}

function fields(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('INVALID_OBJECT', 'Expected ordinary object input');
  for (const name of Reflect.ownKeys(value)) { const d = Object.getOwnPropertyDescriptor(value, name); if (!allowed.includes(name) || !d.enumerable || !Object.hasOwn(d, 'value')) fail('INVALID_OBJECT', 'Invalid object fields'); }
  if (required.some(name => !Object.hasOwn(value, name))) fail('INVALID_OBJECT', 'Missing object field');
}
const id = value => decodeDomain('id', value);
const invalidGrant = () => fail('INVALID_OBJECT_GRANT', 'Object grant is not valid');
const forbidden = () => fail('FORBIDDEN', 'Operation is not permitted');
const filename = value => { if (typeof value !== 'string' || !/^[a-f0-9-]{36}$/.test(value)) fail('CORRUPT_OBJECT', 'Invalid private filename'); return value; };
const metadata = (row, state) => ({ state, bytes: row.bytes, sha256: row.sha256 });
async function syncDirectory(directory) { const handle = await open(directory, constants.O_RDONLY | constants.O_DIRECTORY); try { await handle.sync(); } finally { await handle.close(); } }
async function absentUnlink(path) { try { await unlink(path); } catch (cause) { if (cause.code !== 'ENOENT') throw cause; } }

/** Private local objects over one prepared source catalog. File I/O stays outside
 * engine transactions; private durable intents reconcile the file/DB boundary. */
export async function openObjectStore(configuration) {
  fields(configuration, ['engine', 'identity', 'keyring', 'getRestoreFence', 'directory', 'model', 'clock', 'limits', 'onPhase'], ['engine', 'identity', 'keyring', 'getRestoreFence', 'directory', 'model']);
  const { engine, identity, keyring, getRestoreFence, directory, model, clock = Date.now, onPhase = async () => {} } = configuration;
  if (!engine?.store?.capabilities?.durable || identity?.tenant !== engine.tenant || !engine.catalog?.models[model]?.access || typeof identity?.authenticateBindingInTransaction !== 'function' || typeof keyring?.digest !== 'function' || typeof getRestoreFence !== 'function' || typeof clock !== 'function' || typeof onPhase !== 'function'
    || typeof directory !== 'string' || !isAbsolute(directory) || directory.includes('\0')) fail('INVALID_OBJECT', 'Durable engine, current identity, custody and private directory required');
  fields(configuration.limits ?? {}, ['maxBytes', 'grantMs', 'uploadMs', 'maxUploads', 'maxAttempts', 'maxReconcile'], []);
  const limits = { maxBytes: 16 * 1024 * 1024, grantMs: 300_000, uploadMs: 30_000, maxUploads: 4, maxAttempts: 8, maxReconcile: 100, ...configuration.limits };
  if (!Object.values(limits).every(n => natural(n) && n > 0) || limits.maxBytes > maxObjectBytes) fail('INVALID_OBJECT', 'Invalid object work limits');
  await mkdir(directory, { recursive: true, mode: 0o700 });
  if (!(await lstat(directory)).isDirectory() || (await lstat(directory)).isSymbolicLink()) fail('INVALID_OBJECT', 'Private object directory required');
  await chmod(directory, 0o700);
  const stages = join(directory, 'stages'), objects = join(directory, 'objects');
  for (const path of [stages, objects]) { await mkdir(path, { mode: 0o700 }).catch(cause => { if (cause.code !== 'EEXIST') throw cause; }); const info = await lstat(path); if (!info.isDirectory() || info.isSymbolicLink()) fail('INVALID_OBJECT', 'Private object subdirectory required'); await chmod(path, 0o700); }
  const tenant = engine.tenant, catalog = engine.catalog, prefix = `objects/v1/${canonical(tenant)}/`, rowsPrefix = prefix + 'row/';
  const rowKey = version => rowsPrefix + canonical(version), mapKey = source => prefix + 'source/' + canonical(source), grantKey = digest => prefix + 'grant/' + canonical(digest);
  const scope = (purpose, subject, fence) => ({ tenant, purpose, subject, revision: '1', ...(fence === undefined ? {} : { fence }) });
  const now = () => { const n = clock(); if (!natural(n)) fail('INVALID_OBJECT', 'Invalid trusted clock'); return n; };
  const phase = (name, row) => onPhase(name, { version: row.version, id: row.id });
  let uploads = 0;
  function row(value) {
    try {
      fields(value, ['protocol', 'version', 'tenant', 'model', 'id', 'owner', 'beginOperation', 'bytes', 'sha256', 'file', 'status', 'attempts', 'grantDigest', 'keyId', 'binding', 'expiresAt', 'upload', 'stage', 'finalize'], ['protocol', 'version', 'tenant', 'model', 'id', 'owner', 'beginOperation', 'bytes', 'sha256', 'file', 'status', 'attempts', 'grantDigest', 'keyId', 'binding', 'expiresAt']);
      id(value.beginOperation);
      fields(value.binding, ['tenant', 'actor', 'digest', 'keyId', 'credentialRevision', 'fence', 'expiresAt']);
      if (value.binding.tenant !== tenant || typeof value.keyId !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(value.keyId) || typeof value.grantDigest !== 'string' || !new RegExp(`^${value.keyId}:[A-Za-z0-9_-]{43}$`).test(value.grantDigest) || typeof value.binding.credentialRevision !== 'string' || typeof value.binding.expiresAt !== 'string' || decodeDomain('integer', value.binding.credentialRevision) < 1n || decodeDomain('integer', value.binding.expiresAt) < 1n) throw Error('Invalid binding');
      id(value.binding.actor); id(value.binding.fence);
      if (typeof value.binding.keyId !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(value.binding.keyId) || typeof value.binding.digest !== 'string' || !new RegExp(`^${value.binding.keyId}:[A-Za-z0-9_-]{43}$`).test(value.binding.digest)) throw Error('Invalid session digest');
      if (value.status === 'uploading' || value.upload !== undefined) { fields(value.upload, ['file', 'leaseUntil']); filename(value.upload.file); if (!natural(value.upload.leaseUntil) || !value.attempts.includes(value.upload.file)) throw Error('Invalid upload'); }
      if (['staged', 'finalizing', 'ready'].includes(value.status) || value.stage !== undefined) { filename(value.stage); if (!value.attempts.includes(value.stage)) throw Error('Invalid staged file'); }
      if (['finalizing', 'ready'].includes(value.status) || value.finalize !== undefined) {
        fields(value.finalize, ['operationId', 'receiptId', 'digest', 'binding']); id(value.finalize.operationId);
        if (value.finalize.receiptId !== prefix + 'finalize/' + canonical([value.version, value.finalize.operationId]) || canonical(value.finalize.binding) !== canonical(value.binding) || !new RegExp(`^${value.keyId}:[A-Za-z0-9_-]{43}$`).test(value.finalize.digest)) throw Error('Invalid finalize receipt');
      }
    } catch { fail('CORRUPT_OBJECT', 'Invalid private object protocol'); }
    if (!value || value.protocol !== 'local-object-1' || value.tenant !== tenant || value.model !== model || !['pending', 'uploading', 'staged', 'finalizing', 'ready', 'aborted', 'tombstone', 'cleaned'].includes(value.status) || !natural(value.bytes) || value.bytes > limits.maxBytes || !/^[a-f0-9]{64}$/.test(value.sha256) || !natural(value.expiresAt) || !Array.isArray(value.attempts) || value.attempts.length > limits.maxAttempts) fail('CORRUPT_OBJECT', 'Invalid durable object');
    filename(value.version); filename(value.file); id(value.id); id(value.owner); for (const attempt of value.attempts) filename(attempt);
    return value;
  }
  function tokenParts(token) {
    if (typeof token !== 'string') invalidGrant(); const match = /^o1\.([A-Za-z0-9_-]{1,64})\.([A-Za-z0-9_-]{43})$/.exec(token);
    if (!match || Buffer.from(match[2], 'base64url').toString('base64url') !== match[2]) invalidGrant();
    try { return { keyId: match[1], digest: keyring.digest(token, scope('object-upload-grant', 'upload'), match[1]) }; } catch (cause) { if (cause.code === 'UNSUPPORTED_SECRET') invalidGrant(); throw cause; }
  }
  async function authorized(tx, binding, current, action, { pending = false } = {}) {
    const authenticated = await identity.authenticateBindingInTransaction(tx, binding), source = await tx.get(sourceKey(tenant, current.id));
    const authority = await resolveRequestAuthority(tx, catalog, authenticated.context, authenticated.getAuthority);
    if (!source || source.model !== model || !permits(authority, source, action)) forbidden();
    if (pending && (source.owner !== current.owner || canonical(source.data) !== canonical(metadata(current, 'pending')))) invalidGrant();
    return { ...authenticated, source };
  }
  async function lookupGrant(tx, token, uploadToken, { allowFinalizing = false } = {}) {
    const authentication = await identity.authenticateInTransaction(tx, token), parts = tokenParts(uploadToken), version = await tx.get(grantKey(parts.digest));
    if (!version) invalidGrant(); const current = row(await tx.get(rowKey(version)));
    if (current.grantDigest !== parts.digest || current.keyId !== parts.keyId || canonical(current.binding) !== canonical(authentication.binding) || current.expiresAt <= now() && !(allowFinalizing && current.status === 'finalizing') || !['pending', 'uploading', 'staged', 'finalizing'].includes(current.status)) invalidGrant();
    if (await tx.get(mapKey(current.id)) !== current.version) invalidGrant();
    await authorized(tx, authentication.binding, current, 'update', { pending: true }); return current;
  }
  const fenceHook = binding => async () => { if (id(await getRestoreFence()) !== binding.fence) fail('RESTORE_FENCED', 'Object intent belongs to another incarnation'); };
  async function begin(token, input) {
    fields(input, ['operationId', 'id', 'owner', 'bytes', 'sha256']); canonical(input); const fixed = cloneData(input);
    id(fixed.id); id(fixed.owner); id(fixed.operationId);
    if (!natural(fixed.bytes) || fixed.bytes > limits.maxBytes || typeof fixed.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(fixed.sha256)) fail('INVALID_OBJECT', 'Invalid expected object bytes');
    const result = await identity.transact(token, { operationId: fixed.operationId, request: { operation: 'object-begin', ...fixed } }, async (tx, context, getAuthority) => {
      const authority = await resolveRequestAuthority(tx, catalog, context, getAuthority), prospective = { tenant, model, owner: fixed.owner };
      if (!permits(authority, prospective, 'create') || !permits(authority, prospective, 'update')) forbidden();
      const occupied = await tx.get(sourceKey(tenant, fixed.id));
      if (occupied) { if (occupied.model !== model || !permits(authority, occupied, 'update')) forbidden(); fail('OBJECT_EXISTS', 'Object source already exists'); }
      const authenticated = await identity.authenticateInTransaction(tx, token), uploadToken = `o1.${keyring.activeKey}.${randomBytes(32).toString('base64url')}`, parts = tokenParts(uploadToken);
      const current = { protocol: 'local-object-1', version: randomUUID(), tenant, model, id: fixed.id, owner: fixed.owner, beginOperation: fixed.operationId, bytes: fixed.bytes, sha256: fixed.sha256, file: randomUUID(), status: 'pending', attempts: [], grantDigest: parts.digest, keyId: parts.keyId, binding: authenticated.binding, expiresAt: Math.min(now() + limits.grantMs, Number(authenticated.binding.expiresAt)) };
      await mutateAuthorizedInTransaction(tx, catalog, context, [{ type: 'put', record: { id: current.id, tenant, model, owner: current.owner, createdAt: String(now()), data: metadata(current, 'pending') } }], { getAuthority });
      await tx.set(rowKey(current.version), current); await tx.set(mapKey(current.id), current.version); await tx.set(grantKey(parts.digest), current.version);
      return { id: current.id, uploadToken, expiresAt: String(current.expiresAt) };
    });
    await identity.read(token, async tx => {
      const authenticated = await identity.authenticateInTransaction(tx, token), version = await tx.get(mapKey(fixed.id)); if (!version) invalidGrant();
      const current = row(await tx.get(rowKey(version)));
      if (current.beginOperation !== fixed.operationId || current.owner !== fixed.owner || canonical(current.binding) !== canonical(authenticated.binding)) invalidGrant();
      await authorized(tx, authenticated.binding, current, 'update');
    }); return result;
  }
  async function upload(token, uploadToken, stream) {
    if (!stream || typeof stream[Symbol.asyncIterator] !== 'function') fail('INVALID_OBJECT', 'Expected a byte stream');
    if (uploads >= limits.maxUploads) fail('WORK_LIMIT', 'Upload admission exhausted'); uploads++;
    let current, attempt, handle, completed = false, iterator, timeout, claimBinding;
    try {
      current = await engine.transact(async tx => {
        const previous = await lookupGrant(tx, token, uploadToken);
        claimBinding = previous.binding;
        if (!['pending', 'uploading'].includes(previous.status) || previous.status === 'uploading' && previous.upload.leaseUntil > now()) fail('OBJECT_BUSY', 'Object upload is already active or staged');
        if (previous.attempts.length >= limits.maxAttempts) fail('WORK_LIMIT', 'Object attempt budget exhausted');
        attempt = randomUUID(); const next = { ...previous, status: 'uploading', attempts: [...previous.attempts, attempt], upload: { file: attempt, leaseUntil: Math.min(now() + limits.uploadMs, previous.expiresAt) } };
        await tx.set(rowKey(next.version), next); return next;
      }, { beforeCommit: () => fenceHook(claimBinding)() });
      handle = await open(join(stages, attempt), constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      iterator = stream[Symbol.asyncIterator](); const hash = createHash('sha256'); let bytes = 0;
      const expired = new Promise((_, reject) => { timeout = setTimeout(() => reject(Object.assign(Error('Upload deadline exceeded'), { code: 'OBJECT_TIMEOUT' })), limits.uploadMs); });
      while (true) {
        const next = await Promise.race([iterator.next(), expired]); if (next.done) break;
        if (!(next.value instanceof Uint8Array) || next.value.buffer instanceof SharedArrayBuffer) fail('INVALID_OBJECT', 'Object chunks require private bytes');
        const chunk = Buffer.from(next.value); bytes += chunk.length;
        if (bytes > current.bytes || bytes > limits.maxBytes) fail('OBJECT_SIZE', 'Object byte limit exceeded');
        hash.update(chunk); let offset = 0; while (offset < chunk.length) { const result = await handle.write(chunk, offset, chunk.length - offset); if (!result.bytesWritten) fail('OBJECT_STORAGE', 'Object write made no progress'); offset += result.bytesWritten; }
      }
      clearTimeout(timeout); if (bytes !== current.bytes || hash.digest('hex') !== current.sha256) fail('OBJECT_DIGEST', 'Object bytes do not match their declaration');
      await handle.sync(); await handle.close(); handle = undefined; await syncDirectory(stages); await phase('upload:staged-file', current);
      await engine.transact(async tx => {
        const latest = await lookupGrant(tx, token, uploadToken);
        if (latest.status !== 'uploading' || latest.upload.file !== attempt || latest.upload.leaseUntil <= now()) invalidGrant();
        const next = { ...latest, status: 'staged', stage: attempt }; delete next.upload; await tx.set(rowKey(next.version), next);
      }, { beforeCommit: fenceHook(current.binding) });
      completed = true; await phase('upload:staged', current); return { staged: true, bytes: current.bytes, sha256: current.sha256 };
    } catch (cause) {
      if (!completed && attempt && cause.code !== 'COMMIT_UNKNOWN') {
        await handle?.close().catch(() => {}); handle = undefined; await absentUnlink(join(stages, attempt)).catch(() => {});
        if (current) await engine.transact(async tx => { const latest = row(await tx.get(rowKey(current.version))); if (latest.status === 'uploading' && latest.upload.file === attempt) { const next = { ...latest, status: 'pending' }; delete next.upload; await tx.set(rowKey(next.version), next); } }).catch(() => {});
      } throw cause;
    } finally { clearTimeout(timeout); await handle?.close().catch(() => {}); if (!completed) { stream.destroy?.(); void iterator?.return?.().catch?.(() => {}); } uploads--; }
  }
  async function verifyFile(path, current) {
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = await handle.stat(); if (!info.isFile() || info.size !== current.bytes) fail('OBJECT_DIGEST', 'Persisted object size differs');
      const hash = createHash('sha256'), buffer = Buffer.alloc(64 * 1024); let offset = 0;
      while (offset < current.bytes) { const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, current.bytes - offset), offset); if (!bytesRead) fail('OBJECT_DIGEST', 'Persisted object is truncated'); hash.update(buffer.subarray(0, bytesRead)); offset += bytesRead; }
      if (hash.digest('hex') !== current.sha256) fail('OBJECT_DIGEST', 'Persisted object digest differs');
    } finally { await handle.close(); }
  }
  async function promote(current) {
    const target = join(objects, filename(current.file));
    try { await verifyFile(target, current); }
    catch (cause) {
      if (cause.code !== 'ENOENT') throw cause;
      const stage = join(stages, filename(current.stage));
      try { await verifyFile(stage, current); await rename(stage, target); }
      catch (stageError) {
        if (stageError.code !== 'ENOENT') throw stageError;
        // Another driver may have atomically moved the stage after our first
        // destination read. Absence is safe only after checking that exact file.
        await verifyFile(target, current);
      }
    }
    await chmod(target, 0o400); const handle = await open(target, constants.O_RDONLY | constants.O_NOFOLLOW); try { await handle.sync(); } finally { await handle.close(); }
    await syncDirectory(objects); await syncDirectory(stages); await verifyFile(target, current); await phase('finalize:promoted', current);
  }
  async function driveFinalize(current) {
    const state = await engine.read(async tx => {
      const latest = row(await tx.get(rowKey(current.version)));
      if (!['finalizing', 'ready'].includes(latest.status) || canonical(latest.finalize) !== canonical(current.finalize) || await tx.get(mapKey(latest.id)) !== latest.version) invalidGrant();
      await authorized(tx, latest.finalize.binding, latest, latest.status === 'ready' ? 'read' : 'update', { pending: latest.status !== 'ready' }); return latest.status;
    });
    if (state === 'ready') { const receipt = await engine.getReceipt(current.finalize.receiptId); if (!receipt || receipt.digest !== current.finalize.digest) fail('CORRUPT_OBJECT', 'Ready object lacks original receipt'); return cloneData(receipt.result); }
    await promote(current);
    return engine.transact(async tx => {
      const latest = row(await tx.get(rowKey(current.version)));
      if (latest.status !== 'finalizing' || canonical(latest.finalize) !== canonical(current.finalize) || await tx.get(mapKey(latest.id)) !== latest.version) invalidGrant();
      const authenticated = await authorized(tx, latest.finalize.binding, latest, 'update', { pending: true });
      await mutateAuthorizedInTransaction(tx, catalog, authenticated.context, [{ type: 'put', record: { ...authenticated.source, data: metadata(latest, 'ready') } }], { getAuthority: authenticated.getAuthority });
      await identity.authenticateBindingInTransaction(tx, latest.finalize.binding);
      await tx.set(rowKey(latest.version), { ...latest, status: 'ready' }); await tx.delete(grantKey(latest.grantDigest));
      return { ready: true, bytes: latest.bytes, sha256: latest.sha256 };
    }, { receipt: { id: current.finalize.receiptId, digest: current.finalize.digest }, verifyReplay: async tx => { const authenticated = await identity.authenticateBindingInTransaction(tx, current.finalize.binding); const source = await tx.get(sourceKey(tenant, current.id)); const authority = await resolveRequestAuthority(tx, catalog, authenticated.context, authenticated.getAuthority); if (!source || !permits(authority, source, 'read')) forbidden(); }, beforeCommit: fenceHook(current.finalize.binding), afterCommit: () => phase('finalize:ready', current) });
  }
  async function finalize(token, input) {
    fields(input, ['operationId', 'id', 'uploadToken']); canonical(input); const fixed = cloneData(input); id(fixed.id); id(fixed.operationId); const parts = tokenParts(fixed.uploadToken);
    let intentBinding;
    const current = await engine.transact(async tx => {
      const authenticated = await identity.authenticateInTransaction(tx, token), version = await tx.get(mapKey(fixed.id)); if (!version) invalidGrant();
      intentBinding = authenticated.binding;
      const latest = row(await tx.get(rowKey(version))), receiptId = prefix + 'finalize/' + canonical([version, fixed.operationId]), digest = keyring.digest({ operationId: fixed.operationId, id: fixed.id, grantDigest: parts.digest }, scope('object-finalize-request', version, latest.binding.fence), latest.keyId);
      if (latest.status === 'ready' && latest.finalize?.receiptId === receiptId && latest.finalize.digest === digest && canonical(latest.finalize.binding) === canonical(authenticated.binding)) { await authorized(tx, authenticated.binding, latest, 'read'); return latest; }
      const granted = await lookupGrant(tx, token, fixed.uploadToken, { allowFinalizing: true });
      if (granted.version !== latest.version || granted.id !== fixed.id || !['staged', 'finalizing'].includes(latest.status)) invalidGrant();
      const intent = { operationId: fixed.operationId, receiptId, digest, binding: authenticated.binding };
      if (latest.status === 'finalizing' && canonical(latest.finalize) !== canonical(intent)) fail('RECEIPT_MISMATCH', 'Finalize identity belongs to another request');
      const next = { ...latest, status: 'finalizing', finalize: intent }; await tx.set(rowKey(version), next); return next;
    }, { beforeCommit: () => fenceHook(intentBinding)() });
    if (current.status === 'ready') { const receipt = await engine.getReceipt(current.finalize.receiptId); if (!receipt || receipt.digest !== current.finalize.digest) fail('CORRUPT_OBJECT', 'Ready object lacks original receipt'); return cloneData(receipt.result); }
    await phase('finalize:intent', current); return driveFinalize(current);
  }
  async function remove(token, input) {
    fields(input, ['operationId', 'id']); canonical(input); const fixed = cloneData(input); id(fixed.id); id(fixed.operationId);
    return identity.transact(token, { operationId: fixed.operationId, request: { operation: 'object-remove', ...fixed } }, async (tx, context, getAuthority) => {
      const version = await tx.get(mapKey(fixed.id)); if (!version) forbidden(); const current = row(await tx.get(rowKey(version))), source = await tx.get(sourceKey(tenant, fixed.id));
      const authority = await resolveRequestAuthority(tx, catalog, context, getAuthority); if (!source || source.model !== model || !permits(authority, source, 'delete')) forbidden();
      await mutateAuthorizedInTransaction(tx, catalog, context, [{ type: 'delete', id: fixed.id }], { getAuthority });
      await tx.set(rowKey(version), { ...current, status: 'tombstone' }); await tx.delete(grantKey(current.grantDigest)); return { removed: true };
    });
  }
  async function transfer(token, input) {
    fields(input, ['operationId', 'id', 'owner']); canonical(input); const fixed = cloneData(input); id(fixed.id); id(fixed.owner); id(fixed.operationId);
    return identity.transact(token, { operationId: fixed.operationId, request: { operation: 'object-transfer', ...fixed } }, async (tx, context, getAuthority) => {
      const version = await tx.get(mapKey(fixed.id)); if (!version) forbidden(); const current = row(await tx.get(rowKey(version))), source = await tx.get(sourceKey(tenant, fixed.id));
      if (!source || source.model !== model || canonical(source.data) !== canonical(metadata(current, current.status === 'ready' ? 'ready' : 'pending'))) forbidden();
      await mutateAuthorizedInTransaction(tx, catalog, context, [{ type: 'put', record: { ...source, owner: fixed.owner } }], { getAuthority });
      if (source.owner !== fixed.owner) {
        const next = { ...current, owner: fixed.owner, ...(current.status === 'ready' ? {} : { status: 'aborted' }) };
        await tx.set(rowKey(current.version), next); if (next.status === 'aborted') await tx.delete(grantKey(current.grantDigest));
      } return { transferred: true };
    });
  }
  async function status(token, objectId) {
    id(objectId); return identity.read(token, async (tx, context, getAuthority) => {
      const version = await tx.get(mapKey(objectId)); if (!version) forbidden(); const current = row(await tx.get(rowKey(version))), source = await tx.get(sourceKey(tenant, objectId)), authority = await resolveRequestAuthority(tx, catalog, context, getAuthority);
      if (!source || source.model !== model || !permits(authority, source, 'read')) forbidden(); return { state: current.status, bytes: current.bytes, sha256: current.sha256 };
    });
  }
  async function download(token, objectId) {
    id(objectId); const current = await identity.read(token, async (tx, context, getAuthority) => {
      const version = await tx.get(mapKey(objectId)); if (!version) forbidden(); const latest = row(await tx.get(rowKey(version))), source = await tx.get(sourceKey(tenant, objectId)), authority = await resolveRequestAuthority(tx, catalog, context, getAuthority);
      if (!source || source.model !== model || !permits(authority, source, 'read')) forbidden();
      if (latest.status !== 'ready' || canonical(source.data) !== canonical(metadata(latest, 'ready'))) fail('OBJECT_NOT_READY', 'Object is not ready'); return latest;
    });
    const path = join(objects, filename(current.file)); await verifyFile(path, current); const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    return { bytes: current.bytes, sha256: current.sha256, stream: handle.createReadStream({ autoClose: true }) };
  }
  async function cleanup(current) {
    for (const attempt of current.attempts) await absentUnlink(join(stages, filename(attempt)));
    await absentUnlink(join(objects, filename(current.file))); await syncDirectory(stages); await syncDirectory(objects); await phase('cleanup:deleted', current);
    await engine.transact(async tx => { const latest = row(await tx.get(rowKey(current.version))); if (['tombstone', 'aborted'].includes(latest.status)) { await tx.set(rowKey(latest.version), { ...latest, status: 'cleaned' }); await tx.delete(grantKey(latest.grantDigest)); } });
  }
  async function reconcile(options = {}) {
    fields(options, ['maxJobs', 'after'], []); const maxJobs = options.maxJobs ?? limits.maxReconcile; if (!natural(maxJobs) || maxJobs < 1 || maxJobs > limits.maxReconcile || options.after !== undefined && (typeof options.after !== 'string' || !options.after.startsWith(rowsPrefix))) fail('WORK_LIMIT', 'Invalid object recovery bound');
    const page = await engine.read(tx => tx.scan({ prefix: rowsPrefix, ...(options.after === undefined ? {} : { gt: options.after }), limit: maxJobs + 1 })), candidates = page.slice(0, maxJobs);
    const result = { examined: candidates.length, activated: 0, cleaned: 0, pending: 0, errors: [], cursor: page.length > maxJobs ? candidates.at(-1).key : null };
    for (const entry of candidates) {
      let current = row(entry.value);
      if (['pending', 'staged', 'uploading'].includes(current.status) && (current.expiresAt <= now() || current.status === 'uploading' && current.upload.leaseUntil <= now())) {
        current = await engine.transact(async tx => { const latest = row(await tx.get(rowKey(current.version))); if (['pending', 'staged', 'uploading'].includes(latest.status) && (latest.expiresAt <= now() || latest.status === 'uploading' && latest.upload.leaseUntil <= now())) { const next = { ...latest, status: 'aborted' }; await tx.set(rowKey(next.version), next); return next; } return latest; });
      }
      if (current.status === 'finalizing') {
        try { await driveFinalize(current); result.activated++; continue; }
        catch (cause) {
          if (cause.code === 'COMMIT_UNKNOWN') {
            const receipt = await engine.getReceipt(current.finalize.receiptId);
            if (receipt?.digest === current.finalize.digest) { result.activated++; continue; }
            result.pending++; result.errors.push({ code: 'COMMIT_UNKNOWN' }); continue;
          }
          if (!['UNAUTHENTICATED', 'FORBIDDEN', 'RESTORE_FENCED', 'INVALID_OBJECT_GRANT', 'OBJECT_DIGEST', 'ENOENT'].includes(cause.code)) { result.pending++; result.errors.push({ code: cause.code ?? 'OBJECT_STORAGE' }); continue; }
          if (['OBJECT_DIGEST', 'ENOENT'].includes(cause.code)) result.errors.push({ code: cause.code });
          current = await engine.transact(async tx => { const latest = row(await tx.get(rowKey(current.version))); if (latest.status === 'finalizing') { const next = { ...latest, status: 'aborted' }; await tx.set(rowKey(next.version), next); return next; } return latest; });
        }
      }
      if (['aborted', 'tombstone'].includes(current.status)) { await cleanup(current); result.cleaned++; } else if (['pending', 'uploading', 'staged', 'finalizing'].includes(current.status)) result.pending++;
      else if (current.status === 'ready') { for (const attempt of current.attempts) await absentUnlink(join(stages, filename(attempt))); await syncDirectory(stages); }
    } return result;
  }
  async function checkFiles(options = {}) {
    fields(options, ['maxFiles', 'maxBytes', 'maxSources', 'maxRows'], []);
    const caps = { maxFiles: 10_000, maxBytes: 256 * 1024 * 1024, maxSources: 10_000, maxRows: 10_000, ...options };
    if (!Object.values(caps).every(n => natural(n) && n > 0)) fail('WORK_LIMIT', 'Invalid object check limits');
    async function inventory(tx, prefix, maximum) {
      const entries = []; let after;
      while (true) {
        const remaining = maximum - entries.length, limit = Math.min(127, remaining) + 1;
        const page = await tx.scan({ prefix, ...(after === undefined ? {} : { gt: after }), limit });
        if (page.length > remaining) fail('WORK_LIMIT', 'Object source/row check budget exceeded');
        entries.push(...page); if (page.length < limit) return entries;
        after = page.at(-1).key;
      }
    }
    async function snapshot() {
      return engine.read(async tx => {
        const [sources, stored] = await Promise.all([inventory(tx, sourcePrefix(tenant), caps.maxSources), inventory(tx, rowsPrefix, caps.maxRows)]);
        const selected = sources.filter(entry => entry.value.model === model).map(entry => {
          const source = validateRecord(catalog, entry.value); if (source.tenant !== tenant || entry.key !== sourceKey(tenant, source.id)) fail('OBJECT_FILE_INTEGRITY', 'Object source key and header differ'); return source;
        });
        const mappings = await tx.getMany(selected.map(source => mapKey(source.id)));
        return { sources, selected, stored, mappings, scannedKeys: tx.inspect().metrics.scannedKeys };
      });
    }
    const before = await snapshot(), byVersion = new Map(), expected = new Map(), completedStages = [], finalizingFiles = [], stagesKnown = new Set(), objectsKnown = new Set();
    for (const entry of before.stored) {
      const current = row(entry.value); if (entry.key !== rowKey(current.version) || byVersion.has(current.version)) fail('OBJECT_FILE_INTEGRITY', 'Object row identity differs'); byVersion.set(current.version, current);
      if (current.status !== 'cleaned') {
        for (const attempt of current.attempts) stagesKnown.add(filename(attempt));
        if (['finalizing', 'ready', 'tombstone', 'aborted'].includes(current.status)) objectsKnown.add(filename(current.file));
      }
    }
    for (let index = 0; index < before.selected.length; index++) {
      const source = before.selected[index], version = before.mappings[index], current = byVersion.get(version);
      if (!current || current.id !== source.id || current.owner !== source.owner || current.bytes !== source.data.bytes || current.sha256 !== source.data.sha256 || !['pending', 'ready'].includes(source.data.state)
        || canonical(source.data) !== canonical({ state: source.data.state, bytes: source.data.bytes, sha256: source.data.sha256 })) fail('OBJECT_FILE_INTEGRITY', 'Object source and private mapping differ');
      if (source.data.state === 'ready') {
        if (current.status !== 'ready' || expected.has(current.file)) fail('OBJECT_FILE_INTEGRITY', 'Ready source has no unique active file');
        expected.set(current.file, { bytes: source.data.bytes, sha256: source.data.sha256 });
      } else if (['ready', 'tombstone'].includes(current.status)) fail('OBJECT_FILE_INTEGRITY', 'Pending source and lifecycle state differ');
      else if (current.status === 'staged') completedStages.push({ name: filename(current.stage), bytes: source.data.bytes, sha256: source.data.sha256 });
      else if (current.status === 'finalizing') finalizingFiles.push({ stage: filename(current.stage), file: filename(current.file), bytes: source.data.bytes, sha256: source.data.sha256 });
    }
    for (const current of byVersion.values()) if (current.status === 'ready' && !expected.has(current.file)) fail('OBJECT_FILE_INTEGRITY', 'Ready object has no current source');
    const names = await inspectFiles(), manifest = []; let bytes = 0;
    if (names.stages.length + names.objects.length > caps.maxFiles) fail('WORK_LIMIT', 'Object file check budget exceeded');
    for (const [kind, directory, known] of [['stages', stages, stagesKnown], ['objects', objects, objectsKnown]]) for (const name of names[kind]) {
      filename(name); if (!known.has(name)) fail('OBJECT_FILE_INTEGRITY', 'Unrecognized private object file');
      const path = join(directory, name), info = await lstat(path, { bigint: true });
      if (!info.isFile() || info.isSymbolicLink() || info.size > BigInt(caps.maxBytes - bytes)) fail(info.size > BigInt(caps.maxBytes - bytes) ? 'WORK_LIMIT' : 'OBJECT_FILE_INTEGRITY', 'Invalid or excessive private file');
      const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW), hash = createHash('sha256'), buffer = Buffer.alloc(64 * 1024); let offset = 0;
      try {
        while (offset < Number(info.size)) { const { bytesRead } = await handle.read(buffer, 0, Math.min(buffer.length, Number(info.size) - offset), offset); if (!bytesRead) fail('OBJECT_FILE_INTEGRITY', 'Private file is truncated'); hash.update(buffer.subarray(0, bytesRead)); offset += bytesRead; }
        const final = await handle.stat({ bigint: true }); if (final.ino !== info.ino || final.size !== info.size || final.mtimeNs !== info.mtimeNs || final.ctimeNs !== info.ctimeNs) fail('OBJECT_STATE_CHANGED', 'Private file changed during checking');
      } finally { await handle.close(); }
      const sha256 = hash.digest('hex'); bytes += offset;
      if (kind === 'objects' && expected.has(name)) { const fact = expected.get(name); if (fact.bytes !== offset || fact.sha256 !== sha256) fail('OBJECT_FILE_INTEGRITY', 'Ready bytes differ from source facts'); expected.delete(name); }
      manifest.push({ kind, name, bytes: offset, sha256 });
    }
    if (expected.size) fail('OBJECT_FILE_INTEGRITY', 'Ready source file is missing');
    const actualStages = new Map(manifest.filter(file => file.kind === 'stages').map(file => [file.name, file])), actualObjects = new Map(manifest.filter(file => file.kind === 'objects').map(file => [file.name, file]));
    for (const expected of completedStages) { const actual = actualStages.get(expected.name); if (!actual || actual.bytes !== expected.bytes || actual.sha256 !== expected.sha256) fail('OBJECT_FILE_INTEGRITY', 'Completed staged bytes differ from source facts'); }
    for (const expected of finalizingFiles) {
      const stage = actualStages.get(expected.stage), object = actualObjects.get(expected.file), actual = stage ?? object;
      if (!actual || stage && object || actual.bytes !== expected.bytes || actual.sha256 !== expected.sha256) fail('OBJECT_FILE_INTEGRITY', 'Finalizing bytes differ from source facts');
    }
    const after = await snapshot();
    if (fingerprint([before.sources, before.stored, before.mappings]) !== fingerprint([after.sources, after.stored, after.mappings]) || canonical(names) !== canonical(await inspectFiles())) fail('OBJECT_STATE_CHANGED', 'Object inventory changed during checking');
    return { sources: before.selected.length, rows: before.stored.length, ready: before.selected.filter(source => source.data.state === 'ready').length, totalBytes: bytes, scannedKeys: before.scannedKeys + after.scannedKeys, files: manifest };
  }
  async function inspectFiles() { return { stages: (await readdir(stages)).sort(), objects: (await readdir(objects)).sort() }; }
  return Object.freeze({ tenant, model, begin, upload, finalize, remove, transfer, status, download, reconcile, inspectFiles, checkFiles });
}
