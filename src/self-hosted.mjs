import { constants } from 'node:fs';
import { AsyncLocalStorage } from 'node:async_hooks';
const contexts = new AsyncLocalStorage();
import { open, link, rm } from 'node:fs/promises';
import { isAbsolute, dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { prepareCatalog } from './catalog.mjs';
import { prepareAuthority } from './authority.mjs';
import { createEngine } from './lifecycle.mjs';
import { openFoundationDbStore } from './foundationdb.mjs';
import { createKeyring } from './keyring.mjs';
import { createIdentity } from './identity.mjs';
import { createIdentityProofs } from './identity-proofs.mjs';
import { createCredentialCustody, createEffectWorker } from './effects.mjs';
import { openObjectStore } from './objects.mjs';
import { createService } from './service.mjs';
import { mutateInTransaction } from './mutate.mjs';
import { canonical, assertJson, cloneData, decodeDomain, fail } from './values.mjs';

function fields(value, allowed, required = allowed, code = 'INVALID_DEPLOYMENT') { if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail(code, 'Invalid deployment fields'); for (const key of Reflect.ownKeys(value)) { const d = Object.getOwnPropertyDescriptor(value, key); if (!allowed.includes(key) || !d.enumerable || !Object.hasOwn(d, 'value')) fail(code, 'Invalid deployment fields'); } if (required.some(key => !Object.hasOwn(value, key))) fail(code, 'Missing deployment field'); }
const absolute = value => { if (typeof value !== 'string' || !isAbsolute(value) || value.includes('\0')) fail('INVALID_DEPLOYMENT', 'Absolute operator path required'); return value; };
function deployment(value) {
  fields(value, ['version', 'clusterFile', 'prefix', 'objectRoot', 'tenant', 'catalogIdentity', 'incarnation', 'previousIncarnations', 'backupId', 'backupDigest', 'restoreOperationId'], ['version', 'clusterFile', 'prefix', 'objectRoot', 'tenant', 'catalogIdentity', 'incarnation']);
  assertJson(value);
  if (value.version !== '1' || typeof value.catalogIdentity !== 'string' || !/^[a-f0-9]{64}$/.test(value.catalogIdentity)) fail('INVALID_DEPLOYMENT', 'Unsupported deployment catalog/version');
  for (const key of ['tenant', 'prefix', 'incarnation']) decodeDomain('id', value[key]); for (const key of ['clusterFile', 'objectRoot']) absolute(value[key]);
  for (const key of ['backupId', 'restoreOperationId']) if (value[key] !== undefined) decodeDomain('id', value[key]); if (value.backupDigest !== undefined && (typeof value.backupDigest !== 'string' || !/^[a-f0-9]{64}$/.test(value.backupDigest))) fail('INVALID_DEPLOYMENT', 'Invalid backup identity');
  if (value.previousIncarnations !== undefined && (!Array.isArray(value.previousIncarnations) || value.previousIncarnations.length > 128 || value.previousIncarnations.some(incarnation => decodeDomain('id', incarnation) === value.incarnation) || new Set(value.previousIncarnations).size !== value.previousIncarnations.length)) fail('INVALID_DEPLOYMENT', 'Bounded distinct previous incarnations required'); return cloneData(value);
}
async function readJson(path, maxBytes = 65536, publicationLink = false) {
  const file = await open(absolute(path), constants.O_RDONLY | constants.O_NOFOLLOW);
  try { const stat = await file.stat(); if (!stat.isFile() || stat.nlink !== 1 && !(publicationLink && stat.nlink === 2) || stat.size > maxBytes || stat.mode & 0o077) fail('INVALID_DEPLOYMENT', 'Private bounded operator file required'); return JSON.parse((await file.readFile()).toString('utf8')); } finally { await file.close(); }
}
export async function readDeployment(path) { return deployment(await readJson(path, 65536, true)); }
export async function readKeyringFile(path) {
  const value = await readJson(path); fields(value, ['version', 'activeKey', 'keys']); if (value.version !== '1') fail('INVALID_KEYRING', 'Unsupported operator keyring');
  fields(value.keys, Object.keys(value.keys ?? {})); const keys = Object.create(null);
  for (const [id, encoded] of Object.entries(value.keys)) { if (typeof encoded !== 'string') fail('INVALID_KEYRING', 'Key must be canonical base64'); const bytes = Buffer.from(encoded, 'base64'); if (bytes.length !== 32 || bytes.toString('base64') !== encoded) fail('INVALID_KEYRING', 'Private 32-byte keys required'); keys[id] = bytes; }
  createKeyring({ activeKey: value.activeKey, keys }); return { activeKey: value.activeKey, keys };
}
export async function writeDeployment({ deploymentFile, descriptor } = {}) {
  deploymentFile = absolute(deploymentFile); descriptor = deployment(descriptor); const staging = `${deploymentFile}.staging-${randomUUID()}`; let created = false, published = false;
  try {
    try { await readDeployment(deploymentFile); fail('DEPLOYMENT_EXISTS', 'Deployment descriptor already exists'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    const file = await open(staging, 'wx', 0o600); created = true; try { await file.writeFile(JSON.stringify(descriptor)); await file.sync(); } finally { await file.close(); }
    try { await link(staging, deploymentFile); } catch (cause) { if (cause.code === 'EEXIST') fail('DEPLOYMENT_EXISTS', 'Deployment descriptor already exists'); throw cause; }
    published = true; await rm(staging); created = false; const parent = await open(dirname(deploymentFile), constants.O_RDONLY | constants.O_DIRECTORY); try { await parent.sync(); } finally { await parent.close(); } return descriptor;
  } catch (cause) { if (published) throw Object.assign(new Error('Deployment published but acknowledgement lost', { cause }), { code: 'COMMIT_UNKNOWN', descriptor }); throw cause; }
  finally { if (created) await rm(staging, { force: true }).catch(() => {}); }
}

/** Trusted Linux runtime. Candidate open does not listen or run workers. Public
 * routes use this one prepared catalog and operator-enrolled username mapping. */
export async function openSelfHosted({ descriptor, deploymentFile, definition, catalog: prepared, keys, activeKey, keyring: suppliedKeyring, keyringFile, logins, contacts, channels, providers,
  deliveryActor = 'admin', deliveryContext: suppliedDeliveryContext, publicModels = ['Ledger', 'Entry', 'Object'], objectModel = 'Object', clock = Date.now,
  passwordPolicy, proofPolicy, objectLimits, objectCheckLimits = {}, engineLimits, serviceLimits = {}, workerLimits = {}, onError = () => {}, applyEffect, onBootstrapPhase = async () => {}, storage: suppliedStorage } = {}) {
  if (process.platform !== 'linux') fail('INVALID_DEPLOYMENT', 'Selected self-hosted target is Linux');
  fields(serviceLimits, ['maxBodyBytes', 'maxResponseBytes', 'maxActive', 'maxPageSize', 'requestTimeoutMs', 'maxUploadBytes', 'maxDownloadBytes', 'writableModels'], []);
  fields(workerLimits, ['workerId', 'leaseMs'], []);
  fields(objectCheckLimits, ['maxFiles', 'maxBytes', 'maxSources', 'maxRows'], []);
  if (!Object.values(objectCheckLimits).every(value => Number.isSafeInteger(value) && value > 0)) fail('INVALID_DEPLOYMENT', 'Finite positive object check limits required');
  objectCheckLimits = Object.freeze(cloneData(objectCheckLimits));
  if (prepared && definition || [keys, suppliedKeyring, keyringFile].filter(value => value !== undefined).length !== 1) fail('INVALID_DEPLOYMENT', 'One catalog and one external custody source required');
  descriptor = deployment(descriptor ?? await readDeployment(deploymentFile)); if (descriptor.previousIncarnations) Object.freeze(descriptor.previousIncarnations); Object.freeze(descriptor); const catalog = prepared ?? prepareCatalog(definition);
  if (catalog.identity !== descriptor.catalogIdentity || typeof clock !== 'function' || typeof onBootstrapPhase !== 'function' || !providers || !logins) fail('INVALID_DEPLOYMENT', 'Fixed catalog, runtime and enrolled login mapping required');
  assertJson(logins); logins = cloneData(logins); if (keyringFile) ({ keys, activeKey } = await readKeyringFile(keyringFile));
  const deliveryContext = suppliedDeliveryContext ?? { tenant: descriptor.tenant, actor: deliveryActor }; fields(deliveryContext, ['tenant', 'actor']); if (deliveryContext.tenant !== descriptor.tenant) fail('INVALID_DEPLOYMENT', 'Delivery scope differs');
  const keyring = suppliedKeyring ?? createKeyring({ keys, activeKey }), storage = suppliedStorage ?? await openFoundationDbStore({ clusterFile: descriptor.clusterFile, prefix: descriptor.prefix });
  if (storage.location?.adapter !== 'foundationdb' || storage.location.clusterFile !== descriptor.clusterFile || storage.location.prefix !== descriptor.prefix) { if (!suppliedStorage) await storage.close(); fail('INVALID_DEPLOYMENT', 'Storage must bind the selected FoundationDB target'); }
  let service, timer, workerPoll, closing = false, closed = false, draining = false, serving = false, starting = false, closePromise, pausePromise, startPromise, quiescePromise, maintenance;
  const work = new Set(), maintenanceWork = new Set(), token = Symbol('runtime admission');
  function track(callback, mode) {
    const context = { token, mode, active: true }, tasks = mode === 'maintenance' ? maintenanceWork : work;
    let resolve, reject; const task = new Promise((yes, no) => { resolve = yes; reject = no; }); tasks.add(task);
    const settled = () => { context.active = false; tasks.delete(task); }; task.then(settled, settled);
    try { Promise.resolve(contexts.run(context, callback)).then(resolve, reject); } catch (error) { reject(error); }
    return task;
  }
  async function settle(tasks) { while (tasks.size) await Promise.allSettled([...tasks]); }
  function admit(callback, allowedMaintenance = false) {
    const context = contexts.getStore();
    if (context?.token === token && context.active) { if (context.mode === 'work' || allowedMaintenance) return track(callback, context.mode); fail('SERVICE_CLOSED', 'Maintenance cannot admit external work'); }
    if (draining || closing || closed) fail('SERVICE_CLOSED', 'Runtime admission stopped');
    return track(callback, 'work');
  }
  function guarded(target, maintenanceMethods = [], pureMethods = []) {
    return Object.freeze(Object.fromEntries(Object.entries(target).map(([key, value]) => [key, typeof value === 'function' && !pureMethods.includes(key) ? (...args) => { try { return admit(() => value.apply(target, args), maintenanceMethods.includes(key)); } catch (error) { return Promise.reject(error); } } : value])));
  }
  const targetFields = ['clusterFile', 'prefix', 'objectRoot', 'tenant', 'catalogIdentity', 'incarnation'];
  const getRestoreFence = async () => {
    if (!deploymentFile) return descriptor.incarnation; const current = await readDeployment(deploymentFile);
    if (targetFields.some(key => current[key] !== descriptor[key]) || canonical(current.previousIncarnations ?? []) !== canonical(descriptor.previousIncarnations ?? [])) fail('RESTORE_FENCED', 'Runtime is no longer the selected installation'); return current.incarnation;
  };
  let engine, identity, custody;
  let objects, proofs, worker;
  try {
    engine = createEngine({ store: storage, catalog, tenant: descriptor.tenant, ...(engineLimits ? { limits: engineLimits } : {}) });
    identity = createIdentity({ engine, keyring, getRestoreFence, clock, ...(passwordPolicy ? { passwordPolicy } : {}) });
    custody = createCredentialCustody({ catalog, keyring, getAuthority: tx => identity.getAuthorityInTransaction(tx), getRestoreFence, getPreviousRestoreFence: () => descriptor.previousIncarnations ?? [], clock });
    proofs = createIdentityProofs({ engine, identity, keyring, getRestoreFence, custody, providers, deliveryContext, contacts, channels, clock, ...(proofPolicy ? { proofPolicy } : {}) });
    objects = await openObjectStore({ engine, identity, keyring, getRestoreFence, directory: descriptor.objectRoot, model: objectModel, clock, ...(objectLimits ? { limits: objectLimits } : {}) });
    worker = createEffectWorker({ engine, custody, providers, workerId: 'self-hosted-worker', clock, ...workerLimits,
      apply: async (tx, event) => { const changes = await proofs.applyDelivery(tx, event); return event.effect.id.startsWith('identity-proof:') || !applyEffect ? changes : [...changes, ...await applyEffect(tx, event)]; } });
  } catch (error) { await storage.close(); throw error; }
  const prefix = `self-hosted/v1/${canonical(descriptor.tenant)}/`, claimKey = prefix + 'bootstrap', operationKey = operationId => prefix + 'operation/' + canonical(operationId);
  async function bootstrap(input) {
    if (serving || starting || draining || closing || closed) fail('SERVICE_CLOSED', 'Bootstrap precedes service admission');
    await getRestoreFence();
    fields(input, ['operationId', 'policy', 'credentials', 'sources', 'providerCredentials']); assertJson(input); const request = cloneData(input); decodeDomain('id', request.operationId);
    if (!Array.isArray(request.sources) || !Array.isArray(request.providerCredentials)) fail('INVALID_DEPLOYMENT', 'Explicit source/provider enrollment arrays required');
    const preparedPolicy = prepareAuthority(catalog, request.policy), binding = { tenant: descriptor.tenant, purpose: 'deployment-bootstrap', subject: request.operationId, revision: '1' }, old = await engine.read(tx => tx.get(claimKey)), keyId = old?.keyId ?? keyring.activeKey, digest = keyring.digest({ ...request, catalogIdentity: catalog.identity }, binding, keyId);
    const original = await engine.getReceipt(operationKey(request.operationId)); if (original) { if (original.digest !== digest) fail('BOOTSTRAP_MISMATCH', 'Bootstrap identity has another request'); return original.result; }
    await engine.transact(async tx => {
      const claim = await tx.get(claimKey); if (claim) { if (claim.operationId !== request.operationId || claim.digest !== digest) fail('BOOTSTRAP_MISMATCH', 'Bootstrap target belongs to another original request'); return; }
      if ((await tx.scan({ limit: 1 })).length) fail('BOOTSTRAP_TARGET_NOT_EMPTY', 'Bootstrap requires an empty selected namespace');
      await tx.set(claimKey, { version: '1', operationId: request.operationId, keyId, digest, phase: 'claimed' });
    }); await onBootstrapPhase('claimed');
    let existing;
    try { existing = await engine.read(tx => identity.getAuthorityInTransaction(tx)); } catch (error) { if (error.code !== 'IDENTITY_UNINITIALIZED') throw error; }
    if (!existing) { try { await identity.initialize({ policy: request.policy, credentials: request.credentials }); } catch (error) { if (!['IDENTITY_EXISTS', 'STORAGE_CONFLICT', 'COMMIT_UNKNOWN'].includes(error.code)) throw error; } existing = await engine.read(tx => identity.getAuthorityInTransaction(tx)); }
    if (existing.identity !== preparedPolicy.identity) fail('BOOTSTRAP_MISMATCH', 'Current policy differs from original enrollment');
    await engine.read(async tx => { for (const credential of request.credentials) await identity.inspectActorInTransaction(tx, credential.actor); }); await onBootstrapPhase('identity');
    const result = await engine.transact(async tx => {
      const claim = await tx.get(claimKey); if (!claim || claim.digest !== digest || claim.phase !== 'claimed') fail('BOOTSTRAP_MISMATCH', 'Original bootstrap claim differs');
      await mutateInTransaction(tx, catalog, descriptor.tenant, request.sources.map(record => ({ type: 'put', record })));
      for (const credential of request.providerCredentials) await custody.storeRevision(tx, deliveryContext, credential);
      await tx.set(claimKey, { ...claim, phase: 'complete' }); return { bootstrapped: true, catalogIdentity: catalog.identity, actors: request.credentials.length, sources: request.sources.length, providerCredentials: request.providerCredentials.length };
    }, { receipt: { id: operationKey(request.operationId), digest }, beforeCommit: getRestoreFence, afterCommit: () => onBootstrapPhase('seeded') });
    await engine.check(); await objects.checkFiles(objectCheckLimits); return result;
  }
  async function health() {
    if (closed || closing || draining) return { status: 'stopped' };
    try { await getRestoreFence(); const ready = await engine.read(async tx => { const claim = await tx.get(claimKey); if (claim?.phase !== 'complete') return false; await identity.getAuthorityInTransaction(tx); return true; }); return { status: ready ? 'ready' : 'uninitialized' }; }
    catch { return { status: 'unavailable' }; }
  }
  const safeIdentity = guarded(identity, ['getAuthorityInTransaction', 'inspectActorInTransaction']), safeProofs = guarded(proofs), safeObjects = guarded(objects, ['checkFiles', 'inspectFiles', 'reconcile']);
  const safeEngine = guarded(engine, ['read', 'getReceipt', 'check'], ['releaseCursor']), safeCustody = guarded(custody, [], ['open', 'sealRequest', 'openRequest', 'requestDigest']), safeWorker = guarded(worker);
  const operations = {
    '/v1/proof/begin': { authenticated: false, handle: (_token, input) => { fields(input, ['username', 'channel', 'operationId'], undefined, 'BAD_REQUEST'); const actor = Object.hasOwn(logins, input.username) ? logins[input.username] : 'unmapped-public'; return safeProofs.begin({ actor, channel: input.channel, operationId: input.operationId }); } },
    '/v1/proof/complete': { authenticated: false, handle: (_token, input) => { fields(input, ['proofId', 'code', 'operationId'], undefined, 'BAD_REQUEST'); return safeProofs.complete(input); } },
  };
  function drainEffects(options = {}) { try { return admit(async () => { await getRestoreFence(); if (draining || closing || closed) fail('SERVICE_CLOSED', 'Worker admission stopped'); return worker.drain(options); }); } catch (error) { return Promise.reject(error); } }
  function stopAdmission() {
    if (!pausePromise) {
      draining = true; if (timer) clearInterval(timer); timer = undefined;
      pausePromise = (async () => { await startPromise?.catch(() => {}); await service?.close(); serving = false; await settle(work); })();
    }
    return pausePromise;
  }
  function close() {
    if (contexts.getStore()?.token === token && contexts.getStore().active) return Promise.reject(Object.assign(Error('Cannot close inside admitted runtime work'), { code: 'NESTED_RUNTIME' }));
    if (!closePromise) { closing = true; closePromise = (async () => { await stopAdmission(); await maintenance?.catch(() => {}); await storage.close(); closed = true; })(); }
    return closePromise;
  }
  function quiesce(callback) {
    if (contexts.getStore()?.token === token && contexts.getStore().active) return Promise.reject(Object.assign(Error('Cannot quiesce inside admitted runtime work'), { code: 'NESTED_RUNTIME' }));
    if (typeof callback !== 'function' || closed || closing || quiescePromise) return Promise.reject(Object.assign(Error('One trusted quiesced operation required'), { code: 'SERVICE_CLOSED' }));
    quiescePromise = (async () => { await stopAdmission(); try { maintenance = track(callback, 'maintenance'); const result = await maintenance; await settle(maintenanceWork); return result; } finally { await settle(maintenanceWork); await close(); } })(); return quiescePromise;
  }
  function start({ host = '127.0.0.1', port = 0, workerIntervalMs = 250, maxJobs = 1 } = {}) {
    if (contexts.getStore()?.token === token && contexts.getStore().active) return Promise.reject(Object.assign(Error('Cannot start inside admitted runtime work'), { code: 'NESTED_RUNTIME' }));
    if (closing || closed || draining || serving || starting) return Promise.reject(Object.assign(Error('Runtime cannot admit another start'), { code: 'SERVICE_CLOSED' }));
    if (!Number.isSafeInteger(workerIntervalMs) || workerIntervalMs < 0 || workerIntervalMs > 2147483647 || !Number.isSafeInteger(maxJobs) || maxJobs < 1) return Promise.reject(Object.assign(Error('Finite worker schedule required'), { code: 'INVALID_DEPLOYMENT' }));
    starting = true;
    startPromise = admit(async () => {
      try {
        await getRestoreFence(); if ((await health()).status !== 'ready') fail('IDENTITY_UNINITIALIZED', 'Bootstrap must complete before service start'); await engine.check(); await objects.checkFiles(objectCheckLimits);
        if (closing || draining || closed) fail('SERVICE_CLOSED', 'Startup lost admission');
        service = createService({ engine: safeEngine, identity: safeIdentity, objects: safeObjects, logins, publicModels, operations, getHealth: admittedHealth, admitRequest: callback => admit(callback), clock, onError, ...serviceLimits });
        const address = await service.listen({ host, port }); if (closing || draining || closed) { await service.close(); fail('SERVICE_CLOSED', 'Startup lost admission'); } serving = true;
        if (workerIntervalMs !== 0) { timer = setInterval(() => {
          if (!workerPoll && !draining && !closing) {
            workerPoll = drainEffects({ maxJobs }).catch(error => { try { onError({ code: error.code ?? 'WORKER_ERROR' }); } catch {} }).finally(() => { workerPoll = undefined; });
          }
        }, workerIntervalMs); timer.unref(); }
        return { ...address, url: 'http://' + (address.host.includes(':') ? '[' + address.host + ']' : address.host) + ':' + address.port };
      } finally { starting = false; }
    }); return startPromise;
  }
  const admittedBootstrap = input => { try { return admit(() => bootstrap(input)); } catch (error) { return Promise.reject(error); } };
  const admittedHealth = () => { if (closed || closing || draining) return Promise.resolve({ status: 'stopped' }); try { return admit(health, true); } catch (error) { return Promise.reject(error); } };
  return Object.freeze({ descriptor: Object.freeze(descriptor), storage, catalog, engine: safeEngine, identity: safeIdentity, keyring, custody: safeCustody, proofs: safeProofs, objects: safeObjects, worker: safeWorker, bootstrap: admittedBootstrap, health: admittedHealth, start, drainEffects, quiesce, close });
}
