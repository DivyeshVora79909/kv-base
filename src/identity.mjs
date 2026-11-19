import { randomBytes, scrypt as scryptCallback, timingSafeEqual } from 'node:crypto';
import { promisify } from 'node:util';
import { prepareAuthority, resolveAuthority } from './authority.mjs';
import { mutateAuthorizedInTransaction } from './authorized-mutate.mjs';
import { assertJson, canonical, cloneData, decodeDomain, fail } from './values.mjs';

const scrypt = promisify(scryptCallback), own = (value, key) => Object.hasOwn(value, key);
const natural = value => Number.isSafeInteger(value) && value >= 0;
const denied = () => fail('UNAUTHENTICATED', 'Authentication failed');
function fields(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('INVALID_IDENTITY', 'Expected ordinary identity data');
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!allowed.includes(key) || !descriptor.enumerable || !own(descriptor, 'value')) fail('INVALID_IDENTITY', 'Invalid identity fields');
  }
  if (required.some(key => !own(value, key))) fail('INVALID_IDENTITY', 'Missing identity field');
}
function hooks(value) {
  fields(value, ['beforeCommit', 'afterCommit'], []);
  for (const name of ['beforeCommit', 'afterCommit']) if (value[name] !== undefined && typeof value[name] !== 'function') fail('INVALID_IDENTITY', 'Invalid identity commit hook');
  return { ...value };
}
const id = value => decodeDomain('id', value);
const revision = value => { const n = decodeDomain('integer', value); if (n < 1n) fail('CORRUPT_IDENTITY', 'Invalid credential revision'); return String(n); };
const nextRevision = value => String(BigInt(revision(value)) + 1n);
const wireTime = value => { if (!natural(value)) fail('INVALID_IDENTITY', 'Invalid trusted clock'); return String(value); };

/** Password authentication over the existing owner graph. Policy replacement and
 * enrollment are trusted operator functions. All client context derives from a
 * live opaque session; password material and original bearer receipts stay sealed. */
export function createIdentity(configuration) {
  fields(configuration, ['engine', 'keyring', 'getRestoreFence', 'clock', 'passwordPolicy'], ['engine', 'keyring', 'getRestoreFence']);
  const { engine, keyring, getRestoreFence, clock = Date.now } = configuration;
  if (!engine?.catalog || !engine?.store?.capabilities?.durable || typeof engine.getReceipt !== 'function'
    || typeof keyring?.seal !== 'function' || typeof keyring?.open !== 'function' || typeof keyring?.digest !== 'function'
    || typeof getRestoreFence !== 'function' || typeof clock !== 'function') fail('INVALID_IDENTITY', 'A durable engine, custody keys and external restore fence are required');
  const tenant = engine.tenant, catalog = engine.catalog, prefix = `identity/v1/${canonical(tenant)}/`;
  const stateKey = `${prefix}state`, policyKey = `${prefix}policy`;
  const credentialKey = actor => `${prefix}credential/${canonical(actor)}`;
  const failureKey = actor => `${prefix}failure/${canonical(actor)}`;
  const sessionKey = digest => `${prefix}session/${canonical(digest)}`;
  const inputPolicy = configuration.passwordPolicy ?? {};
  fields(inputPolicy, ['N', 'maxN', 'minBytes', 'maxBytes', 'maxParallel', 'sessionMs', 'maxFailures', 'failureWindowMs', 'lockMs'], []);
  const policy = { N: 131072, maxN: 262144, minBytes: 12, maxBytes: 4096, maxParallel: 2, sessionMs: 3600000, maxFailures: 5, failureWindowMs: 900000, lockMs: 900000, ...inputPolicy };
  for (const value of Object.values(policy)) if (!natural(value) || value < 1) fail('INVALID_IDENTITY', 'Invalid password policy');
  const power = value => natural(value) && value >= 2 && Math.log2(value) % 1 === 0;
  if (!power(policy.N) || !power(policy.maxN) || policy.N > policy.maxN || policy.maxN > 1048576 || policy.minBytes > policy.maxBytes) fail('INVALID_IDENTITY', 'Invalid password work policy');
  let hashing = 0;
  const dummy = { version: 'scrypt-1', N: policy.N, r: 8, p: 1, salt: randomBytes(16).toString('base64url'), hash: randomBytes(32).toString('base64url') };
  const scope = (purpose, subject, rev = '1', fence) => ({ tenant, purpose, subject, revision: rev, ...(fence === undefined ? {} : { fence }) });
  const now = () => { const value = clock(); wireTime(value); return value; };
  async function fence() { return id(await getRestoreFence()); }
  function password(value, enrollment = false) {
    if (typeof value !== 'string' || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value)) fail('INVALID_IDENTITY', 'Password requires well-formed text');
    const size = Buffer.byteLength(value);
    if (size < (enrollment ? policy.minBytes : 1) || size > policy.maxBytes) fail('INVALID_IDENTITY', 'Password byte policy failed');
    return value;
  }
  async function derive(value, specification) {
    fields(specification, ['version', 'N', 'r', 'p', 'salt', 'hash']);
    if (specification.version !== 'scrypt-1' || !power(specification.N) || specification.N > policy.maxN || specification.r !== 8 || specification.p !== 1
      || typeof specification.salt !== 'string' || !/^[A-Za-z0-9_-]{22}$/.test(specification.salt) || Buffer.from(specification.salt, 'base64url').toString('base64url') !== specification.salt
      || typeof specification.hash !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(specification.hash) || Buffer.from(specification.hash, 'base64url').toString('base64url') !== specification.hash) fail('CORRUPT_IDENTITY', 'Unsupported password envelope');
    if (hashing >= policy.maxParallel) fail('AUTH_WORK_LIMIT', 'Password work budget exhausted');
    hashing++;
    const secret = Buffer.from(value);
    try { return await scrypt(secret, Buffer.from(specification.salt, 'base64url'), 32, { N: specification.N, r: 8, p: 1, maxmem: 128 * specification.N * 8 + 32 * 1024 * 1024 }); }
    finally { secret.fill(0); hashing--; }
  }
  async function newCredential(actor, value, rev = '1') {
    const specification = { ...dummy, salt: randomBytes(16).toString('base64url') }, hash = await derive(password(value, true), specification);
    try {
      specification.hash = hash.toString('base64url');
      return { version: 'credential-1', actor, revision: rev, updatedAt: wireTime(now()), envelope: keyring.seal(specification, scope('identity-password', actor, rev)) };
    } finally { hash.fill(0); }
  }
  function credential(actor, stored) {
    if (stored === undefined) return undefined;
    fields(stored, ['version', 'actor', 'revision', 'updatedAt', 'envelope']);
    if (stored.version !== 'credential-1' || stored.actor !== actor) fail('CORRUPT_IDENTITY', 'Credential identity differs');
    const rev = revision(stored.revision); decodeDomain('integer', stored.updatedAt);
    return { stored, revision: rev, password: keyring.open(stored.envelope, scope('identity-password', actor, rev)) };
  }
  async function authority(tx) {
    const [state, graph] = await tx.getMany([stateKey, policyKey]);
    if (state === undefined && graph === undefined) fail('IDENTITY_UNINITIALIZED', 'Identity has not been initialized');
    fields(state, ['version', 'tenant', 'catalog']);
    if (state.version !== 'identity-1' || state.catalog !== catalog.identity || state.tenant !== tenant || !graph || graph.tenant !== tenant) fail('CORRUPT_IDENTITY', 'Identity authority scope differs');
    return { graph, prepared: prepareAuthority(catalog, graph) };
  }
  const actorNode = (graph, actor) => graph.principals.find(node => node.id === actor && node.kind === 'user' && node.active);
  function tokenParts(token) {
    if (typeof token !== 'string') denied();
    const match = /^s1\.([A-Za-z0-9_-]{1,64})\.([A-Za-z0-9_-]{43})$/.exec(token);
    if (!match || Buffer.from(match[2], 'base64url').toString('base64url') !== match[2]) denied();
    try { return { keyId: match[1], digest: keyring.digest(token, scope('identity-session-digest', 'session'), match[1]) }; }
    catch (error) { if (error.code === 'UNSUPPORTED_SECRET') denied(); throw error; }
  }
  async function sessionFromDigest(tx, parts) {
    try { keyring.digest(null, scope('identity-session-digest', 'session'), parts.keyId); }
    catch (error) { if (error.code === 'UNSUPPORTED_SECRET') denied(); throw error; }
    const currentFence = await fence(), record = await tx.get(sessionKey(parts.digest));
    if (!record) denied();
    fields(record, ['version', 'tenant', 'actor', 'keyId', 'credentialRevision', 'issuedAt', 'expiresAt', 'fence']);
    const issuedAt = decodeDomain('integer', record.issuedAt), expiresAt = decodeDomain('integer', record.expiresAt);
    id(record.actor); revision(record.credentialRevision);
    if (issuedAt < 0n || expiresAt <= issuedAt) fail('CORRUPT_IDENTITY', 'Invalid session lifetime');
    if (record.version !== 'session-1' || record.tenant !== tenant || record.keyId !== parts.keyId || record.fence !== currentFence || expiresAt <= BigInt(now())) denied();
    const { graph, prepared } = await authority(tx), current = credential(record.actor, await tx.get(credentialKey(record.actor)));
    if (!actorNode(graph, record.actor) || !current || current.revision !== record.credentialRevision) denied();
    return { actor: record.actor, record, prepared, parts, fence: currentFence };
  }
  async function inspectSession(tx, token) { return sessionFromDigest(tx, tokenParts(token)); }
  function sessionBinding(session) {
    return Object.freeze({ tenant, actor: session.actor, digest: session.parts.digest, keyId: session.parts.keyId, credentialRevision: session.record.credentialRevision, fence: session.fence, expiresAt: session.record.expiresAt });
  }
  async function authenticateBindingInTransaction(tx, binding) {
    fields(binding, ['tenant', 'actor', 'digest', 'keyId', 'credentialRevision', 'fence', 'expiresAt']); assertJson(binding);
    const fixed = cloneData(binding);
    if (fixed.tenant !== tenant || typeof fixed.digest !== 'string' || !/^[A-Za-z0-9_-]{1,64}:[A-Za-z0-9_-]{43}$/.test(fixed.digest) || fixed.digest.split(':')[0] !== fixed.keyId) denied();
    const initial = await sessionFromDigest(tx, { digest: fixed.digest, keyId: fixed.keyId });
    if (canonical(sessionBinding(initial)) !== canonical(fixed)) denied();
    const context = Object.freeze({ tenant, actor: initial.actor });
    return Object.freeze({ context, binding: Object.freeze(fixed), getAuthority: async freshTx => {
      const current = await sessionFromDigest(freshTx, { digest: fixed.digest, keyId: fixed.keyId });
      if (canonical(sessionBinding(current)) !== canonical(fixed)) denied();
      return current.prepared;
    } });
  }
  async function authenticateInTransaction(tx, token) {
    const initial = await inspectSession(tx, token), context = Object.freeze({ tenant, actor: initial.actor });
    return Object.freeze({ context, binding: sessionBinding(initial), getAuthority: async freshTx => {
      const current = await inspectSession(freshTx, token);
      if (current.actor !== context.actor) denied();
      return current.prepared;
    } });
  }
  function receiptId(kind, subject, operationId) { return `${prefix}operation/${canonical([kind, subject, id(operationId)])}`; }
  function resultBinding(kind, subject, operationId, currentFence) { return scope(`identity-result-${kind}`, canonical([subject, operationId]), '1', currentFence); }
  function pack(value, binding) { return { binding, envelope: keyring.seal(value, binding) }; }
  function unpack(value, expected) {
    fields(value, ['binding', 'envelope']);
    if (canonical(value.binding) !== canonical(expected)) fail('RESTORE_FENCED', 'Original result belongs to another identity incarnation');
    return keyring.open(value.envelope, expected);
  }
  function requestDigest(kind, subject, request, keyId) { return keyring.digest(request, scope(`identity-request-${kind}`, subject), keyId); }
  function digestKey(receipt, fallback) { return receipt ? receipt.digest.slice(0, receipt.digest.indexOf(':')) : fallback; }
  const commitFence = (expected, callback) => async evidence => {
    if (callback) await callback(evidence);
    if (await fence() !== expected) fail('RESTORE_FENCED', 'Identity incarnation changed before publication');
  };

  async function failureState(tx, actor, time) {
    const value = await tx.get(failureKey(actor));
    if (value !== undefined) {
      fields(value, ['count', 'windowStart', 'lockedUntil']);
      if (![value.count, value.windowStart, value.lockedUntil].every(natural) || value.count > policy.maxFailures) fail('CORRUPT_IDENTITY', 'Invalid authentication attempt state');
    }
    return { value, locked: value?.lockedUntil > time };
  }
  async function recordFailure(tx, actor, time, state) {
    const failures = state.value, start = failures && time - failures.windowStart < policy.failureWindowMs ? failures.windowStart : time;
    const count = Math.min(policy.maxFailures, start === failures?.windowStart ? failures.count + 1 : 1), lockedUntil = state.locked ? failures.lockedUntil : count >= policy.maxFailures ? time + policy.lockMs : 0;
    if (!natural(lockedUntil)) fail('INVALID_IDENTITY', 'Authentication lock deadline overflow');
    await tx.set(failureKey(actor), { count, windowStart: start, lockedUntil });
  }
  async function issueSessionInTransaction(tx, actor) {
    id(actor); const { graph } = await authority(tx), current = credential(actor, await tx.get(credentialKey(actor)));
    if (!current || !actorNode(graph, actor)) denied();
    const time = now(), currentFence = await fence(), token = `s1.${keyring.activeKey}.${randomBytes(32).toString('base64url')}`, parts = tokenParts(token), expiresAt = wireTime(time + policy.sessionMs);
    await tx.set(sessionKey(parts.digest), { version: 'session-1', tenant, actor, keyId: parts.keyId, credentialRevision: current.revision, issuedAt: wireTime(time), expiresAt, fence: currentFence });
    return { token, expiresAt };
  }

  async function initialize(input) {
    fields(input, ['policy', 'credentials']); assertJson(input);
    const fixed = cloneData(input), preparedAuthority = prepareAuthority(catalog, fixed.policy);
    if (fixed.policy.tenant !== tenant || !Array.isArray(fixed.credentials) || !fixed.credentials.length) fail('INVALID_IDENTITY', 'Invalid enrollment scope');
    const seen = new Set(), prepared = [];
    for (const entry of fixed.credentials) {
      fields(entry, ['actor', 'password']); id(entry.actor);
      if (seen.has(entry.actor) || !actorNode(fixed.policy, entry.actor)) fail('INVALID_IDENTITY', 'Credential requires one active user');
      seen.add(entry.actor); prepared.push(await newCredential(entry.actor, entry.password));
    }
    if (!prepared.some(entry => resolveAuthority(preparedAuthority, { tenant, actor: entry.actor }).grants.admin)) fail('INVALID_IDENTITY', 'Enroll an active administrator');
    return engine.transact(async tx => {
      if (await tx.get(stateKey) !== undefined) fail('IDENTITY_EXISTS', 'Identity is already initialized');
      await tx.set(stateKey, { version: 'identity-1', tenant, catalog: catalog.identity }); await tx.set(policyKey, fixed.policy);
      for (const value of prepared) await tx.set(credentialKey(value.actor), value);
      return { enrolled: prepared.length };
    });
  }
  async function replacePolicy(graph) {
    assertJson(graph); const fixed = cloneData(graph); prepareAuthority(catalog, fixed);
    if (fixed.tenant !== tenant) fail('FORBIDDEN', 'Identity tenant differs');
    return engine.transact(async tx => { await authority(tx); await tx.set(policyKey, fixed); return { replaced: true }; });
  }
  async function login(input, options = {}) {
    fields(input, ['actor', 'password', 'operationId']); options = hooks(options); assertJson(input);
    const request = cloneData(input); id(request.actor); id(request.operationId); password(request.password);
    const operation = receiptId('login', request.actor, request.operationId), previous = await engine.getReceipt(operation);
    const candidate = await engine.read(async tx => {
      const { graph } = await authority(tx), value = credential(request.actor, await tx.get(credentialKey(request.actor)));
      return { value, active: !!actorNode(graph, request.actor), fence: await fence(), locked: value ? (await failureState(tx, request.actor, now())).locked : false };
    });
    if (candidate.locked && !previous) denied();
    const specification = candidate.value?.password ?? dummy, hash = await derive(request.password, specification);
    let matches;
    try { matches = timingSafeEqual(hash, Buffer.from(specification.hash, 'base64url')); } finally { hash.fill(0); }
    if (!candidate.value || !candidate.active) denied();
    const binding = resultBinding('login', request.actor, request.operationId, candidate.fence);
    const digest = requestDigest('login', request.actor, request, digestKey(previous, candidate.value?.stored.envelope.keyId ?? keyring.activeKey));
    const result = await engine.transact(async tx => {
      const { graph } = await authority(tx), current = credential(request.actor, await tx.get(credentialKey(request.actor))), currentFence = await fence(), time = now();
      if (currentFence !== candidate.fence || current?.revision !== candidate.value?.revision) fail('AUTH_CHANGED', 'Credential changed during password verification');
      const failures = await failureState(tx, request.actor, time);
      if (failures.locked) denied();
      if (!matches || !actorNode(graph, request.actor) || !current) {
        await recordFailure(tx, request.actor, time, failures);
        return pack({ status: 'denied' }, binding);
      }
      const issued = await issueSessionInTransaction(tx, request.actor);
      await tx.delete(failureKey(request.actor)); return pack({ status: 'ok', ...issued }, binding);
    }, { receipt: { id: operation, digest }, verifyReplay: async (tx, row) => {
      const original = unpack(row.result, binding);
      if (original.status === 'ok') { const session = await inspectSession(tx, original.token); if (session.actor !== request.actor || !matches) denied(); }
    }, beforeCommit: commitFence(candidate.fence, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
    const opened = unpack(result, binding);
    if (opened.status !== 'ok') denied();
    return { token: opened.token, expiresAt: opened.expiresAt };
  }
  async function read(token, callback) {
    if (typeof callback !== 'function') fail('INVALID_IDENTITY', 'Expected authenticated callback');
    tokenParts(token);
    return engine.read(async tx => { const authenticated = await authenticateInTransaction(tx, token), result = await callback(tx, authenticated.context, authenticated.getAuthority); await inspectSession(tx, token); return result; });
  }
  async function authorizedPage(token, query, options = {}) {
    fields(query, ['model', 'index', 'partition', 'range', 'limit'], ['model', 'index', 'partition', 'limit']); canonical(query);
    fields(options, ['cursor', 'onMetrics', 'maxDiscovery', 'maxStreams', 'maxCandidates', 'maxRefills', 'maxNodeVisits', 'maxBatchCalls', 'maxReadBytes', 'maxOutputBytes', 'maxScannedKeys', 'batchSize'], []);
    const fixedQuery = cloneData(query), fixedOptions = { ...options };
    const initial = await engine.read(tx => authenticateInTransaction(tx, token));
    return engine.authorizedPage(initial.context, fixedQuery, { ...fixedOptions, getAuthority: initial.getAuthority, getContinuationFence: async (tx, context) => {
      const current = await authenticateInTransaction(tx, token);
      if (canonical(current.context) !== canonical(context)) denied();
      return current.binding;
    } });
  }
  async function transact(token, input, callback, options = {}) {
    fields(input, ['operationId', 'request']); options = hooks(options); assertJson(input);
    if (typeof callback !== 'function') fail('INVALID_IDENTITY', 'Expected authenticated callback');
    const fixed = cloneData(input), initial = await engine.read(tx => inspectSession(tx, token)), operation = receiptId('request', initial.actor, fixed.operationId), previous = await engine.getReceipt(operation);
    const binding = resultBinding('request', initial.actor, fixed.operationId, initial.fence), digest = requestDigest('request', initial.actor, fixed.request, digestKey(previous, initial.parts.keyId));
    const result = await engine.transact(async tx => {
      const authenticated = await authenticateInTransaction(tx, token);
      if (authenticated.context.actor !== initial.actor) denied();
      const value = await callback(tx, authenticated.context, authenticated.getAuthority);
      const finalSession = await inspectSession(tx, token); if (finalSession.actor !== initial.actor) denied(); return pack(value, binding);
    }, { receipt: { id: operation, digest }, verifyReplay: async (tx, row) => {
      const session = await inspectSession(tx, token); if (session.actor !== initial.actor) denied(); unpack(row.result, binding);
    }, beforeCommit: commitFence(initial.fence, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
    return unpack(result, binding);
  }
  async function mutate(token, changes, options) {
    fields(options, ['operationId', 'beforeCommit', 'afterCommit', 'maxProducers', 'maxEdges', 'maxRouteVisits', 'maxPotentialVisits'], ['operationId']);
    assertJson(changes); const fixed = cloneData(changes), { operationId, beforeCommit, afterCommit, ...limits } = options;
    const commitHooks = hooks({ ...(beforeCommit === undefined ? {} : { beforeCommit }), ...(afterCommit === undefined ? {} : { afterCommit }) });
    return transact(token, { operationId, request: fixed }, (tx, context, getAuthority) => mutateAuthorizedInTransaction(tx, catalog, context, fixed, { ...limits, getAuthority }), commitHooks);
  }
  async function logout(token, input, options = {}) {
    fields(input, ['operationId']); options = hooks(options);
    const parts = tokenParts(token), operationId = id(input.operationId), operation = receiptId('logout', parts.digest, operationId), previous = await engine.getReceipt(operation), currentFence = await fence();
    const binding = resultBinding('logout', parts.digest, operationId, currentFence), digest = requestDigest('logout', parts.digest, { token, operationId }, digestKey(previous, parts.keyId));
    const result = await engine.transact(async tx => { await inspectSession(tx, token); await tx.delete(sessionKey(parts.digest)); return pack({ loggedOut: true }, binding); }, {
      receipt: { id: operation, digest }, verifyReplay: async (_tx, row) => { unpack(row.result, binding); }, beforeCommit: commitFence(currentFence, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}),
    });
    return unpack(result, binding);
  }
  async function changePassword(token, input, options = {}) {
    fields(input, ['oldPassword', 'newPassword', 'operationId']); options = hooks(options); assertJson(input);
    const request = cloneData(input); password(request.oldPassword); password(request.newPassword, true); id(request.operationId);
    const parts = tokenParts(token), operation = receiptId('password', parts.digest, request.operationId), previous = await engine.getReceipt(operation), currentFence = await fence();
    const binding = resultBinding('password', parts.digest, request.operationId, currentFence), digest = requestDigest('password', parts.digest, { ...request, token }, digestKey(previous, parts.keyId));
    if (previous) {
      const result = await engine.transact(() => { fail('INTEGRITY', 'Existing password receipt was not recovered'); }, { receipt: { id: operation, digest }, verifyReplay: async (_tx, row) => { unpack(row.result, binding); } });
      const original = unpack(result, binding); if (original.status !== 'ok') denied(); return { changed: true };
    }
    const initial = await engine.read(async tx => { const session = await inspectSession(tx, token); return { session, credential: credential(session.actor, await tx.get(credentialKey(session.actor))), locked: (await failureState(tx, session.actor, now())).locked }; });
    if (initial.locked) denied();
    const hash = await derive(request.oldPassword, initial.credential.password); let matches;
    try { matches = timingSafeEqual(hash, Buffer.from(initial.credential.password.hash, 'base64url')); } finally { hash.fill(0); }
    const next = matches ? await newCredential(initial.session.actor, request.newPassword, nextRevision(initial.credential.revision)) : undefined;
    const result = await engine.transact(async tx => {
      const current = await inspectSession(tx, token); if (current.record.credentialRevision !== initial.credential.revision) fail('AUTH_CHANGED', 'Credential changed during password verification');
      const time = now(), failures = await failureState(tx, current.actor, time);
      if (failures.locked) denied();
      if (!matches) { await recordFailure(tx, current.actor, time, failures); return pack({ status: 'denied' }, binding); }
      await tx.set(credentialKey(current.actor), next); await tx.delete(failureKey(current.actor)); return pack({ status: 'ok', changed: true }, binding);
    }, { receipt: { id: operation, digest }, verifyReplay: async (_tx, row) => { unpack(row.result, binding); }, beforeCommit: commitFence(currentFence, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
    const opened = unpack(result, binding); if (opened.status !== 'ok') denied(); return { changed: true };
  }
  return Object.freeze({ tenant, initialize, replacePolicy, login, authenticateInTransaction, authenticateBindingInTransaction, issueSessionInTransaction, read, authorizedPage, transact, mutate, logout, changePassword });
}
