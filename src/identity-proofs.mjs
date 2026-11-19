import { randomBytes, randomInt, timingSafeEqual } from 'node:crypto';
import { acceptEffectInTransaction } from './effects.mjs';
import { assertJson, canonical, cloneData, decodeDomain, fail } from './values.mjs';

const own = (value, key) => Object.hasOwn(value, key), natural = value => Number.isSafeInteger(value) && value >= 0;
const denied = () => fail('UNAUTHENTICATED', 'Authentication failed');
function fields(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('INVALID_PROOF', 'Expected ordinary proof data');
  for (const key of Reflect.ownKeys(value)) { const descriptor = Object.getOwnPropertyDescriptor(value, key); if (!allowed.includes(key) || !descriptor.enumerable || !own(descriptor, 'value')) fail('INVALID_PROOF', 'Invalid proof field'); }
  if (required.some(key => !own(value, key))) fail('INVALID_PROOF', 'Missing proof field');
}
function hooks(value) { fields(value, ['beforeCommit', 'afterCommit'], []); for (const key of Object.keys(value)) if (typeof value[key] !== 'function') fail('INVALID_PROOF', 'Invalid proof hook'); return { ...value }; }
const id = value => decodeDomain('id', value), integer = value => decodeDomain('integer', value);
const sameDigest = (a, b) => typeof a === 'string' && typeof b === 'string' && Buffer.byteLength(a) === Buffer.byteLength(b) && timingSafeEqual(Buffer.from(a), Buffer.from(b));

/** Private delivery and one-use session issuance for operator-enrolled actors.
 * Recipient configuration, delivery principal, keys and worker callbacks are host
 * inputs. Begin/complete return no delivery payload or proof verifier. */
export function createIdentityProofs(configuration) {
  fields(configuration, ['engine', 'identity', 'keyring', 'getRestoreFence', 'custody', 'providers', 'deliveryContext', 'contacts', 'channels', 'clock', 'proofPolicy'], ['engine', 'identity', 'keyring', 'getRestoreFence', 'custody', 'providers', 'deliveryContext', 'contacts', 'channels']);
  const { engine, identity, keyring, getRestoreFence, custody, providers, clock = Date.now } = configuration;
  if (!engine?.store?.capabilities?.durable || typeof identity?.inspectActorInTransaction !== 'function' || typeof identity?.issueSessionInTransaction !== 'function' || typeof identity?.authenticateInTransaction !== 'function' || typeof keyring?.digest !== 'function' || typeof keyring?.seal !== 'function' || typeof getRestoreFence !== 'function' || typeof clock !== 'function') fail('INVALID_PROOF', 'Durable identity and trusted custody configuration required');
  assertJson(configuration.deliveryContext); assertJson(configuration.contacts); assertJson(configuration.channels);
  const tenant = engine.tenant, catalog = engine.catalog, deliveryContext = cloneData(configuration.deliveryContext), contacts = cloneData(configuration.contacts), channels = cloneData(configuration.channels);
  fields(deliveryContext, ['tenant', 'actor']); if (identity.tenant !== tenant || deliveryContext.tenant !== tenant) fail('FORBIDDEN', 'Proof identity or delivery tenant differs'); id(deliveryContext.actor);
  fields(channels, ['email', 'phone'], []); if (!Object.keys(channels).length) fail('INVALID_PROOF', 'Configure a delivery channel');
  for (const [channel, configuration] of Object.entries(channels)) {
    fields(configuration, ['provider', 'credentialId', 'credentialRevision', 'from'], ['provider', 'credentialId', 'credentialRevision', ...(channel === 'email' ? ['from'] : [])]);
    id(configuration.provider); id(configuration.credentialId); if (integer(configuration.credentialRevision) < 1n) fail('INVALID_PROOF', 'Invalid delivery credential revision');
    if (channel === 'email' && (typeof configuration.from !== 'string' || !configuration.from.includes('@'))) fail('INVALID_PROOF', 'Invalid proof sender');
    if (channel === 'phone' && own(configuration, 'from')) fail('INVALID_PROOF', 'Phone delivery has no sender field');
  }
  for (const [actor, recipient] of Object.entries(contacts)) {
    id(actor); fields(recipient, ['email', 'phone'], []);
    if (recipient.email !== undefined && (typeof recipient.email !== 'string' || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipient.email))) fail('INVALID_PROOF', 'Invalid proof recipient');
    if (recipient.phone !== undefined && (typeof recipient.phone !== 'string' || !/^\+[1-9]\d{7,14}$/.test(recipient.phone))) fail('INVALID_PROOF', 'Invalid proof recipient');
  }
  const inputPolicy = configuration.proofPolicy ?? {}; fields(inputPolicy, ['expiryMs', 'digits', 'maxAttempts', 'maxStarts', 'startWindowMs', 'providerLimit', 'providerWindowMs'], []);
  const policy = { expiryMs: 300000, digits: 8, maxAttempts: 5, maxStarts: 3, startWindowMs: 900000, providerLimit: 100, providerWindowMs: 60000, ...inputPolicy };
  for (const value of Object.values(policy)) if (!natural(value) || value < 1) fail('INVALID_PROOF', 'Invalid proof policy');
  if (policy.digits < 8 || policy.digits > 10) fail('INVALID_PROOF', 'Proof codes require eight to ten digits');
  const prefix = `identity-proof/v1/${canonical(tenant)}/`, proofKey = proofId => `${prefix}proof/${canonical(proofId)}`, rateKey = actor => `${prefix}rate/${canonical(actor)}`;
  const scope = (purpose, subject, fence, revision = '1') => ({ tenant, purpose, subject, revision, fence });
  const now = () => { const time = clock(); if (!natural(time)) fail('INVALID_PROOF', 'Invalid proof clock'); return time; };
  const fence = async () => id(await getRestoreFence());
  const receiptId = (kind, subject, operationId) => `${prefix}operation/${canonical([kind, subject, id(operationId)])}`;
  const resultBinding = (kind, subject, operationId, incarnation) => scope(`proof-result-${kind}`, canonical([subject, operationId]), incarnation);
  const pack = (value, binding) => ({ binding, envelope: keyring.seal(value, binding) });
  function unpack(value, binding) { fields(value, ['binding', 'envelope']); if (canonical(value.binding) !== canonical(binding)) fail('RESTORE_FENCED', 'Proof result belongs to another incarnation'); return keyring.open(value.envelope, binding); }
  const digestKey = (receipt, fallback) => receipt ? receipt.digest.slice(0, receipt.digest.indexOf(':')) : fallback;
  const commitFence = (expected, callback) => async evidence => { if (callback) await callback(evidence); if (await fence() !== expected) fail('RESTORE_FENCED', 'Proof incarnation changed before publication'); };
  const contactDigest = (row, contact) => keyring.digest(contact, scope('proof-contact', row.actor, row.fence, row.credentialRevision), row.keyId);
  async function inspect(tx, proofId, incarnation, allowExpired = false) {
    const row = await tx.get(proofKey(proofId)); if (!row) denied();
    fields(row, ['version', 'tenant', 'actor', 'channel', 'keyId', 'fence', 'credentialRevision', 'contactDigest', 'issuedAt', 'expiresAt', 'digest', 'attempts', 'delivery', 'state', 'effectId', 'providerOperationId']);
    if (row.version !== 'proof-1' || row.tenant !== tenant || !['email', 'phone'].includes(row.channel) || !['pending', 'delivered'].includes(row.delivery) || !['open', 'consumed', 'locked'].includes(row.state) || !natural(row.attempts) || integer(row.issuedAt) < 0n || integer(row.expiresAt) <= integer(row.issuedAt)) fail('CORRUPT_PROOF', 'Invalid proof state');
    if (row.fence !== incarnation || !allowExpired && integer(row.expiresAt) <= BigInt(now())) denied();
    const actor = await identity.inspectActorInTransaction(tx, row.actor), contact = contacts[row.actor]?.[row.channel];
    if (actor.tenant !== tenant || actor.credentialRevision !== row.credentialRevision || !contact || !sameDigest(contactDigest(row, contact), row.contactDigest)) denied();
    return row;
  }
  async function begin(input, options = {}) {
    fields(input, ['actor', 'channel', 'operationId']); options = hooks(options); assertJson(input);
    const request = cloneData(input); id(request.actor); id(request.operationId);
    if (!['email', 'phone'].includes(request.channel)) denied();
    const contact = own(contacts, request.actor) ? contacts[request.actor][request.channel] : undefined, channel = channels[request.channel];
    if (!contact || !channel) denied();
    const incarnation = await fence(), operation = receiptId('begin', request.actor, request.operationId), previous = await engine.getReceipt(operation), binding = resultBinding('begin', request.actor, request.operationId, incarnation);
    const digest = keyring.digest(request, scope('proof-request-begin', request.actor, incarnation), digestKey(previous, keyring.activeKey));
    const result = await engine.transact(async tx => {
      const actor = await identity.inspectActorInTransaction(tx, request.actor); if (actor.tenant !== tenant) fail('FORBIDDEN', 'Proof actor tenant differs');
      const time = now(), previousRate = await tx.get(rateKey(request.actor));
      if (previousRate !== undefined) {
        try { fields(previousRate, ['fence', 'windowStart', 'count']); id(previousRate.fence); if (!natural(previousRate.windowStart) || !natural(previousRate.count)) fail('CORRUPT_PROOF', 'Invalid proof admission'); }
        catch { fail('CORRUPT_PROOF', 'Invalid proof admission'); }
      }
      const rate = previousRate?.fence === incarnation && time - previousRate.windowStart < policy.startWindowMs ? previousRate : { fence: incarnation, windowStart: time, count: 0 };
      if (rate.count >= policy.maxStarts) fail('AUTH_WORK_LIMIT', 'Proof admission budget exhausted');
      const expiresAt = time + policy.expiryMs; if (!natural(expiresAt)) fail('INVALID_PROOF', 'Proof deadline overflow');
      const proofId = `p1.${randomBytes(32).toString('base64url')}`, code = String(randomInt(10 ** policy.digits)).padStart(policy.digits, '0');
      const row = { version: 'proof-1', tenant, actor: actor.actor, channel: request.channel, keyId: keyring.activeKey, fence: incarnation, credentialRevision: actor.credentialRevision, issuedAt: String(time), expiresAt: String(expiresAt), digest: keyring.digest(code, scope('proof-verifier', proofId, incarnation), keyring.activeKey), attempts: 0, delivery: 'pending', state: 'open', effectId: `identity-proof:${proofId}` };
      row.contactDigest = contactDigest(row, contact);
      const message = request.channel === 'email' ? { from: channel.from, to: [contact], subject: 'Your sign-in code', text: `Your sign-in code is ${code}.` } : { to: contact, text: `Your sign-in code is ${code}.` };
      const accepted = await acceptEffectInTransaction(tx, catalog, deliveryContext, { id: row.effectId, provider: channel.provider, protocolVersion: '1', operation: request.channel === 'email' ? 'email.send' : 'sms.send', credentialId: channel.credentialId, credentialRevision: channel.credentialRevision, request: message, rate: { limit: policy.providerLimit, windowMs: policy.providerWindowMs } }, { custody, providers, clock });
      row.providerOperationId = accepted.providerOperationId; await tx.set(proofKey(proofId), row); await tx.set(rateKey(request.actor), { ...rate, count: rate.count + 1 });
      const finalActor = await identity.inspectActorInTransaction(tx, request.actor); if (finalActor.credentialRevision !== actor.credentialRevision) denied();
      return pack({ proofId, expiresAt: row.expiresAt }, binding);
    }, { receipt: { id: operation, digest }, verifyReplay: async (tx, receipt) => { const original = unpack(receipt.result, binding); await inspect(tx, original.proofId, incarnation); }, beforeCommit: commitFence(incarnation, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
    return unpack(result, binding);
  }
  async function complete(input, options = {}) {
    fields(input, ['proofId', 'code', 'operationId']); options = hooks(options); assertJson(input);
    const request = cloneData(input); id(request.proofId); id(request.operationId); if (typeof request.code !== 'string' || !new RegExp(`^\\d{${policy.digits}}$`).test(request.code)) denied();
    const incarnation = await fence(), operation = receiptId('complete', request.proofId, request.operationId), previous = await engine.getReceipt(operation), binding = resultBinding('complete', request.proofId, request.operationId, incarnation);
    if (previous) {
      const digest = keyring.digest(request, scope('proof-request-complete', request.proofId, incarnation), digestKey(previous, keyring.activeKey));
      if (digest !== previous.digest) {
        const mismatchBinding = resultBinding('recovery', request.proofId, request.operationId, incarnation);
        const mismatch = await engine.transact(async tx => {
          const row = await inspect(tx, request.proofId, incarnation, true); if (row.attempts >= policy.maxAttempts) denied();
          row.attempts++; if (row.state === 'open' && row.attempts >= policy.maxAttempts) row.state = 'locked'; await tx.set(proofKey(request.proofId), row); return pack({ status: 'mismatch' }, mismatchBinding);
        }, { receipt: { id: operation + '/mismatch/' + digest, digest }, verifyReplay: async (_tx, receipt) => { unpack(receipt.result, mismatchBinding); }, beforeCommit: commitFence(incarnation, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
        unpack(mismatch, mismatchBinding); fail('RECEIPT_MISMATCH', 'Operation identity has another proof request');
      }
      const recovered = await engine.transact(() => { fail('CORRUPT_PROOF', 'Original proof receipt was not recovered'); }, { receipt: { id: operation, digest }, verifyReplay: async (tx, receipt) => { const original = unpack(receipt.result, binding); if (original.status === 'ok') { const row = await inspect(tx, request.proofId, incarnation, true); if (row.attempts >= policy.maxAttempts) denied(); await identity.authenticateInTransaction(tx, original.token); } } });
      const original = unpack(recovered, binding); if (original.status !== 'ok') denied(); return { token: original.token, expiresAt: original.expiresAt };
    }
    const initial = await engine.read(tx => inspect(tx, request.proofId, incarnation)); if (initial.state !== 'open' || initial.attempts >= policy.maxAttempts || initial.delivery !== 'delivered') denied();
    const digest = keyring.digest(request, scope('proof-request-complete', request.proofId, incarnation), initial.keyId);
    const result = await engine.transact(async tx => {
      const row = await inspect(tx, request.proofId, incarnation); if (row.state !== 'open' || row.delivery !== 'delivered' || row.attempts >= policy.maxAttempts) denied();
      const candidate = keyring.digest(request.code, scope('proof-verifier', request.proofId, incarnation), row.keyId);
      if (!sameDigest(candidate, row.digest)) { row.attempts++; if (row.attempts >= policy.maxAttempts) row.state = 'locked'; await tx.set(proofKey(request.proofId), row); return pack({ status: 'denied' }, binding); }
      const issued = await identity.issueSessionInTransaction(tx, row.actor); row.state = 'consumed'; await tx.set(proofKey(request.proofId), row); return pack({ status: 'ok', ...issued }, binding);
    }, { receipt: { id: operation, digest }, verifyReplay: async (tx, receipt) => { const original = unpack(receipt.result, binding); if (original.status === 'ok') await identity.authenticateInTransaction(tx, original.token); }, beforeCommit: commitFence(incarnation, options.beforeCommit), ...(options.afterCommit ? { afterCommit: options.afterCommit } : {}) });
    const original = unpack(result, binding); if (original.status !== 'ok') denied(); return { token: original.token, expiresAt: original.expiresAt };
  }
  async function applyDelivery(tx, { effect, fact }) {
    if (!effect.id.startsWith('identity-proof:')) return [];
    const proofId = effect.id.slice('identity-proof:'.length), row = await tx.get(proofKey(proofId)); if (!row) fail('CORRUPT_PROOF', 'Proof delivery has no proof');
    if (row.effectId !== effect.id || row.providerOperationId !== effect.data.providerOperationId || fact.data.effectId !== effect.id || fact.data.providerOperationId !== row.providerOperationId || fact.data.result.status !== 'accepted') fail('CORRUPT_PROOF', 'Proof delivery identity differs');
    try { await inspect(tx, proofId, await fence()); } catch (error) { if (error.code === 'UNAUTHENTICATED') return []; throw error; }
    if (row.state === 'open' && row.delivery === 'pending') { row.delivery = 'delivered'; await tx.set(proofKey(proofId), row); } return [];
  }
  return Object.freeze({ tenant, begin, complete, applyDelivery });
}
