import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { pipelineDefinition, pipelinePolicy, pipelineSources, ledgerIndex, ownerIndex } from '../../examples/pipeline.mjs';
import { prepareCatalog } from '../../src/catalog.mjs';
import { createEngine } from '../../src/lifecycle.mjs';
import { createIdentity } from '../../src/identity.mjs';
import { createKeyring } from '../../src/keyring.mjs';
import { createIdentityProofs } from '../../src/identity-proofs.mjs';
import { createCredentialCustody, createEffectWorker } from '../../src/effects.mjs';
import { createFakeProviderServer, createFakeEmailClient, createFakeSmsClient } from '../../src/providers.mjs';
import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { sourcePrefix } from '../../src/keys.mjs';
import { canonical, fail } from '../../src/values.mjs';
import { openObjectStore } from '../../src/objects.mjs';
import { createService } from '../../src/service.mjs';

export { pipelineDefinition, pipelinePolicy, pipelineSources, ledgerIndex, ownerIndex };
const fields = (value, allowed) => { if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length !== allowed.length || allowed.some(key => !Object.hasOwn(value, key))) fail('BAD_REQUEST', 'Invalid request fields'); };

export async function pipelineFixture(t, options = {}) {
  const f = { prefix: `pipeline-${randomUUID()}`, tenant: 't', time: 1000, fence: 'incarnation-A', password: 'long pipeline password value', keys: { old: Buffer.alloc(32, 19) }, activeKey: 'old',
    logins: { admin: 'admin', reader: 'reader', other: 'other' }, contacts: { reader: { email: 'reader@example.test', phone: '+919876543210' }, other: { email: 'other@example.test', phone: '+919876543211' } },
    catalog: prepareCatalog(pipelineDefinition()), directory: await mkdtemp(join(tmpdir(), 'kv-base-pipeline-')),
  };
  const clock = () => f.time, getRestoreFence = () => f.fence, deliveryContext = { tenant: f.tenant, actor: 'admin' }, clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE;
  const providerStore = await openFoundationDbStore({ clusterFile, prefix: `pipeline-provider-${randomUUID()}` });
  const emailServer = await createFakeProviderServer({ protocol: 'resend', store: providerStore, secret: 'pipeline-email-secret', afterAccepted: options.afterAccepted }), smsServer = await createFakeProviderServer({ protocol: 'brevo', store: providerStore, secret: 'pipeline-sms-secret', afterAccepted: options.afterAccepted });
  f.providers = { email: createFakeEmailClient({ protocol: 'resend', baseUrl: emailServer.baseUrl }), sms: createFakeSmsClient({ protocol: 'brevo', baseUrl: smsServer.baseUrl }) };
  f.channels = { email: { provider: 'email', credentialId: 'email-credential', credentialRevision: '1', from: 'signin@example.test' }, phone: { provider: 'sms', credentialId: 'sms-credential', credentialRevision: '1' } };
  f.open = async () => {
    f.store = await openFoundationDbStore({ clusterFile, prefix: f.prefix }); f.engine = createEngine({ store: f.store, catalog: f.catalog, tenant: f.tenant });
    f.keyring = createKeyring({ keys: f.keys, activeKey: f.activeKey }); f.identity = createIdentity({ engine: f.engine, keyring: f.keyring, getRestoreFence, clock, passwordPolicy: { N: 1024, sessionMs: 1_000_000 } });
    f.custody = createCredentialCustody({ catalog: f.catalog, keyring: f.keyring, getAuthority: tx => f.identity.getAuthorityInTransaction(tx), clock });
    f.proofs = createIdentityProofs({ engine: f.engine, identity: f.identity, keyring: f.keyring, getRestoreFence, custody: f.custody, providers: f.providers, deliveryContext, contacts: f.contacts, channels: f.channels, clock, proofPolicy: { expiryMs: 1000, ...options.proofPolicy } });
    f.worker = createEffectWorker({ engine: f.engine, custody: f.custody, providers: f.providers, workerId: 'pipeline-worker', clock, leaseMs: 1000, apply: f.proofs.applyDelivery });
    f.objects = await openObjectStore({ engine: f.engine, identity: f.identity, keyring: f.keyring, getRestoreFence, directory: f.directory, model: 'Object', clock, limits: { grantMs: 1000, uploadMs: 500, ...options.objectLimits }, onPhase: options.onPhase });
    f.operations = {
      '/v1/proof/begin': { authenticated: false, handle: (_token, input) => { fields(input, ['username', 'channel', 'operationId']); const actor = Object.hasOwn(f.logins, input.username) ? f.logins[input.username] : 'unmapped-public'; return f.proofs.begin({ actor, channel: input.channel, operationId: input.operationId }); } },
      '/v1/proof/complete': { authenticated: false, handle: (_token, input) => { fields(input, ['proofId', 'code', 'operationId']); return f.proofs.complete(input); } },
    };
  };
  await f.open(); await f.identity.initialize({ policy: pipelinePolicy(), credentials: Object.values(f.logins).map(actor => ({ actor, password: f.password })) });
  await f.engine.mutate(pipelineSources().map(record => ({ type: 'put', record })));
  await f.engine.transact(async tx => { await f.custody.storeRevision(tx, deliveryContext, { id: 'email-credential', provider: 'email', secret: 'pipeline-email-secret' }); await f.custody.storeRevision(tx, deliveryContext, { id: 'sms-credential', provider: 'sms', secret: 'pipeline-sms-secret' }); });
  f.sourceRows = () => f.engine.read(tx => tx.scan({ prefix: sourcePrefix(f.tenant) })); f.rows = () => f.engine.read(tx => tx.scan());
  f.login = async (actor = 'reader', operationId = `login-${randomUUID()}`) => (await f.identity.login({ actor, password: f.password, operationId })).token;
  f.delivery = async begun => {
    const proof = await f.engine.read(tx => tx.get(`identity-proof/v1/${canonical(f.tenant)}/proof/${canonical(begun.proofId)}`)); await f.worker.runOne(proof.effectId);
    const ledger = await (proof.channel === 'email' ? emailServer : smsServer).ledger(), accepted = ledger.find(entry => entry.value.operationId === proof.providerOperationId), match = /code is (\d{8})\./.exec(accepted?.value.request.text ?? '');
    if (!match) throw Error('No independent delivered proof exists'); return match[1];
  };
  f.startService = async configuration => { f.service = createService({ engine: f.engine, identity: f.identity, objects: f.objects, logins: f.logins, publicModels: ['Ledger', 'Entry', 'Object'], operations: f.operations, clock, ...configuration }); const address = await f.service.listen(); f.baseUrl = `http://127.0.0.1:${address.port}`; return f.service; };
  f.reopen = async () => { await f.service?.close(); f.service = undefined; await f.store.close(); await f.open(); };
  f.providerStats = () => ({ email: emailServer.stats(), sms: smsServer.stats() });
  t.after(async () => { await f.service?.close(); await emailServer.close(); await smsServer.close(); await f.store.close(); await providerStore.close(); await rm(f.directory, { recursive: true, force: true }); });
  return f;
}
