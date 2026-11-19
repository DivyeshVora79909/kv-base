import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { prepareCatalog } from '../../src/catalog.mjs';
import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { createFakeProviderServer, createFakeEmailClient, createFakeSmsClient } from '../../src/providers.mjs';
import { openSelfHosted, writeDeployment } from '../../src/self-hosted.mjs';
import { pipelineDefinition, pipelinePolicy, pipelineSources } from '../../examples/pipeline.mjs';
import { sourceKey } from '../../src/keys.mjs';
export { openSelfHosted, pipelineDefinition, pipelinePolicy, pipelineSources };
export const clusterFile = process.env.KV_BASE_FDB_CLUSTER_FILE;
export async function runtimeFixture(t, options = {}) {
  const f = { directory: await mkdtemp(join(tmpdir(), 'kv-base-runtime-')), prefix: 'runtime-' + randomUUID(), providerPrefix: 'runtime-provider-' + randomUUID(), time: 1000, keys: { old: Buffer.alloc(32, 17) }, activeKey: 'old', password: 'a long runtime actor password', catalog: prepareCatalog(pipelineDefinition()) };
  f.deploymentFile = join(f.directory, 'deployment.json'); f.keyringFile = join(f.directory, 'keyring.json'); f.objectRoot = join(f.directory, 'object-files');
  f.descriptor = { version: '1', clusterFile, prefix: f.prefix, objectRoot: f.objectRoot, tenant: 't', catalogIdentity: f.catalog.identity, incarnation: 'incarnation-A' };
  await writeDeployment({ deploymentFile: f.deploymentFile, descriptor: f.descriptor }); await writeFile(f.keyringFile, JSON.stringify({ version: '1', activeKey: 'old', keys: { old: f.keys.old.toString('base64') } }), { mode: 0o600 });
  const ledger = await openFoundationDbStore({ clusterFile, prefix: f.providerPrefix });
  f.email = await createFakeProviderServer({ protocol: 'resend', store: ledger, secret: 'runtime-email-secret', afterAccepted: options.afterAccepted }); f.sms = await createFakeProviderServer({ protocol: 'brevo', store: ledger, secret: 'runtime-sms-secret', afterAccepted: options.afterAccepted });
  f.providers = { email: createFakeEmailClient({ protocol: 'resend', baseUrl: f.email.baseUrl }), sms: createFakeSmsClient({ protocol: 'brevo', baseUrl: f.sms.baseUrl }) };
  f.logins = { admin: 'admin', reader: 'reader', other: 'other' }; f.contacts = { reader: { email: 'reader@example.test', phone: '+919876543210' } }; f.channels = { email: { provider: 'email', credentialId: 'email-credential', credentialRevision: '1', from: 'signin@example.test' }, phone: { provider: 'sms', credentialId: 'sms-credential', credentialRevision: '1' } };
  f.configuration = () => ({ deploymentFile: f.deploymentFile, catalog: f.catalog, keyringFile: f.keyringFile, logins: f.logins, contacts: f.contacts, channels: f.channels, providers: f.providers, clock: () => f.time, passwordPolicy: { N: 1024, sessionMs: 1000000 }, proofPolicy: { expiryMs: 1000 }, objectLimits: { grantMs: 1000, uploadMs: 500 }, workerLimits: { leaseMs: 1000 }, serviceLimits: options.serviceLimits, onBootstrapPhase: options.onBootstrapPhase });
  f.open = async patch => { f.runtime = await openSelfHosted({ ...f.configuration(), ...patch }); return f.runtime; }; await f.open();
  f.bootstrap = { operationId: 'bootstrap', policy: pipelinePolicy(), credentials: Object.values(f.logins).map(actor => ({ actor, password: f.password })), sources: pipelineSources(), providerCredentials: [{ id: 'email-credential', provider: 'email', secret: 'runtime-email-secret' }, { id: 'sms-credential', provider: 'sms', secret: 'runtime-sms-secret' }] };
  f.call = async (path, input, token, method = 'POST') => { const response = await fetch(f.url + path, { method, headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) }, ...(input === undefined ? {} : { body: JSON.stringify(input) }) }); return { status: response.status, body: await response.json() }; };
  f.start = async patch => { const address = await f.runtime.start({ workerIntervalMs: 0, ...patch }); f.url = address.url; return address; };
  f.login = async operationId => { const response = await f.call('/v1/login', { username: 'reader', password: f.password, operationId: operationId ?? randomUUID() }); if (response.status !== 200) throw Error(JSON.stringify(response)); return response.body.token; };
  f.delivered = async (begun, channel) => { const effect = await f.runtime.engine.read(tx => tx.get(sourceKey('t', 'identity-proof:' + begun.proofId))), ledger = await (channel === 'phone' ? f.sms : f.email).ledger(), delivered = ledger.find(row => row.value.operationId === effect.data.providerOperationId); if (!delivered) throw Error('Expected exact proof provider operation'); return delivered; };
  f.proof = async channel => { const response = await f.call('/v1/proof/begin', { username: 'reader', channel, operationId: 'proof-' + randomUUID() }); if (response.status !== 200) throw Error(JSON.stringify(response)); const begun = response.body; await f.runtime.drainEffects({ maxJobs: 1 }); const delivered = await f.delivered(begun, channel), code = /code is (\d{8})\./.exec(delivered.value.request.text)[1]; const issued = await f.call('/v1/proof/complete', { proofId: begun.proofId, code, operationId: 'complete-' + randomUUID() }); if (issued.status !== 200) throw Error(JSON.stringify(issued)); return { begun, code, token: issued.body.token }; };
  t.after(async () => { await f.runtime.close(); await f.email.close(); await f.sms.close(); await ledger.close(); await rm(f.directory, { recursive: true, force: true }); }); return f;
}
export function barrier() { let enter, release; return { entered: new Promise(resolve => { enter = resolve; }), blocked: new Promise(resolve => { release = resolve; }), enter: () => enter(), release: () => release() }; }
