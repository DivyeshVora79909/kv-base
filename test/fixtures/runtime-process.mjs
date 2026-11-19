import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// This is an operator-owned test entry module outside the installed release.
const packageRoot = process.env.KV_BASE_RELEASE_PACKAGE, configurationFile = process.env.KV_BASE_RUNTIME_CONFIGURATION;
const api = await import(pathToFileURL(join(packageRoot, 'src/index.mjs'))), { pipelineDefinition, pipelinePolicy, pipelineSources } = await import(pathToFileURL(join(packageRoot, 'examples/pipeline.mjs')));
const configuration = JSON.parse(await readFile(configurationFile, 'utf8'));
const catalog = api.prepareCatalog(pipelineDefinition()), runtime = await api.openSelfHosted({
  deploymentFile: configuration.deploymentFile, keyringFile: configuration.keyringFile, catalog,
  logins: configuration.logins, contacts: configuration.contacts, channels: configuration.channels,
  providers: { email: api.createFakeEmailClient({ protocol: 'resend', baseUrl: configuration.emailUrl }), sms: api.createFakeSmsClient({ protocol: 'brevo', baseUrl: configuration.smsUrl }) },
  clock: () => configuration.time, passwordPolicy: { N: 1024, sessionMs: 1000000 }, proofPolicy: { expiryMs: 1000 }, objectLimits: { grantMs: 1000, uploadMs: 500 }, workerLimits: { leaseMs: 1000 },
});
let stopping;
const stop = () => stopping ??= (async () => { await runtime.close(); process.exit(0); })();
process.on('SIGTERM', () => { void stop(); }); process.on('SIGINT', () => { void stop(); });
process.on('message', message => {
  if (message?.type !== 'drain') return;
  void runtime.drainEffects({ maxJobs: 10 }).then(() => process.send?.({ type: 'drained', id: message.id }), error => process.send?.({ type: 'error', id: message.id, code: error.code ?? 'ERROR' }));
});
const bootstrap = await runtime.bootstrap({ operationId: 'bootstrap', policy: pipelinePolicy(), credentials: Object.values(configuration.logins).map(actor => ({ actor, password: configuration.password })), sources: pipelineSources(), providerCredentials: [{ id: 'email-credential', provider: 'email', secret: 'runtime-email-secret' }, { id: 'sms-credential', provider: 'sms', secret: 'runtime-sms-secret' }] });
const address = await runtime.start({ workerIntervalMs: 0 });
process.send?.({ type: 'ready', url: address.url, bootstrap, catalogIdentity: catalog.identity, packageRoot });
