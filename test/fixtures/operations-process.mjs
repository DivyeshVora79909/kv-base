import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = process.env.KV_BASE_OPERATIONS_MODULE_ROOT, configuration = JSON.parse(await readFile(process.env.KV_BASE_RUNTIME_CONFIGURATION, 'utf8'));
const api = await import(pathToFileURL(join(root, 'src/index.mjs'))), { pipelineDefinition } = await import(pathToFileURL(join(root, 'examples/pipeline.mjs')));
const catalog = api.prepareCatalog(pipelineDefinition()), selected = await api.readDeployment(configuration.deploymentFile);
const host = { catalog, keyringFile: configuration.keyringFile, logins: configuration.logins, contacts: configuration.contacts, channels: configuration.channels,
  providers: { email: api.createFakeEmailClient({ protocol: 'resend', baseUrl: configuration.emailUrl }), sms: api.createFakeSmsClient({ protocol: 'brevo', baseUrl: configuration.smsUrl }) }, clock: () => configuration.time,
  passwordPolicy: { N: 1024, sessionMs: 1000000 }, proofPolicy: { expiryMs: 1000 }, objectLimits: { grantMs: 1000, uploadMs: 500 }, workerLimits: { leaseMs: 1000 } };
const previous = await api.openSelfHosted({ ...host, deploymentFile: configuration.deploymentFile });
async function hold(phase, details) { process.send?.({ type: 'phase', phase, ...details }); await new Promise(() => {}); }
if (configuration.mode === 'backup') {
  await api.writeBackup({ runtime: previous, objectRoot: selected.objectRoot, directory: configuration.backupDirectory, keyring: previous.keyring, incarnation: selected.incarnation, sourceIdentity: configuration.sourceIdentity,
    quiesce: callback => previous.quiesce(callback), after: ({ phase, directory }) => phase === 'staged' ? hold('backup:staged', { directory }) : undefined });
} else {
  const storage = await api.openFoundationDbStore({ clusterFile: selected.clusterFile, prefix: configuration.targetPrefix });
  const handle = await api.prepareRestoration({ backupDirectory: configuration.backupDirectory, keyring: previous.keyring, catalog, tenant: selected.tenant,
    target: { store: storage, clusterFile: selected.clusterFile, prefix: configuration.targetPrefix, objectRoot: configuration.targetObjectRoot },
    openCandidate: ({ store, objectRoot, incarnation, previousIncarnations }) => api.openSelfHosted({ ...host, deploymentFile: undefined, descriptor: { ...selected, prefix: configuration.targetPrefix, objectRoot, incarnation, previousIncarnations }, storage: store }),
    after: ({ phase, descriptor }) => configuration.mode === 'prepare' && phase === 'prepared' ? hold('candidate:prepared', { descriptor }) : undefined });
  await api.activateRestoration({ handle, deploymentFile: configuration.deploymentFile, operationId: configuration.operationId, stopPrevious: () => previous.close(),
    after: ({ phase, descriptor }) => phase === configuration.activationPhase ? hold(`activation:${phase}`, { descriptor }) : undefined });
}
