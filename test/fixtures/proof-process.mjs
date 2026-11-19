import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { prepareCatalog } from '../../src/catalog.mjs';
import { createEngine } from '../../src/lifecycle.mjs';
import { createKeyring } from '../../src/keyring.mjs';
import { createIdentity } from '../../src/identity.mjs';
import { createIdentityProofs } from '../../src/identity-proofs.mjs';
import { proofDefinition } from './proof-domain.mjs';
const [clusterFile, prefix, proofId, code, phase, time, fence] = process.argv.slice(2);
const store = await openFoundationDbStore({ clusterFile, prefix }), catalog = prepareCatalog(proofDefinition()), engine = createEngine({ store, catalog, tenant: 't' });
const keyring = createKeyring({ keys: { old: Buffer.alloc(32, 31), next: Buffer.alloc(32, 32) }, activeKey: 'old' });
const clock = () => Number(time), getRestoreFence = () => fence, identity = createIdentity({ engine, keyring, getRestoreFence, clock, passwordPolicy: { N: 1024, sessionMs: 100000 } });
const proofs = createIdentityProofs({ engine, identity, keyring, getRestoreFence, clock, custody: {}, providers: {}, deliveryContext: { tenant: 't', actor: 'admin' }, contacts: { reader: { email: 'reader@example.test', phone: '+919876543210' } }, channels: { email: { provider: 'email', credentialId: 'email-credential', credentialRevision: '1', from: 'signin@example.test' }, phone: { provider: 'sms', credentialId: 'sms-credential', credentialRevision: '1' } }, proofPolicy: { expiryMs: 1000, maxAttempts: 3, maxStarts: 3, startWindowMs: 100 } });
try {
  await proofs.complete({ proofId, code, operationId: 'crash-' + phase }, { [phase]: async () => { process.send({ phase }); await new Promise(() => {}); } });
  process.send({ error: 'crash hook was not reached' });
} catch (error) { process.send({ error: error.code ?? error.message }); }
