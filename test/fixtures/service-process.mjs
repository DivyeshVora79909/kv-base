import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { prepareCatalog } from '../../src/catalog.mjs';
import { createEngine } from '../../src/lifecycle.mjs';
import { createIdentity } from '../../src/identity.mjs';
import { createKeyring } from '../../src/keyring.mjs';
import { openObjectStore } from '../../src/objects.mjs';
import { createService } from '../../src/service.mjs';
import { pipelineDefinition } from '../../examples/pipeline.mjs';
const [clusterFile, prefix, directory, phase, time, fence] = process.argv.slice(2);
try {
  const store = await openFoundationDbStore({ clusterFile, prefix }), engine = createEngine({ store, catalog: prepareCatalog(pipelineDefinition()), tenant: 't' }), clock = () => Number(time), getRestoreFence = () => fence;
  const keyring = createKeyring({ keys: { old: Buffer.alloc(32, 19) }, activeKey: 'old' }), original = createIdentity({ engine, keyring, getRestoreFence, clock, passwordPolicy: { N: 1024, sessionMs: 1000000 } });
  const identity = { ...original, transact: (token, input, callback) => original.transact(token, input, callback, input.operationId === 'crash-' + phase ? { [phase]: async () => { process.send({ phase }); await new Promise(() => {}); } } : {}) };
  const objects = await openObjectStore({ engine, identity: original, keyring, getRestoreFence, directory, model: 'Object', clock });
  const service = createService({ engine, identity, objects, clock, logins: { admin: 'admin', reader: 'reader', other: 'other' }, publicModels: ['Ledger', 'Entry', 'Object'] });
  const address = await service.listen(); process.send({ ready: true, port: address.port });
} catch (error) { process.send({ error: error.code ?? error.message }); }
