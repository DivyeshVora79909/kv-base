import { openFoundationDbStore } from '../../src/foundationdb.mjs';
const [clusterFile, prefix, phase] = process.argv.slice(2);
const store = await openFoundationDbStore({ clusterFile, prefix });
await store.transact(async tx => {
  for (const namespace of ['source/', 'derived/', 'reference/', 'route/', 'authority/', 'application/']) await tx.set(namespace + 'committed', { n: 42n });
  return { n: 42n };
}, { receipt: { id: 'crash-operation', digest: 'same-request' }, [phase]: async () => { process.send({ phase }); await new Promise(() => {}); } });
await store.close();
