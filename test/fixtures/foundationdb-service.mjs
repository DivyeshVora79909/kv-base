// Loopback-only conformance host, never a production endpoint. Each fixed route
// executes trusted prepared work behind the real engine and transactional adapter.
import { createServer } from 'node:http';
import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { createEngine, prepareCatalog } from '../../src/index.mjs';
import { mutateInTransaction } from '../../src/mutate.mjs';
import { sourceKey, sourcePrefix } from '../../src/keys.mjs';
import { conflictSchedules } from '../helpers/kv-conformance.mjs';

const [clusterFile, prefix] = process.argv.slice(2);
const store = await openFoundationDbStore({ clusterFile, prefix });
const catalog = prepareCatalog({ name: 'foundationdb-service-conformance', version: '1', contracts: {}, views: {}, models: {
  Datum: { schema: { type: 'object', properties: { amount: { type: 'string', pattern: '^(0|[1-9][0-9]*)$' } }, additionalProperties: false }, access: { orders: {} } },
} });
const engine = createEngine({ store, catalog, tenant: 'conformance' });
// Publish immutable startup metadata before concurrent requests arrive; it is
// not a shared write on the disjoint application transaction path.
await engine.mutate([]);
const progress = new Set(), releases = new Map(); let applied = 0;
const schedule = name => { const value = conflictSchedules.find(item => item.name === name); if (!value) throw Error('Unknown schedule'); return value; };
function scoped(tx, name) {
  const base = `test/${name}/`;
  return { ...tx, get: key => tx.get(base + key), set: (key, value) => tx.set(base + key, value), delete: key => tx.delete(base + key),
    getMany: keys => tx.getMany(keys.map(key => base + key)), scan: async (range = {}) => {
      const fixed = { ...range, prefix: base + (range.prefix ?? '') };
      for (const key of ['gte', 'gt', 'lte', 'lt']) if (range[key] !== undefined) fixed[key] = base + range[key];
      return (await tx.scan(fixed)).map(row => ({ ...row, key: row.key.slice(base.length) }));
    } };
}
function wait(name) { progress.add(name); return new Promise(resolve => { releases.set(name, resolve); }); }
function release(name) { releases.get(name)?.(); releases.delete(name); }
function send(response, status, value) { response.writeHead(status, { 'content-type': 'application/json' }); response.end(JSON.stringify(value, (_key, value) => typeof value === 'bigint' ? String(value) : value)); }
const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url, 'http://localhost'), name = url.searchParams.get('name');
    if (url.pathname === '/health') { send(response, 200, { pid: process.pid, progress: [...progress] }); return; }
    if (url.pathname === '/release') { release(name); send(response, 200, { released: true }); return; }
    if (url.pathname === '/prepare') {
      const spec = schedule(name); await engine.transact(async tx => { const local = scoped(tx, name); for (const [key, value] of spec.rows) await local.set(key, value); });
      send(response, 200, { prepared: true }); return;
    }
    if (url.pathname === '/loser') {
      const spec = schedule(name);
      const result = await engine.transact(async tx => {
        const local = scoped(tx, name); await spec.read(local); await wait(name);
        for (const prefix of ['source/', 'derived/', 'reference/', 'route/', 'authority/', 'application/']) await local.set(prefix + 'loser', true);
        return { stale: true };
      }, { receipt: { id: `test/${name}/loser`, digest: name } }); send(response, 200, result); return;
    }
    if (url.pathname === '/winner') {
      await engine.transact(tx => schedule(name).write(scoped(tx, name)), { receipt: { id: `test/${name}/winner`, digest: name } });
      send(response, 200, { committed: true }); return;
    }
    if (url.pathname === '/state') {
      const rows = await engine.read(tx => scoped(tx, name).scan()), loserReceipt = await engine.getReceipt(`test/${name}/loser`);
      send(response, 200, { rows, loserReceipt: loserReceipt ?? null }); return;
    }
    if (url.pathname === '/disjoint') {
      if (!['a', 'b'].includes(name)) throw Error('Invalid disjoint key');
      const result = await engine.transact(async tx => {
        const record = { id: `disjoint-${name}`, tenant: 'conformance', model: 'Datum', owner: 'operator', createdAt: '0', data: {} };
        if (await tx.get(sourceKey('conformance', record.id)) !== undefined) throw Error('Expected absent source');
        await wait(`disjoint-${name}`);
        await mutateInTransaction(tx, catalog, 'conformance', [{ type: 'put', record }]); return { committed: name };
      }, { receipt: { id: `test/disjoint/${name}`, digest: name } }); send(response, 200, result); return;
    }
    if (url.pathname === '/engine-state') {
      send(response, 200, { sources: await engine.read(tx => tx.scan({ prefix: sourcePrefix('conformance') })), check: await engine.check() }); return;
    }
    if (['/lost', '/recover'].includes(url.pathname)) {
      const result = await engine.transact(async tx => {
        const previous = await tx.get(sourceKey('conformance', 'once')), n = BigInt(previous?.data.amount ?? '0'); applied++;
        await mutateInTransaction(tx, catalog, 'conformance', [{ type: 'put', record: { id: 'once', tenant: 'conformance', model: 'Datum', owner: 'operator', createdAt: '0', data: { amount: String(n + 1n) } } }]);
        return { applied: String(n + 1n) };
      }, { receipt: { id: 'test/once/original', digest: 'exact-once-request' }, ...(url.pathname === '/lost' ? { afterCommit() { throw Error('Drop committed response'); } } : {}) });
      send(response, 200, result); return;
    }
    if (url.pathname === '/once-state') {
      send(response, 200, { counter: await engine.read(async tx => (await tx.get(sourceKey('conformance', 'once')))?.data.amount), applied, receipt: await engine.getReceipt('test/once/original') }); return;
    }
    send(response, 404, { error: 'NOT_FOUND' });
  } catch (error) {
    if (error.code === 'COMMIT_UNKNOWN') response.destroy();
    else send(response, error.code === 'STORAGE_CONFLICT' ? 409 : 500, { error: error.code ?? 'ERROR' });
  }
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
process.send?.({ event: 'ready', port: server.address().port, pid: process.pid });
process.on('message', async message => {
  if (message === 'close') { for (const name of releases.keys()) release(name); await new Promise(resolve => server.close(resolve)); await store.close(); process.exit(0); }
});
