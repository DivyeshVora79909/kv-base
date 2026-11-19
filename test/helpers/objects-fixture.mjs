import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { prepareCatalog } from '../../src/catalog.mjs';
import { createEngine } from '../../src/lifecycle.mjs';
import { createIdentity } from '../../src/identity.mjs';
import { createKeyring } from '../../src/keyring.mjs';
import { openFoundationDbStore } from '../../src/foundationdb.mjs';
import { openObjectStore } from '../../src/objects.mjs';

export const definition = { name: 'objects-witness', version: '1', models: {
  Object: { schema: { type: 'object', additionalProperties: false, required: ['state', 'bytes', 'sha256'], properties: {
    state: { enum: ['pending', 'ready'] }, bytes: { type: 'integer', minimum: 0, maximum: 16777216 }, sha256: { type: 'string', pattern: '^[a-f0-9]{64}$' },
  } }, access: { orders: {} } },
} };
export function graph() {
  const n = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  return { tenant: 't', principals: [n('t', 'tenant', []), n('admin', 'user', ['t'], {}, true), n('visible', 'group', ['admin'], { Object: ['read', 'create', 'update', 'delete'] }), n('hidden', 'group', ['admin'], { Object: ['read', 'create', 'update', 'delete'] }), n('reader', 'user', ['visible']), n('other', 'user', ['hidden'])] };
}
export const password = 'a sufficiently long password';
export const digest = bytes => createHash('sha256').update(bytes).digest('hex');
export async function* chunks(bytes) { for (let n = 0; n < bytes.length; n += 3) yield bytes.subarray(n, n + 3); }
export async function collect(stream) { const values = []; for await (const chunk of stream) values.push(chunk); return Buffer.concat(values); }
export async function fixture(t, options = {}) {
  const f = { prefix: `objects-${randomUUID()}`, directory: await mkdtemp(join(tmpdir(), 'kv-base-objects-')), time: 1000, fence: 'incarnation-A', catalog: prepareCatalog(definition), ...options };
  f.open = async configuration => {
    f.store = await openFoundationDbStore({ clusterFile: process.env.KV_BASE_FDB_CLUSTER_FILE, prefix: f.prefix }); f.engine = createEngine({ store: f.store, catalog: f.catalog, tenant: 't' });
    f.keyring = createKeyring({ keys: { old: Buffer.alloc(32, 7) }, activeKey: 'old' });
    f.identity = createIdentity({ engine: f.engine, keyring: f.keyring, getRestoreFence: () => f.fence, clock: () => f.time, passwordPolicy: { N: 1024, minBytes: 12, sessionMs: 1_000_000 } });
    f.objects = await openObjectStore({ engine: f.engine, identity: f.identity, keyring: f.keyring, getRestoreFence: () => f.fence, clock: () => f.time, directory: f.directory, model: 'Object', limits: { grantMs: 1000, uploadMs: 500, maxReconcile: 3, ...options.limits }, ...configuration });
  };
  await f.open(); t.after(async () => { await f.store.close(); await rm(f.directory, { recursive: true, force: true }); });
  await f.identity.initialize({ policy: graph(), credentials: ['admin', 'reader', 'other'].map(actor => ({ actor, password })) });
  f.login = async (actor = 'reader', operationId = `login-${randomUUID()}`) => (await f.identity.login({ actor, password, operationId })).token;
  f.token = await f.login();
  f.begin = (bytes, id = 'blob', owner = 'visible', operationId = `begin-${id}`) => f.objects.begin(f.token, { operationId, id, owner, bytes: bytes.length, sha256: digest(bytes) });
  return f;
}
