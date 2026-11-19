import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openSqliteStore } from '../src/sqlite.mjs';
import { registerDurableEngineConformance } from './helpers/durable-engine.mjs';
registerDurableEngineConformance('optional SQLite', async t => {
 const directory = await mkdtemp(join(tmpdir(), 'kv-base-durable-engine-')), filename = join(directory, 'engine.sqlite'), handles = [];
 t.after(async () => { for (const store of handles) await store.close(); await rm(directory, { recursive: true, force: true }); });
 return async () => { const store = openSqliteStore(filename); handles.push(store); return store; };
});
