import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore } from '../src/memory.mjs';
import { openSqliteStore } from '../src/sqlite.mjs';
import { registerKvConformance } from './helpers/kv-conformance.mjs';

registerKvConformance('memory', async () => createMemoryStore());
registerKvConformance('optional SQLite', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-conformance-')), store = openSqliteStore(join(directory, 'state.sqlite'));
  t.after(async () => { await store.close(); await rm(directory, { recursive: true, force: true }); }); return store;
}, { receipts: true });
