import { mkdtemp, rm, copyFile, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openSqliteStore } from '../src/sqlite.mjs';
import { prepareIdentityFixture, registerIdentityConformance } from './helpers/identity-conformance.mjs';

registerIdentityConformance('optional SQLite', async (t, options = {}) => {
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-identity-')), path = join(directory, 'state.sqlite'), backup = join(directory, 'backup.sqlite');
  return prepareIdentityFixture(t, options, {
    open: () => openSqliteStore(path),
    physical: () => readFile(path),
    capture: async () => { await copyFile(path, backup); return backup; },
    restore: snapshot => copyFile(snapshot, path),
    cleanup: () => rm(directory, { recursive: true, force: true }),
  });
});
