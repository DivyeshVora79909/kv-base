import { randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, isAbsolute } from 'node:path';
import { spawn } from 'node:child_process';
import net from 'node:net';

/** Own only the local server ChildProcess created here. Existing clusters and
 * processes are never adopted, reconfigured, signalled or removed. */
export async function openFoundationDbTestEnvironment({ server, cli, environment = process.env } = {}) {
  for (const path of [server, cli]) if (typeof path !== 'string' || !isAbsolute(path) || path.includes('\0')) throw Error('Explicit absolute server and CLI paths required');
  const directory = await mkdtemp(join(tmpdir(), 'kv-base-fdb-owned-'));
  const clusterFile = join(directory, 'fdb.cluster'), data = join(directory, 'data'), logs = join(directory, 'logs');
  await mkdir(data, { mode: 0o700 }); await mkdir(logs, { mode: 0o700 });
  const listener = net.createServer(); await new Promise((resolve, reject) => { listener.once('error', reject); listener.listen(0, '127.0.0.1', resolve); });
  const port = listener.address().port; await new Promise(resolve => listener.close(resolve));
  await writeFile(clusterFile, `kvbase:${randomUUID().replaceAll('-', '')}@127.0.0.1:${port}\n`, { mode: 0o600 });
  const output = await open(join(directory, 'server.log'), 'a', 0o600);
  let child, closing;
  async function stop() {
    if (closing) return closing;
    closing = (async () => {
      const owned = child; if (owned && owned.exitCode === null && owned.signalCode === null) {
        owned.kill('SIGTERM');
        await new Promise((resolve, reject) => { const timeout = setTimeout(() => reject(Error('Owned server did not stop')), 10_000); owned.once('close', () => { clearTimeout(timeout); resolve(); }); });
      }
      child = undefined; await output.close();
    })(); return closing;
  }
  try {
    child = spawn(server, ['-p', `127.0.0.1:${port}`, '-C', clusterFile, '-d', data, '-L', logs, '-m', '512', '--cache-memory', '128', '--storage-memory', '128'], { env: { ...environment }, stdio: ['ignore', output.fd, output.fd] });
    let spawnError; child.on('error', cause => { spawnError = cause; });
    const deadline = Date.now() + 10_000;
    while (true) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null || child.signalCode !== null) throw Error('Owned server exited before readiness');
      const listening = await new Promise(resolve => {
        const socket = net.createConnection({ host: '127.0.0.1', port }); socket.setTimeout(100);
        const finish = result => { socket.destroy(); resolve(result); };
        socket.once('connect', () => finish(true)); socket.once('error', () => finish(false)); socket.once('timeout', () => finish(false));
      });
      if (listening) break;
      if (Date.now() >= deadline) throw Error('Owned server startup timed out');
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    const bootstrap = spawn(cli, ['-C', clusterFile, '--no-status', '--exec', 'configure new single ssd'], { env: { ...environment }, stdio: ['ignore', 'pipe', 'pipe'] });
    await new Promise((resolve, reject) => {
      const chunks = []; let bytes = 0;
      const timeout = setTimeout(() => { bootstrap.kill('SIGTERM'); reject(Error('Owned database bootstrap timed out')); }, 20_000);
      for (const stream of [bootstrap.stdout, bootstrap.stderr]) stream.on('data', chunk => { bytes += chunk.length; if (bytes > 1_048_576) { bootstrap.kill('SIGTERM'); reject(Error('Bootstrap output limit')); } else chunks.push(chunk); });
      bootstrap.once('error', cause => { clearTimeout(timeout); reject(cause); });
      bootstrap.once('close', code => { clearTimeout(timeout); if (code === 0) resolve(); else reject(Error(`Bootstrap failed: ${Buffer.concat(chunks).toString().slice(-2048)}`)); });
    });
    return Object.freeze({ clusterFile, directory, port, pid: child.pid, close: stop });
  } catch (cause) { await stop().catch(() => {}); throw cause; }
}
