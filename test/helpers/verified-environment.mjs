import { mkdtemp, mkdir, readFile, writeFile, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { prepareRelease, checkRelease } from '../../src/release.mjs';
import { prepareCatalog } from '../../src/catalog.mjs';
import { pipelineDefinition } from '../../examples/pipeline.mjs';
import { openFoundationDbTestEnvironment } from './foundationdb-environment.mjs';

async function command(program, args, { cwd, env = process.env, stream = false } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(program, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] }), chunks = []; let bytes = 0;
    const timer = setTimeout(() => { child.kill('SIGTERM'); reject(Error('Verification command timed out')); }, 15 * 60 * 1000);
    for (const [output, sink] of [[child.stdout, process.stdout], [child.stderr, process.stderr]]) output.on('data', chunk => {
      bytes += chunk.length; if (bytes > 64 * 1024 * 1024) { child.kill('SIGTERM'); reject(Error('Verification output limit exceeded')); return; }
      chunks.push(chunk); if (stream) sink.write(chunk);
    });
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('close', code => { clearTimeout(timer); const output = Buffer.concat(chunks).toString(); if (code === 0) resolve(output); else reject(Error(`${program} exited ${code}: ${output.slice(-6000)}`)); });
  });
}
const repository = resolve(import.meta.dirname, '../..'), directory = await mkdtemp(join(tmpdir(), 'kv-base-verified-environment-'));
const nativeVersions = { server: (await command(process.env.KV_BASE_FDB_SERVER, ['--version'])).trim(), cli: (await command(process.env.KV_BASE_FDB_CLI, ['--version'])).trim() };
if (!Object.values(nativeVersions).every(value => /\bv?7\.3\.81\b/.test(value))) throw Error('Verified native FoundationDB 7.3.81 is required');
const target = await openFoundationDbTestEnvironment({ server: process.env.KV_BASE_FDB_SERVER, cli: process.env.KV_BASE_FDB_CLI });
try {
  const revision = (await command('git', ['rev-parse', 'HEAD'], { cwd: repository })).trim() + ((await command('git', ['status', '--porcelain'], { cwd: repository })).trim() ? '+verified-worktree' : '');
  const catalog = prepareCatalog(pipelineDefinition()), release = await prepareRelease({ repository, directory: join(directory, 'source'), sourceIdentity: revision, catalogIdentity: catalog.identity });
  await checkRelease({ directory: release.directory }); await command('npm', ['ci', '--ignore-scripts', '--no-fund'], { cwd: release.directory });
  const packed = JSON.parse(await command('npm', ['pack', '--ignore-scripts', '--json', '--pack-destination', directory], { cwd: release.directory })), item = Array.isArray(packed) ? packed[0] : Object.values(packed)[0];
  const archive = join(directory, item.filename), archiveSha256 = createHash('sha256').update(await readFile(archive)).digest('hex'), extracted = join(directory, 'installed'); await mkdir(extracted);
  await command('tar', ['-xzf', archive, '-C', extracted]); const packageRoot = join(extracted, 'package');
  await checkRelease({ directory: packageRoot }); await copyFile(join(packageRoot, 'RELEASE-LOCK.json'), join(packageRoot, 'package-lock.json'));
  await command('npm', ['ci', '--ignore-scripts', '--no-fund'], { cwd: packageRoot }); const installed = await checkRelease({ directory: packageRoot });
  const env = { ...process.env, FORCE_COLOR: '0', KV_BASE_FDB_CLUSTER_FILE: target.clusterFile, KV_BASE_RELEASE_PACKAGE: packageRoot };
  const output = await command('npm', ['test', '--', '--test-reporter=spec'], { cwd: repository, env, stream: true });
  const count = name => { const match = output.match(new RegExp(`ℹ ${name} (\\d+)`)); if (!match) throw Error(`Missing ${name} test count`); return Number(match[1]); };
  const tests = { total: count('tests'), passed: count('pass'), failed: count('fail'), skipped: count('skipped'), cancelled: count('cancelled') };
  if (tests.total === 0 || tests.passed !== tests.total || tests.failed || tests.skipped || tests.cancelled) throw Error('Real-backend verification requires all tests and zero skips/cancellations/failures');
  const report = { revision, runtime: process.version, nativeVersions, tests, sourceIdentity: installed.sourceIdentity, archive, archiveSha256, packageRoot, target: { ...target, close: undefined } };
  await writeFile(process.env.KV_BASE_VERIFICATION_REPORT ?? join(directory, 'verification.json'), JSON.stringify(report, null, 2) + '\n'); process.stdout.write(JSON.stringify(report) + '\n');
} finally { await target.close(); }
