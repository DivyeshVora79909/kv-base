import { lstat, readdir, mkdir, open, rename, rm, readFile } from 'node:fs/promises';
import { isAbsolute, join, dirname, resolve } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { canonical, fail } from './values.mjs';

const roots = ['src', 'examples', 'docs', 'package.json', 'package-lock.json', 'README.md', 'LICENSE', 'CONTEXT.md', 'AGENTS.md', '.nvmrc'];
const protocol = 'kv-base-node-release-1', digest = value => createHash('sha256').update(value).digest('hex');
const absolute = value => { if (typeof value !== 'string' || !isAbsolute(value) || value.includes('\0')) fail('INVALID_RELEASE', 'Absolute release path required'); return resolve(value); };
const compare = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
async function syncDirectory(path) { const file = await open(path, 'r'); try { await file.sync(); } finally { await file.close(); } }
async function write(path, value) { const file = await open(path, 'wx', 0o600); try { await file.writeFile(value); await file.sync(); } finally { await file.close(); } }
async function inventory(repository, maxFiles, maxBytes, packaged = false) {
  const files = []; let bytes = 0;
  try { await lstat(join(repository, 'npm-shrinkwrap.json')); fail('RELEASE_INTEGRITY', 'A second dependency lock is not allowed'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  async function visit(relative) {
    let path = join(repository, relative), info;
    try { info = await lstat(path); } catch (error) {
      if (!packaged || relative !== 'package-lock.json' || error.code !== 'ENOENT') throw error;
      path = join(repository, 'RELEASE-LOCK.json'); info = await lstat(path);
    }
    if (info.isSymbolicLink()) fail('UNSAFE_RELEASE_FILE', 'Release sources cannot be symlinks');
    if (info.isDirectory()) { for (const name of (await readdir(path)).sort(compare)) await visit(`${relative}/${name}`); return; }
    if (!info.isFile() || info.nlink !== 1) fail('UNSAFE_RELEASE_FILE', 'Regular release sources required');
    if (info.size > maxBytes - bytes || files.length >= maxFiles) fail('RELEASE_LIMIT', 'Release inventory limit exceeded');
    const value = await readFile(path); bytes += value.length; if (bytes > maxBytes) fail('RELEASE_LIMIT', 'Release byte limit exceeded');
    files.push({ path: relative, bytes: value.length, sha256: digest(value), value });
  }
  for (const root of roots) await visit(root);
  // npm may strip its reserved lock filenames from archives. The derived copy
  // carries the authoritative bytes for a checked, extracted npm ci install.
  try {
    const path = join(repository, 'RELEASE-LOCK.json'), info = await lstat(path), lock = files.find(file => file.path === 'package-lock.json');
    if (!info.isFile() || info.isSymbolicLink() || info.nlink !== 1) fail('UNSAFE_RELEASE_FILE', 'Regular derived dependency lock required');
    if (info.size !== lock.bytes || !(await readFile(path)).equals(lock.value)) fail('RELEASE_INTEGRITY', 'Derived dependency lock differs from the authoritative lock');
  } catch (error) { if (error.code !== 'ENOENT') throw error; if (packaged) fail('RELEASE_INTEGRITY', 'Derived dependency lock missing'); }
  return files.sort((a, b) => compare(a.path, b.path));
}
/** Copies the complete engine, importable examples, owner docs and locked npm
 * metadata. Operator keyring/deployment files and installed dependencies are
 * outside this fixed source inventory. npm ci/pack remain ordinary commands. */
export async function prepareRelease({ repository, directory, sourceIdentity, catalogIdentity, maxFiles = 10000, maxBytes = 128 * 1024 * 1024 } = {}) {
  repository = absolute(repository); directory = absolute(directory);
  if (typeof sourceIdentity !== 'string' || !sourceIdentity.length || typeof catalogIdentity!=='string'||!/^[a-f0-9]{64}$/.test(catalogIdentity)||!Number.isSafeInteger(maxFiles) || maxFiles < 1 || !Number.isSafeInteger(maxBytes) || maxBytes < 1) fail('INVALID_RELEASE', 'Source identity and finite release limits required');
  const files = await inventory(repository, maxFiles, maxBytes), metadata = files.map(({ value, ...file }) => file), identity={revision:sourceIdentity,sourceHash:digest(canonical(metadata.filter(row=>row.path.startsWith('src/')||row.path.startsWith('examples/')))),lockHash:metadata.find(row=>row.path==='package-lock.json').sha256,catalogIdentity}, body = { protocol, sourceIdentity:identity, files: metadata }, manifest = { ...body, digest: digest(canonical(body)) }, staging = `${directory}.staging-${randomUUID()}`;
  let created = false;
  try {
    await mkdir(staging, { mode: 0o700 }); created = true;
    for (const file of files) { await mkdir(dirname(join(staging, file.path)), { recursive: true, mode: 0o700 }); await write(join(staging, file.path), file.value); }
    await write(join(staging, 'RELEASE-LOCK.json'), files.find(file=>file.path==='package-lock.json').value);
    await write(join(staging, 'RELEASE.json'), Buffer.from(JSON.stringify(manifest))); await syncDirectory(staging);
    try { await lstat(directory); fail('RELEASE_TARGET_EXISTS', 'Release destination already exists'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
    await rename(staging, directory); created = false; await syncDirectory(dirname(directory));
    return { directory, digest: manifest.digest, sourceIdentity:identity, files: files.length, bytes: metadata.reduce((sum, file) => sum + file.bytes, 0) };
  } finally { if (created) await rm(staging, { recursive: true, force: true }); }
}
export async function checkRelease({ directory, maxFiles = 10000, maxBytes = 128 * 1024 * 1024 } = {}) {
  directory = absolute(directory); let manifest;
  try { manifest = JSON.parse(await readFile(join(directory, 'RELEASE.json'), 'utf8')); } catch { fail('RELEASE_INVALID', 'Release manifest missing or invalid'); }
  if (manifest?.protocol !== protocol || typeof manifest.sourceIdentity?.revision !== 'string'||typeof manifest.sourceIdentity.catalogIdentity!=='string'||!/^[a-f0-9]{64}$/.test(manifest.sourceIdentity.catalogIdentity) || !Array.isArray(manifest.files) || typeof manifest.digest !== 'string') fail('RELEASE_INVALID', 'Unsupported release manifest');
  const files = await inventory(directory, maxFiles, maxBytes, true), metadata = files.map(({ value, ...file }) => file), identity={revision:manifest.sourceIdentity.revision,sourceHash:digest(canonical(metadata.filter(row=>row.path.startsWith('src/')||row.path.startsWith('examples/')))),lockHash:metadata.find(row=>row.path==='package-lock.json').sha256,catalogIdentity:manifest.sourceIdentity.catalogIdentity}, body = { protocol, sourceIdentity:identity, files: metadata };
  if (canonical(identity)!==canonical(manifest.sourceIdentity)||canonical(metadata) !== canonical(manifest.files) || digest(canonical(body)) !== manifest.digest) fail('RELEASE_INTEGRITY', 'Release inventory differs');
  return { checked: true, sourceIdentity: manifest.sourceIdentity, digest: manifest.digest, files: files.length, bytes: metadata.reduce((sum, file) => sum + file.bytes, 0) };
}
