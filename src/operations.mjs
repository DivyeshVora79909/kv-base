import { isDeepStrictEqual } from 'node:util';
import { constants } from 'node:fs';
import { lstat, mkdir, open, readFile, rename, readdir, rm, chmod } from 'node:fs/promises';
import { dirname, join, resolve, isAbsolute } from 'node:path';
import { randomUUID, createHash, timingSafeEqual } from 'node:crypto';
import { serialize, deserialize } from 'node:v8';
import { canonical, decodeDomain, fail } from './values.mjs';

const protocol = 'kv-base-local-backup-1', restoreHandles = new WeakMap(),activations=new Map();
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const hash = value => createHash('sha256').update(value).digest('hex');
const same = (left, right) => { const a = Buffer.from(left), b = Buffer.from(right); return a.length === b.length && timingSafeEqual(a, b); };
const positive = value => Number.isSafeInteger(value) && value > 0;
const validSourceIdentity=value=>value&&typeof value==='object'&&!Array.isArray(value)&&typeof value.revision==='string'&&value.revision.length>0&&['sourceHash','lockHash','catalogIdentity'].every(key=>typeof value[key]==='string'&&/^[a-f0-9]{64}$/.test(value[key]))&&Object.keys(value).length===4;
function limits(input = {}) {
  const value = { maxFiles: 10000, maxFileBytes: 256 * 1024 * 1024, maxManifestBytes: 1024 * 1024, maxSnapshotBytes: 16 * 1024 * 1024, ...input };
  for (const [key, amount] of Object.entries(value)) if (!['maxFiles', 'maxFileBytes', 'maxManifestBytes', 'maxSnapshotBytes'].includes(key) || !positive(amount)) fail('INVALID_BACKUP', 'Finite backup limits required');
  return value;
}
function absolute(value) { if (typeof value !== 'string' || !isAbsolute(value) || value.includes('\0')) fail('INVALID_BACKUP', 'Absolute caller-owned path required'); return resolve(value); }
async function directory(path) { const stat = await lstat(path); if (!stat.isDirectory() || stat.isSymbolicLink()) fail('UNSAFE_BACKUP_FILE', 'Owned regular directory required'); }
async function privateWrite(path, value, mode = 0o600) { const file = await open(path, 'wx', mode); try { await file.writeFile(value); await file.sync(); } finally { await file.close(); } }
async function syncDirectory(path) { const file = await open(path, constants.O_RDONLY | constants.O_DIRECTORY); try { await file.sync(); } finally { await file.close(); } }
function fileIdentity(file) {
  if (!file || !['stages', 'objects'].includes(file.kind) || typeof file.name !== 'string' || !uuid.test(file.name) || !Number.isSafeInteger(file.bytes) || file.bytes < 0 || typeof file.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(file.sha256)) fail('BACKUP_INVALID', 'Invalid object file identity');
  return `${file.kind}/${file.name}`;
}
async function copyOne(source, target, expected, remaining) {
  const input = await open(source, constants.O_RDONLY | constants.O_NOFOLLOW), digest = createHash('sha256'), chunk = Buffer.alloc(65536); let bytes = 0, stat, output;
  try {
    output=await open(target,'wx',0o600);
    stat = await input.stat(); if (!stat.isFile() || stat.nlink !== 1 || ![0o400, 0o600].includes(stat.mode & 0o777)) fail('UNSAFE_BACKUP_FILE', 'Private regular object file required');
    if (stat.size !== expected.bytes || stat.size > remaining) fail('BACKUP_LIMIT', 'Object file changed or exceeds byte limit');
    while (true) {
      const read = await input.read(chunk, 0, chunk.length, null); if (!read.bytesRead) break;
      bytes += read.bytesRead; if (bytes > expected.bytes || bytes > remaining) fail('BACKUP_LIMIT', 'Object file changed or exceeds byte limit');
      digest.update(chunk.subarray(0, read.bytesRead));
      for (let position = 0; position < read.bytesRead;) { const written = await output.write(chunk, position, read.bytesRead - position, null); if (!written.bytesWritten) fail('BACKUP_IO', 'Object write made no progress'); position += written.bytesWritten; }
    }
    const final = await input.stat(), sha256 = digest.digest('hex');
    if (bytes !== expected.bytes || sha256 !== expected.sha256 || final.size !== stat.size || final.mtimeMs !== stat.mtimeMs) fail('BACKUP_CHANGED', 'Object bytes changed during copy');
    if (expected.mode !== undefined && expected.mode !== (stat.mode & 0o777)) fail('BACKUP_CHANGED', 'Object mode changed');
    await output.sync(); return { ...expected, mode: stat.mode & 0o777 };
  } finally { await input.close(); await output?.close(); }
}
async function copyFiles(source, target, files, caps) {
  if (!Array.isArray(files) || files.length > caps.maxFiles) fail('BACKUP_LIMIT', 'Object inventory exceeds file limit');
  await directory(source); await mkdir(target, { mode: 0o700 });
  for (const kind of ['stages', 'objects']) { await directory(join(source, kind)); await mkdir(join(target, kind), { mode: 0o700 }); }
  const result = [], identities = new Set(); let bytes = 0;
  for (const file of [...files].sort((a, b) => Buffer.compare(Buffer.from(fileIdentity(a)), Buffer.from(fileIdentity(b))))) {
    const identity = fileIdentity(file); if (identities.has(identity)) fail('BACKUP_INVALID', 'Repeated object file identity'); identities.add(identity);
    const copied = await copyOne(join(source, identity), join(target, identity), file, caps.maxFileBytes - bytes); bytes += copied.bytes; result.push(copied); await chmod(join(target, identity), copied.mode);
  }
  for (const kind of ['stages', 'objects']) await syncDirectory(join(target, kind)); await syncDirectory(target); return result;
}
const backupBinding = manifest => ({ tenant: manifest.tenant, purpose: 'deployment-backup', subject: manifest.backupId, revision: '1' });
function requireRuntime(runtime) { if (!(runtime?.storage??runtime?.engine?.store)?.captureCheckpoint || !runtime.objects?.checkFiles) fail('INVALID_BACKUP', 'Complete FoundationDB/object runtime required'); }

/** The host quiesce function must stop admission and drain transactions, uploads,
 * provider calls and workers for the entire callback. This is privileged I/O. */
export async function writeBackup({ runtime, objectRoot, directory: target, keyring, incarnation, sourceIdentity, backupId = randomUUID(), quiesce, limits: inputLimits, checkpointLimits, after } = {}) {
  requireRuntime(runtime); if (typeof quiesce !== 'function' || !keyring?.digest || !validSourceIdentity(sourceIdentity) || after !== undefined && typeof after !== 'function') fail('INVALID_BACKUP', 'Trusted pause, custody and source identity required');
  if(sourceIdentity.catalogIdentity!==runtime.engine.catalog.identity)fail('BACKUP_SCOPE','Source identity catalog differs');
  sourceIdentity=JSON.parse(JSON.stringify(sourceIdentity));
  decodeDomain('id', incarnation); decodeDomain('id', backupId); objectRoot = absolute(objectRoot); target = absolute(target); const caps = limits(inputLimits);
  return quiesce(async () => {
    const staging = `${target}.staging-${randomUUID()}`; let created = false;
    try {
      await mkdir(staging, { mode: 0o700 }); created = true;
      await runtime.engine.check(); const before = await runtime.objects.checkFiles({ maxFiles: caps.maxFiles, maxBytes: caps.maxFileBytes });
      const snapshot = await (runtime.storage??runtime.engine.store).captureCheckpoint(checkpointLimits), encoded = serialize(snapshot); if (encoded.length > caps.maxSnapshotBytes) fail('BACKUP_LIMIT', 'Checkpoint artifact exceeds byte limit');
      await privateWrite(join(staging, 'snapshot.bin'), encoded);
      const files = await copyFiles(objectRoot, join(staging, 'files'), before.files, caps), afterFiles = await runtime.objects.checkFiles({ maxFiles: caps.maxFiles, maxBytes: caps.maxFileBytes });
      if (canonical(before) !== canonical(afterFiles)) fail('BACKUP_CHANGED', 'Complete object/source inventory changed');
      const manifest = { protocol, backupId, tenant: runtime.engine.tenant, catalogIdentity: runtime.engine.catalog.identity, incarnation, sourceIdentity, checkpoint: { bytes: encoded.length, sha256: hash(encoded) }, files };
      const signed = { manifest, proof: keyring.digest(manifest, backupBinding(manifest)) }, json = Buffer.from(JSON.stringify(signed)); if (json.length > caps.maxManifestBytes) fail('BACKUP_LIMIT', 'Backup manifest exceeds byte limit');
      await privateWrite(join(staging, 'manifest.json'), json); await syncDirectory(staging);
      if (after) await after({ phase: 'staged', directory: staging, manifest });
      try { await lstat(target); fail('BACKUP_TARGET_EXISTS', 'Backup destination already exists'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
      await rename(staging, target); created = false; await syncDirectory(dirname(target));
      if(after)try{await after({phase:'published',directory:target,manifest});}catch(cause){throw Object.assign(new Error('Backup published but acknowledgement lost',{cause}),{code:'COMMIT_UNKNOWN',backupId,directory:target});}
      return { directory: target, backupId, digest: hash(canonical(signed)), checkpointBytes: encoded.length, files: files.length, fileBytes: files.reduce((sum, file) => sum + file.bytes, 0) };
    } finally { if (created) await rm(staging, { recursive: true, force: true }); }
  });
}
async function readPrivate(path, maximum) {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try { const stat = await file.stat(); if (!stat.isFile() || stat.nlink !== 1 || stat.size > maximum) fail('BACKUP_LIMIT', 'Invalid or oversized backup artifact'); return await file.readFile(); } finally { await file.close(); }
}
/** Authentication precedes V8 decoding; custody keys are not part of backups. */
export async function readBackup({ directory: target, keyring, limits: inputLimits } = {}) {
  target = absolute(target); const caps = limits(inputLimits); if (!keyring?.digest) fail('INVALID_BACKUP', 'Custody required'); await directory(target);
  const entries = (await readdir(target)).sort(); if (canonical(entries) !== canonical(['files', 'manifest.json', 'snapshot.bin'])) fail('BACKUP_INVALID', 'Incomplete or unexpected backup artifacts');
  let signed; try { signed = JSON.parse((await readPrivate(join(target, 'manifest.json'), caps.maxManifestBytes)).toString('utf8')); } catch (error) { if (error.code) throw error; fail('BACKUP_INVALID', 'Malformed backup manifest'); }
  if(!signed||typeof signed!=='object'||Array.isArray(signed)||Object.keys(signed).length!==2||!Object.hasOwn(signed,'manifest')||!Object.hasOwn(signed,'proof'))fail('BACKUP_INVALID','Invalid signed backup wrapper');
  const manifest = signed?.manifest;
  if (signed?.proof === undefined || typeof signed.proof !== 'string' || !/^[A-Za-z0-9_-]{1,64}:[A-Za-z0-9_-]{43}$/.test(signed.proof) || manifest?.protocol !== protocol || typeof manifest.backupId !== 'string' || typeof manifest.tenant !== 'string' || typeof manifest.catalogIdentity !== 'string' || typeof manifest.incarnation !== 'string' || !validSourceIdentity(manifest.sourceIdentity) || !manifest.checkpoint || !positive(manifest.checkpoint.bytes) || !/^[a-f0-9]{64}$/.test(manifest.checkpoint.sha256) || !Array.isArray(manifest.files) || manifest.files.length > caps.maxFiles) fail('BACKUP_INVALID', 'Unsupported backup manifest');
  if (!same(signed.proof, keyring.digest(manifest, backupBinding(manifest), signed.proof.split(':')[0]))) fail('BACKUP_INTEGRITY', 'Backup authentication failed');
  const encoded = await readPrivate(join(target, 'snapshot.bin'), caps.maxSnapshotBytes); if (encoded.length !== manifest.checkpoint.bytes || hash(encoded) !== manifest.checkpoint.sha256) fail('BACKUP_INTEGRITY', 'Checkpoint bytes changed');
  await directory(join(target, 'files')); const kinds = (await readdir(join(target, 'files'))).sort(); if (canonical(kinds) !== canonical(['objects', 'stages'])) fail('BACKUP_INVALID', 'Incomplete object directories');
  let totalBytes = 0; const identities = new Set();
  for (const file of manifest.files) { const identity = fileIdentity(file); if (identities.has(identity) || ![0o400, 0o600].includes(file.mode)) fail('BACKUP_INVALID', 'Invalid repeated object file'); identities.add(identity); totalBytes += file.bytes; if (totalBytes > caps.maxFileBytes) fail('BACKUP_LIMIT', 'Object bytes exceed limit'); }
  for (const kind of ['stages', 'objects']) { await directory(join(target, 'files', kind)); const found = (await readdir(join(target, 'files', kind))).sort(), expected = manifest.files.filter(file => file.kind === kind).map(file => file.name).sort(); if (canonical(found) !== canonical(expected)) fail('BACKUP_INVALID', 'Object inventory differs from manifest'); }
  let snapshot; try { snapshot = deserialize(encoded); } catch { fail('BACKUP_INVALID', 'Malformed checkpoint serialization'); }
  return { directory: target, manifest, snapshot, digest: hash(canonical(signed)), limits: caps };
}

/** Restores a separate caller-owned empty namespace/file root. Only a fully
 * checked candidate returns a process-local activation handle. */
export async function prepareRestoration({ backupDirectory, keyring, target, catalog, tenant, openCandidate, limits: inputLimits, checkpointLimits, previousIncarnations, after } = {}) {
  if (!target?.store?.restoreCheckpoint || !catalog?.identity || typeof openCandidate !== 'function' || after !== undefined && typeof after !== 'function') fail('INVALID_BACKUP', 'Candidate store/catalog/opener required');
  const loaded = await readBackup({ directory: backupDirectory, keyring, limits: inputLimits });
  if (loaded.manifest.tenant !== tenant || loaded.manifest.catalogIdentity !== catalog.identity) fail('BACKUP_SCOPE', 'Backup tenant/catalog differs');
  const objectRoot = absolute(target.objectRoot), incarnation = randomUUID(), prefix = decodeDomain('id', target.prefix), clusterFile = absolute(target.clusterFile);
  if(target.store.location?.adapter!=='foundationdb'||target.store.location.prefix!==prefix||target.store.location.clusterFile!==clusterFile)fail('RESTORE_TARGET_MISMATCH','Actual FoundationDB namespace differs from candidate descriptor');
  if(previousIncarnations!==undefined&&(!Array.isArray(previousIncarnations)||previousIncarnations.length>128))fail('INVALID_BACKUP','Bounded previous incarnation array required');
  const ancestors=[...new Set([loaded.manifest.incarnation,...(previousIncarnations??[])])].map(value=>decodeDomain('id',value));
  if(ancestors.length>128)fail('BACKUP_LIMIT','Previous incarnation inventory exceeds limit');Object.freeze(ancestors);
  try { await lstat(objectRoot); fail('RESTORE_TARGET_EXISTS', 'Candidate file root already exists'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  await target.store.restoreCheckpoint(loaded.snapshot, checkpointLimits);
  await copyFiles(join(loaded.directory, 'files'), objectRoot, loaded.manifest.files, loaded.limits);
  const runtime = await openCandidate({ store: target.store, objectRoot, incarnation,previousIncarnations:ancestors }); requireRuntime(runtime);
  if(typeof runtime.quiesce!=='function')fail('INVALID_BACKUP','Candidate must expose trusted admission-and-worker quiescence');
  if(runtime.storage!==target.store)fail('RESTORE_TARGET_MISMATCH','Candidate opener replaced the selected storage handle');
  if (runtime.engine.catalog.identity !== catalog.identity || runtime.engine.tenant !== tenant) fail('BACKUP_SCOPE', 'Candidate runtime differs');
  try {
    const engine = await runtime.engine.check(), objects = await runtime.objects.checkFiles({ maxFiles: loaded.limits.maxFiles, maxBytes: loaded.limits.maxFileBytes });
    const expected = loaded.manifest.files.map(({ mode, ...file }) => file); const ordered=files=>[...files].sort((a,b)=>Buffer.compare(Buffer.from(fileIdentity(a)),Buffer.from(fileIdentity(b)))); if (canonical(ordered(objects.files)) !== canonical(ordered(expected))) fail('BACKUP_INTEGRITY', 'Restored complete file inventory differs');
    const descriptor = { version: '1', clusterFile, prefix, objectRoot, tenant, catalogIdentity: catalog.identity, incarnation,previousIncarnations:ancestors, backupId: loaded.manifest.backupId, backupDigest: loaded.digest };
    const handle = Object.freeze({ runtime, descriptor: Object.freeze(descriptor), checked: { engine, objects } }); restoreHandles.set(handle, { loaded, runtime, descriptor,checkpointLimits });
    if (after) await after({ phase: 'prepared', descriptor }); return handle;
  } catch (error) { await runtime.close?.(); throw error; }
}

/** Publication is one fsynced external descriptor rename after the old service
 * stops. Candidate quiescence covers final verification and publication, then
 * closes the candidate; start the selected service by reopening its descriptor.
 * The descriptor and custody keys are never included in database backups. */
export async function activateRestoration({ handle, deploymentFile, operationId, stopPrevious, after } = {}) {
  const held = restoreHandles.get(handle); if (!held || typeof stopPrevious!=='function' || after !== undefined && typeof after !== 'function') fail('INVALID_RESTORE_HANDLE', 'Verified process-local restoration handle required');
  deploymentFile = absolute(deploymentFile); operationId = decodeDomain('id', operationId);
  if(activations.has(deploymentFile))fail('RESTORE_BUSY','Another activation owns this deployment');const lock=Symbol('activation');activations.set(deploymentFile,lock);
  try{
  let prior; try { prior = JSON.parse((await readPrivate(deploymentFile, 1024 * 1024)).toString('utf8')); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (prior?.restoreOperationId === operationId) { if (prior.backupDigest !== held.descriptor.backupDigest) fail('RESTORE_OPERATION_MISMATCH', 'Restore identity belongs to another backup'); return prior; }
  await stopPrevious();
  return await held.runtime.quiesce(async()=>{
  const captured=await held.runtime.storage.captureCheckpoint(held.checkpointLimits);if(!isDeepStrictEqual(captured,held.loaded.snapshot))fail('RESTORE_CANDIDATE_CHANGED','Candidate differs from the authenticated checkpoint');
  await held.runtime.engine.check();const checkedFiles=await held.runtime.objects.checkFiles({maxFiles:held.loaded.limits.maxFiles,maxBytes:held.loaded.limits.maxFileBytes});const order=files=>[...files].sort((a,b)=>Buffer.compare(Buffer.from(fileIdentity(a)),Buffer.from(fileIdentity(b))));if(canonical(order(checkedFiles.files))!==canonical(order(held.loaded.manifest.files.map(({mode,...file})=>file))))fail('RESTORE_CANDIDATE_CHANGED','Candidate file inventory differs from signed backup');for(const file of held.loaded.manifest.files){const info=await lstat(join(held.descriptor.objectRoot,fileIdentity(file)));if((info.mode&0o777)!==file.mode)fail('RESTORE_CANDIDATE_CHANGED','Candidate file mode differs');}
  const descriptor = { ...prior, ...held.descriptor, restoreOperationId: operationId }, staging = `${deploymentFile}.staging-${randomUUID()}`; let created = false;
  try {
    await privateWrite(staging, Buffer.from(JSON.stringify(descriptor))); created = true;
    if (after) await after({ phase: 'beforeActivation', descriptor }); await rename(staging, deploymentFile); created = false; await syncDirectory(dirname(deploymentFile));
    if (after) try { await after({ phase: 'afterActivation', descriptor }); } catch (cause) { const error = Object.assign(new Error('Restoration published but acknowledgement lost', { cause }), { code: 'COMMIT_UNKNOWN', operationId }); throw error; }
    return descriptor;
  } finally { if (created) await rm(staging, { force: true }); }
  });
  }finally{if(activations.get(deploymentFile)===lock)activations.delete(deploymentFile);}
}
