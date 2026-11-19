import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,cp,mkdir,writeFile,readFile,unlink,lstat,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {prepareRelease,checkRelease} from '../src/release.mjs';
const repository=process.env.KV_BASE_RELEASE_SOURCE??resolve(import.meta.dirname,'../');
const run=promisify(execFile);

test('D5 release source artifact is deterministic and excludes external credentials and installed dependencies',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'kv-base-release-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const source=join(directory,'source');await mkdir(source);for(const name of ['src','examples','docs','package.json','package-lock.json','README.md','LICENSE','CONTEXT.md','AGENTS.md','.nvmrc'])await cp(join(repository,name),join(source,name),{recursive:true});
  await writeFile(join(source,'keyring.json'),'external-custody-key');await writeFile(join(source,'deployment.json'),'external-incarnation');await writeFile(join(source,'.env'),'external-password');
  const first=await prepareRelease({repository:source,directory:join(directory,'release-a'),sourceIdentity:'verified-source-identity',catalogIdentity:'a'.repeat(64)}),second=await prepareRelease({repository:source,directory:join(directory,'release-b'),sourceIdentity:'verified-source-identity',catalogIdentity:'a'.repeat(64)});
  assert.equal(first.digest,second.digest);assert.deepEqual(await readFile(join(first.directory,'RELEASE.json')),await readFile(join(second.directory,'RELEASE.json')));assert.equal((await lstat(first.directory)).mode&0o777,0o700);
  assert.deepEqual((await checkRelease({directory:first.directory})).digest,first.digest);const names=await readdir(first.directory);for(const name of ['keyring.json','deployment.json','.env','node_modules'])assert.equal(names.includes(name),false);
  const manifest=JSON.parse(await readFile(join(first.directory,'RELEASE.json'),'utf8'));assert.ok(manifest.files.some(row=>row.path==='src/index.mjs'));assert.ok(manifest.files.some(row=>row.path==='package-lock.json'));assert.ok(manifest.files.some(row=>row.path==='docs/api.md'));assert.equal(manifest.files.some(row=>row.path.startsWith('test/')),false);
});

test('D5 changed or incomplete release never reports a successful source check',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'kv-base-release-failure-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const release=await prepareRelease({repository,directory:join(directory,'release'),sourceIdentity:'verified-source-identity',catalogIdentity:'a'.repeat(64)});await writeFile(join(release.directory,'src/index.mjs'),'export const changed=true;');await assert.rejects(checkRelease({directory:release.directory}),{code:'RELEASE_INTEGRITY'});
  await unlink(join(release.directory,'package-lock.json'));await unlink(join(release.directory,'RELEASE-LOCK.json'));await assert.rejects(checkRelease({directory:release.directory}),{code:'ENOENT'});await assert.rejects(prepareRelease({repository,directory:join(directory,'too-small'),sourceIdentity:'source',catalogIdentity:'a'.repeat(64),maxBytes:1}),{code:'RELEASE_LIMIT'});await assert.rejects(lstat(join(directory,'too-small')),{code:'ENOENT'});
});

test('D5 actual npm archive retains the authoritative dependency lock and rejects dependency tampering or missing derived bytes',async t=>{
  const directory=await mkdtemp(join(tmpdir(),'kv-base-release-lock-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  const release=await prepareRelease({repository,directory:join(directory,'release'),sourceIdentity:'verified-source-identity',catalogIdentity:'a'.repeat(64)}),original=await readFile(join(repository,'package-lock.json')),derived=join(release.directory,'RELEASE-LOCK.json');
  assert.deepEqual(await readFile(derived),original);assert.equal((await checkRelease({directory:release.directory})).digest,release.digest);
  const changed=Buffer.from(original),version=Buffer.from(JSON.parse(original).packages[''].dependencies.ajv),offset=changed.indexOf(version);assert.ok(offset>=0);assert.match(version.toString(),/\d$/);changed[offset+version.length-1]=String((Number(version.toString().at(-1))+1)%10).charCodeAt(0);assert.notDeepEqual(changed,original);assert.equal(changed.length,original.length);
  await writeFile(derived,changed);await assert.rejects(checkRelease({directory:release.directory}),{code:'RELEASE_INTEGRITY'});assert.deepEqual(await readFile(join(release.directory,'package-lock.json')),original);await writeFile(derived,original);
  const alternate=join(release.directory,'npm-shrinkwrap.json');await writeFile(alternate,changed);await assert.rejects(checkRelease({directory:release.directory}),{code:'RELEASE_INTEGRITY'});await assert.rejects(prepareRelease({repository:release.directory,directory:join(directory,'alternate-lock-release'),sourceIdentity:'verified-source-identity',catalogIdentity:'a'.repeat(64)}),{code:'RELEASE_INTEGRITY'});await unlink(alternate);
  const archives=join(directory,'archives'),extracted=join(directory,'installed');await mkdir(archives);await mkdir(extracted);await run('npm',['pack','--ignore-scripts','--json','--pack-destination',archives],{cwd:release.directory,maxBuffer:4*1024*1024});const names=await readdir(archives);assert.equal(names.length,1);await run('tar',['-xzf',join(archives,names[0]),'-C',extracted]);const installed=join(extracted,'package');
  await assert.rejects(lstat(join(installed,'package-lock.json')),{code:'ENOENT'});assert.deepEqual(await readFile(join(installed,'RELEASE-LOCK.json')),original);const checked=await checkRelease({directory:installed});assert.deepEqual(checked.sourceIdentity,release.sourceIdentity);assert.equal(checked.digest,release.digest);
  await writeFile(join(installed,'RELEASE-LOCK.json'),changed);await assert.rejects(checkRelease({directory:installed}),{code:'RELEASE_INTEGRITY'});await writeFile(join(installed,'RELEASE-LOCK.json'),original);
  await cp(join(installed,'RELEASE-LOCK.json'),join(installed,'package-lock.json'));assert.equal((await checkRelease({directory:installed})).digest,release.digest);await writeFile(join(installed,'package-lock.json'),changed);await assert.rejects(checkRelease({directory:installed}),{code:'RELEASE_INTEGRITY'});
  await unlink(derived);await assert.rejects(checkRelease({directory:release.directory}),{code:'RELEASE_INTEGRITY'});
});
