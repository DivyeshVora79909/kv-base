import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,writeFile,unlink,chmod,lstat,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {deserialize} from 'node:v8';
import {fixture,collect,password,sha256} from './helpers/operations-fixture.mjs';
import {writeBackup,readBackup,activateRestoration} from '../src/operations.mjs';
import {sourcePrefix} from '../src/keys.mjs';
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';

test('D5 signed backup and fresh candidate restore complete source,ready/staged file,receipt and identity inventories',{skip},async t=>{
  const f=await fixture(t),ready=Buffer.from([0,255,128,7,9]),staged=Buffer.from('staged private object');await f.addObject('ready',ready);const grant=await f.addObject('pending',staged,false);await f.accept(f.runtime,'job');
  const expected=await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')})),backup=await f.backup(),loaded=await readBackup({directory:backup.directory,keyring:f.keyring}),snapshot=await readFile(join(backup.directory,'snapshot.bin'));
  assert.equal(backup.files,2);assert.equal(backup.fileBytes,ready.length+staged.length);assert.equal(snapshot.includes(Buffer.from(password)),false);assert.equal(snapshot.includes(Buffer.from(f.token)),false);assert.equal(snapshot.includes(Buffer.from('external-secret')),false);
  assert.equal(loaded.manifest.incarnation,'incarnation-A');assert.equal(loaded.manifest.checkpoint.sha256,sha256(snapshot));assert.equal(loaded.manifest.sourceIdentity.revision,'D5-independent-witness');assert.equal((await lstat(backup.directory)).mode&0o777,0o700);
  const prepared=await f.prepare(backup),runtime=prepared.runtime;assert.notEqual(prepared.descriptor.incarnation,'incarnation-A');assert.equal(Object.isFrozen(prepared.descriptor.previousIncarnations),true);assert.throws(()=>prepared.descriptor.previousIncarnations.push('unverified'),TypeError);assert.deepEqual(await runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')})),expected);assert.deepEqual(await runtime.engine.getReceipt('accept:job'),await f.runtime.engine.getReceipt('accept:job'));
  await assert.rejects(runtime.identity.read(f.token,()=>true),{code:'UNAUTHENTICATED'});await assert.rejects(runtime.identity.login({actor:'reader',password,operationId:'reader-login'}),{code:'RESTORE_FENCED'});
  const fresh=(await runtime.identity.login({actor:'reader',password,operationId:'restored-reader'})).token;assert.deepEqual(await collect((await runtime.objects.download(fresh,'ready')).stream),ready);
  await assert.rejects(runtime.objects.finalize(fresh,{operationId:'old-grant',id:'pending',uploadToken:grant.uploadToken}),{code:'INVALID_OBJECT_GRANT'});assert.equal((await runtime.objects.checkFiles()).ready,1);await runtime.engine.check();
});

test('D5 malformed incomplete authenticated-byte and file corruption never publish a restoration descriptor',{skip},async t=>{
  const f=await fixture(t),bytes=Buffer.from('verified file');await f.addObject('ready',bytes);const deploymentFile=join(f.directory,'deployment.json'),original={version:'1',prefix:f.prefix,objectRoot:f.objectRoot,incarnation:f.fence};await writeFile(deploymentFile,JSON.stringify(original),{mode:0o600});
  const changed=await f.backup(),manifestFile=join(changed.directory,'manifest.json'),signed=JSON.parse(await readFile(manifestFile,'utf8'));signed.manifest.sourceIdentity.revision='tampered';await writeFile(manifestFile,JSON.stringify(signed));await assert.rejects(f.prepare(changed),{code:'BACKUP_INTEGRITY'});
  const incomplete=await f.backup();await unlink(join(incomplete.directory,'snapshot.bin'));await assert.rejects(f.prepare(incomplete),{code:'BACKUP_INVALID'});
  const fileChanged=await f.backup(),info=JSON.parse(await readFile(join(fileChanged.directory,'manifest.json'),'utf8')).manifest.files[0],path=join(fileChanged.directory,'files',info.kind,info.name);await chmod(path,0o600);await writeFile(path,Buffer.alloc(bytes.length,17));await assert.rejects(f.prepare(fileChanged),{code:'BACKUP_CHANGED'});
  assert.deepEqual(JSON.parse(await readFile(deploymentFile,'utf8')),original);assert.deepEqual(await collect((await f.runtime.objects.download(f.token,'ready')).stream),bytes);
  const target=join(f.directory,'oversized-backup');await assert.rejects(writeBackup({runtime:f.runtime,objectRoot:f.objectRoot,directory:target,keyring:f.keyring,incarnation:f.fence,sourceIdentity:f.sourceIdentity,quiesce:callback=>callback(),limits:{maxFileBytes:1}}),{code:'WORK_LIMIT'});await assert.rejects(lstat(target),{code:'ENOENT'});assert.equal((await readdir(f.directory)).some(name=>name.startsWith('oversized-backup.staging-')),false);
});

test('D5 external descriptor activation rollback and committed lost response preserve original selected installation',{skip},async t=>{
  const f=await fixture(t),bytes=Buffer.from('active artifact');await f.addObject('ready',bytes);const backup=await f.backup(),aborted=await f.prepare(backup),deploymentFile=join(f.directory,'deployment.json'),original={version:'1',prefix:f.prefix,objectRoot:f.objectRoot,incarnation:f.fence};await writeFile(deploymentFile,JSON.stringify(original),{mode:0o600});
  await assert.rejects(activateRestoration({handle:aborted,deploymentFile,operationId:'restore-one',stopPrevious:()=>f.runtime.close(),after({phase}){if(phase==='beforeActivation')throw Error('stop publication');}}),/stop publication/);assert.deepEqual(JSON.parse(await readFile(deploymentFile,'utf8')),original);await assert.rejects(aborted.runtime.engine.check(),{code:'SERVICE_CLOSED'});
  const prepared=await f.prepare(backup);
  await assert.rejects(activateRestoration({handle:prepared,deploymentFile,operationId:'restore-one',stopPrevious:()=>f.runtime.close(),after({phase}){if(phase==='afterActivation')throw Error('lost restored reply');}}),{code:'COMMIT_UNKNOWN'});
  const selected=JSON.parse(await readFile(deploymentFile,'utf8'));assert.equal(selected.prefix,prepared.descriptor.prefix);assert.equal(selected.objectRoot,prepared.descriptor.objectRoot);assert.equal(selected.incarnation,prepared.descriptor.incarnation);assert.notEqual(selected.incarnation,original.incarnation);
  let replayCalls=0;assert.deepEqual(await activateRestoration({handle:prepared,deploymentFile,operationId:'restore-one',stopPrevious:()=>f.runtime.close(),after(){replayCalls++;}}),selected);assert.equal(replayCalls,0);assert.equal((await lstat(deploymentFile)).mode&0o777,0o600);
  await assert.rejects(activateRestoration({handle:{runtime:prepared.runtime,descriptor:prepared.descriptor},deploymentFile,operationId:'forged',stopPrevious:()=>f.runtime.close()}),{code:'INVALID_RESTORE_HANDLE'});
});
