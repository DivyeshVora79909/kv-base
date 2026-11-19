import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { readFile, writeFile, lstat, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash, randomUUID } from 'node:crypto';
import { runtimeFixture, clusterFile, openSelfHosted } from './helpers/runtime-fixture.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { writeBackup, prepareRestoration, activateRestoration } from '../src/operations.mjs';
import { readDeployment } from '../src/self-hosted.mjs';
const skip = clusterFile ? false : 'Selected real FDB cluster required', root = fileURLToPath(new URL('../', import.meta.url));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
async function seeded(t) {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);f.token=(await f.runtime.identity.login({actor:'reader',password:f.password,operationId:'crash-login'})).token;f.bytes=Buffer.from([0,128,255,13,7,0]);
 const grant=await f.runtime.objects.begin(f.token,{operationId:'crash-begin',id:'crash-object',owner:'visible',bytes:f.bytes.length,sha256:hash(f.bytes)});await f.runtime.objects.upload(f.token,grant.uploadToken,(async function*(){yield f.bytes;})());await f.runtime.objects.finalize(f.token,{operationId:'crash-finalize',id:'crash-object',uploadToken:grant.uploadToken});
 f.originalDescriptor=await readDeployment(f.deploymentFile);f.checkpoint=await f.runtime.storage.captureCheckpoint();f.sourceIdentity={revision:'runtime-crash-independent-witness',sourceHash:hash(await readFile(new URL('../src/self-hosted.mjs',import.meta.url))),lockHash:hash(await readFile(new URL('../package-lock.json',import.meta.url))),catalogIdentity:f.catalog.identity};return f;
}
async function backup(f) { return writeBackup({runtime:f.runtime,objectRoot:f.objectRoot,directory:join(f.directory,'complete-backup'),keyring:f.runtime.keyring,incarnation:f.descriptor.incarnation,sourceIdentity:f.sourceIdentity,quiesce:callback=>f.runtime.quiesce(callback)}); }
async function killedPhase(t,f,patch,phase) {
 const configurationFile=join(f.directory,'crash-operator-'+randomUUID()+'.json');await writeFile(configurationFile,JSON.stringify({deploymentFile:f.deploymentFile,keyringFile:f.keyringFile,logins:f.logins,contacts:f.contacts,channels:f.channels,emailUrl:f.email.baseUrl,smsUrl:f.sms.baseUrl,time:f.time,sourceIdentity:f.sourceIdentity,...patch}),{mode:0o600});
 const child=fork(fileURLToPath(new URL('./fixtures/operations-process.mjs',import.meta.url)),[],{env:{...process.env,KV_BASE_OPERATIONS_MODULE_ROOT:root,KV_BASE_RUNTIME_CONFIGURATION:configurationFile},stdio:['ignore','pipe','pipe','ipc']});let errors='';child.stderr.on('data',chunk=>{errors+=chunk;});t.after(async()=>{if(child.exitCode===null&&child.signalCode===null){const exit=new Promise(resolve=>child.once('exit',resolve));child.kill('SIGKILL');await exit;}});
 const state=await new Promise((resolve,reject)=>{const timer=setTimeout(()=>finish(Error(`Child phase timed out ${phase}: ${errors}`)),15000),message=value=>{if(value?.type==='phase'&&value.phase===phase)finish(undefined,value);},exit=(code,signal)=>finish(Error(`Child ended ${code}/${signal}: ${errors}`));function finish(error,value){clearTimeout(timer);child.off('message',message);child.off('exit',exit);error?reject(error):resolve(value);}child.on('message',message);child.once('exit',exit);});
 const outcome=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal})));child.kill('SIGKILL');assert.deepEqual(await outcome,{code:null,signal:'SIGKILL'});return state;
}
async function checkedCandidate(t,f,descriptor) {
 const storage=await openFoundationDbStore({clusterFile,prefix:descriptor.prefix}),runtime=await openSelfHosted({...f.configuration(),deploymentFile:undefined,descriptor,storage});t.after(()=>runtime.close());assert.deepEqual(await storage.captureCheckpoint(),f.checkpoint);await runtime.engine.check();const files=await runtime.objects.checkFiles();assert.equal(files.ready,1);assert.equal(files.files[0].sha256,hash(f.bytes));return runtime;
}
async function collect(stream) { const parts=[];for await(const part of stream)parts.push(part);return Buffer.concat(parts); }
async function recover(t,f,backupDirectory,operationId) {
 const prefix='runtime-crash-recovery-'+randomUUID(),storage=await openFoundationDbStore({clusterFile,prefix}),objectRoot=join(f.directory,'recovered-'+randomUUID());t.after(()=>storage.close());
 const handle=await prepareRestoration({backupDirectory,keyring:f.runtime.keyring,catalog:f.catalog,tenant:'t',target:{store:storage,clusterFile,prefix,objectRoot},openCandidate:({store,objectRoot,incarnation,previousIncarnations})=>openSelfHosted({...f.configuration(),deploymentFile:undefined,descriptor:{...f.descriptor,prefix,objectRoot,incarnation,previousIncarnations},storage:store})});t.after(()=>handle.runtime.close());return activateRestoration({handle,deploymentFile:f.deploymentFile,operationId,stopPrevious:()=>f.runtime.close()});
}

test('D5 real backup child SIGKILL after complete staging never selects an incomplete backup', { skip }, async t => {
 const f=await seeded(t);await f.runtime.close();const target=join(f.directory,'killed-backup'),state=await killedPhase(t,f,{mode:'backup',backupDirectory:target},'backup:staged');assert.deepEqual(await readDeployment(f.deploymentFile),f.originalDescriptor);await assert.rejects(lstat(target),{code:'ENOENT'});assert.equal((await readdir(state.directory)).includes('manifest.json'),true);
 await f.open();assert.deepEqual(await f.runtime.storage.captureCheckpoint(),f.checkpoint);assert.deepEqual(await collect((await f.runtime.objects.download(f.token,'crash-object')).stream),f.bytes);await f.runtime.engine.check();
});

test('D5 real restore child SIGKILL after checked preparation keeps the previous installation selected', { skip }, async t => {
 const f=await seeded(t),saved=await backup(f),targetPrefix='runtime-crash-prepared-'+randomUUID(),targetObjectRoot=join(f.directory,'prepared-files');const state=await killedPhase(t,f,{mode:'prepare',backupDirectory:saved.directory,targetPrefix,targetObjectRoot},'candidate:prepared');assert.deepEqual(await readDeployment(f.deploymentFile),f.originalDescriptor);
 await checkedCandidate(t,f,state.descriptor);await f.open();assert.deepEqual(await f.runtime.storage.captureCheckpoint(),f.checkpoint);assert.deepEqual(await collect((await f.runtime.objects.download(f.token,'crash-object')).stream),f.bytes);
});

for(const phase of ['beforeActivation','afterActivation'])test(`D5 real activation child SIGKILL ${phase} preserves the exact selected descriptor and original recovery`, { skip }, async t => {
 const f=await seeded(t),saved=await backup(f),targetPrefix='runtime-crash-activation-'+randomUUID(),targetObjectRoot=join(f.directory,'activation-files'),operationId='crash-activation';const state=await killedPhase(t,f,{mode:'activate',backupDirectory:saved.directory,targetPrefix,targetObjectRoot,activationPhase:phase,operationId},`activation:${phase}`),selected=await readDeployment(f.deploymentFile);
 assert.deepEqual(selected,phase==='beforeActivation'?f.originalDescriptor:state.descriptor);await checkedCandidate(t,f,state.descriptor);const recovered=await recover(t,f,saved.directory,operationId);
 if(phase==='afterActivation')assert.deepEqual(recovered,selected);else{assert.notEqual(recovered.prefix,selected.prefix);assert.equal(recovered.restoreOperationId,operationId);}
 await f.open();assert.deepEqual(await f.runtime.storage.captureCheckpoint(),f.checkpoint);assert.deepEqual(await f.runtime.bootstrap(f.bootstrap),{bootstrapped:true,catalogIdentity:f.catalog.identity,actors:3,sources:3,providerCredentials:2});await assert.rejects(f.runtime.identity.read(f.token,()=>true),{code:'UNAUTHENTICATED'});const fresh=(await f.runtime.identity.login({actor:'reader',password:f.password,operationId:'fresh-afterkill'})).token;assert.deepEqual(await collect((await f.runtime.objects.download(fresh,'crash-object')).stream),f.bytes);await f.runtime.engine.check();
});
