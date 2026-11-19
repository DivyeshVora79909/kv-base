import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { writeFile, readFile, chmod, lstat, rename } from 'node:fs/promises';
import { request as httpRequest } from 'node:http';
import { join } from 'node:path';
import { serialize } from 'node:v8';
import { runtimeFixture, clusterFile, openSelfHosted, barrier } from './helpers/runtime-fixture.mjs';
import { readDeployment, readKeyringFile } from '../src/self-hosted.mjs';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { writeBackup, prepareRestoration, activateRestoration } from '../src/operations.mjs';
import { sourcePrefix } from '../src/keys.mjs';
import { ownerIndex, ledgerIndex } from '../examples/pipeline.mjs';
const skip = clusterFile ? false : 'KV_BASE_FDB_CLUSTER_FILE must select a real test cluster';
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
async function selectedDescriptor(f, patch) { const temporary = join(f.directory, `selected-${randomUUID()}.json`); await writeFile(temporary, JSON.stringify({ ...f.descriptor, ...patch }), { mode: 0o600 }); await rename(temporary, f.deploymentFile); }
async function until(callback) { for (let tries=0;tries<100;tries++){if(await callback())return;await new Promise(resolve=>setTimeout(resolve,5));}assert.fail('Expected controlled asynchronous phase'); }
test('primary FDB runtime remains offline until explicit bootstrap then starts a sanitized shared pipeline', { skip }, async t => {
 const f = await runtimeFixture(t); assert.deepEqual(await f.runtime.health(), { status: 'uninitialized' }); await assert.rejects(f.start(), { code: 'IDENTITY_UNINITIALIZED' });
 const result = await f.runtime.bootstrap(f.bootstrap); assert.equal(result.bootstrapped,true); assert.deepEqual(await f.runtime.health(),{status:'ready'}); await f.start();
 assert.deepEqual(await f.call('/health',undefined,undefined,'GET'),{status:200,body:{status:'ready'}}); const proof=await f.proof('email');
 const beforeBadRequest=await f.runtime.storage.captureCheckpoint();for(const [path,input]of [['/v1/proof/begin',{username:'reader',channel:'email',operationId:'bad-proof',actor:'admin'}],['/v1/proof/complete',{proofId:proof.begun.proofId,code:proof.code,operationId:'bad-complete',context:{actor:'admin'}}]])assert.deepEqual(await f.call(path,input),{status:400,body:{error:'BAD_REQUEST'}});assert.deepEqual(await f.runtime.storage.captureCheckpoint(),beforeBadRequest);
 const rows=await f.call('/v1/query',{kind:'index',query:{model:'Entry',index:ownerIndex,partition:['ledger'],limit:10}},proof.token); assert.deepEqual(rows.body.records.map(record=>record.id),['visible-entry']);
 const summary=await f.call('/v1/query',{kind:'summary',query:{index:ledgerIndex,partition:['ledger']}},proof.token);assert.deepEqual(summary.body,{summary:{balance:'20'}});
 for(const path of ['/v1/bootstrap','/v1/credentials','/v1/keyring','/v1/authority','/v1/raw'])assert.deepEqual(await f.call(path,{},proof.token),{status:404,body:{error:'NOT_FOUND'}});
 await f.runtime.close();assert.deepEqual(await f.runtime.health(),{status:'stopped'});await f.open();assert.deepEqual(await f.runtime.bootstrap(f.bootstrap),result);await f.start();assert.equal((await f.call('/v1/query',{kind:'point',model:'Entry',query:{id:'visible-entry'}},proof.token)).status,200);
});

test('primary FDB runtime quiescence settles earlier asynchronous worker admission before maintenance', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);let settled=false;
 const earlier=f.runtime.drainEffects({maxJobs:0}).then(()=>{settled=true;},()=>{settled=true;});
 await f.runtime.quiesce(async()=>{assert.equal(settled,true);await f.runtime.engine.check();await f.runtime.objects.checkFiles();await assert.rejects(f.runtime.worker.runOne('none'),{code:'SERVICE_CLOSED'});await assert.rejects(f.runtime.identity.login({actor:'reader',password:f.password,operationId:'late'}),{code:'SERVICE_CLOSED'});});await earlier;assert.deepEqual(await f.runtime.health(),{status:'stopped'});
});

test('primary FDB runtime owns exactly one concurrent startup and close defeats asynchronous startup admission', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);const outcomes=await Promise.allSettled([f.runtime.start({workerIntervalMs:0}),f.runtime.start({workerIntervalMs:0})]);assert.equal(outcomes.filter(row=>row.status==='fulfilled').length,1);assert.equal(outcomes.filter(row=>row.status==='rejected').length,1);await f.runtime.close();
 await f.open();const starting=f.runtime.start({workerIntervalMs:0});const failure=assert.rejects(starting,error=>['SERVICE_CLOSED','IDENTITY_UNINITIALIZED'].includes(error.code));await f.runtime.close();await failure;assert.deepEqual(await f.runtime.health(),{status:'stopped'});
});

test('primary FDB runtime rejects nested lifecycle calls instead of waiting on their own work or maintenance', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);const issued=await f.runtime.identity.login({actor:'reader',password:f.password,operationId:'nested'});
 await f.runtime.identity.read(issued.token,async()=>{await assert.rejects(f.runtime.close(),{code:'NESTED_RUNTIME'});await assert.rejects(f.runtime.quiesce(()=>null),{code:'NESTED_RUNTIME'});await assert.rejects(f.runtime.start(),{code:'NESTED_RUNTIME'});});
 await f.runtime.quiesce(async()=>{await assert.rejects(f.runtime.close(),{code:'NESTED_RUNTIME'});await assert.rejects(f.runtime.quiesce(()=>null),{code:'NESTED_RUNTIME'});await f.runtime.engine.check();});
});

for (const phase of ['claimed','identity','seeded']) test(`primary FDB runtime original bootstrap resumes after ${phase} interruption without reseeding`, { skip }, async t => {
 let interrupted=false;const f=await runtimeFixture(t,{onBootstrapPhase:current=>{if(current===phase&&!interrupted){interrupted=true;throw Error(`interrupted ${phase}`);}}});
 await assert.rejects(f.runtime.bootstrap(f.bootstrap),error=>phase==='seeded'?error.code==='COMMIT_UNKNOWN':error.message===`interrupted ${phase}`);
 const interruptedSources=await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}));assert.equal(interruptedSources.filter(row=>row.value.model==='Entry').length,phase==='seeded'?2:0);
 const previous=await f.runtime.storage.captureCheckpoint();await f.runtime.close();await f.open({onBootstrapPhase:undefined});const result=await f.runtime.bootstrap(f.bootstrap);assert.equal(result.bootstrapped,true);
 const complete=await f.runtime.storage.captureCheckpoint();assert.equal((await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}))).filter(row=>row.value.model==='Entry').length,2);
 if(phase==='seeded')assert.deepEqual(complete,previous);assert.deepEqual(await f.runtime.bootstrap(f.bootstrap),result);assert.deepEqual(await f.runtime.storage.captureCheckpoint(),complete);
 const changed=structuredClone(f.bootstrap);changed.credentials[0].password+=' different';await assert.rejects(f.runtime.bootstrap(changed),{code:'BOOTSTRAP_MISMATCH'});await assert.rejects(f.runtime.bootstrap({...f.bootstrap,operationId:'takeover'}),{code:'BOOTSTRAP_MISMATCH'});assert.deepEqual(await f.runtime.storage.captureCheckpoint(),complete);
 const physical=serialize(complete);for(const plaintext of [f.password,'runtime-email-secret','runtime-sms-secret'])assert.equal(physical.includes(Buffer.from(plaintext)),false);
 await f.runtime.engine.check();
});

test('primary FDB runtime concurrent original bootstrap publishes one complete receipt and refuses nonempty namespace takeover', { skip }, async t => {
 const f=await runtimeFixture(t);const results=await Promise.allSettled([f.runtime.bootstrap(f.bootstrap),f.runtime.bootstrap(f.bootstrap)]);assert.ok(results.some(row=>row.status==='fulfilled'));const complete=await f.runtime.bootstrap(f.bootstrap);for(const row of results)if(row.status==='fulfilled')assert.deepEqual(row.value,complete);assert.equal((await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}))).filter(row=>row.value.model==='Entry').length,2);
 const other=await runtimeFixture(t);await other.runtime.storage.transact(tx=>tx.set('operator-existing',{private:true}));await assert.rejects(other.runtime.bootstrap(other.bootstrap),{code:'BOOTSTRAP_TARGET_NOT_EMPTY'});assert.deepEqual(await other.runtime.storage.read(tx=>tx.get('operator-existing')),{private:true});
});

test('primary FDB runtime binds actual storage and confines trusted tuning to bounded fields', { skip }, async t => {
 const f=await runtimeFixture(t);const foreign=await openFoundationDbStore({clusterFile,prefix:'runtime-foreign-'+randomUUID()});t.after(()=>foreign.close());const before=await foreign.captureCheckpoint();
 await assert.rejects(openSelfHosted({...f.configuration(),storage:foreign}),{code:'INVALID_DEPLOYMENT'});assert.deepEqual(await foreign.captureCheckpoint(),before);
 for(const patch of [{serviceLimits:{operations:{}}},{serviceLimits:{getHealth:()=>({status:'ready'})}},{serviceLimits:{engine:f.runtime.engine}},{workerLimits:{providers:f.providers}},{workerLimits:{apply:()=>[]}},{deliveryContext:{tenant:'other',actor:'admin'}},{descriptor:{...f.descriptor,previousIncarnations:['incarnation-A']}},{descriptor:{...f.descriptor,previousIncarnations:['B','B']}}])await assert.rejects(openSelfHosted({...f.configuration(),...patch}),{code:'INVALID_DEPLOYMENT'});
 assert.equal(Object.isFrozen(f.runtime.storage.location),true);assert.throws(()=>{f.runtime.storage.location.prefix='other';},TypeError);
});

test('primary FDB runtime private operator files reject public permissions malformed data and noncanonical keys', { skip }, async t => {
 const f=await runtimeFixture(t);assert.equal((await lstat(f.deploymentFile)).mode&0o777,0o600);assert.equal((await lstat(f.keyringFile)).mode&0o777,0o600);
 await chmod(f.keyringFile,0o644);await assert.rejects(readKeyringFile(f.keyringFile),{code:'INVALID_DEPLOYMENT'});await chmod(f.keyringFile,0o600);
 await writeFile(f.keyringFile,JSON.stringify({version:'1',activeKey:'bad',keys:{bad:'not canonical base64'}}));await assert.rejects(readKeyringFile(f.keyringFile),{code:'INVALID_KEYRING'});
 await selectedDescriptor(f,{incarnation:'',catalogIdentity:'bad'});await assert.rejects(readDeployment(f.deploymentFile),{code:'INVALID_DEPLOYMENT'});
 await writeFile(f.deploymentFile,'{unfinished');await assert.rejects(readDeployment(f.deploymentFile),SyntaxError);await assert.rejects(f.start());
});

test('primary FDB runtime HTTP health reports selected incarnation change and unavailable storage without disclosing metadata', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);await f.start();const token=await f.login();await selectedDescriptor(f,{incarnation:'incarnation-B',previousIncarnations:['incarnation-A']});
 assert.deepEqual(await f.call('/health',undefined,undefined,'GET'),{status:503,body:{status:'unavailable'}});await assert.rejects(f.runtime.identity.read(token,()=>true),{code:'RESTORE_FENCED'});await assert.rejects(f.runtime.drainEffects(),{code:'RESTORE_FENCED'});await f.runtime.close();await f.open();await f.start();assert.deepEqual(await f.call('/health',undefined,undefined,'GET'),{status:200,body:{status:'ready'}});
 assert.deepEqual(await f.call('/v1/query',{kind:'point',model:'Entry',query:{id:'visible-entry'}},token),{status:401,body:{error:'UNAUTHENTICATED'}});const fresh=await f.login();assert.equal((await f.call('/v1/query',{kind:'point',model:'Entry',query:{id:'visible-entry'}},fresh)).status,200);
 await f.runtime.storage.close();assert.deepEqual(await f.call('/health',undefined,undefined,'GET'),{status:503,body:{status:'unavailable'}});
});

test('primary FDB runtime quiescence drains an admitted real HTTP upload before complete file checking', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);await f.start();const token=await f.login(),bytes=Buffer.from('runtime upload final bytes'),begin=await f.call('/v1/objects/begin',{operationId:'begin-held',id:'held-object',owner:'visible',bytes:bytes.length,sha256:digest(bytes)},token);assert.equal(begin.status,200);
 let request;const completed=new Promise((resolve,reject)=>{request=httpRequest(f.url+'/v1/objects/upload',{method:'PUT',headers:{authorization:`Bearer ${token}`,'content-type':'application/octet-stream','content-length':bytes.length,'x-upload-grant':begin.body.uploadToken}},response=>{const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('end',()=>resolve({status:response.statusCode,body:JSON.parse(Buffer.concat(chunks))}));});request.on('error',reject);request.write(bytes.subarray(0,1));});t.after(()=>request.destroy());
 await until(async()=> (await f.runtime.objects.status(token,'held-object')).state==='uploading');let entered=false;const stopped=f.runtime.quiesce(async()=>{entered=true;const checked=await f.runtime.objects.checkFiles();assert.equal(checked.files.filter(file=>file.kind==='stages').length,1);assert.equal(checked.files[0].bytes,bytes.length);assert.equal(checked.files[0].sha256,digest(bytes));});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(entered,false);request.end(bytes.subarray(1));assert.deepEqual(await completed,{status:200,body:{staged:true,bytes:bytes.length,sha256:digest(bytes)}});await stopped;assert.equal(entered,true);
});

test('primary FDB runtime quiescence waits for the full external provider call and its atomic application', { skip }, async t => {
 const gate=barrier(),f=await runtimeFixture(t,{afterAccepted:async()=>{gate.enter();await gate.blocked;}});await f.runtime.bootstrap(f.bootstrap);await f.start();const begin=await f.call('/v1/proof/begin',{username:'reader',channel:'email',operationId:'held-proof'});assert.equal(begin.status,200);
 const working=f.runtime.drainEffects({maxJobs:1});await gate.entered;let entered=false;const stopped=f.runtime.quiesce(async()=>{entered=true;const rows=await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}));assert.equal(rows.filter(row=>row.value.model==='EffectFact').length,1);await f.runtime.engine.check();});await new Promise(resolve=>setImmediate(resolve));assert.equal(entered,false);gate.release();await working;await stopped;assert.equal(entered,true);
});

test('primary FDB runtime restores the shared proof session object attachment publication and receipt pipeline', { skip }, async t => {
 const f=await runtimeFixture(t);await f.runtime.bootstrap(f.bootstrap);await f.start();const prior=await f.proof('email'),bytes=Buffer.from([0,255,13,128,0,7]),begin=await f.call('/v1/objects/begin',{operationId:'pipeline-begin',id:'attachment',owner:'visible',bytes:bytes.length,sha256:digest(bytes)},prior.token);assert.equal(begin.status,200);
 const uploaded=await fetch(f.url+'/v1/objects/upload',{method:'PUT',headers:{authorization:`Bearer ${prior.token}`,'content-type':'application/octet-stream','x-upload-grant':begin.body.uploadToken},body:bytes});assert.equal(uploaded.status,200);assert.equal((await f.call('/v1/objects/finalize',{operationId:'pipeline-finalize',id:'attachment',uploadToken:begin.body.uploadToken},prior.token)).status,200);
 const changes=[{type:'put',record:{id:'visible-entry',model:'Entry',data:{ledgerId:'ledger',at:'1',sequence:'1',amount:'100',attachment:'attachment'}}},{type:'put',record:{id:'second-entry',model:'Entry',data:{ledgerId:'ledger',at:'2',sequence:'1',amount:'0'}}}];assert.equal((await f.call('/v1/mutate',{operationId:'pipeline-attach',changes},prior.token)).status,200);
 const page={kind:'page',query:{model:'Entry',index:ownerIndex,partition:['ledger'],limit:1}},held=await f.call('/v1/query',page,prior.token);assert.equal(typeof held.body.cursor,'string');
 const pending=await f.call('/v1/proof/begin',{username:'reader',channel:'email',operationId:'prebackup-proof'});assert.equal(pending.status,200);await f.runtime.drainEffects({maxJobs:1});const oldCode=/code is (\d{8})\./.exec((await f.delivered(pending.body,'email')).value.request.text)[1];
 const sources=await f.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')})),checkpoint=await f.runtime.storage.captureCheckpoint(),sourceIdentity={revision:'runtime-independent-witness',sourceHash:digest(await readFile(new URL('../src/self-hosted.mjs',import.meta.url))),lockHash:digest(await readFile(new URL('../package-lock.json',import.meta.url))),catalogIdentity:f.catalog.identity};
 const backup=await writeBackup({runtime:f.runtime,objectRoot:f.objectRoot,directory:join(f.directory,'pipeline-backup'),keyring:f.runtime.keyring,incarnation:f.descriptor.incarnation,sourceIdentity,quiesce:callback=>f.runtime.quiesce(callback)});assert.deepEqual(await f.runtime.health(),{status:'stopped'});
 const targetPrefix='runtime-restored-'+randomUUID(),storage=await openFoundationDbStore({clusterFile,prefix:targetPrefix});t.after(()=>storage.close());const handle=await prepareRestoration({backupDirectory:backup.directory,keyring:f.runtime.keyring,catalog:f.catalog,tenant:'t',target:{store:storage,clusterFile,prefix:targetPrefix,objectRoot:join(f.directory,'restored-files')},openCandidate:({store,objectRoot,incarnation,previousIncarnations})=>openSelfHosted({...f.configuration(),deploymentFile:undefined,descriptor:{...f.descriptor,prefix:targetPrefix,objectRoot,incarnation,previousIncarnations},storage:store})});t.after(()=>handle.runtime.close());
 assert.deepEqual(await handle.runtime.storage.captureCheckpoint(),checkpoint);assert.deepEqual(await handle.runtime.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')})),sources);
 const publication=barrier(),activation=activateRestoration({handle,deploymentFile:f.deploymentFile,operationId:'pipeline-restore',stopPrevious:()=>f.runtime.close(),after:async({phase})=>{if(phase==='beforeActivation'){publication.enter();await publication.blocked;}}});await publication.entered;
 await assert.rejects(handle.runtime.engine.mutate([{type:'delete',id:'visible-entry'}],{operationId:'activation-race'}),{code:'SERVICE_CLOSED'});await assert.rejects(handle.runtime.identity.login({actor:'reader',password:f.password,operationId:'activation-login'}),{code:'SERVICE_CLOSED'});await assert.rejects(handle.runtime.worker.runOne('unknown'),{code:'SERVICE_CLOSED'});
 publication.release();const selected=await activation;assert.notEqual(selected.incarnation,f.descriptor.incarnation);assert.deepEqual(selected.previousIncarnations,[f.descriptor.incarnation]);assert.deepEqual(await handle.runtime.health(),{status:'stopped'});await handle.runtime.close();await f.open();await f.start();
 assert.deepEqual(await f.call('/v1/query',{kind:'point',model:'Entry',query:{id:'visible-entry'}},prior.token),{status:401,body:{error:'UNAUTHENTICATED'}});assert.deepEqual(await f.call('/v1/proof/complete',{proofId:pending.body.proofId,code:oldCode,operationId:'old-proof-complete'}),{status:401,body:{error:'UNAUTHENTICATED'}});
 const fresh=await f.proof('phone');assert.deepEqual(await f.call('/v1/query',{...page,cursor:held.body.cursor},fresh.token),{status:400,body:{error:'INVALID_CURSOR'}});
 const visible=await f.call('/v1/query',{kind:'index',query:{model:'Entry',index:ownerIndex,partition:['ledger'],limit:10}},fresh.token);assert.deepEqual(visible.body.records.map(row=>row.id),['visible-entry','second-entry']);assert.equal(visible.body.records[0].data.attachment,'attachment');assert.deepEqual((await f.call('/v1/query',{kind:'summary',query:{index:ledgerIndex,partition:['ledger']}},fresh.token)).body,{summary:{balance:'20'}});
 const downloaded=await fetch(f.url+'/v1/objects/attachment/data',{headers:{authorization:`Bearer ${fresh.token}`}});assert.equal(downloaded.status,200);assert.deepEqual(Buffer.from(await downloaded.arrayBuffer()),bytes);assert.equal((await f.call('/v1/objects/remove',{operationId:'still-attached',id:'attachment'},fresh.token)).status,403);await f.runtime.engine.check();await f.runtime.objects.checkFiles();
 const physical=serialize(await f.runtime.storage.captureCheckpoint());for(const plaintext of [prior.token,fresh.token,f.password,'runtime-email-secret','runtime-sms-secret',oldCode])assert.equal(physical.includes(Buffer.from(plaintext)),false);
});
