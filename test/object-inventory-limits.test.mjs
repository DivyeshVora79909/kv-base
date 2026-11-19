import test from 'node:test';
import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import {fixture,definition,chunks} from './helpers/objects-fixture.mjs';
import {runtimeFixture} from './helpers/runtime-fixture.mjs';
import {prepareCatalog} from '../src/catalog.mjs';
import {openObjectStore} from '../src/objects.mjs';
import {sourceKey,sourcePrefix} from '../src/keys.mjs';
import {pipelineDefinition} from '../examples/pipeline.mjs';
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
const marker={schema:{type:'object',required:['value'],properties:{value:{type:'integer'}},additionalProperties:false}};
const record=(id,value)=>({id,tenant:'t',model:'Marker',owner:'reader',createdAt:'0',data:{value}});

test('object inventory budgets count retained versions independently of active sources and exact file bytes',{skip},async t=>{
 const f=await fixture(t),bytes=Buffer.from('exact file byte boundary');
 for(let version=0;version<3;version++){const grant=await f.begin(bytes,'blob','reader','begin-'+version);await f.objects.upload(f.token,grant.uploadToken,chunks(bytes));await f.objects.finalize(f.token,{operationId:'finish-'+version,id:'blob',uploadToken:grant.uploadToken});await f.objects.remove(f.token,{operationId:'remove-'+version,id:'blob'});let after;do{const page=await f.objects.reconcile(after?{after}:{});after=page.cursor;}while(after);}
 const ready=await f.begin(bytes,'blob','reader','begin-current');await f.objects.upload(f.token,ready.uploadToken,chunks(bytes));await f.objects.finalize(f.token,{operationId:'finish-current',id:'blob',uploadToken:ready.uploadToken});await f.begin(Buffer.alloc(0),'pending','reader');
 const expected=await f.engine.read(async tx=>({sources:(await tx.scan({prefix:sourcePrefix('t')})).length,rows:(await tx.scan({prefix:'objects/v1/'})).filter(entry=>entry.value.protocol==='local-object-1').length}));assert.deepEqual(expected,{sources:2,rows:5});
 const caps={maxSources:2,maxRows:5,maxFiles:1,maxBytes:bytes.length},checked=await f.objects.checkFiles(caps);assert.equal(checked.sources,2);assert.equal(checked.rows,5);assert.equal(checked.totalBytes,bytes.length);assert.equal(checked.files.length,1);
 for(const reduced of [{maxSources:1},{maxRows:4},{maxBytes:bytes.length-1}])await assert.rejects(f.objects.checkFiles({...caps,...reduced}),{code:'WORK_LIMIT'});assert.deepEqual((await f.objects.checkFiles(caps)).files,checked.files);
});

test('object inventory reads actual bounded pages while preserving complete source and retained-row caps',{skip,timeout:30000},async t=>{
 const f=await fixture(t,{catalog:prepareCatalog({...definition,models:{...definition.models,Marker:marker}})});
 for(let offset=0;offset<260;offset+=64)await f.store.transact(tx=>Promise.all(Array.from({length:Math.min(64,260-offset)},(_,index)=>{const value=record('marker-'+String(offset+index).padStart(3,'0'),offset+index);return tx.set(sourceKey('t',value.id),value);})));
 for(let index=0;index<129;index++){const id='retained-'+index;await f.begin(Buffer.alloc(0),id,'reader','begin-'+id);await f.objects.remove(f.token,{operationId:'remove-'+id,id});}
 const calls=[],engine={...f.engine,read:callback=>f.engine.read(tx=>callback({...tx,scan:async options=>{calls.push(structuredClone(options));return tx.scan(options);}}))};
 const objects=await openObjectStore({engine,identity:f.identity,keyring:f.keyring,getRestoreFence:()=>f.fence,clock:()=>f.time,directory:f.directory,model:'Object'}),caps={maxSources:260,maxRows:129,maxFiles:1,maxBytes:1},checked=await objects.checkFiles(caps);assert.equal(checked.sources,0);assert.equal(checked.rows,129);assert.equal(checked.totalBytes,0);
 assert.ok(calls.length>=10);assert.ok(calls.every(call=>Number.isSafeInteger(call.limit)&&call.limit>0&&call.limit<=128));assert.ok(calls.some(call=>call.prefix===sourcePrefix('t')&&call.gt));assert.ok(calls.some(call=>call.prefix.startsWith('objects/')&&call.gt));
 await assert.rejects(objects.checkFiles({...caps,maxSources:259}),{code:'WORK_LIMIT'});await assert.rejects(objects.checkFiles({...caps,maxRows:128}),{code:'WORK_LIMIT'});
});

test('runtime object inventory source cap defaults to10000 and an explicit higher budget permits a valid restart',{skip,timeout:60000},async t=>{
 const f=await runtimeFixture(t);await f.runtime.close();const definition=pipelineDefinition();definition.models.Marker=marker;f.catalog=prepareCatalog(definition);f.descriptor={...f.descriptor,catalogIdentity:f.catalog.identity};await writeFile(f.deploymentFile,JSON.stringify(f.descriptor),{mode:0o600});await f.open();await f.runtime.bootstrap(f.bootstrap);
 const initial=await f.runtime.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).length),required=10000-initial;
 for(let offset=0;offset<required;offset+=128)await f.runtime.storage.transact(tx=>Promise.all(Array.from({length:Math.min(128,required-offset)},(_,index)=>{const value=record('bulk-'+String(offset+index).padStart(5,'0'),offset+index);return tx.set(sourceKey('t',value.id),value);})));
 assert.equal(await f.runtime.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).length),10000);await f.runtime.start({workerIntervalMs:0});await f.runtime.close();await f.open();const overflow=record('bulk-overflow',required);await f.runtime.storage.transact(tx=>tx.set(sourceKey('t',overflow.id),overflow));
 assert.equal(await f.runtime.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).length),10001);await f.runtime.engine.check();await assert.rejects(f.runtime.start({workerIntervalMs:0}),{code:'WORK_LIMIT'});await f.runtime.close();
 await f.open({objectCheckLimits:{maxSources:10001}});const address=await f.runtime.start({workerIntervalMs:0});const health=await fetch(address.url+'/health');assert.equal(health.status,200);assert.deepEqual(await health.json(),{status:'ready'});await f.runtime.close();
});
