import test from 'node:test';
import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {openSelfHosted} from '../src/self-hosted.mjs';
import {fixture,graph,password} from './helpers/operations-fixture.mjs';
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
const witness=(name,run)=>test(name,{skip},run);


async function runtime(f){
 const opened=await f.openStore(),descriptor={version:'1',clusterFile:f.clusterFile,prefix:opened.prefix,objectRoot:join(f.directory,'hosted-objects'),tenant:'t',catalogIdentity:f.catalog.identity,incarnation:'incarnation-hosted'},deploymentFile=join(f.directory,'hosted.json');
 await writeFile(deploymentFile,JSON.stringify(descriptor),{mode:0o600});
 return openSelfHosted({descriptor,deploymentFile,catalog:f.catalog,storage:opened.store,keys:{old:Buffer.alloc(32,7)},activeKey:'old',logins:{reader:'reader'},contacts:{reader:{email:'reader@example.test'}},channels:{email:{provider:'brevo',credentialId:'credential',credentialRevision:'1',from:'sender@example.test'}},providers:f.providers,publicModels:['Object','Message'],passwordPolicy:{N:1024,sessionMs:1000000},clock:()=>f.time});
}

witness('D5 independent quiesce stops a drain whose descriptor fence check was already waiting',async t=>{
 const f=await fixture(t),r=await runtime(f);let completed=false;
 const pending=r.drainEffects({maxJobs:0}).finally(()=>{completed=true;});void pending.catch(()=>{});
 await r.quiesce(async()=>{assert.equal(completed,true,'quiesce must finish earlier worker admission before entering snapshot callback');});
});

witness('D5 independent simultaneous start admits one HTTP listener and close owns all admitted listeners',async t=>{
 const f=await fixture(t),r=await runtime(f);
 await r.bootstrap({operationId:'start-audit',policy:graph(),credentials:['admin','reader'].map(actor=>({actor,password})),sources:[],providerCredentials:[{id:'credential',provider:'brevo',secret:'external-secret'}]});
 const known=new Set(process._getActiveHandles());
 const outcomes=await Promise.allSettled([r.start({workerIntervalMs:0}),r.start({workerIntervalMs:0})]);
 await r.close();
 const newServers=process._getActiveHandles().filter(handle=>!known.has(handle)&&handle.constructor?.name==='Server');
 for(const handle of newServers)await new Promise(resolve=>handle.close(()=>resolve()));
 assert.equal(outcomes.filter(outcome=>outcome.status==='fulfilled').length,1,'at most one service may pass startup admission');
});

witness('D5 independent escaped asynchronous callback cannot reuse ended admission during maintenance',async t=>{
 const f=await fixture(t);let release,escaped,r;const delayed=new Promise(resolve=>{release=resolve;}),base=f.providers.brevo;
 f.providers.brevo={...base,send:async input=>{if(input.request.subject==='trigger'){escaped=delayed.then(()=>r.worker.runOne('effect:escaped'));void escaped.catch(()=>{});}return base.send(input);}};
 r=await runtime(f);
 await r.bootstrap({operationId:'escaped-audit',policy:graph(),credentials:['admin','reader'].map(actor=>({actor,password})),sources:[],providerCredentials:[{id:'credential',provider:'brevo',secret:'external-secret'}]});
 await f.accept(r,'trigger');await f.accept(r,'escaped');await r.worker.runOne('effect:trigger');
 await r.quiesce(async()=>{release();await assert.rejects(escaped,{code:'SERVICE_CLOSED'});});
});

witness('D5 independent nested asynchronous work remains registered after its outer provider call returns',async t=>{
 const f=await fixture(t);let r,nested,enterSecond,releaseSecond;const atSecond=new Promise(resolve=>{enterSecond=resolve;}),heldSecond=new Promise(resolve=>{releaseSecond=resolve;}),base=f.providers.brevo;
 f.providers.brevo={...base,send:async input=>{if(input.request.subject==='trigger'){nested=r.worker.runOne('effect:nested');void nested.catch(()=>{});await atSecond;}if(input.request.subject==='nested'){enterSecond();await heldSecond;}return base.send(input);}};
 r=await runtime(f);await r.bootstrap({operationId:'nested-work-audit',policy:graph(),credentials:['admin','reader'].map(actor=>({actor,password})),sources:[],providerCredentials:[{id:'credential',provider:'brevo',secret:'external-secret'}]});
 await f.accept(r,'trigger');await f.accept(r,'nested');await r.worker.runOne('effect:trigger');await atSecond;
 let maintenanceEntered=false;const stopping=r.quiesce(()=>{maintenanceEntered=true;});
 await new Promise(resolve=>setImmediate(resolve));
 const enteredTooSoon=maintenanceEntered;releaseSecond();await Promise.allSettled([nested,stopping]);
 assert.equal(enteredTooSoon,false,'quiesce must await the nested provider call even after outer admission ends');
});
