import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {writeFile,readFile} from 'node:fs/promises';
import {fixture} from './helpers/operations-fixture.mjs';
import {prepareRestoration,activateRestoration} from '../src/operations.mjs';
import {sourceKey} from '../src/keys.mjs';
const source=(r,id)=>r.engine.read(tx=>tx.get(sourceKey('t',id)));
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
const witness=(name,run)=>test(name,{skip},run);


witness('D5 independent a restored pending lease must fence the delayed post-backup original start', async t=>{
  const f=await fixture(t); await f.accept(f.runtime,'delayed'); const backup=await f.backup(),oldWorker=f.runtime.worker();
  const oldToken=await oldWorker.claim('effect:delayed'),oldStart=await oldWorker.start(oldToken),secret=f.runtime.custody.open(oldStart.credential);
  try {
    const oldPreflight=await f.providers.brevo.lookup({operationId:oldStart.effect.data.providerOperationId,secret,fence:oldToken.epoch,incarnation:oldToken.incarnation});
    assert.equal(oldPreflight.status,'absent');
    const restored=await f.prepare(backup); await f.runtime.close();
    await restored.runtime.worker().runOne('effect:delayed');await restored.runtime.worker().runOne('effect:delayed');
    const late=await f.providers.brevo.send({operationId:oldStart.effect.data.providerOperationId,request:oldStart.request,secret,fence:oldToken.epoch,incarnation:oldToken.incarnation});
    const ledger=await f.provider.ledger();
    assert.equal(late.status,'rejected','restoration must advance past a possibly delayed original fence');
    assert.equal(ledger[0].value.deliveries,1);
  } finally {secret.fill(0);}
});

witness('D5 independent restored external fencing rejects a delayed obsolete higher epoch lookup and send',async t=>{
  const f=await fixture(t);await f.accept(f.runtime,'high-epoch');const backup=await f.backup(),oldWorker=f.runtime.worker();
  let oldToken;for(let epoch=1;epoch<=100;epoch++){oldToken=await oldWorker.claim('effect:high-epoch');assert.equal(oldToken.epoch,String(epoch));if(epoch<100)f.time+=11;}
  const oldStart=await oldWorker.start(oldToken),secret=f.runtime.custody.open(oldStart.credential),restored=await f.prepare(backup);await restored.runtime.worker().runOne('effect:high-epoch');
  let admitCurrent,releaseCurrent;const atCurrentSend=new Promise(resolve=>{admitCurrent=resolve;}),release=new Promise(resolve=>{releaseCurrent=resolve;});
  const held={...f.providers.brevo,send:async input=>{admitCurrent(input);await release;return f.providers.brevo.send(input);}},current=restored.runtime.worker({brevo:held}).runOne('effect:high-epoch');
  try {
    const currentRequest=await atCurrentSend;await f.runtime.close();
    const oldLookup=await f.providers.brevo.lookup({operationId:oldStart.effect.data.providerOperationId,secret,fence:oldToken.epoch,incarnation:oldToken.incarnation});
    const late=await f.providers.brevo.send({operationId:oldStart.effect.data.providerOperationId,request:oldStart.request,secret,fence:oldToken.epoch,incarnation:oldToken.incarnation});
    assert.equal(oldLookup.status,'unknown');
    assert.equal(late.status,'rejected','obsolete restore incarnation must not raise current fence and deliver at its larger epoch');
    assert.ok(BigInt(currentRequest.fence)<BigInt(oldToken.epoch));
  }finally{secret.fill(0);releaseCurrent();await current;}
});

witness('D5 independent old lease token cannot start the restored snapshot lease', async t=>{
  const f=await fixture(t); await f.accept(f.runtime,'token'); const oldToken=await f.runtime.worker().claim('effect:token'),backup=await f.backup(),restored=await f.prepare(backup);
  await assert.rejects(restored.runtime.worker().start(oldToken),{code:'RESTORE_FENCED'});
});

witness('D5 independent verified candidate descriptor must identify the store actually restored', async t=>{
  const f=await fixture(t); await f.accept(f.runtime,'wrong-prefix'); const backup=await f.backup(),candidate=await f.openStore();
  await assert.rejects(prepareRestoration({backupDirectory:backup.directory,keyring:f.keyring,target:{...candidate,prefix:'wrong-'+randomUUID(),objectRoot:join(f.directory,'wrong-descriptor'),clusterFile:f.clusterFile},catalog:f.catalog,tenant:'t',openCandidate:f.openRuntime}),{code:'RESTORE_TARGET_MISMATCH'});
});

witness('D5 independent changes after prepare cannot publish a different source inventory as verified backup',async t=>{
  const f=await fixture(t);await f.accept(f.runtime,'source');const backup=await f.backup(),prepared=await f.prepare(backup),deploymentFile=join(f.directory,'deployment.json');
  const before={version:'1',prefix:f.prefix,objectRoot:f.objectRoot,incarnation:f.fence};await writeFile(deploymentFile,JSON.stringify(before));
  await f.accept(prepared.runtime,'unbacked-source');await prepared.runtime.engine.check();
  await assert.rejects(activateRestoration({handle:prepared,deploymentFile,operationId:'activate-mutated',stopPrevious:()=>f.runtime.close()}),{code:'RESTORE_CANDIDATE_CHANGED'});
  assert.deepEqual(JSON.parse(await readFile(deploymentFile)),before);
});

witness('D5 independent concurrent activation cannot bind one operation identity to two different backups',async t=>{
 const f=await fixture(t),firstBackup=await f.backup(),secondBackup=await f.backup(),first=await f.prepare(firstBackup),second=await f.prepare(secondBackup),deploymentFile=join(f.directory,'concurrent.json');
 await writeFile(deploymentFile,JSON.stringify({version:'1',prefix:f.prefix,objectRoot:f.objectRoot,incarnation:f.fence}));
 let arrivals=0,release,firstArrives;const held=new Promise(resolve=>{release=resolve;}),atFirstStop=new Promise(resolve=>{firstArrives=resolve;}),stopPrevious=async()=>{arrivals++;if(arrivals===1){firstArrives();await held;}};
 const firstActivation=activateRestoration({handle:first,deploymentFile,operationId:'same-original-operation',stopPrevious});await atFirstStop;
 const secondActivation=activateRestoration({handle:second,deploymentFile,operationId:'same-original-operation',stopPrevious});void secondActivation.catch(()=>{});await new Promise(resolve=>setImmediate(resolve));release();
 const results=await Promise.allSettled([firstActivation,secondActivation]);
 assert.equal(results.filter(result=>result.status==='fulfilled').length,1,'one operation identity can commit only one original backup');
 assert.ok(['RESTORE_OPERATION_MISMATCH','RESTORE_BUSY'].includes(results.find(result=>result.status==='rejected').reason.code));
});

witness('D5 independent repeated restores preserve one provider operation and reject retired reservation replay',async t=>{
 const f=await fixture(t);await f.accept(f.runtime,'multi-restore');const original=await source(f.runtime,'effect:multi-restore'),backup=await f.backup(),first=await f.prepare(backup),provider=f.providers.brevo,secret=Buffer.from('external-secret');
 const prepare=async previousIncarnations=>{const candidate=await f.openStore();return prepareRestoration({backupDirectory:backup.directory,keyring:f.keyring,target:{...candidate,objectRoot:join(f.directory,'multi-'+randomUUID()),clusterFile:f.clusterFile},catalog:f.catalog,tenant:'t',openCandidate:f.openRuntime,previousIncarnations});};
 try{
   await first.runtime.worker().runOne(original.id);const firstReserved=await source(first.runtime,original.id);assert.equal(firstReserved.data.state,'pending');assert.equal(firstReserved.data.dispatchIncarnation,first.descriptor.incarnation);
   const replayArgs={operationId:original.data.providerOperationId,secret,fence:'1',incarnation:first.descriptor.incarnation,expectedIncarnations:[f.fence]},replay=await provider.reserveRestore(replayArgs);assert.equal(replay.status,'absent');assert.equal(replay.reserved,true);
   const second=await prepare([first.descriptor.incarnation]);await second.runtime.worker().runOne(original.id);const secondReserved=await source(second.runtime,original.id);assert.equal(secondReserved.data.dispatchIncarnation,second.descriptor.incarnation);
   const atSecond=(await f.provider.ledger())[0].value;assert.equal(atSecond.incarnation,second.descriptor.incarnation);assert.ok(BigInt(atSecond.minimumFence)>BigInt(replay.fence));
   assert.equal((await provider.reserveRestore({...replayArgs,fence:'100'})).status,'unknown');
   assert.equal((await provider.lookup({...replayArgs,fence:'100'})).status,'unknown');
   const request=f.runtime.custody.openRequest(original);assert.equal((await provider.send({operationId:original.data.providerOperationId,secret,request,fence:'100',incarnation:first.descriptor.incarnation})).status,'rejected');
   assert.deepEqual((await f.provider.ledger())[0].value,atSecond,'retired requests must not change current external floor or incarnation');
   await second.runtime.worker().runOne(original.id);assert.equal((await source(second.runtime,'multi-restore')).data.state,'delivered');const delivered=(await f.provider.ledger())[0].value;assert.equal(delivered.deliveries,1);
   const third=await prepare([second.descriptor.incarnation]);await third.runtime.worker().runOne(original.id);assert.equal((await source(third.runtime,'multi-restore')).data.state,'delivered');const terminal=(await f.provider.ledger())[0].value;assert.equal(terminal.deliveries,1);assert.equal(terminal.providerId,delivered.providerId);assert.equal(terminal.incarnation,third.descriptor.incarnation);
   assert.equal((await provider.reserveRestore({operationId:original.data.providerOperationId,secret,fence:'999',incarnation:second.descriptor.incarnation,expectedIncarnations:[f.fence,first.descriptor.incarnation]})).status,'unknown');assert.deepEqual((await f.provider.ledger())[0].value,terminal);
   assert.equal((await source(third.runtime,original.id)).data.providerOperationId,original.data.providerOperationId);await third.runtime.engine.check();
 }finally{secret.fill(0);}
});

witness('D5 independent candidate writes cannot race exact checking and descriptor publication',async t=>{
 const f=await fixture(t),backup=await f.backup(),prepared=await f.prepare(backup),deploymentFile=join(f.directory,'publication-race.json'),original={version:'1',prefix:f.prefix,objectRoot:f.objectRoot,incarnation:f.fence};await writeFile(deploymentFile,JSON.stringify(original));
 await assert.rejects(activateRestoration({handle:prepared,deploymentFile,operationId:'publication-race',stopPrevious:()=>f.runtime.close(),after:async({phase})=>{if(phase==='beforeActivation')await f.accept(prepared.runtime,'after-exact-check');}}),{code:'SERVICE_CLOSED'});
 assert.deepEqual(JSON.parse(await readFile(deploymentFile)),original);
});
