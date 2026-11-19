import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {openSqliteStore} from '../src/sqlite.mjs';
import {openFoundationDbStore} from '../src/foundationdb.mjs';
import {createEngine} from '../src/lifecycle.mjs';
import {createKeyring} from '../src/keyring.mjs';
import {prepareAuthority} from '../src/authority.mjs';
import {mutateInTransaction} from '../src/mutate.mjs';
import {sourceKey,sourcePrefix} from '../src/keys.mjs';
import {createCredentialCustody,acceptEffectInTransaction,createEffectWorker} from '../src/effects.mjs';
import {createFakeProviderServer,createFakeEmailClient} from '../src/providers.mjs';
import {prepare,authority,context,externalKey,message,intent} from './effects-fixture.mjs';

const clusterFile=process.env.KV_BASE_FDB_CLUSTER_FILE;
async function fixture(t,backend){
  const f={directory:await mkdtemp(join(tmpdir(),'kv-base-effect-recovery-')),prefix:'recovery-'+randomUUID(),time:1000,rejectDelivery:false};
  f.store=backend==='foundationdb'?await openFoundationDbStore({clusterFile,prefix:f.prefix}):openSqliteStore(join(f.directory,'app.sqlite'));const ledger=openSqliteStore(join(f.directory,'provider.sqlite'));
  f.catalog=prepare((_summary,{scope})=>!(f.rejectDelivery&&scope.partition[0]==='delivered'));f.engine=createEngine({store:f.store,catalog:f.catalog,tenant:'tenant'});f.keyring=createKeyring({keys:{root:externalKey},activeKey:'root'});const clock=()=>f.time;
  await f.engine.transact(tx=>tx.set('fixture-authority',authority()));f.custody=createCredentialCustody({catalog:f.catalog,keyring:f.keyring,getAuthority:async tx=>prepareAuthority(f.catalog,await tx.get('fixture-authority')),clock});
  await f.engine.transact(async tx=>{for(const id of ['credential','other-credential'])await f.custody.storeRevision(tx,context,{id,provider:'resend',secret:'recovery-provider-secret'});});
  f.server=await createFakeProviderServer({protocol:'resend',store:ledger,secret:'recovery-provider-secret'});f.provider=createFakeEmailClient({protocol:'resend',baseUrl:f.server.baseUrl});f.providers={resend:f.provider};
  f.source=id=>f.engine.read(tx=>tx.get(sourceKey('tenant',id)));f.sources=()=>f.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('tenant')})).map(row=>row.value));
  f.accept=(id,credentialId='credential')=>f.engine.transact(async tx=>{await mutateInTransaction(tx,f.catalog,'tenant',[{type:'put',record:message(id)}]);return acceptEffectInTransaction(tx,f.catalog,context,{...intent(id),credentialId},{custody:f.custody,providers:f.providers,clock});});
  f.apply=async(tx,{effect,fact})=>[{type:'put',record:{...(await tx.get(sourceKey('tenant',effect.id.slice('effect:'.length)))),data:{status:'delivered',providerId:fact.data.result.providerId}}}];
  f.worker=(apply=f.apply,providers=f.providers,engine=f.engine)=>createEffectWorker({engine,custody:f.custody,providers,clock,leaseMs:10,apply});
  t.after(async()=>{await f.server.close();await f.store.close();await ledger.close();if(backend==='foundationdb'){const{default:fdb}=await import('foundationdb'),db=fdb.open(clusterFile),root=fdb.tuple.pack(['kv-base',f.prefix]),upper=Buffer.from(root);let end=upper;for(let i=upper.length-1;i>=0;i--)if(upper[i]<255){upper[i]++;end=upper.subarray(0,i+1);break;}const tx=db.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,end);await tx.rawCommit();}finally{tx.rawCancel();db.close();}}await rm(f.directory,{recursive:true,force:true});});return f;
}
const factsFor=async(f,id)=>(await f.sources()).filter(row=>row.model==='EffectFact'&&row.data.effectId===id);

for(const [label,backend,skip] of [['optional SQLite','sqlite',false],['primary FoundationDB','foundationdb',clusterFile?false:'Selected real FDB cluster required']]){
  test(label+' recovery: provider secret buffers are cleared before either dispatch or reconciliation application',{skip},async t=>{
    const f=await fixture(t,backend);for(const id of ['dispatch','reconcile'])await f.accept(id);let transportSecret,applied=0;const providers={resend:{...f.provider,send:async input=>{transportSecret=input.secret;return f.provider.send(input);},lookup:async input=>{transportSecret=input.secret;return f.provider.lookup(input);}}},worker=f.worker(async(tx,input)=>{assert.ok(Buffer.isBuffer(transportSecret));assert.ok(transportSecret.every(byte=>byte===0),'provider secret is cleared before the atomic planner runs');applied++;return f.apply(tx,input);},providers);
    await worker.runOne('effect:dispatch');const token=await worker.claim('effect:reconcile'),started=await worker.start(token),secret=f.custody.open(started.credential);try{await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret});}finally{secret.fill(0);}await worker.reconcile('effect:reconcile');assert.equal(applied,2);assert.equal(f.server.stats().starts,2);for(const id of ['dispatch','reconcile'])assert.equal((await f.source(id)).data.status,'delivered');await f.engine.check();
  });

  test(label+' recovery: fresh-start custody failures remain local and drain owned starts without inventing provider facts',{skip},async t=>{
    const f=await fixture(t,backend);for(const id of ['direct','drained'])await f.accept(id);const version=(await f.sources()).find(row=>row.model==='CredentialRevision'&&row.data.credentialId==='credential'),envelope=f.keyring.seal({secret:''},{tenant:'tenant',purpose:'provider-credential',subject:'credential',revision:'1'});await f.engine.mutate([{type:'put',record:{...version,data:{...version.data,envelope}}}]);
    const worker=f.worker();await assert.rejects(worker.runOne('effect:direct'),{code:'INVALID_CREDENTIAL'});assert.equal((await f.source('effect:direct')).data.state,'started');assert.equal((await factsFor(f,'effect:direct')).length,0);assert.equal(f.server.stats().starts,0);
    await f.accept('healthy','other-credential');const result=await worker.drain({maxJobs:5});assert.ok(result.outcomes.some(outcome=>outcome.effectId==='effect:drained'&&outcome.error==='INVALID_CREDENTIAL'&&outcome.isolated));assert.equal((await f.source('effect:drained')).data.state,'isolated');assert.equal((await factsFor(f,'effect:drained')).length,0);assert.equal((await f.source('healthy')).data.status,'delivered');assert.equal(f.server.stats().starts,1);await f.engine.check();
  });

  test(label+' recovery: post-claim local read and provider configuration failures retain the owned cleanup token',{skip},async t=>{
    const f=await fixture(t,backend);for(const [id,code] of [['provider','UNSUPPORTED_EFFECT'],['read','STORAGE_ERROR']]){
      await f.accept(id);const original=f.worker(),token=await original.claim('effect:'+id);await original.start(token);f.time+=11;const providers={resend:f.provider};let injected=false;
      const engine={...f.engine,read:async callback=>{const result=await f.engine.read(callback);if(!injected&&result?.id==='effect:'+id&&result.data.state==='unknown'){injected=true;if(id==='read')throw Object.assign(Error('controlled post-claim read failure'),{code});delete providers.resend;}return result;}},result=await f.worker(f.apply,providers,engine).drain({maxJobs:1}),outcome=result.outcomes[0],effect=await f.source('effect:'+id);
      assert.equal(injected,true);assert.equal(outcome.error,code);assert.equal(outcome.stale,undefined);if(id==='read'){assert.equal(outcome.deferred,true);assert.equal(outcome.recoveryRequired,true);assert.equal(effect.data.state,'unknown');assert.ok(BigInt(effect.data.dueAt)>BigInt(f.time));}else{assert.equal(outcome.isolated,true);assert.equal(effect.data.state,'isolated');}assert.equal((await factsFor(f,effect.id)).length,0);assert.equal((await f.source(id)).data.status,'pending');
    }assert.equal(f.server.stats().starts,0);await f.accept('healthy','other-credential');await f.worker().drain({maxJobs:5});assert.equal((await f.source('healthy')).data.status,'delivered');assert.equal(f.server.stats().starts,1);await f.engine.check();
  });

  test(label+' recovery: failed atomic application defers the accepted fact and drains independent jobs without resend',{skip},async t=>{
    const f=await fixture(t,backend);for(const id of ['bad','good-one','good-two'])await f.accept(id);let repaired=false;
    const worker=f.worker(async(tx,input)=>{f.rejectDelivery=input.effect.id==='effect:bad'&&!repaired;return f.apply(tx,input);}),first=await worker.drain({maxJobs:10}),bad=await f.source('effect:bad');
    assert.equal(bad.data.state,'succeeded');assert.equal((await f.source('bad')).data.status,'pending');assert.ok(BigInt(bad.data.dueAt)>BigInt(f.time));assert.equal((await factsFor(f,bad.id)).length,1);const fact=await f.source(bad.data.lastFactId);assert.equal(fact.data.result.status,'accepted');
    assert.equal((await f.source('good-one')).data.status,'delivered');assert.equal((await f.source('good-two')).data.status,'delivered');assert.ok(first.outcomes.some(outcome=>outcome.error==='VALIDATION_FAILED'&&outcome.applicationPending&&outcome.deferred));assert.equal(f.server.stats().starts,3);
    const originalAttempt=bad.data.attemptId,originalOperation=bad.data.providerOperationId,originalFact=bad.data.lastFactId;repaired=true;f.rejectDelivery=false;f.time=Number(bad.data.dueAt);await worker.drain({maxJobs:10});
    const completed=await f.source(bad.id);assert.equal(completed.data.state,'applied');assert.equal(completed.data.attemptId,originalAttempt);assert.equal(completed.data.providerOperationId,originalOperation);assert.equal(completed.data.lastFactId,originalFact);assert.deepEqual(await f.source(originalFact),fact);assert.equal(f.server.stats().starts,3);assert.equal((await f.server.ledger()).filter(row=>row.value.operationId===originalOperation)[0].value.deliveries,1);await f.engine.check();
  });

  test(label+' recovery: inconsistent local start proof creates no provider uncertainty fact and isolates without blocking another credential',{skip},async t=>{
    const f=await fixture(t,backend);await f.accept('bad');let lookups=0;const providers={resend:{...f.provider,lookup:async input=>{lookups++;return f.provider.lookup(input);}}},worker=f.worker(f.apply,providers),token=await worker.claim('effect:bad'),started=await worker.start(token),attempt=await f.source(started.attemptId);
    await f.engine.mutate([{type:'put',record:{...attempt,data:{...attempt.data,revision:'2'}}}]);await assert.rejects(worker.reconcile('effect:bad'),{code:'CORRUPT_STATE'});assert.equal(lookups,0);assert.equal((await factsFor(f,'effect:bad')).length,0);assert.equal((await f.source('bad')).data.status,'pending');
    const otherRevision=(await f.sources()).find(row=>row.model==='CredentialRevision'&&row.data.credentialId==='other-credential');for(const data of [{...attempt.data,credentialId:'other-credential'},{...attempt.data,revisionId:otherRevision.id}]){await f.engine.mutate([{type:'put',record:{...attempt,data}}]);await assert.rejects(worker.reconcile('effect:bad'),{code:'CORRUPT_STATE'});assert.equal(lookups,0);assert.equal((await factsFor(f,'effect:bad')).length,0);}
    f.time+=11;await f.accept('healthy','other-credential');const result=await worker.drain({maxJobs:5});assert.ok(result.outcomes.some(outcome=>outcome.error==='CORRUPT_STATE'&&outcome.isolated));assert.equal((await f.source('effect:bad')).data.state,'isolated');assert.equal((await f.source('healthy')).data.status,'delivered');assert.equal((await factsFor(f,'effect:bad')).length,0);assert.equal(f.server.stats().starts,1);await f.engine.check();
  });

  test(label+' recovery: authenticated credential corruption remains a local custody error and never invents a lookup result',{skip},async t=>{
    const f=await fixture(t,backend);await f.accept('bad');let lookups=0;const providers={resend:{...f.provider,lookup:async input=>{lookups++;return f.provider.lookup(input);}}},worker=f.worker(f.apply,providers),token=await worker.claim('effect:bad'),started=await worker.start(token),version=await f.source(started.effect.data.revisionId),envelope={...version.data.envelope};envelope.tag=(envelope.tag[0]==='A'?'B':'A')+envelope.tag.slice(1);
    await f.engine.mutate([{type:'put',record:{...version,data:{...version.data,envelope}}}]);await assert.rejects(worker.reconcile('effect:bad'),{code:'SECRET_INTEGRITY'});assert.equal(lookups,0);assert.equal((await factsFor(f,'effect:bad')).length,0);
    for(const payload of [null,{}, {secret:42}, {secret:''}]){const wrongPayload=f.keyring.seal(payload,{tenant:'tenant',purpose:'provider-credential',subject:'credential',revision:'1'});await f.engine.mutate([{type:'put',record:{...version,data:{...version.data,envelope:wrongPayload}}}]);await assert.rejects(worker.reconcile('effect:bad'),{code:'INVALID_CREDENTIAL'});assert.equal(lookups,0);assert.equal((await factsFor(f,'effect:bad')).length,0);}
    f.time+=11;await f.accept('healthy','other-credential');await worker.drain({maxJobs:5});assert.equal((await f.source('effect:bad')).data.state,'isolated');assert.equal((await f.source('healthy')).data.status,'delivered');assert.equal(f.server.stats().starts,1);assert.equal((await factsFor(f,'effect:bad')).length,0);await f.engine.check();
  });

  test(label+' recovery: an actual provider lookup exception retains unknown state and exact original start',{skip},async t=>{
    const f=await fixture(t,backend);await f.accept('transport');let lookups=0;const worker=f.worker(f.apply,{resend:{...f.provider,lookup:async()=>{lookups++;throw Error('controlled socket failure');}}}),token=await worker.claim('effect:transport'),started=await worker.start(token),result=await worker.reconcile('effect:transport');
    assert.equal(result.state,'unknown');assert.equal(lookups,1);assert.equal(f.server.stats().starts,0);const effect=await f.source('effect:transport'),facts=await factsFor(f,effect.id);assert.equal(effect.data.attemptId,started.attemptId);assert.equal(effect.data.providerOperationId,started.effect.data.providerOperationId);assert.equal(facts.length,1);assert.deepEqual(facts[0].data.result,{status:'unknown',reason:'provider lookup unavailable'});assert.equal((await f.source('transport')).data.status,'pending');await f.engine.check();
  });
}

test('primary FoundationDB recovery: a successful concurrent application cannot be downgraded by the failed drain retry',{skip:clusterFile?false:'Selected real FDB cluster required'},async t=>{
  const f=await fixture(t,'foundationdb');await f.accept('raced');const worker=f.worker(),token=await worker.claim('effect:raced'),started=await worker.start(token),secret=f.custody.open(started.credential);try{await worker.recordResult(token,started.attemptId,await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret}));}finally{secret.fill(0);}
  let entered,release;const atPlanner=new Promise(resolve=>{entered=resolve;}),held=new Promise(resolve=>{release=resolve;}),draining=f.worker(async(tx,input)=>{entered();await held;const changes=await f.apply(tx,input);changes[0].record.data.status='invalid';return changes;}).drain({maxJobs:1});await atPlanner;
  let committed;try{const second=await openFoundationDbStore({clusterFile,prefix:f.prefix});t.after(()=>second.close());const otherEngine=createEngine({store:second,catalog:f.catalog,tenant:'tenant'});assert.deepEqual(await f.worker(f.apply,f.providers,otherEngine).apply('effect:raced'),{applied:true});committed=await f.source('effect:raced');}finally{release();await draining;}
  assert.deepEqual(await f.source('effect:raced'),committed);assert.equal((await f.source('raced')).data.status,'delivered');assert.equal(f.server.stats().starts,1);assert.equal((await factsFor(f,'effect:raced')).length,1);await f.engine.check();
});

test('primary FoundationDB recovery: an obsolete local failure cannot isolate a repaired newer lease',{skip:clusterFile?false:'Selected real FDB cluster required'},async t=>{
  const f=await fixture(t,'foundationdb');await f.accept('raced');const worker=f.worker(),token=await worker.claim('effect:raced'),started=await worker.start(token),attempt=await f.source(started.attemptId);await f.engine.mutate([{type:'put',record:{...attempt,data:{...attempt.data,revision:'2'}}}]);f.time+=11;
  let entered,release;const atFailure=new Promise(resolve=>{entered=resolve;}),held=new Promise(resolve=>{release=resolve;}),custody={...f.custody,resolveForRecovery:async(tx,effect)=>{try{return await f.custody.resolveForRecovery(tx,effect);}catch(error){entered();await held;throw error;}}},old=createEffectWorker({engine:f.engine,custody,providers:f.providers,workerId:'same-worker',clock:()=>f.time,leaseMs:10,apply:f.apply}),draining=old.drain({maxJobs:1});await atFailure;
  let before,result;try{await f.engine.mutate([{type:'put',record:attempt}]);f.time+=11;const current=createEffectWorker({engine:f.engine,custody:f.custody,providers:f.providers,workerId:'same-worker',clock:()=>f.time,leaseMs:10,apply:f.apply}),newLease=await current.claim('effect:raced',true);assert.equal(newLease.claimed,true);before=await f.source('effect:raced');}finally{release();result=await draining;}
  assert.ok(result.outcomes.some(outcome=>outcome.error==='CORRUPT_STATE'&&outcome.stale));assert.deepEqual(await f.source('effect:raced'),before);assert.equal((await factsFor(f,'effect:raced')).length,0);assert.equal(f.server.stats().starts,0);await f.engine.check();
});
