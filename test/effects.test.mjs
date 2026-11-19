import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm,readFile } from 'node:fs/promises';
import { randomUUID,createHmac,hkdfSync } from 'node:crypto';
import { openFoundationDbStore } from '../src/foundationdb.mjs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { openSqliteStore } from '../src/sqlite.mjs';
import { createEngine } from '../src/lifecycle.mjs';
import { createKeyring } from '../src/keyring.mjs';
import { prepareAuthority } from '../src/authority.mjs';
import { mutateInTransaction } from '../src/mutate.mjs';
import { checkStore } from '../src/check.mjs';
import { sourceKey,sourcePrefix } from '../src/keys.mjs';
import { treeAggregate } from '../src/tree.mjs';
import { readPointAuthorized } from '../src/authorized-read.mjs';
import { fingerprint,canonical } from '../src/values.mjs';
import { createCredentialCustody,acceptEffectInTransaction,createEffectWorker,EFFECT_STARTS } from '../src/effects.mjs';
import { createFakeProviderServer,createFakeEmailClient,createEmailProvider,createSmsProvider,createFakeSmsClient } from '../src/providers.mjs';
import { prepare,authority,context,externalKey,message,intent } from './effects-fixture.mjs';

async function prepareFixture(t,{backend='sqlite',protocol='resend',operation='email.send',afterAccepted,validator,clockStart=1000,grantMs=300000}={}){
  const dir=await mkdtemp(join(tmpdir(),'kv-effects-'));const db=join(dir,'app.sqlite'),providerDb=join(dir,'provider.sqlite');
  const prefix=`effects-${randomUUID()}`,clusterFile=process.env.KV_BASE_FDB_CLUSTER_FILE;
  const openApp=()=>backend==='foundationdb'?openFoundationDbStore({clusterFile,prefix}):Promise.resolve(openSqliteStore(db));
  const cleanup=async()=>{if(backend==='foundationdb'){const {default:fdb}=await import('foundationdb'),native=fdb.open(clusterFile),root=fdb.tuple.pack(['kv-base',prefix]),end=Buffer.from(root);let bound=end;for(let i=end.length-1;i>=0;i--)if(end[i]<255){end[i]++;bound=end.subarray(0,i+1);break;}const tx=native.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,bound);await tx.rawCommit();}finally{tx.rawCancel();native.close();}}};
  const store=await openApp(),ledger=openSqliteStore(providerDb),catalog=prepare(validator),engine=createEngine({store,catalog,tenant:'tenant'}),keyring=createKeyring({keys:{root:externalKey},activeKey:'root'});let now=clockStart;
  const clock=()=>now,server=await createFakeProviderServer({protocol,store:ledger,secret:'private-provider-secret',afterAccepted}),provider=(operation==='sms.send'?createFakeSmsClient:createFakeEmailClient)({protocol,baseUrl:server.baseUrl}),providers={[protocol]:provider};
  await engine.transact(tx=>tx.set('fixture-authority',authority()));
  const getAuthority=async tx=>prepareAuthority(catalog,await tx.get('fixture-authority')),custody=createCredentialCustody({catalog,keyring,getAuthority,clock,grantMs});
  await engine.transact(tx=>custody.storeRevision(tx,context,{id:'credential',provider:protocol,secret:'private-provider-secret'}));
  let applied=0;const apply=async(tx,{effect,fact})=>{applied++;const id=effect.id.slice('effect:'.length),old=await tx.get(sourceKey('tenant',id));return [{type:'put',record:{...old,data:{status:'delivered',providerId:fact.data.result.providerId}}}];};
  const worker=(id='worker',registry=providers)=>createEffectWorker({engine,custody,providers:registry,workerId:id,clock,leaseMs:10,apply});
  const accept=async(id,options={},revision='1',rate)=>engine.transact(async tx=>{await mutateInTransaction(tx,catalog,'tenant',[{type:'put',record:message(id)}]);return acceptEffectInTransaction(tx,catalog,context,(operation==='sms.send'?{...intent(id,protocol,revision,rate),operation,request:{to:'+919876543210',text:'locally delivered fixture'}}:intent(id,protocol,revision,rate)),{custody,providers,clock});},options);
  const source=id=>engine.read(tx=>tx.get(sourceKey('tenant',id))),snapshot=()=>store.read(tx=>tx.scan());
  t.after(async()=>{await server.close();await store.close();await ledger.close();await cleanup();await rm(dir,{recursive:true,force:true});});
  return {dir,db,providerDb,backend,prefix,clusterFile,openApp,store,ledger,catalog,engine,keyring,custody,provider,providers,server,worker,apply,accept,source,snapshot,clock,advance:delta=>{now+=delta;},applied:()=>applied};
}
async function independentlyCheckStarts(f){
  const facts=await f.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('tenant')})).map(row=>row.value));
  const starts=facts.filter(record=>record.model==='EffectAttempt');
  for(const revision of new Set(starts.map(row=>row.data.revision))){const count=starts.filter(row=>row.data.revision===revision).length;assert.equal((await f.engine.read(tx=>treeAggregate(tx,f.catalog,{tenant:'tenant',index:EFFECT_STARTS,partition:['credential',revision]}))).starts,BigInt(count));}
  await checkStore(f.engine.store,f.catalog,'tenant');return starts;
}

function registerConformance(label,backend){
const fixture=(t,options={})=>prepareFixture(t,{...options,backend});
const witness=(name,fn)=>test(label+' '+name,fn);
witness('D3 domain,intent and original receipt commit together or none; original replay never accepts another effect',async t=>{
  const f=await fixture(t),before=await f.snapshot();
  await assert.rejects(f.accept('abort',{receipt:{id:'operation-abort',digest:'digest'},beforeCommit(){throw Error('abort acceptance');}}),/abort acceptance/);
  assert.deepEqual(await f.snapshot(),before);assert.equal(await f.store.getReceipt('operation-abort'),undefined);assert.equal(f.server.stats().starts,0);
  const receipt={id:'operation-one',digest:fingerprint(intent('one'))},first=await f.accept('one',{receipt});
  const replay=await f.engine.transact(()=>{throw Error('must not rerun');},{receipt});assert.deepEqual(replay,first);
  assert.ok(await f.source('one'));assert.ok(await f.source('effect:one'));assert.equal((await f.store.getReceipt(receipt.id)).result.effectId,'effect:one');
  const uncertain={id:'operation-uncertain',digest:fingerprint(intent('uncertain'))};await assert.rejects(f.accept('uncertain',{receipt:uncertain,afterCommit(){throw Error('lost acceptance acknowledgement');}}),{code:'COMMIT_UNKNOWN'});
  const committed=await f.store.getReceipt(uncertain.id),recovered=await f.engine.transact(()=>{throw Error('original acceptance must not run twice');},{receipt:uncertain});assert.deepEqual(recovered,committed.result);assert.ok(await f.source('uncertain'));assert.equal((await f.source('effect:uncertain')).data.providerOperationId,committed.result.providerOperationId);await independentlyCheckStarts(f);
});

witness('D3 both concrete provider protocols send after durable start and apply once with encrypted custody',async t=>{
  for(const protocol of ['resend','brevo']){
    const f=await fixture(t,{protocol});const send=f.provider.send;
    f.providers[protocol]={...f.provider,send:async args=>{const committed=await f.source('effect:'+protocol);assert.equal(committed.data.state,'started');assert.ok(await f.source(committed.data.attemptId));return send(args);}};
    await f.accept(protocol);const worker=f.worker();await worker.runOne('effect:'+protocol);
    assert.equal((await f.source('effect:'+protocol)).data.state,'applied');assert.equal((await f.source(protocol)).data.status,'delivered');assert.equal(f.applied(),1);
    await worker.runOne('effect:'+protocol);assert.deepEqual(await worker.apply('effect:'+protocol),{applied:false});assert.equal(f.applied(),1);
    const starts=await independentlyCheckStarts(f);assert.equal(starts.length,1);assert.equal(f.server.stats().starts,1);assert.equal((await f.server.ledger())[0].value.deliveries,1);
    const all=await f.snapshot();assert.equal(canonical(all).includes('private-provider-secret'),false);assert.equal(canonical(all).includes('locally delivered fixture'),false);
    const options={getAuthority:async tx=>prepareAuthority(f.catalog,await tx.get('fixture-authority'))};
    const metadata=await readPointAuthorized(f.engine.store,f.catalog,context,{id:'credential'},options);assert.equal(metadata.records.length,1);assert.deepEqual(Object.keys(metadata.records[0].data).sort(),['activeRevision','provider','status']);
    assert.deepEqual((await readPointAuthorized(f.engine.store,f.catalog,context,{id:'effect:'+protocol},options)).records,[]);
    assert.deepEqual((await readPointAuthorized(f.engine.store,f.catalog,context,{id:(await f.source('effect:'+protocol)).data.revisionId},options)).records,[]);
  }
});

witness('D3 stale leased worker cannotstart, repeated exact start charges once and exact quota includes timezero',async t=>{
  const f=await fixture(t,{clockStart:0});await f.accept('one',{},'1',{limit:1,windowMs:100});const a=f.worker('a'),b=f.worker('b'),old=await a.claim('effect:one');f.advance(11);const current=await b.claim('effect:one');
  await assert.rejects(a.start(old),{code:'STALE_LEASE'});assert.equal(f.server.stats().starts,0);
  const started=await b.start(current),replayed=await b.start(current);assert.equal(replayed.replayed,true);assert.equal(Object.hasOwn(replayed,'request'),false);assert.equal(Object.hasOwn(replayed,'credential'),false);assert.equal((await independentlyCheckStarts(f)).length,1);
  const secret=f.custody.open(started.credential);const result=await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret});secret.fill(0);await b.recordResult(current,started.attemptId,result);await b.apply('effect:one');
  await f.accept('two',{},'1',{limit:1,windowMs:100});assert.equal((await b.runOne('effect:two')).error,'RATE_LIMIT');assert.equal(f.server.stats().starts,1);
  f.advance(101);await b.runOne('effect:two');assert.equal(f.server.stats().starts,2);await independentlyCheckStarts(f);
});

witness('D3 accepted with lost response reconciles original provider key using retained revision after expiry/revocation',async t=>{
  for(const protocol of ['resend','brevo']){
    let dropped=false;const f=await fixture(t,{protocol,grantMs:20,afterAccepted:({response})=>{if(!dropped){dropped=true;response.destroy();}}});await f.accept('unknown');const worker=f.worker();await worker.runOne('effect:unknown');
    const accepted=await f.source('effect:unknown');assert.equal(accepted.data.state,'unknown');assert.equal((await f.source('unknown')).data.status,'pending');assert.equal(f.server.stats().starts,1);
    f.advance(50);await f.engine.transact(tx=>f.custody.revoke(tx,context,{id:'credential'}));await worker.reconcile('effect:unknown');
    assert.equal((await f.source('effect:unknown')).data.state,'applied');assert.equal((await f.server.ledger())[0].value.operationId,accepted.data.providerOperationId);assert.equal((await f.server.ledger())[0].value.deliveries,1);assert.equal(f.server.stats().starts,1);assert.equal((await independentlyCheckStarts(f)).length,1);
    await assert.rejects(f.accept('denied'),{code:'FORBIDDEN'});assert.equal(await f.source('denied'),undefined);
  }
});

witness('D3 unknown without lookup never resends and unsupported jobs isolate without blocking unrelated work',async t=>{
  let dropped=false;const f=await fixture(t,{protocol:'brevo',afterAccepted:({response})=>{if(!dropped){dropped=true;response.destroy();}}});await f.accept('unknown');
  const withoutLookup=createEmailProvider({protocol:'brevo',baseUrl:f.server.baseUrl}),worker=f.worker('worker',{brevo:withoutLookup});await worker.runOne('effect:unknown');f.advance(11);await worker.reconcile('effect:unknown');assert.equal(f.server.stats().starts,1);assert.equal((await f.source('effect:unknown')).data.state,'unknown');
  await f.accept('unsupported');const changed=await f.source('effect:unsupported');changed.data.protocolVersion='2';await f.engine.mutate([{type:'put',record:changed}]);await f.accept('good');await f.worker().drain({maxJobs:10});
  assert.equal((await f.source('effect:unsupported')).data.state,'isolated');assert.equal((await f.source('good')).data.status,'delivered');assert.equal(f.server.stats().starts,2);await independentlyCheckStarts(f);
});

witness('D3 authoritative absence retries the same original identity, and stale completion cannot apply a superseded lease',async t=>{
  const f=await fixture(t);await f.accept('missing');const a=f.worker('a'),b=f.worker('b'),token=await a.claim('effect:missing'),started=await a.start(token);f.advance(11);
  const recovery=await b.claim('effect:missing',true);const stale=await a.recordResult(token,started.attemptId,{status:'accepted',providerId:'stale-observation'});assert.equal(stale.current,false);assert.equal((await f.source('missing')).data.status,'pending');
  const observed=await f.provider.lookup({operationId:started.effect.data.providerOperationId,secret:Buffer.from('private-provider-secret')});assert.equal(observed.status,'absent');await b.recordResult(recovery,started.attemptId,observed);await b.runOne('effect:missing');
  const ledger=await f.server.ledger();assert.equal(ledger.length,1);assert.equal(ledger[0].value.operationId,started.effect.data.providerOperationId);assert.equal(ledger[0].value.deliveries,1);assert.equal((await independentlyCheckStarts(f)).length,2);
});

witness('D3 key and credential rotation preserves pinned revision; revocation and request tampering prevent fresh starts',async t=>{
  const f=await fixture(t);await f.accept('old');await f.engine.transact(tx=>f.custody.storeRevision(tx,context,{id:'credential',provider:'resend',secret:'new-unused-secret'}));
  const rotatedKeyring=createKeyring({keys:{root:externalKey,next:Buffer.alloc(32,29)},activeKey:'next'}),getAuthority=async tx=>prepareAuthority(f.catalog,await tx.get('fixture-authority')),rotated=createCredentialCustody({catalog:f.catalog,keyring:rotatedKeyring,getAuthority,clock:f.clock});
  const worker=createEffectWorker({engine:f.engine,custody:rotated,providers:f.providers,clock:f.clock,leaseMs:10,apply:async(tx,{effect,fact})=>[{type:'put',record:{...(await tx.get(sourceKey('tenant',effect.id.slice(7)))),data:{status:'delivered',providerId:fact.data.result.providerId}}}]});
  await worker.runOne('effect:old');assert.equal((await f.source('old')).data.status,'delivered');assert.equal((await independentlyCheckStarts(f))[0].data.revision,'1');
  await f.accept('revoked',{},'1');await f.engine.transact(tx=>f.custody.revoke(tx,context,{id:'credential',revision:'1'}));assert.equal((await worker.runOne('effect:revoked')).error,'FORBIDDEN');assert.equal(f.server.stats().starts,1);
  await f.accept('tampered',{},'2');const tampered=await f.source('effect:tampered');tampered.data.requestDigest='changed';await f.engine.mutate([{type:'put',record:tampered}]);assert.equal((await worker.runOne('effect:tampered')).error,'CORRUPT_STATE');assert.equal(f.server.stats().starts,1);
});

witness('D3 accepted facts survive failed atomic application and retry without another provider start',async t=>{
  let reject=false;const f=await fixture(t,{validator:(_summary,{scope})=>!(reject&&scope.partition[0]==='delivered')});await f.accept('apply');reject=true;
  await assert.rejects(f.worker().runOne('effect:apply'),{code:'VALIDATION_FAILED'});assert.equal((await f.source('effect:apply')).data.state,'succeeded');assert.equal((await f.source('apply')).data.status,'pending');assert.equal(f.server.stats().starts,1);
  reject=false;await f.worker().apply('effect:apply');assert.equal((await f.source('apply')).data.status,'delivered');assert.equal(f.server.stats().starts,1);await independentlyCheckStarts(f);
});

async function subprocess(t,filename,environment){
  const {fork}=await import('node:child_process');const child=fork(new URL(filename,import.meta.url),[],{env:{...process.env,...environment},stdio:['ignore','ignore','pipe','ipc']}),messages=[],waiters=[];let stderr='';
  child.stderr.on('data',data=>{stderr+=data.toString();});
  child.on('message',message=>{messages.push(message);for(const waiter of [...waiters])if(waiter.phase===message.phase){waiters.splice(waiters.indexOf(waiter),1);waiter.resolve(message);}});
  const ended=new Promise(resolve=>child.once('exit',(code,signal)=>resolve({code,signal,stderr})));
  t.after(async()=>{if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await ended;}});
  const phase=async name=>{const prior=messages.find(message=>message.phase===name);if(prior)return prior;return Promise.race([new Promise(resolve=>waiters.push({phase:name,resolve})),ended.then(value=>{throw Error('Subprocess exited before '+name+': '+value.stderr);}),new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error('Subprocess phase timeout')),5000);timer.unref();})]);};
  return {child,phase,ended};
}

witness('D3 SIGKILL after provider acceptance before fact reopens original start and reconciles without duplicate delivery',async t=>{
  const f=await fixture(t,{protocol:'brevo'});await f.accept('crash');
  const child=await subprocess(t,'./effects-crash-worker.mjs',{APP_DATABASE:f.db,APP_ADAPTER:f.backend,APP_PREFIX:f.prefix,KV_BASE_FDB_CLUSTER_FILE:f.clusterFile??'',PROVIDER_URL:f.server.baseUrl,CLOCK_VALUE:String(f.clock()),CUSTODY_KEY:externalKey.toString('base64')});
  await child.phase('accepted');child.child.kill('SIGKILL');assert.equal((await child.ended).signal,'SIGKILL');
  assert.equal((await f.source('effect:crash')).data.state,'started');assert.equal((await f.source('crash')).data.status,'pending');
  f.advance(11);const reopened=await f.openApp(),engine=createEngine({store:reopened,catalog:f.catalog,tenant:'tenant'});t.after(()=>reopened.close());
  const custody=createCredentialCustody({catalog:f.catalog,keyring:f.keyring,clock:f.clock,getAuthority:async tx=>prepareAuthority(f.catalog,await tx.get('fixture-authority'))});
  const worker=createEffectWorker({engine,custody,providers:f.providers,clock:f.clock,leaseMs:10,workerId:'recovered',apply:async(tx,{fact})=>[{type:'put',record:{...(await tx.get(sourceKey('tenant','crash'))),data:{status:'delivered',providerId:fact.data.result.providerId}}}]});
  await worker.reconcile('effect:crash');assert.equal((await f.source('crash')).data.status,'delivered');assert.equal(f.server.stats().starts,1);assert.equal((await f.server.ledger())[0].value.deliveries,1);await independentlyCheckStarts(f);
});

witness('D3 provider SIGKILL after durable acceptance returns unknown and fresh provider process retains original lookup',async t=>{
  const f=await fixture(t,{protocol:'brevo'});await f.accept('provider-crash');
  const crashed=await subprocess(t,'./effects-provider-process.mjs',{PROVIDER_DATABASE:f.providerDb,CRASH_AFTER_ACCEPT:'1'}),ready=await crashed.phase('ready');
  const client=createFakeEmailClient({protocol:'brevo',baseUrl:ready.url}),worker=f.worker('worker',{brevo:client});await worker.runOne('effect:provider-crash');await crashed.phase('accepted');assert.equal((await crashed.ended).signal,'SIGKILL');
  assert.equal((await f.source('effect:provider-crash')).data.state,'unknown');
  const restored=await subprocess(t,'./effects-provider-process.mjs',{PROVIDER_DATABASE:f.providerDb,CRASH_AFTER_ACCEPT:'0'}),after=await restored.phase('ready');
  f.advance(11);await f.worker('recovery',{brevo:createFakeEmailClient({protocol:'brevo',baseUrl:after.url})}).reconcile('effect:provider-crash');
  assert.equal((await f.source('provider-crash')).data.status,'delivered');assert.equal((await f.server.ledger())[0].value.deliveries,1);await independentlyCheckStarts(f);
  restored.child.send('close');assert.equal((await restored.ended).code,0);
});

witness('D3 external provider fence rejects a delayed obsolete non-idempotent start; plain absence never permits replay',async t=>{
  const f=await fixture(t,{protocol:'brevo'});await f.accept('late');const a=f.worker('old'),token=await a.claim('effect:late'),started=await a.start(token);f.advance(11);
  const unsafe=createEmailProvider({protocol:'brevo',baseUrl:f.server.baseUrl,lookup:async()=>({status:'absent'})});await f.worker('unsafe',{brevo:unsafe}).reconcile('effect:late');
  assert.equal((await f.source('effect:late')).data.state,'unknown');assert.equal(f.server.stats().starts,0);
  f.advance(11);const b=f.worker('new');await b.reconcile('effect:late');assert.equal((await f.source('effect:late')).data.state,'pending');await b.runOne('effect:late');
  const secret=f.custody.open(started.credential),late=await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret,fence:token.epoch});secret.fill(0);
  assert.equal(late.status,'rejected');assert.equal((await a.recordResult(token,started.attemptId,late)).current,false);
  assert.equal((await f.source('late')).data.status,'delivered');assert.equal((await f.server.ledger())[0].value.deliveries,1);assert.equal((await independentlyCheckStarts(f)).length,2);
});

witness('D3 Use must be explicit, grant expiry prevents start and another tenant cannot use custody',async t=>{
  const f=await fixture(t,{grantMs:10}),before=await f.snapshot();f.providers.resend=createEmailProvider({protocol:'resend',baseUrl:f.server.baseUrl,maxRequestBytes:1});
  await assert.rejects(f.accept('oversized'),{code:'UNSUPPORTED_EFFECT'});assert.deepEqual(await f.snapshot(),before);assert.equal(f.server.stats().starts,0);f.providers.resend=f.provider;
  await f.accept('expires');f.advance(10);assert.equal((await f.worker().runOne('effect:expires')).error,'FORBIDDEN');assert.equal(f.server.stats().starts,0);assert.equal((await independentlyCheckStarts(f)).length,0);
  await f.engine.transact(async tx=>{const policy=await tx.get('fixture-authority');policy.principals.find(node=>node.id==='alice').grants.resources.Credential=['create','read','update'];await tx.set('fixture-authority',policy);});
  await assert.rejects(f.accept('use-denied'),{code:'FORBIDDEN'});assert.equal(await f.source('use-denied'),undefined);
  await assert.rejects(f.engine.transact(tx=>f.custody.storeRevision(tx,{tenant:'other',actor:'alice'},{id:'cross',provider:'resend',secret:'hidden'})),{code:'FORBIDDEN'});
});


witness('D3 SMS uses the same catalog encrypted request and custody digest, and lost acceptance reconciles once',async t=>{
  let dropped=false;const f=await fixture(t,{protocol:'brevo',operation:'sms.send',afterAccepted:({response})=>{if(!dropped){dropped=true;response.destroy();}}});
  await f.accept('sms');const effect=await f.source('effect:sms'),request={to:'+919876543210',text:'locally delivered fixture'};
  const scope={tenant:'tenant',purpose:'effect-request-digest',subject:effect.id,revision:'1'};
  const derived=Buffer.from(hkdfSync('sha256',externalKey,Buffer.from('kv-base custody v1'),Buffer.from(canonical([scope.tenant,scope.purpose])),32));
  const expected='root:'+createHmac('sha256',derived).update(canonical([scope,['brevo','1','sms.send',request]])).digest('base64url');derived.fill(0);
  assert.equal(effect.data.requestDigest,expected);assert.notEqual(effect.data.requestDigest,fingerprint(['brevo','1','sms.send',request]));
  assert.notEqual(f.custody.requestDigest(context,{...intent('sms','brevo'),operation:'sms.send',request,id:'another-effect'}),expected);
  await f.worker().runOne(effect.id);assert.equal((await f.source(effect.id)).data.state,'unknown');f.advance(11);await f.worker('recovered').reconcile(effect.id);
  const ledger=await f.server.ledger();assert.equal(ledger.length,1);assert.equal(ledger[0].value.operation,'sms.send');assert.deepEqual(ledger[0].value.request,request);assert.equal(ledger[0].value.deliveries,1);
  assert.equal((await f.source('sms')).data.status,'delivered');assert.equal(canonical(await f.snapshot()).includes(request.text),false);assert.equal(canonical(await f.snapshot()).includes(request.to),false);
  const before=await f.snapshot();await assert.rejects(f.engine.transact(tx=>acceptEffectInTransaction(tx,f.catalog,context,{...intent('invalid','brevo'),operation:'sms.send',request:{to:'12345678',text:'invalid'}},{custody:f.custody,providers:f.providers,clock:f.clock})),{code:'UNSUPPORTED_EFFECT'});assert.deepEqual(await f.snapshot(),before);await independentlyCheckStarts(f);
});

witness('D3 cancellation fences unstarted leases and retains admitted evidence without retrying a proven absent send',async t=>{
  const f=await fixture(t,{protocol:'brevo'}),a=f.worker('a'),b=f.worker('b');await f.accept('cancelled');const token=await a.claim('effect:cancelled');
  assert.equal((await b.cancel('effect:cancelled')).cancelled,true);assert.deepEqual(await b.cancel('effect:cancelled'),{cancelled:true});await assert.rejects(a.start(token),{code:'STALE_LEASE'});assert.equal((await independentlyCheckStarts(f)).length,0);
  await f.accept('admitted');const next=await a.claim('effect:admitted'),started=await a.start(next),identity=started.effect.data.providerOperationId;
  assert.deepEqual(await b.cancel('effect:admitted'),{cancelled:false,inFlight:true});f.advance(11);await b.reconcile('effect:admitted');
  const cancelled=await f.source('effect:admitted');assert.equal(cancelled.data.state,'cancelled');assert.equal(cancelled.data.providerOperationId,identity);assert.equal(cancelled.data.attemptId,started.attemptId);assert.ok(cancelled.data.lastFactId);
  await b.drain({maxJobs:5});assert.equal(f.server.stats().starts,0);assert.equal((await independentlyCheckStarts(f)).length,1);
  await f.accept('delivered');const current=await a.claim('effect:delivered'),original=await a.start(current),secret=f.custody.open(original.credential),result=await f.provider.send({operationId:original.effect.data.providerOperationId,request:original.request,secret,fence:current.epoch});secret.fill(0);
  assert.equal((await b.cancel('effect:delivered')).inFlight,true);await a.recordResult(current,original.attemptId,result);await a.apply('effect:delivered');assert.equal((await f.source('effect:delivered')).data.cancelRequested,true);assert.equal((await f.server.ledger()).find(row=>row.value.operationId===original.effect.data.providerOperationId).value.deliveries,1);assert.equal((await b.cancel('effect:delivered')).alreadyApplied,true);await independentlyCheckStarts(f);
});

witness('D3 process death at claim,start,fact and application boundaries recovers original identity independently',async t=>{
  for(const phase of ['claimed','started','fact','applied']){
    const f=await fixture(t,{protocol:'brevo'});await f.accept('crash');const original=(await f.source('effect:crash')).data.providerOperationId;
    const child=await subprocess(t,'./effects-crash-worker.mjs',{APP_DATABASE:f.db,APP_ADAPTER:f.backend,APP_PREFIX:f.prefix,KV_BASE_FDB_CLUSTER_FILE:f.clusterFile??'',PROVIDER_URL:f.server.baseUrl,CLOCK_VALUE:String(f.clock()),CUSTODY_KEY:externalKey.toString('base64'),KILL_PHASE:phase});
    await child.phase(phase);child.child.kill('SIGKILL');assert.equal((await child.ended).signal,'SIGKILL');f.advance(11);
    const reopened=await f.openApp(),engine=createEngine({store:reopened,catalog:f.catalog,tenant:'tenant'});t.after(()=>reopened.close());
    const custody=createCredentialCustody({catalog:f.catalog,keyring:f.keyring,clock:f.clock,getAuthority:async tx=>prepareAuthority(f.catalog,await tx.get('fixture-authority'))});
    const worker=createEffectWorker({engine,custody,providers:f.providers,clock:f.clock,leaseMs:10,workerId:'recovered',apply:f.apply});
    if(phase==='started')await worker.reconcile('effect:crash');await worker.drain({maxJobs:4});
    assert.equal((await f.source('crash')).data.status,'delivered');assert.equal((await f.source('effect:crash')).data.state,'applied');assert.equal((await f.source('effect:crash')).data.providerOperationId,original);
    const ledger=await f.server.ledger();assert.equal(ledger.length,1);assert.equal(ledger[0].value.operationId,original);assert.equal(ledger[0].value.deliveries,1);assert.equal(f.server.stats().starts,1);assert.equal((await independentlyCheckStarts(f)).length,phase==='started'?2:1);
  }
});


witness('D3 lost start acknowledgement preserves the admitted proof and requires original-key reconciliation before send',async t=>{
  const f=await fixture(t,{protocol:'brevo'});await f.accept('uncertain-start');let transactions=0;
  const engine={...f.engine,transact:(callback,options)=>f.engine.transact(callback,{...options,...(++transactions===2?{afterCommit(){throw Error('lost start acknowledgement');}}:{})})};
  const worker=createEffectWorker({engine,custody:f.custody,providers:f.providers,clock:f.clock,leaseMs:10,workerId:'uncertain',apply:f.apply}),outcome=await worker.runOne('effect:uncertain-start');
  assert.equal(outcome.error,'COMMIT_UNKNOWN');assert.equal(outcome.recoveryRequired,true);const admitted=await f.source('effect:uncertain-start');assert.equal(admitted.data.state,'started');assert.ok(await f.source(admitted.data.attemptId));assert.equal(f.server.stats().starts,0);assert.equal((await independentlyCheckStarts(f)).length,1);
  await worker.runOne(admitted.id);assert.equal(f.server.stats().starts,0);f.advance(11);const recovered=f.worker('recovered');await recovered.reconcile(admitted.id);await recovered.runOne(admitted.id);
  assert.equal((await f.source('uncertain-start')).data.status,'delivered');assert.equal((await f.source(admitted.id)).data.providerOperationId,admitted.data.providerOperationId);assert.equal((await independentlyCheckStarts(f)).length,2);assert.equal(f.server.stats().starts,1);assert.equal((await f.server.ledger())[0].value.deliveries,1);
});

}
registerConformance('optional SQLite','sqlite');
if(process.env.KV_BASE_FDB_CLUSTER_FILE)registerConformance('primary FoundationDB','foundationdb');
else test('primary FoundationDB effects require a selected real cluster',{skip:'KV_BASE_FDB_CLUSTER_FILE required'},()=>{});

const primarySkip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'KV_BASE_FDB_CLUSTER_FILE required';
function barrier(){let enter,release;return{entered:new Promise(resolve=>{enter=resolve;}),blocked:new Promise(resolve=>{release=resolve;}),enter:()=>enter(),release:()=>release()};}
function stagedWorker(f,gate,id='staged'){
  let calls=0;const engine={...f.engine,transact:(callback,options)=>f.engine.transact(async tx=>{calls++;return callback(tx);},{...options,beforeCommit:async()=>{gate.enter();await gate.blocked;}})};
  return{worker:createEffectWorker({engine,custody:f.custody,providers:f.providers,workerId:id,clock:f.clock,leaseMs:10,apply:f.apply}),calls:()=>calls};
}

test('primary FoundationDB D3 overlapping claims publish one epoch and never retry callback',{skip:primarySkip},async t=>{
  const f=await prepareFixture(t,{backend:'foundationdb'});await f.accept('claim');const gate=barrier(),staged=stagedWorker(f,gate),loser=staged.worker.claim('effect:claim'),failed=assert.rejects(loser,{code:'STORAGE_CONFLICT'});await gate.entered;
  const winner=await f.worker('winner').claim('effect:claim');assert.equal(winner.claimed,true);gate.release();await failed;assert.equal(staged.calls(),1);
  assert.equal((await f.source('effect:claim')).data.leaseOwner,'winner');assert.equal((await f.source('effect:claim')).data.epoch,'1');assert.equal((await f.worker('loser').claim('effect:claim')).claimed,false);assert.equal((await independentlyCheckStarts(f)).length,0);
});

test('primary FoundationDB D3 overlapping starts protect exact quota and preserve only admitted original proof',{skip:primarySkip},async t=>{
  const f=await prepareFixture(t,{backend:'foundationdb'});await f.accept('one',{},'1',{limit:1,windowMs:100});await f.accept('two',{},'1',{limit:1,windowMs:100});
  const gate=barrier(),staged=stagedWorker(f,gate),a=await f.worker('staged').claim('effect:one'),b=await f.worker('winner').claim('effect:two'),loser=staged.worker.start(a),failed=assert.rejects(loser,{code:'STORAGE_CONFLICT'});await gate.entered;
  const admitted=await f.worker('winner').start(b);gate.release();await failed;assert.equal(staged.calls(),1);assert.equal((await independentlyCheckStarts(f)).length,1);assert.equal((await f.source('effect:one')).data.attemptId,undefined);assert.equal((await f.source('effect:two')).data.attemptId,admitted.attemptId);
  await assert.rejects(f.worker('staged').start(a),{code:'RATE_LIMIT'});assert.equal(f.server.stats().starts,0);assert.equal((await independentlyCheckStarts(f)).length,1);
});

test('primary FoundationDB D3 concurrent policy or credential revocation blocks staged start and charge',{skip:primarySkip},async t=>{
  for(const target of ['policy','revision']){
    const f=await prepareFixture(t,{backend:'foundationdb'});await f.accept(target);const gate=barrier(),staged=stagedWorker(f,gate),token=await f.worker('staged').claim('effect:'+target),loser=staged.worker.start(token),failed=assert.rejects(loser,{code:'STORAGE_CONFLICT'});await gate.entered;
    if(target==='policy'){const graph=authority();graph.principals.find(row=>row.id==='alice').grants.resources.Credential=['create','read','update'];await f.engine.transact(tx=>tx.set('fixture-authority',graph));}
    else await f.engine.transact(tx=>f.custody.revoke(tx,context,{id:'credential'}));
    gate.release();await failed;assert.equal(staged.calls(),1);assert.equal((await independentlyCheckStarts(f)).length,0);await assert.rejects(f.worker('staged').start(token),{code:'FORBIDDEN'});assert.equal(f.server.stats().starts,0);
  }
});

test('primary FoundationDB D3 conflicting old completion cannot replace current lookup or reapply the domain',{skip:primarySkip},async t=>{
  const f=await prepareFixture(t,{backend:'foundationdb'}),old=f.worker('staged');await f.accept('completion');const token=await old.claim('effect:completion'),started=await old.start(token),secret=f.custody.open(started.credential),result=await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret,fence:token.epoch});secret.fill(0);
  const gate=barrier(),staged=stagedWorker(f,gate),loser=staged.worker.recordResult(token,started.attemptId,result),failed=assert.rejects(loser,{code:'STORAGE_CONFLICT'});await gate.entered;f.advance(11);await f.worker('winner').reconcile('effect:completion');gate.release();await failed;
  assert.equal(staged.calls(),1);const committed=await f.source('effect:completion');assert.equal(committed.data.state,'applied');const before=await f.source('completion');assert.equal((await old.recordResult(token,started.attemptId,result)).current,false);assert.deepEqual(await old.apply(committed.id),{applied:false});assert.deepEqual(await f.source('completion'),before);
  assert.equal(f.server.stats().starts,1);assert.equal((await f.server.ledger())[0].value.deliveries,1);assert.equal((await independentlyCheckStarts(f)).length,1);
});

test('primary FoundationDB D3 concurrent application publishes domain and effect once while retaining accepted fact',{skip:primarySkip},async t=>{
  const f=await prepareFixture(t,{backend:'foundationdb'}),worker=f.worker('original');await f.accept('application');const token=await worker.claim('effect:application'),started=await worker.start(token),secret=f.custody.open(started.credential),result=await f.provider.send({operationId:started.effect.data.providerOperationId,request:started.request,secret,fence:token.epoch});secret.fill(0);await worker.recordResult(token,started.attemptId,result);
  const fact=(await f.source('effect:application')).data.lastFactId,gate=barrier(),staged=stagedWorker(f,gate),loser=staged.worker.apply('effect:application'),failed=assert.rejects(loser,{code:'STORAGE_CONFLICT'});await gate.entered;
  assert.deepEqual(await f.worker('winner').apply('effect:application'),{applied:true});gate.release();await failed;assert.equal(staged.calls(),1);assert.equal((await f.source('effect:application')).data.lastFactId,fact);assert.equal((await f.source('application')).data.status,'delivered');assert.deepEqual(await worker.apply('effect:application'),{applied:false});
  assert.equal(f.server.stats().starts,1);assert.equal((await f.server.ledger())[0].value.deliveries,1);assert.equal((await independentlyCheckStarts(f)).length,1);assert.ok(await f.source(fact));
});
