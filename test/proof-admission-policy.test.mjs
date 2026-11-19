import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {prepareCatalog} from '../src/catalog.mjs';
import {openSqliteStore} from '../src/sqlite.mjs';
import {openFoundationDbStore} from '../src/foundationdb.mjs';
import {createEngine} from '../src/lifecycle.mjs';
import {createKeyring} from '../src/keyring.mjs';
import {createIdentity} from '../src/identity.mjs';
import {createIdentityProofs} from '../src/identity-proofs.mjs';
import {createCredentialCustody,createEffectWorker} from '../src/effects.mjs';
import {createFakeProviderServer,createFakeEmailClient} from '../src/providers.mjs';
import {canonical} from '../src/values.mjs';
import {sourcePrefix} from '../src/keys.mjs';
import {proofDefinition} from './fixtures/proof-domain.mjs';

const clusterFile=process.env.KV_BASE_FDB_CLUSTER_FILE,rateKey=`identity-proof/v1/${canonical('t')}/rate/${canonical('reader')}`;
async function fixture(t,backend){
  const f={directory:await mkdtemp(join(tmpdir(),'kv-base-proof-policy-')),prefix:'proof-policy-'+randomUUID(),time:1000,fence:'incarnation-A',catalog:prepareCatalog(proofDefinition())},ledger=openSqliteStore(join(f.directory,'provider.sqlite'));
  f.server=await createFakeProviderServer({protocol:'resend',store:ledger,secret:'proof-policy-secret'});f.providers={email:createFakeEmailClient({protocol:'resend',baseUrl:f.server.baseUrl})};f.keyring=createKeyring({keys:{root:Buffer.alloc(32,23)},activeKey:'root'});const clock=()=>f.time,deliveryContext={tenant:'t',actor:'admin'},contacts={reader:{email:'reader@example.test'}},channels={email:{provider:'email',credentialId:'credential',credentialRevision:'1',from:'signin@example.test'}};
  f.open=async()=>{f.store=backend==='foundationdb'?await openFoundationDbStore({clusterFile,prefix:f.prefix}):openSqliteStore(join(f.directory,'app.sqlite'));f.engine=createEngine({store:f.store,catalog:f.catalog,tenant:'t'});f.identity=createIdentity({engine:f.engine,keyring:f.keyring,getRestoreFence:()=>f.fence,clock,passwordPolicy:{N:1024}});f.custody=createCredentialCustody({catalog:f.catalog,keyring:f.keyring,getAuthority:tx=>f.identity.getAuthorityInTransaction(tx),getRestoreFence:()=>f.fence,clock});};await f.open();
  const principal=(id,parents,resources={},admin=false)=>({id,kind:id==='t'?'tenant':'user',parents,active:true,grants:{admin,resources}});await f.identity.initialize({policy:{tenant:'t',principals:[principal('t',[]),principal('admin',['t'],{Credential:['use']},true),principal('reader',['t'])]},credentials:[{actor:'admin',password:'a long admission admin password'},{actor:'reader',password:'a long admission reader password'}]});await f.engine.transact(tx=>f.custody.storeRevision(tx,deliveryContext,{id:'credential',provider:'email',secret:'proof-policy-secret'}));
  f.proofs=policy=>createIdentityProofs({engine:f.engine,identity:f.identity,keyring:f.keyring,getRestoreFence:()=>f.fence,custody:f.custody,providers:f.providers,deliveryContext,contacts,channels,clock,proofPolicy:{expiryMs:1000,maxStarts:4,startWindowMs:100,...policy}});
  f.rate=()=>f.engine.read(tx=>tx.get(rateKey));f.rows=()=>f.engine.read(tx=>tx.scan());f.sourceRows=()=>f.engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}));f.begin=(proofs,operationId)=>proofs.begin({actor:'reader',channel:'email',operationId});f.reopen=async()=>{await f.store.close();await f.open();};
  f.proof=proofId=>f.engine.read(tx=>tx.get(`identity-proof/v1/${canonical('t')}/proof/${canonical(proofId)}`));
  f.deliver=async(proofs,begun)=>{const proof=await f.proof(begun.proofId);await createEffectWorker({engine:f.engine,custody:f.custody,providers:f.providers,clock,leaseMs:10,apply:proofs.applyDelivery}).runOne(proof.effectId);const entry=(await f.server.ledger()).find(row=>row.value.operationId===proof.providerOperationId);assert.ok(entry,'delivery is independently recorded by the provider');const match=/code is (\d{8})\./.exec(entry.value.request.text);assert.ok(match);return match[1];};
  t.after(async()=>{await f.server.close();await f.store.close();await ledger.close();if(backend==='foundationdb'){const{default:fdb}=await import('foundationdb'),db=fdb.open(clusterFile),root=fdb.tuple.pack(['kv-base',f.prefix]),upper=Buffer.from(root);let end=upper;for(let i=upper.length-1;i>=0;i--)if(upper[i]<255){upper[i]++;end=upper.subarray(0,i+1);break;}const tx=db.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,end);await tx.rawCommit();}finally{tx.rawCancel();db.close();}}await rm(f.directory,{recursive:true,force:true});});return f;
}

for(const [label,backend,skip] of [['optional SQLite','sqlite',false],['primary FoundationDB','foundationdb',clusterFile?false:'Selected real FDB cluster required']]){
  test(label+' proof admission: a lowered attempt cap denies new completion and receipt recovery while preserving valid historical state',{skip},async t=>{
    const f=await fixture(t,backend),old=f.proofs({maxAttempts:5}),open=await f.begin(old,'historic-open'),openCode=await f.deliver(old,open),consumed=await f.begin(old,'historic-consumed'),consumedCode=await f.deliver(old,consumed);
    const complete=(proofs,begun,code,operationId)=>proofs.complete({proofId:begun.proofId,code,operationId}),openWrong=['00000000','11111111'].find(code=>code!==openCode),guesses=['00000000','11111111','22222222','33333333'].filter(code=>code!==consumedCode);
    for(let n=0;n<3;n++)await assert.rejects(complete(old,open,openWrong,'historic-wrong-'+n),{code:'UNAUTHENTICATED'});
    const issued=await complete(old,consumed,consumedCode,'historic-success');for(const guess of guesses.slice(0,3))await assert.rejects(complete(old,consumed,guess,'historic-success'),{code:'RECEIPT_MISMATCH'});
    assert.equal((await f.proof(open.proofId)).attempts,3);assert.equal((await f.proof(open.proofId)).state,'open');assert.equal((await f.proof(consumed.proofId)).attempts,3);assert.equal((await f.proof(consumed.proofId)).state,'consumed');assert.equal((await f.identity.read(issued.token,(_tx,context)=>context)).actor,'reader');
    await f.reopen();const lower=f.proofs({maxAttempts:1}),before=await f.rows();assert.deepEqual(await f.begin(lower,'historic-open'),open);
    for(const [begun,code,operationId] of [[open,openCode,'new-correct'],[open,openWrong,'new-wrong'],[consumed,consumedCode,'historic-success'],[consumed,guesses[3]??'99999999','historic-success']])await assert.rejects(complete(lower,begun,code,operationId),{code:'UNAUTHENTICATED'});
    assert.deepEqual(await f.rows(),before);assert.equal(f.server.stats().starts,2);assert.equal((await f.rows()).filter(row=>row.key.includes('/session/')).length,1);await f.engine.check();
  });

  test(label+' proof admission: malformed persisted attempt counts still reject before completion or receipt allocation',{skip},async t=>{
    const f=await fixture(t,backend),proofs=f.proofs(),begun=await f.begin(proofs,'attempt-shape'),code=await f.deliver(proofs,begun),key=`identity-proof/v1/${canonical('t')}/proof/${canonical(begun.proofId)}`,valid=await f.proof(begun.proofId);let n=0;
    for(const attempts of [-1,1.5,Number.MAX_SAFE_INTEGER+1,'3',null]){await f.engine.transact(tx=>tx.set(key,{...valid,attempts}));const before=await f.rows();await assert.rejects(proofs.complete({proofId:begun.proofId,code,operationId:'malformed-attempt-'+n++}),{code:'CORRUPT_PROOF'});assert.deepEqual(await f.rows(),before);}
    await f.engine.transact(tx=>tx.set(key,valid));assert.equal(f.server.stats().starts,1);await f.engine.check();
  });

  test(label+' proof admission: lowering the current cap preserves legitimate historical counts and expiry resets before admission',{skip},async t=>{
    const f=await fixture(t,backend),original=f.proofs();for(let n=0;n<4;n++)await f.begin(original,'original-'+n);assert.deepEqual(await f.rate(),{fence:'incarnation-A',windowStart:1000,count:4});await f.reopen();const lower=f.proofs({maxStarts:1}),before=await f.rows();
    await assert.rejects(f.begin(lower,'lower-active'),{code:'AUTH_WORK_LIMIT'});assert.deepEqual(await f.rows(),before);f.time=1100;const begun=await f.begin(lower,'lower-expired');assert.ok(begun.proofId);assert.deepEqual(await f.rate(),{fence:'incarnation-A',windowStart:1100,count:1});assert.equal((await f.sourceRows()).filter(row=>row.value.model==='Effect').length,5);assert.equal(f.server.stats().starts,0);await f.engine.check();
  });

  test(label+' proof admission: a fresh incarnation resets a valid counter above the newly selected cap',{skip},async t=>{
    const f=await fixture(t,backend),old=f.proofs();for(let n=0;n<4;n++)await f.begin(old,'old-'+n);f.fence='incarnation-B';const begun=await f.begin(f.proofs({maxStarts:1}),'new-incarnation');assert.ok(begun.proofId);assert.deepEqual(await f.rate(),{fence:'incarnation-B',windowStart:1000,count:1});assert.equal((await f.sourceRows()).filter(row=>row.value.model==='Effect').length,5);assert.equal(f.server.stats().starts,0);await f.engine.check();
  });

  test(label+' proof admission: malformed durable counter shape rejects without allocating proof intent or receipt',{skip},async t=>{
    const f=await fixture(t,backend),proofs=f.proofs({maxStarts:1});let n=0;for(const bad of [{fence:3,windowStart:1000,count:1},{fence:'',windowStart:1000,count:1},{fence:'incarnation-A',windowStart:'1000',count:1},{fence:'incarnation-A',windowStart:-1,count:1},{fence:'incarnation-A',windowStart:1000,count:'1'},{fence:'incarnation-A',windowStart:1000,count:-1},{fence:'incarnation-A',windowStart:1000,count:1,extra:true}]){
      await f.engine.transact(tx=>tx.set(rateKey,bad));const before=await f.rows(),operationId='bad-'+n++;await assert.rejects(f.begin(proofs,operationId),{code:'CORRUPT_PROOF'});assert.deepEqual(await f.rows(),before);const operation=`identity-proof/v1/${canonical('t')}/operation/${canonical(['begin','reader',operationId])}`;assert.equal(await f.engine.getReceipt(operation),undefined);
    }assert.equal((await f.sourceRows()).filter(row=>row.value.model==='Effect').length,0);assert.equal(f.server.stats().starts,0);await f.engine.check();
  });
}
