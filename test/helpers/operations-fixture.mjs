import {mkdtemp,rm,writeFile,readFile,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {AsyncLocalStorage} from 'node:async_hooks';
import {prepareCatalog} from '../../src/catalog.mjs';
import {createEngine} from '../../src/lifecycle.mjs';
import {createKeyring} from '../../src/keyring.mjs';
import {createIdentity} from '../../src/identity.mjs';
import {openObjectStore} from '../../src/objects.mjs';
import {openFoundationDbStore} from '../../src/foundationdb.mjs';
import {effectModels,createCredentialCustody,createEffectWorker,acceptEffectInTransaction} from '../../src/effects.mjs';
import {createFakeProviderServer,createFakeEmailClient} from '../../src/providers.mjs';
import {sourceKey} from '../../src/keys.mjs';
import {writeBackup,prepareRestoration} from '../../src/operations.mjs';
export const password='a long backup actor password';
export const sha256=value=>createHash('sha256').update(value).digest('hex');
export async function* chunks(bytes){yield bytes;}
export async function collect(stream){const rows=[];for await(const row of stream)rows.push(row);return Buffer.concat(rows);}
function fixtureHost(runtime){
  const contexts=new AsyncLocalStorage(),work=new Set();let stopped=false,closed=false;
  const settle=async()=>{while(work.size)await Promise.allSettled([...work]);};
  function admit(callback,maintenance=false){
    const current=contexts.getStore();
    if(current==='maintenance'&&!maintenance||stopped&&current!=='maintenance')return Promise.reject(Object.assign(Error('Fixture host admission stopped'),{code:'SERVICE_CLOSED'}));
    const task=Promise.resolve().then(()=>contexts.run(current??'work',callback));if(current!=='maintenance'){work.add(task);task.then(()=>work.delete(task),()=>work.delete(task));}return task;
  }
  const guard=(target,maintenance=[])=>Object.freeze(Object.fromEntries(Object.entries(target).map(([key,value])=>[key,typeof value==='function'?(...args)=>admit(()=>value.apply(target,args),maintenance.includes(key)):value])));
  const close=async()=>{stopped=true;await settle();if(!closed){closed=true;await runtime.storage.close();}};
  return Object.freeze({...runtime,engine:guard(runtime.engine,['check','read','inspect','getReceipt']),identity:guard(runtime.identity),objects:guard(runtime.objects,['checkFiles','inspectFiles','reconcile']),worker:(...args)=>guard(runtime.worker(...args)),close,quiesce:async callback=>{if(stopped||contexts.getStore())throw Object.assign(Error('One offline fixture operation required'),{code:'SERVICE_CLOSED'});stopped=true;await settle();try{return await contexts.run('maintenance',callback);}finally{await close();}}});
}
const schema=properties=>({type:'object',properties,additionalProperties:false,required:Object.keys(properties)});
export function definition(){return{name:'complete-backup-witness',version:'1',models:{...effectModels(),Object:{access:{orders:{}},schema:schema({state:{enum:['pending','ready']},bytes:{type:'integer',minimum:0,maximum:16777216},sha256:{type:'string',pattern:'^[a-f0-9]{64}$'}})},Message:{access:{orders:{}},schema:schema({state:{enum:['pending','delivered']}})}}};}
export function graph(){const n=(id,kind,parents,resources={},admin=false)=>({id,kind,parents,active:true,grants:{admin,resources}});return{tenant:'t',principals:[n('t','tenant',[]),n('admin','user',['t'],{Credential:['use']},true),n('visible','group',['admin'],{Object:['create','update','read','delete'],Message:['create','update','read']}),n('reader','user',['visible'])]};}
export async function fixture(t,{protocol='brevo'}={}){
  const f={directory:await mkdtemp(join(tmpdir(),'kv-base-backup-')),prefixes:[],stores:[],time:1000,fence:'incarnation-A',catalog:prepareCatalog(definition()),clusterFile:process.env.KV_BASE_FDB_CLUSTER_FILE};
  const sourceDirectory=new URL('../../src/',import.meta.url),names=(await readdir(sourceDirectory)).sort(),sourceRows=await Promise.all(names.filter(name=>name.endsWith('.mjs')).map(async name=>[name,sha256(await readFile(new URL(name,sourceDirectory)))]));
  f.sourceIdentity={revision:'D5-independent-witness',sourceHash:sha256(JSON.stringify(sourceRows)),lockHash:sha256(await readFile(new URL('../../package-lock.json',import.meta.url))),catalogIdentity:f.catalog.identity};
  f.keyring=createKeyring({keys:{old:Buffer.alloc(32,7)},activeKey:'old'});
  f.openStore=async()=>{const prefix='backup-'+randomUUID(),store=await openFoundationDbStore({clusterFile:f.clusterFile,prefix});f.prefixes.push(prefix);f.stores.push(store);return{prefix,store};};
  f.openRuntime=async({store,objectRoot,incarnation,previousIncarnations=[]})=>{const engine=createEngine({store,catalog:f.catalog,tenant:'t'}),identity=createIdentity({engine,keyring:f.keyring,getRestoreFence:()=>incarnation,clock:()=>f.time,passwordPolicy:{N:1024,sessionMs:1000000}}),objects=await openObjectStore({engine,identity,keyring:f.keyring,getRestoreFence:()=>incarnation,clock:()=>f.time,directory:objectRoot,model:'Object'}),custody=createCredentialCustody({catalog:f.catalog,keyring:f.keyring,getAuthority:tx=>identity.getAuthorityInTransaction(tx),getRestoreFence:()=>incarnation,getPreviousRestoreFence:()=>previousIncarnations,clock:()=>f.time}),apply=async(tx,{effect})=>[{type:'put',record:{...(await tx.get(sourceKey('t',effect.id.slice('effect:'.length)))),data:{state:'delivered'}}}];return fixtureHost({storage:store,engine,identity,objects,custody,objectRoot,incarnation,worker:(providers=f.providers)=>createEffectWorker({engine,custody,providers,workerId:'backup-worker',clock:()=>f.time,leaseMs:10,apply})});};
  const source=await f.openStore();f.prefix=source.prefix;f.objectRoot=join(f.directory,'original-objects');f.runtime=await f.openRuntime({...source,objectRoot:f.objectRoot,incarnation:f.fence});
  await f.runtime.identity.initialize({policy:graph(),credentials:['admin','reader'].map(actor=>({actor,password}))});f.token=(await f.runtime.identity.login({actor:'reader',password,operationId:'reader-login'})).token;
  const ledger=await f.openStore();f.provider=await createFakeProviderServer({protocol,store:ledger.store,secret:'external-secret'});f.providers={[protocol]:createFakeEmailClient({protocol,baseUrl:f.provider.baseUrl})};
  await f.runtime.engine.transact(tx=>f.runtime.custody.storeRevision(tx,{tenant:'t',actor:'admin'},{id:'credential',provider:protocol,secret:'external-secret'}));
  f.accept=(runtime,id)=>runtime.engine.transact(async tx=>{await tx.set(sourceKey('t',id),{id,tenant:'t',owner:'admin',model:'Message',createdAt:'0',data:{state:'pending'}});return acceptEffectInTransaction(tx,f.catalog,{tenant:'t',actor:'admin'},{id:'effect:'+id,provider:protocol,protocolVersion:'1',operation:'email.send',credentialId:'credential',credentialRevision:'1',request:{from:'from@example.test',to:['to@example.test'],subject:id,text:'restored original request'},rate:{limit:100,windowMs:60000}},{custody:runtime.custody,providers:f.providers,clock:()=>f.time});},{receipt:{id:'accept:'+id,digest:'fixed:'+id}});
  f.addObject=async(id,bytes,finalize=true)=>{const grant=await f.runtime.objects.begin(f.token,{operationId:'begin:'+id,id,owner:'visible',bytes:bytes.length,sha256:sha256(bytes)});await f.runtime.objects.upload(f.token,grant.uploadToken,chunks(bytes));if(finalize)await f.runtime.objects.finalize(f.token,{operationId:'finalize:'+id,id,uploadToken:grant.uploadToken});return grant;};
  f.backup=()=>writeBackup({runtime:f.runtime,objectRoot:f.objectRoot,directory:join(f.directory,'backup-'+randomUUID()),keyring:f.keyring,incarnation:f.fence,sourceIdentity:f.sourceIdentity,quiesce:callback=>callback()});
  f.prepare=async backup=>{const candidate=await f.openStore();return prepareRestoration({backupDirectory:backup.directory,keyring:f.keyring,target:{...candidate,objectRoot:join(f.directory,'candidate-objects-'+randomUUID()),clusterFile:f.clusterFile},catalog:f.catalog,tenant:'t',openCandidate:f.openRuntime});};
  t.after(async()=>{await f.provider.close();for(const store of f.stores)await store.close().catch(()=>{});const{default:fdb}=await import('foundationdb'),native=fdb.open(f.clusterFile);try{for(const prefix of f.prefixes){const root=fdb.tuple.pack(['kv-base',prefix]),end=Buffer.from(root);let upper=end;for(let i=end.length-1;i>=0;i--)if(end[i]<255){end[i]++;upper=end.subarray(0,i+1);break;}const tx=native.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,upper);await tx.rawCommit();}finally{tx.rawCancel();}}}finally{native.close();}await rm(f.directory,{recursive:true,force:true});});
  return f;
}
