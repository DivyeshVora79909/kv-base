import { mutateInTransaction } from './mutate.mjs';
import { timingSafeEqual } from 'node:crypto';
import { validateRecord } from './catalog.mjs';
import { sourceKey } from './keys.mjs';
import { resolveRequestAuthority, permits } from './authority.mjs';
import { treeAggregate } from './tree.mjs';
import { treeReadWindow } from './read-window.mjs';
import { assertJson, canonical, cloneData, decodeDomain, fingerprint, fail } from './values.mjs';

const own=(value,key)=>Object.hasOwn(value,key);
const integer={type:'string',pattern:'^(0|-?[1-9][0-9]*)$'},string={type:'string',minLength:1};
const schema=(properties,required=Object.keys(properties))=>({type:'object',properties,required,additionalProperties:false});
const reference=(slot,field,target)=>({slot,field,targets:[target],many:false,onDelete:'reject'});
const put=record=>({type:'put',record});
const forbidden=()=>fail('FORBIDDEN','Credential operation is not permitted');
const same=(a,b)=>canonical(a)===canonical(b);
const proofMatches=(a,b)=>{const left=Buffer.from(a),right=Buffer.from(b);return left.length===right.length&&timingSafeEqual(left,right);};
const positive=value=>Number.isSafeInteger(value)&&value>0;
const time=clock=>{const value=clock();if(typeof value==='bigint'&&value>=0n)return value;if(Number.isSafeInteger(value)&&value>=0)return BigInt(value);fail('INVALID_EFFECT','Clock must return nonnegative integer milliseconds');};
const envelope=schema({version:string,keyId:string,nonce:string,ciphertext:string,tag:string});
const emailRequest=schema({from:string,to:{type:'array',items:string,minItems:1},subject:string,text:{type:'string'}});
const smsRequest=schema({to:{type:'string',pattern:'^\\+[1-9][0-9]{7,14}$'},text:{type:'string',minLength:1}});
const grantBody=schema({tenant:string,actor:string,effectId:string,credentialId:string,revision:integer,provider:string,requestDigest:string,expiresAt:integer});
export const EFFECT_QUEUE=['Effect',['state'],['dueAt']];
export const EFFECT_STARTS=['CredentialRevision',['credentialId','revision'],['startedAt']];
export const effectRevisionId=(id,revision)=>`@system/credential/${fingerprint([id,String(revision)])}`;
const attemptId=(id,epoch)=>`@system/attempt/${fingerprint([id,epoch])}`;
const factId=(attempt,result)=>`@system/fact/${fingerprint([attempt,result])}`;
const row=(tenant,id,owner,model,createdAt,data)=>({tenant,id,owner,model,createdAt:String(createdAt),data});
const binding=(tenant,purpose,subject,revision)=>({tenant,purpose,subject,revision:String(revision)});
function ordinary(value,allowed,required=allowed){
  assertJson(value);if(!value||Array.isArray(value)||typeof value!=='object'||Object.keys(value).some(key=>!allowed.includes(key))||required.some(key=>!own(value,key)))fail('INVALID_EFFECT','Invalid effect request');return cloneData(value);
}

/** Ordinary model declarations, merged into the application's one prepared catalog. */
export function effectModels(){const models={
  Credential:{access:{orders:{}},schema:schema({provider:string,activeRevision:integer,status:{enum:['active','revoked']}})},
  CredentialRevision:{schema:schema({credentialId:string,revision:integer,provider:string,status:{enum:['active','revoked']},envelope}),references:[reference('credential','credentialId','Credential')],indexes:[{version:'1',partitionFields:['credentialId','revision'],orderFields:['startedAt'],roles:{credentialId:'id',revision:'integer',startedAt:'integer'},inputs:{one:'integer'},aggregates:{starts:{monoid:'sum',input:'one'}}}]},
  Effect:{schema:schema({provider:string,protocolVersion:string,operation:{enum:['email.send','sms.send']},credentialId:string,credentialRevision:integer,revisionId:string,request:{oneOf:[emailRequest,smsRequest]},requestDigest:string,providerOperationId:string,grant:schema({body:grantBody,proof:string}),state:{enum:['pending','leased','started','unknown','succeeded','rejected','applied','isolated','cancelled']},cancelRequested:{type:'boolean'},incarnation:string,dispatchIncarnation:string,dispatchFence:integer,leaseIncarnation:string,dueAt:integer,epoch:integer,rate:schema({limit:{type:'integer',minimum:1},windowMs:{type:'integer',minimum:1}}),leaseOwner:string,leaseUntil:integer,attemptId:string,lastFactId:string,reason:string},['provider','protocolVersion','operation','credentialId','credentialRevision','revisionId','request','requestDigest','providerOperationId','grant','state','dueAt','epoch','rate']),references:[reference('credential','credentialId','Credential'),reference('revision','revisionId','CredentialRevision'),reference('attempt','attemptId','EffectAttempt'),reference('fact','lastFactId','EffectFact')],indexes:[{version:'1',partitionFields:['state'],orderFields:['dueAt'],roles:{state:'string',dueAt:'integer'},inputs:{one:'integer'},aggregates:{count:{monoid:'sum',input:'one'}}}],memberships:[{slot:'queue',index:EFFECT_QUEUE,partition:[{field:'state'}],order:[{field:'dueAt'}],inputs:{one:{constant:'1'}},onMissing:'reject'}]},
  EffectAttempt:{schema:schema({effectId:string,credentialId:string,revision:integer,revisionId:string,providerOperationId:string,requestDigest:string,epoch:integer,worker:string,incarnation:string,startedAt:integer,state:{enum:['started','unknown','known']}},['effectId','credentialId','revision','revisionId','providerOperationId','requestDigest','epoch','worker','startedAt','state']),references:[reference('effect','effectId','Effect'),reference('revision','revisionId','CredentialRevision')],memberships:[{slot:'charged-start',index:EFFECT_STARTS,partition:[{field:'credentialId'},{field:'revision'}],order:[{field:'startedAt'}],inputs:{one:{constant:'1'}},onMissing:'reject'}]},
  EffectFact:{schema:schema({effectId:string,attemptId:string,providerOperationId:string,observedAt:integer,observerEpoch:integer,observerIncarnation:string,result:schema({status:{enum:['accepted','rejected','unknown','absent']},providerId:string,reason:string,fence:integer,incarnation:string,reserved:{type:'boolean'}},['status'])},['effectId','attemptId','providerOperationId','observedAt','observerEpoch','result']),references:[reference('effect','effectId','Effect'),reference('attempt','attemptId','EffectAttempt')]},
};
  grantBody.properties.incarnation=string;
  models.Effect.schema.properties.requestEnvelope=envelope;
  models.Effect.schema.required=models.Effect.schema.required.filter(name=>name!=='request');
  models.Effect.schema.oneOf=[{properties:{request:{}},required:['request'],not:{properties:{requestEnvelope:{}},required:['requestEnvelope']}},{properties:{requestEnvelope:{}},required:['requestEnvelope'],not:{properties:{request:{}},required:['request']}}];
  models.Effect.schema.allOf=[{if:{properties:{operation:{const:'sms.send'}},required:['operation']},then:{properties:{request:smsRequest}},else:{properties:{request:emailRequest}}}];
  return models;
}
async function load(tx,catalog,tenant,id,model){
  const value=await tx.get(sourceKey(tenant,id));if(value===undefined)return;
  try{const record=validateRecord(catalog,value);if(record.id!==id||record.tenant!==tenant||model&&record.model!==model)throw Error();return record;}catch{fail('CORRUPT_STATE','Invalid service source');}
}
function configured(providers,effect){
  const provider=providers[effect.data?.provider??effect.provider];
  const data=effect.data??effect;
  return provider&&provider.version===data.protocolVersion&&provider.operation===data.operation?provider:undefined;
}

/** Encrypted custody with explicit request-bound Use, including retained revisions. */
export function createCredentialCustody({catalog,keyring,getAuthority,clock=Date.now,grantMs=300000,getRestoreFence,getPreviousRestoreFence}){
  if(!catalog?.models?.Credential||!keyring?.seal||!keyring?.open||!keyring?.digest||typeof getAuthority!=='function'||!positive(grantMs)||getRestoreFence!==undefined&&typeof getRestoreFence!=='function'||getPreviousRestoreFence!==undefined&&typeof getPreviousRestoreFence!=='function')fail('INVALID_EFFECT','Invalid credential custody');
  const scope=(tx,context)=>resolveRequestAuthority(tx,catalog,context,getAuthority);
  const restoreFence=async()=>getRestoreFence===undefined?undefined:decodeDomain('id',await getRestoreFence());
  const previousRestoreFence=async()=>{if(getPreviousRestoreFence===undefined)return[];const value=await getPreviousRestoreFence();return(value===undefined?[]:Array.isArray(value)?value:[value]).map(value=>decodeDomain('id',value));};
  async function storeRevision(tx,context,input){
    input=ordinary(input,['id','owner','provider','secret'],['id','provider','secret']);assertJson(context);context=cloneData(context);
    if(typeof input.secret!=='string'||!input.secret.length)fail('INVALID_CREDENTIAL','Provider secret must be nonempty text');
    const now=time(clock),authority=await scope(tx,context),old=await load(tx,catalog,context.tenant,input.id,'Credential');
    const owner=input.owner??old?.owner??context.actor,revision=old?BigInt(old.data.activeRevision)+1n:1n;
    const credential=row(context.tenant,input.id,owner,'Credential',old?.createdAt??now,{provider:input.provider,activeRevision:String(revision),status:'active'});
    if(old&&!permits(authority,old,'update')||!permits(authority,credential,old?'update':'create'))forbidden();
    if(old&&old.data.provider!==input.provider)fail('INVALID_CREDENTIAL','Provider is immutable');
    const secretEnvelope=keyring.seal({secret:input.secret},binding(context.tenant,'provider-credential',input.id,revision));
    const version=row(context.tenant,effectRevisionId(input.id,revision),owner,'CredentialRevision',now,{credentialId:input.id,revision:String(revision),provider:input.provider,status:'active',envelope:secretEnvelope});
    await mutateInTransaction(tx,catalog,context.tenant,[put(credential),put(version)]);
    const finalAuthority=await scope(tx,context);if(old&&!permits(finalAuthority,old,'update')||!permits(finalAuthority,credential,old?'update':'create'))forbidden();
    return {id:input.id,revision:String(revision)};
  }
  async function revoke(tx,context,{id,revision}){
    const authority=await scope(tx,context),credential=await load(tx,catalog,context.tenant,id,'Credential');
    if(!credential||!permits(authority,credential,'update'))forbidden();
    const version=await load(tx,catalog,context.tenant,effectRevisionId(id,revision??credential.data.activeRevision),'CredentialRevision');if(!version)fail('INVALID_CREDENTIAL','Unknown credential revision');
    const changes=[put({...version,data:{...version.data,status:'revoked'}})];
    if(revision===undefined||String(revision)===credential.data.activeRevision)changes.push(put({...credential,data:{...credential.data,status:'revoked'}}));
    await mutateInTransaction(tx,catalog,context.tenant,changes);if(!permits(await scope(tx,context),credential,'update'))forbidden();return {revoked:true};
  }
  async function grant(tx,context,effect){
    const authority=await scope(tx,context),credential=await load(tx,catalog,context.tenant,effect.credentialId,'Credential'),version=await load(tx,catalog,context.tenant,effectRevisionId(effect.credentialId,effect.credentialRevision),'CredentialRevision');
    if(!credential||!version||credential.data.status!=='active'||version.data.status!=='active'||credential.data.provider!==effect.provider||!permits(authority,credential,'use')||!permits(authority,{...credential,owner:version.owner},'use'))forbidden();
    const body={tenant:context.tenant,actor:context.actor,effectId:effect.id,credentialId:effect.credentialId,revision:String(effect.credentialRevision),provider:effect.provider,requestDigest:effect.requestDigest,...(effect.incarnation===undefined?{}:{incarnation:effect.incarnation}),expiresAt:String(time(clock)+BigInt(grantMs))};
    return {body,proof:keyring.digest(body,binding(context.tenant,'credential-use-grant',effect.id,effect.credentialRevision))};
  }
  async function resolveForStart(tx,effect){
    const {body,proof}=effect.data.grant,expected={tenant:effect.tenant,actor:body.actor,effectId:effect.id,credentialId:effect.data.credentialId,revision:effect.data.credentialRevision,provider:effect.data.provider,requestDigest:effect.data.requestDigest,...(effect.data.incarnation===undefined?{}:{incarnation:effect.data.incarnation}),expiresAt:body.expiresAt};
    if(!same(body,expected)||!proofMatches(proof,keyring.digest(body,binding(effect.tenant,'credential-use-grant',effect.id,body.revision),proof.split(':')[0]))||BigInt(body.expiresAt)<=time(clock))forbidden();
    const authority=await scope(tx,{tenant:effect.tenant,actor:body.actor}),credential=await load(tx,catalog,effect.tenant,body.credentialId,'Credential'),version=await load(tx,catalog,effect.tenant,effect.data.revisionId,'CredentialRevision');
    if(!credential||!version||credential.data.status!=='active'||version.data.status!=='active'||version.data.provider!==body.provider||version.data.credentialId!==credential.id||version.data.revision!==body.revision||!permits(authority,credential,'use')||!permits(authority,{...credential,owner:version.owner},'use'))forbidden();
    const sealed={envelope:version.data.envelope,binding:binding(effect.tenant,'provider-credential',credential.id,body.revision)};
    const checked=keyring.open(sealed.envelope,sealed.binding);if(typeof checked.secret!=='string')fail('INVALID_CREDENTIAL','Provider secret must be text');
    return sealed;
  }
  async function resolveForRecovery(tx,effect){
    const attempt=await load(tx,catalog,effect.tenant,effect.data.attemptId,'EffectAttempt');
    if(!attempt||attempt.data.effectId!==effect.id||attempt.data.credentialId!==effect.data.credentialId||attempt.data.revisionId!==effect.data.revisionId||attempt.data.providerOperationId!==effect.data.providerOperationId||attempt.data.requestDigest!==effect.data.requestDigest||attempt.data.revision!==effect.data.credentialRevision||effect.data.providerOperationId!==fingerprint(['external-effect',effect.tenant,effect.id])||effect.data.revisionId!==effectRevisionId(effect.data.credentialId,effect.data.credentialRevision))fail('CORRUPT_STATE','Recovery requires the exact durable start proof');
    const version=await load(tx,catalog,effect.tenant,effect.data.revisionId,'CredentialRevision');
    if(!version||version.data.credentialId!==effect.data.credentialId||version.data.revision!==attempt.data.revision||version.data.provider!==effect.data.provider)fail('CORRUPT_STATE','Recovery credential identity differs');
    return {envelope:version.data.envelope,binding:binding(effect.tenant,'provider-credential',version.data.credentialId,version.data.revision)};
  }
  function open(sealed){const value=keyring.open(sealed.envelope,sealed.binding);if(!value||typeof value.secret!=='string'||!value.secret.length)fail('INVALID_CREDENTIAL','Provider secret must be nonempty text');return Buffer.from(value.secret,'utf8');}
  const sealRequest=(tenant,id,revision,value)=>keyring.seal(value,binding(tenant,'effect-request',id,revision));
  const openRequest=effect=>keyring.open(effect.data.requestEnvelope,binding(effect.tenant,'effect-request',effect.id,effect.data.credentialRevision));
  const requestDigest=(context,input,keyId=keyring.activeKey)=>keyring.digest([input.provider,input.protocolVersion,input.operation,input.request,...(input.incarnation===undefined?[]:[input.incarnation])],binding(context.tenant,'effect-request-digest',input.id,input.credentialRevision),keyId);
  return Object.freeze({storeRevision,revoke,grant,resolveForStart,resolveForRecovery,open,sealRequest,openRequest,requestDigest,restoreFence,previousRestoreFence});
}

/** Host composition point: accept alongside domain changes and the original receipt. */
export async function acceptEffectInTransaction(tx,catalog,context,input,{custody,providers,clock=Date.now}={}){
  assertJson(context);context=cloneData(context);
  input=ordinary(input,['id','owner','provider','protocolVersion','operation','credentialId','credentialRevision','request','rate'],['id','provider','protocolVersion','operation','credentialId','credentialRevision','request','rate']);
  if(!positive(input.rate?.limit)||!positive(input.rate?.windowMs))fail('INVALID_EFFECT','Effect admission requires finite positive rate limits');
  const provider=configured(providers,input);if(!provider)fail('UNSUPPORTED_EFFECT','Provider protocol or operation is unsupported');provider.validate(input.request);
  const existing=await load(tx,catalog,context.tenant,input.id,'Effect');if(existing)fail('DUPLICATE_EFFECT','Effect identity is already accepted');
  const incarnation=await custody.restoreFence();if(incarnation!==undefined)input.incarnation=incarnation;
  const now=time(clock),requestDigest=custody.requestDigest(context,input),grant=await custody.grant(tx,context,{...input,requestDigest});
  const effect=row(context.tenant,input.id,input.owner??context.actor,'Effect',now,{...input,requestDigest,providerOperationId:fingerprint(['external-effect',context.tenant,input.id]),revisionId:effectRevisionId(input.credentialId,input.credentialRevision),grant,state:'pending',dueAt:String(now),epoch:'0'});delete effect.data.id;delete effect.data.owner;
  validateRecord(catalog,effect);
  effect.data.requestEnvelope=custody.sealRequest(context.tenant,effect.id,effect.data.credentialRevision,effect.data.request);delete effect.data.request;
  await mutateInTransaction(tx,catalog,context.tenant,[put(effect)]);await custody.resolveForStart(tx,effect);return {effectId:effect.id,providerOperationId:effect.data.providerOperationId};
}

/** Durable leases and start proofs fence effects; provider calls are outside tx. */
export function createEffectWorker({engine,custody,providers,workerId='worker',clock=Date.now,leaseMs=30000,apply}={}){
  if(!engine?.transact||!custody?.resolveForStart||!positive(leaseMs)||typeof workerId!=='string'||!workerId.length||apply!==undefined&&typeof apply!=='function')fail('INVALID_EFFECT','Invalid effect worker');
  const {catalog,tenant}=engine;
  const failedLeases=new WeakMap(),rememberFailure=(error,token)=>{if(error&&typeof error==='object')failedLeases.set(error,token);return error;};
  const get=(tx,id,model='Effect')=>load(tx,catalog,tenant,id,model);
  const save=(tx,records)=>mutateInTransaction(tx,catalog,tenant,records.map(put));
  const owns=(effect,token)=>effect.id===token.effectId&&effect.data.epoch===token.epoch&&effect.data.leaseOwner===token.workerId&&effect.data.leaseIncarnation===token.incarnation;
  const transaction=async(callback,expected)=>{const incarnation=await custody.restoreFence();if(expected!==undefined&&expected!==incarnation)fail('RESTORE_FENCED','Worker token belongs to another incarnation');return engine.transact(tx=>callback(tx,incarnation),{beforeCommit:async()=>{if(await custody.restoreFence()!==incarnation)fail('RESTORE_FENCED','Worker incarnation changed before publication');}});};
  const needsReservation=(effect,incarnation)=>effect.data.incarnation!==undefined&&effect.data.incarnation!==incarnation&&effect.data.dispatchIncarnation!==incarnation;
  const qualified=provider=>provider?.capabilities?.restoreFence&&typeof provider.reserveRestore==='function';
  const predecessors=async effect=>[...new Set([effect.data.incarnation,effect.data.dispatchIncarnation,...await custody.previousRestoreFence()].filter(value=>value!==undefined))];
  async function claim(id,recovery=false){return transaction(async(tx,incarnation)=>{
    const effect=await get(tx,id);if(!effect)return {claimed:false};const now=time(clock),data=effect.data;if(data.incarnation!==undefined&&incarnation===undefined)fail('RESTORE_FENCED','Required external incarnation unavailable');const restoredLease=data.leaseIncarnation!==undefined&&data.leaseIncarnation!==incarnation;
    const allowed=recovery?['started','unknown'].includes(data.state)&&(data.leaseOwner===workerId||BigInt(data.leaseUntil)<=now||restoredLease):data.state==='pending'&&BigInt(data.dueAt)<=now||data.state==='leased'&&(BigInt(data.leaseUntil)<=now||restoredLease);
    if(!allowed)return {claimed:false};
    if(!configured(providers,effect)){await save(tx,[{...effect,data:{...data,state:'isolated',reason:'unsupported provider configuration'}}]);return {claimed:false,isolated:true};}
    const epoch=String(BigInt(data.epoch)+1n),next={...effect,data:{...data,epoch,leaseOwner:workerId,leaseUntil:String(now+BigInt(leaseMs)),dueAt:String(now+BigInt(leaseMs)),...(incarnation===undefined?{}:{leaseIncarnation:incarnation}),state:recovery?'unknown':'leased'}};
    await save(tx,[next]);return {claimed:true,effectId:id,epoch,workerId,recovery,...(incarnation===undefined?{}:{incarnation})};
  });}
  async function start(token){return transaction(async(tx,incarnation)=>{
    if(token.incarnation!==incarnation)fail('RESTORE_FENCED','Worker token incarnation differs');const effect=await get(tx,token.effectId);if(!effect||!owns(effect,token))fail('STALE_LEASE','Effect lease is obsolete');
    const now=time(clock),data=effect.data,aid=attemptId(effect.id,token.epoch),prior=await get(tx,aid,'EffectAttempt');
    if(data.state==='started'&&data.attemptId===aid&&prior)return {effect,attemptId:aid,replayed:true};
    if(data.state!=='leased'||BigInt(data.leaseUntil)<=now)fail('STALE_LEASE','Effect lease expired');
    const provider=configured(providers,effect);if(!provider)fail('UNSUPPORTED_EFFECT','Provider protocol changed');const request=custody.openRequest(effect);provider.validate(request);if(!/^[A-Za-z0-9_-]{1,64}:[A-Za-z0-9_-]{43}$/.test(data.requestDigest))fail('CORRUPT_STATE','Accepted request digest is malformed');
    if(!proofMatches(data.requestDigest,custody.requestDigest({tenant},{...data,id:effect.id,request},data.requestDigest.split(':')[0]))||data.providerOperationId!==fingerprint(['external-effect',tenant,effect.id])||data.revisionId!==effectRevisionId(data.credentialId,data.credentialRevision))fail('CORRUPT_STATE','Accepted effect request identity changed');
    const restored=data.incarnation!==undefined&&data.incarnation!==incarnation;
    if(restored&&!qualified(provider))fail('RESTORED_EFFECT_UNSAFE','Restored operation requires safe original-key status');
    const credential=await custody.resolveForStart(tx,effect),lower=now-BigInt(data.rate.windowMs);
    const charged=(await treeAggregate(tx,catalog,{tenant,index:EFFECT_STARTS,partition:[data.credentialId,data.credentialRevision]},{...(lower<0n?{}:{lower:{value:[lower],inclusive:false}}),upper:{value:[now],inclusive:true}})).starts;
    if(charged>=BigInt(data.rate.limit))fail('RATE_LIMIT','Physical start quota exhausted');
    const attempt=row(tenant,aid,effect.owner,'EffectAttempt',now,{effectId:effect.id,credentialId:data.credentialId,revision:data.credentialRevision,revisionId:data.revisionId,providerOperationId:data.providerOperationId,requestDigest:data.requestDigest,epoch:token.epoch,worker:token.workerId,...(incarnation===undefined?{}:{incarnation}),startedAt:String(now),state:'started'});
    const next={...effect,data:{...data,state:'started',attemptId:aid}};await save(tx,[attempt,next]);await custody.resolveForStart(tx,next);return {effect:next,attemptId:aid,credential,request,replayed:false};
  });}
  async function recordResult(token,aid,result){return transaction(async(tx,incarnation)=>{
    if(token.incarnation!==incarnation)fail('RESTORE_FENCED','Completion token incarnation differs');result=ordinary(result,['status','providerId','reason','fence','incarnation','reserved'],['status']);if(!['accepted','rejected','unknown','absent'].includes(result.status))fail('INVALID_EFFECT','Invalid provider fact');
    const effect=await get(tx,token.effectId),attempt=await get(tx,aid,'EffectAttempt');if(!effect||!attempt||attempt.data.effectId!==effect.id||attempt.data.providerOperationId!==effect.data.providerOperationId||attempt.data.requestDigest!==effect.data.requestDigest)fail('CORRUPT_STATE','Provider fact has no exact start proof');
    if(!token.recovery&&(attempt.data.epoch!==token.epoch||attempt.data.worker!==token.workerId))fail('STALE_LEASE','Provider fact attempt differs from worker lease');
    const now=time(clock),fid=factId(aid,result),fact=row(tenant,fid,effect.owner,'EffectFact',now,{effectId:effect.id,attemptId:aid,providerOperationId:effect.data.providerOperationId,observedAt:String(now),observerEpoch:token.epoch,...(incarnation===undefined?{}:{observerIncarnation:incarnation}),result});
    const old=await get(tx,fid,'EffectFact'),records=old?[]:[fact];if(old&&!same(old.data.result,result))fail('CORRUPT_STATE','Provider fact changed');
    records.push({...attempt,data:{...attempt.data,state:result.status==='unknown'&&attempt.data.state!=='known'?'unknown':'known'}});
    const current=owns(effect,token)&&effect.data.attemptId===aid&&['started','unknown'].includes(effect.data.state);
    const observedState=result.status==='absent'&&effect.data.cancelRequested?'cancelled':{accepted:'succeeded',rejected:'rejected',unknown:'unknown',absent:'pending'}[result.status];
    if(current){if(result.reserved&&(!result.fence||result.incarnation!==incarnation||BigInt(result.fence)<=BigInt(token.epoch)))fail('INVALID_EFFECT','Restore reservation proof differs');const state=observedState,epoch=result.fence!==undefined&&BigInt(result.fence)>BigInt(effect.data.epoch)?result.fence:effect.data.epoch;records.push({...effect,data:{...effect.data,epoch,...(result.reserved?{dispatchIncarnation:incarnation,dispatchFence:result.fence}:{}),state,lastFactId:fid,dueAt:result.status==='unknown'?effect.data.leaseUntil:String(now)}});}
    if(records.length)await save(tx,records);return {current,state:current?observedState:effect.data.state,factId:fid};
  });}
  async function applyResult(id){return transaction(async(tx,incarnation)=>{
    const effect=await get(tx,id);if(!effect||effect.data.state==='applied')return {applied:false};if(effect.data.state!=='succeeded')return {applied:false};
    const fact=await get(tx,effect.data.lastFactId,'EffectFact');if(!fact||fact.data.effectId!==id||fact.data.result.status!=='accepted')fail('CORRUPT_STATE','Application has no accepted fact');
    const changes=apply?await apply(tx,{effect:cloneData(effect),fact:cloneData(fact)}):[];if(!Array.isArray(changes))fail('INVALID_EFFECT','Application planner must return source commands');
    await mutateInTransaction(tx,catalog,tenant,[...changes,put({...effect,data:{...effect.data,state:'applied'}})]);return {applied:true};
  });}
  async function runOne(id){
    const token=await claim(id);if(!token.claimed)return {...token,...await applyResult(id)};
    let started;
    try{started=await start(token);}catch(error){
      if(error.code==='RESTORE_FENCED')return {effectId:id,error:error.code,restorationRequired:true};
      if(error.code==='STALE_LEASE')return {effectId:id,error:error.code};
      if(['STORAGE_CONFLICT','COMMIT_UNKNOWN','STORAGE_ERROR'].includes(error.code))return {effectId:id,error:error.code,recoveryRequired:error.code==='COMMIT_UNKNOWN'};
      await transaction(async(tx,incarnation)=>{if(token.incarnation!==incarnation)fail('RESTORE_FENCED','Worker error belongs to old incarnation');const effect=await get(tx,id);if(effect&&owns(effect,token)&&effect.data.state==='leased')await save(tx,[{...effect,data:{...effect.data,state:error.code==='RATE_LIMIT'?'pending':'isolated',dueAt:String(time(clock)+BigInt(effect.data.rate.windowMs)),reason:error.code??'start rejected'}}]);});
      return {effectId:id,error:error.code??'ERROR'};
    }
    if(started.replayed)return {effectId:id,state:'started',recoveryRequired:true};
    let secret;
    try{
      const provider=configured(providers,started.effect);if(!provider)fail('UNSUPPORTED_EFFECT','Dispatch provider configuration changed');
      secret=custody.open(started.credential);const restoring=needsReservation(started.effect,token.incarnation),expectedIncarnations=restoring?await predecessors(started.effect):undefined,request=cloneData(started.request);let result;
      try{
        let preflight;
        if(restoring||provider.capabilities?.providerFence){
          preflight=restoring?(qualified(provider)?await provider.reserveRestore({operationId:started.effect.data.providerOperationId,secret,fence:token.epoch,incarnation:token.incarnation,expectedIncarnations}):{status:'unknown',reason:'restored operation lacks incarnation reservation'}):provider.lookup?await provider.lookup({operationId:started.effect.data.providerOperationId,secret,fence:token.epoch,incarnation:token.incarnation}):{status:'unknown',reason:'provider fence has no lookup'};
          if(restoring&&(!preflight.reserved||preflight.incarnation!==token.incarnation||preflight.fence===undefined||BigInt(preflight.fence)<=BigInt(token.epoch)))preflight={status:'unknown',reason:'restore reservation evidence missing'};
          if(preflight.status==='absent'&&(preflight.fence===undefined||BigInt(preflight.fence)<BigInt(token.epoch)))preflight={status:'unknown',reason:'original operation absence lacks exact external fence'};
          if(restoring||preflight.status!=='absent'||BigInt(preflight.fence)>BigInt(token.epoch))result=preflight;
        }
        if(!result){result=await provider.send({operationId:started.effect.data.providerOperationId,request,secret,fence:token.epoch,incarnation:token.incarnation});if(preflight?.fence!==undefined)result={...result,fence:preflight.fence,...(preflight.incarnation===undefined?{}:{incarnation:preflight.incarnation})};}
      }catch{result={status:'unknown',reason:'provider outcome unavailable'};}finally{secret.fill(0);}
      const recorded=await recordResult(token,started.attemptId,result);if(recorded.current&&recorded.state==='succeeded')await applyResult(id);return {effectId:id,...recorded};
    }catch(error){throw rememberFailure(error,token);}finally{secret?.fill(0);}
  }
  async function reconcile(id){
    const token=await claim(id,true);if(!token.claimed)return token;
    let secret;
    try{
      const effect=await engine.read(tx=>get(tx,id));if(!effect)fail('CORRUPT_STATE','Recovery effect disappeared');const provider=configured(providers,effect);if(!provider)fail('UNSUPPORTED_EFFECT','Recovery provider configuration changed');
      const sealed=await engine.read(tx=>custody.resolveForRecovery(tx,effect));secret=custody.open(sealed);const restoring=needsReservation(effect,token.incarnation),expectedIncarnations=restoring?await predecessors(effect):undefined;let result;
      try{result=restoring?(qualified(provider)?await provider.reserveRestore({operationId:effect.data.providerOperationId,secret,fence:token.epoch,incarnation:token.incarnation,expectedIncarnations}):{status:'unknown',reason:'restored operation lacks incarnation reservation'}):provider.lookup?await provider.lookup({operationId:effect.data.providerOperationId,secret,fence:token.epoch,incarnation:token.incarnation}):{status:'unknown',reason:'provider has no authoritative lookup'};if(restoring&&(!result.reserved||result.incarnation!==token.incarnation||result.fence===undefined||BigInt(result.fence)<=BigInt(token.epoch)))result={status:'unknown',reason:'restore reservation evidence missing'};if(result.status==='absent'&&(!provider.capabilities?.safeRetryAfterAbsent||provider.capabilities.providerFence&&(result.fence===undefined||BigInt(result.fence)<BigInt(token.epoch))))result={status:'unknown',reason:'provider absence does not fence obsolete starts'};}
      catch{result={status:'unknown',reason:'provider lookup unavailable'};}finally{secret.fill(0);}
      const recorded=await recordResult(token,effect.data.attemptId,result);if(recorded.current&&recorded.state==='succeeded')await applyResult(id);return {effectId:id,...recorded};
    }catch(error){throw rememberFailure(error,token);}finally{secret?.fill(0);}
  }
  async function deferJobFailure(id,error){
    const code=typeof error?.code==='string'&&error.code.length?error.code:'ERROR';
    if(['SERVICE_CLOSED','STORAGE_CLOSED','RESTORE_FENCED'].includes(code))throw error;
    const storageFailure=['STORAGE_CONFLICT','COMMIT_UNKNOWN','STORAGE_ERROR','STORAGE_LIMIT'].includes(code),token=error&&typeof error==='object'?failedLeases.get(error):undefined;
    return transaction(async tx=>{
      const effect=await get(tx,id);if(!effect)return {effectId:id,error:code};
      if(effect.data.state==='applied')return {effectId:id,error:code,applied:true};
      if(effect.data.state!=='succeeded'&&(!token||!owns(effect,token)))return {effectId:id,error:code,stale:true};
      if(effect.data.state==='succeeded'||storageFailure&&['pending','leased','started','unknown'].includes(effect.data.state)){
        const dueAt=String(time(clock)+BigInt(leaseMs));await save(tx,[{...effect,data:{...effect.data,dueAt,reason:code}}]);return {effectId:id,error:code,deferred:true,dueAt,...(effect.data.state==='succeeded'?{applicationPending:true}:{recoveryRequired:true})};
      }
      if(['pending','leased','started','unknown'].includes(effect.data.state)){await save(tx,[{...effect,data:{...effect.data,state:'isolated',reason:code}}]);return {effectId:id,error:code,isolated:true};}
      return {effectId:id,error:code};
    });
  }
  async function drain({maxJobs=100,limits={maxNodes:1000,maxBatches:1000,maxBytes:4*1024*1024,maxOutput:1,batchKeys:32}}={}){
    if(!Number.isSafeInteger(maxJobs)||maxJobs<0)fail('INVALID_EFFECT','Invalid worker budget');const outcomes=[];
    for(let n=0;n<maxJobs;n++){
      const candidates=await engine.read(async tx=>{const found=[];for(const state of ['pending','leased','started','unknown','succeeded']){const page=await treeReadWindow(tx,catalog,{tenant,index:EFFECT_QUEUE,partition:[state]},{ranks:{start:0,end:1},range:{upper:{value:[time(clock)]}},limits});if(page.occurrences[0])found.push({state,...page.occurrences[0]});}return found.sort((a,b)=>a.order[0]<b.order[0]?-1:a.order[0]>b.order[0]?1:a.sourceId.localeCompare(b.sourceId))[0];});
      if(!candidates)break;
      try{outcomes.push(await (['started','unknown'].includes(candidates.state)?reconcile(candidates.sourceId):runOne(candidates.sourceId)));}
      catch(error){outcomes.push(await deferJobFailure(candidates.sourceId,error));}
    }return {outcomes};
  }
  async function cancel(id){return transaction(async(tx,incarnation)=>{
    const effect=await get(tx,id);if(!effect)return {cancelled:false};
    if(effect.data.state==='applied')return {cancelled:false,alreadyApplied:true};
    if(effect.data.state==='cancelled')return {cancelled:true};
    const inFlight=['started','unknown','succeeded'].includes(effect.data.state);
    const data={...effect.data,cancelRequested:true,...(inFlight?{}:{state:'cancelled',epoch:String(BigInt(effect.data.epoch)+1n)})};
    await save(tx,[{...effect,data}]);return {cancelled:!inFlight,inFlight};
  });}
  return Object.freeze({claim,start,recordResult,apply:applyResult,runOne,reconcile,drain,cancel});
}
