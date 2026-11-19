import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {readFile,writeFile} from 'node:fs/promises';
import {prepareCatalog} from '../src/catalog.mjs';
import {createEngine} from '../src/lifecycle.mjs';
import {openFoundationDbStore} from '../src/foundationdb.mjs';
import {populateAdaptive,runSeededWorkload} from '../src/developer-tools.mjs';
import {sourcePrefix} from '../src/keys.mjs';
import {treeAggregate} from '../src/tree.mjs';

const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
const integer={type:'string',pattern:'^(0|-?[1-9][0-9]*)$'},str={type:'string'},field=name=>({field:name});
const index=['Item',['group'],['major','minor']],readLimits={maxNodes:1000,maxBatches:1000,maxBytes:2_000_000,maxOutput:1000,batchKeys:16};
const engineLimits={maxSources:1000,maxSourceBytes:2_000_000,maxSourceScannedKeys:10000,maxCandidateKeys:10000,maxCandidateBytes:16_000_000,maxCursors:100};
const record=(n,amount='1')=>({id:`s${String(n).padStart(3,'0')}`,tenant:'t',owner:'o',model:'Item',createdAt:'1',data:{group:n<96?'hot':n<120?'warm':'cold',major:'7',minor:String(n%4),amount,label:`item ${n}`}});
const compare=(a,b)=>BigInt(a.data.minor)<BigInt(b.data.minor)?-1:BigInt(a.data.minor)>BigInt(b.data.minor)?1:Buffer.compare(Buffer.from(a.id),Buffer.from(b.id));

test('D5 actual FoundationDB workload independently checks skew ties rejection rollback bytes scans CPU and RSS',{skip},async t=>{
  const catalog=prepareCatalog({name:'D5-fdb-workload',version:'1',models:{Item:{schema:{type:'object',additionalProperties:false,properties:{group:str,major:integer,minor:integer,amount:integer,label:str},required:['group','major','minor','amount','label']},indexes:[{version:'1',partitionFields:index[1],orderFields:index[2],roles:{group:'id',major:'integer',minor:'integer'},inputs:{amount:'integer'},aggregates:{total:{monoid:'sum',input:'amount'}},validators:[{scope:'root',use:'nonnegative'}]}],memberships:[{slot:'main',index,partition:[field('group')],order:[field('major'),field('minor')],inputs:{amount:field('amount')},onMissing:'reject'}]}}},{validators:{nonnegative:{version:'1',validate:summary=>summary.total>=0n}}});
  const prefix='workload-'+randomUUID(),clusterFile=process.env.KV_BASE_FDB_CLUSTER_FILE,store=await openFoundationDbStore({clusterFile,prefix,transactionBytes:10_000_000,timeoutMs:4500}),engine=createEngine({store,catalog,tenant:'t',limits:engineLimits});
  t.after(async()=>{await store.close();const{default:fdb}=await import('foundationdb'),native=fdb.open(clusterFile),root=fdb.tuple.pack(['kv-base',prefix]),upper=Buffer.from(root);let end=upper;for(let i=upper.length-1;i>=0;i--)if(upper[i]<255){upper[i]++;end=upper.subarray(0,i+1);break;}const tx=native.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,end);await tx.rawCommit();}finally{tx.rawCancel();native.close();}});
  const initial=Array.from({length:128},(_,n)=>record(n)),expected=new Map(initial.map(row=>[row.id,structuredClone(row)])),trace=[];
  const population=await populateAdaptive(engine,initial,{initialBatch:8,minBatch:1,maxBatch:16,targetMs:1000});assert.equal(population.acceptedRecords,128);assert.equal(population.rejectedRecords,0);
  const query={index,partition:['hot'],limit:1000};
  const report=await runSeededWorkload(engine,{seed:20261119,steps:40,dataset:{initialRows:128,hotRows:96,warmRows:24,coldRows:8,major:'7',minorKeys:4},readQuery:query,readLimits,
    generate({step,randomInt}){const hot=randomInt(10)<8,n=hot?randomInt(96):randomInt(128),next=record(n,String(randomInt(5)+1));next.data.minor=String(randomInt(4));next.data.label=`step ${step}`;const rejected=step%8===0,rollback=step%9===0&&!rejected;if(rejected)next.data.amount='-10000';trace.push({step,n,hot,rejected,rollback,minor:next.data.minor});return{changes:[{type:'put',record:next}],...(rollback?{beforeCommit(){throw Object.assign(Error('intentional native precommit rollback'),{code:'PUBLICATION_FAULT'});}}:{})};},
    async verify({plan,outcome}){if(outcome.accepted)for(const change of plan.changes)expected.set(change.record.id,structuredClone(change.record));const actual=await engine.read(tx=>tx.scan({prefix:sourcePrefix('t')}));assert.deepEqual(actual.map(row=>row.value).sort((a,b)=>Buffer.compare(Buffer.from(a.id),Buffer.from(b.id))),[...expected.values()].sort((a,b)=>Buffer.compare(Buffer.from(a.id),Buffer.from(b.id))));const ordered=[...expected.values()].filter(row=>row.data.group==='hot').sort(compare),page=await engine.window(query,{limits:readLimits});assert.deepEqual(page.occurrences.map(row=>row.sourceId),ordered.map(row=>row.id));const fold=await engine.read(tx=>treeAggregate(tx,catalog,{tenant:'t',index,partition:['hot']}));assert.equal(fold.total,ordered.reduce((sum,row)=>sum+BigInt(row.data.amount),0n));await engine.check();}
  });
  assert.equal(report.accepted,31);assert.equal(report.rejected,9);assert.equal(report.outcomes.filter(row=>row.error?.code==='VALIDATION_FAILED').length,5);assert.equal(report.outcomes.filter(row=>row.error?.code==='PUBLICATION_FAULT').length,4);
  assert.equal(report.work.scans,0);assert.equal(report.work.scannedKeys,0);assert.ok(report.before.metrics.metrics.scans>0);assert.ok(report.before.metrics.metrics.scannedKeys>128);assert.ok(report.work.sourceWitnessScannedKeys>=40*128);assert.ok(report.work.sourceWitnessBytes>0);assert.ok(report.work.nativeReadBytes>0);assert.ok(report.work.attemptedNativeWriteBytes>report.work.committedNativeWriteBytes);assert.ok(report.work.readNodeVisits>0);
  for(const row of report.outcomes.filter(row=>row.error?.code==='PUBLICATION_FAULT')){assert.ok(row.nativeAdapter.nativeWriteBytes>0);assert.equal(row.committedChangeBytes,0);}
  assert.ok(report.processResources.userCpuMs>0);assert.ok(report.processResources.rssStartBytes>0);assert.ok(report.processResources.processMaxRssBytes>=report.processResources.rssEndBytes);assert.ok(report.independentVerificationMs>0);assert.ok(report.readLatencyMs>0);assert.equal(report.after.sources,128);assert.equal(report.before.adapter.adapter,'foundationdb');
  const sourceHash=createHash('sha256').update(await readFile(new URL('../src/developer-tools.mjs',import.meta.url))).digest('hex'),adapterHash=createHash('sha256').update(await readFile(new URL('../src/foundationdb.mjs',import.meta.url))).digest('hex');
  if(process.env.KV_BASE_WORKLOAD_REPORT)await writeFile(process.env.KV_BASE_WORKLOAD_REPORT,JSON.stringify({sourceIdentity:{developerTools:sourceHash,foundationdb:adapterHash,catalog:catalog.identity},adapterCapabilities:store.capabilities,engineLimits,readLimits,population,report,trace},null,2));
});
