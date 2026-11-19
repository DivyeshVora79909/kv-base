import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog,createMemoryStore,canonical } from '../src/index.mjs';
import { createEngine } from '../src/lifecycle.mjs';
import { inspectEngine,populateAdaptive,runSeededWorkload } from '../src/developer-tools.mjs';
import { sourcePrefix } from '../src/keys.mjs';
const integer={type:'string',pattern:'^(0|-?[1-9][0-9]*)$'},str={type:'string'},field=name=>({field:name});
const index=['Item',['group'],['major','minor']],readLimits={maxNodes:1000,maxBatches:1000,maxBytes:1_000_000,maxOutput:1000,batchKeys:8};
function setup(){
  const definition={name:'seeded-workload',version:'1',models:{Item:{
    schema:{type:'object',additionalProperties:false,properties:{group:str,major:integer,minor:integer,amount:integer,label:str},required:['group','major','minor','amount','label']},
    indexes:[{version:'1',partitionFields:index[1],orderFields:index[2],roles:{group:'id',major:'integer',minor:'integer'},inputs:{amount:'integer'},aggregates:{total:{monoid:'sum',input:'amount'}},validators:[{scope:'root',use:'nonnegative'}]}],
    memberships:[{slot:'main',index,partition:[field('group')],order:[field('major'),field('minor')],inputs:{amount:field('amount')},onMissing:'reject'}],
  }}};
  const catalog=prepareCatalog(definition,{validators:{nonnegative:{version:'1',validate:summary=>summary.total>=0n}}});
  return createEngine({store:createMemoryStore(),catalog,tenant:'t'});
}
const record=(n,group='hot',amount='1')=>({id:`s${String(n).padStart(3,'0')}`,tenant:'t',owner:'o',model:'Item',createdAt:'1',data:{group,major:'7',minor:String(n%3),amount,label:`item ${n}`}});
const query={index,partition:['hot'],limit:1000};
const inventory=engine=>engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).map(row=>row.value).sort((a,b)=>a.id.localeCompare(b.id)));

test('inspection and adaptive population report actual whole-batch outcomes through ordinary imports',async()=>{
  const engine=setup(),records=Array.from({length:13},(_,n)=>record(n)),result=await populateAdaptive(engine,records,{initialBatch:1,minBatch:1,maxBatch:4,targetMs:10000});
  assert.equal(result.acceptedRecords,13);assert.equal(result.rejectedRecords,0);assert.equal(result.remainingRecords,0);
  assert.equal(result.batches.reduce((n,b)=>n+b.size,0),13);assert.ok(result.batches.some(b=>b.size>1));
  assert.deepEqual(await inventory(engine),records);const inspection=await inspectEngine(engine);
  assert.equal(inspection.sources,13);assert.ok(inspection.sourceBytes>0);assert.ok(inspection.derivedKeys>0);assert.equal(inspection.adapter.volatile,true);
  const another=setup(),mixed=[record(0),record(1,'hot','invalid'),record(2),record(3),record(4)];
  const rejected=await populateAdaptive(another,mixed,{initialBatch:2,minBatch:1,maxBatch:2,targetMs:10000,continueOnReject:true});
  assert.equal(rejected.batches[0].accepted,false);assert.equal(rejected.batches[0].size,2);
  assert.equal(rejected.acceptedRecords,3);assert.equal(rejected.rejectedRecords,2);
  assert.deepEqual((await inventory(another)).map(r=>r.id),['s002','s003','s004']);await another.check();
});

async function witness(seed){
  const engine=setup(),initial=Array.from({length:32},(_,n)=>record(n,n<24?'hot':'cold')),expected=new Map(initial.map(r=>[r.id,structuredClone(r)])),trace=[];
  await populateAdaptive(engine,initial,{initialBatch:8,minBatch:1,maxBatch:8,targetMs:10000});
  const report=await runSeededWorkload(engine,{seed,steps:35,dataset:{initialRows:32,hotRows:24,coldRows:8,major:'7',minorKeys:3},readQuery:query,readLimits,
    generate({step,randomInt}){
      const hot=randomInt(10)<8,n=hot?randomInt(24):randomInt(32),next=record(n,n<24?'hot':'cold',String(randomInt(5)+1));
      next.data.minor=String(randomInt(3));next.data.label=`step ${step}`;
      if(step%5===0)next.data.amount='-1000';
      const force=step%7===0&&step%5!==0;trace.push({step,n,hot,minor:next.data.minor,rejection:step%5===0,rollback:force});
      return {changes:[{type:'put',record:next}],...(force?{beforeCommit(){throw Object.assign(Error('forced publication rollback'),{code:'PUBLICATION_FAULT'});}}:{})};
    },
    async verify({plan,outcome}){
      if(outcome.accepted)for(const change of plan.changes)expected.set(change.record.id,structuredClone(change.record));
      assert.deepEqual(await inventory(engine),[...expected.values()].sort((a,b)=>a.id.localeCompare(b.id)));
      const rows=[...expected.values()].filter(r=>r.data.group==='hot').sort((a,b)=>BigInt(a.data.minor)<BigInt(b.data.minor)?-1:BigInt(a.data.minor)>BigInt(b.data.minor)?1:a.id.localeCompare(b.id));
      const page=await engine.window(query,{limits:readLimits});assert.deepEqual(page.occurrences.map(r=>r.sourceId),rows.map(r=>r.id));
      await engine.check();
    },
  });return {report,trace,expected};
}

test('seeded skew and tied-key workload independently checks accepted changes, validation rejection and publication rollback',async()=>{
  const {report,trace}=await witness(0x519ab1);
  assert.equal(report.steps,35);assert.equal(report.accepted+report.rejected,35);
  assert.equal(report.rejected,trace.filter(t=>t.rejection||t.rollback).length);
  assert.ok(report.outcomes.some(o=>o.error?.code==='VALIDATION_FAILED'));assert.ok(report.outcomes.some(o=>o.error?.code==='PUBLICATION_FAULT'));
  assert.ok(report.work.gets>0);assert.ok(report.work.stagedWrites>0);assert.equal(report.work.scans,0);assert.equal(report.work.scannedKeys,0);
  assert.ok(report.work.readNodeVisits>0);assert.ok(report.work.readBytes>0);assert.ok(report.work.sourceWitnessBytes>0);
  assert.ok(report.work.sourceWitnessScannedKeys>0);
  assert.ok(report.work.attemptedChangeBytes>report.work.committedChangeBytes);
  assert.ok(report.outcomes.filter(o=>!o.accepted).some(o=>o.work.metrics.stagedWrites>0));
  assert.ok(report.latencyMs.max>=report.latencyMs.p95);assert.ok(report.latencyMs.p95>=report.latencyMs.p50);assert.ok(report.latencyMs.total>0);
  assert.equal(report.after.sources,32);assert.ok(trace.filter(t=>t.hot).length>trace.length/2);
  assert.deepEqual(report.limits.read,readLimits);assert.equal(report.runtime.node,process.version);
});

test('the same workload seed reproduces source outcomes and plan skew without claiming identical timing',async()=>{
  const first=await witness(0x519ab1),second=await witness(0x519ab1);
  assert.deepEqual(first.trace,second.trace);assert.deepEqual([...first.expected],[...second.expected]);
  assert.deepEqual(first.report.outcomes.map(o=>[o.accepted,o.error?.code]),second.report.outcomes.map(o=>[o.accepted,o.error?.code]));
  assert.equal(first.report.work.stagedWrites,second.report.work.stagedWrites);
});

test('validation and publication rollbacks report their actual staged logical write bytes',async()=>{
  const {report}=await witness(0x519ab1),validation=report.outcomes.find(o=>o.error?.code==='VALIDATION_FAILED'),publication=report.outcomes.find(o=>o.error?.code==='PUBLICATION_FAULT');
  assert.ok(validation.work.metrics.stagedWrites>0);assert.ok(validation.attemptedChangeBytes>0);assert.equal(validation.publicationDeltaBytes,0);assert.equal(validation.committedChangeBytes,0);
  assert.ok(publication.attemptedChangeBytes>0);assert.ok(publication.publicationDeltaBytes>0);assert.equal(publication.committedChangeBytes,0);
});

test('workload reads can use and report the engine default finite window limits',async()=>{
  const engine=setup();await engine.mutate([{type:'put',record:record(0)}]);
  const report=await runSeededWorkload(engine,{steps:1,generate:()=>({changes:[{type:'put',record:record(0,'hot','2')}]}),readQuery:query});
  assert.equal(report.accepted,1);assert.ok(report.work.readNodeVisits>0);assert.ok(report.limits.read.maxNodes>0);assert.equal(report.limits.steps,1);
});
