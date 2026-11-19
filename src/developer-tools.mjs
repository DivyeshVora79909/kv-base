import { performance } from 'node:perf_hooks';
import { mutateInTransaction } from './mutate.mjs';
import { canonical, fail } from './values.mjs';

const natural=value=>Number.isSafeInteger(value)&&value>=0;
const runtime=()=>({node:process.version,platform:process.platform,architecture:process.arch});
const byteLength=value=>Buffer.byteLength(canonical(value));
const percentile=(sorted,p)=>sorted.length?sorted[Math.min(sorted.length-1,Math.floor((sorted.length-1)*p))]:0;
const resourceStart=()=>({cpu:process.cpuUsage(),rss:process.memoryUsage().rss,wall:performance.now()});
const resourceReport=begin=>{const cpu=process.cpuUsage(begin.cpu);return {wallMs:performance.now()-begin.wall,userCpuMs:cpu.user/1000,systemCpuMs:cpu.system/1000,rssStartBytes:begin.rss,rssEndBytes:process.memoryUsage().rss,processMaxRssBytes:process.resourceUsage().maxRSS*1024};};
function seeded(seed){let value=seed>>>0;return ()=>{value=(Math.imul(value,1664525)+1013904223)>>>0;return value;};}
async function attempt(engine,changes,options={}){
  let work,changeBytes=0,stagedBytes=0,result,nativeAdapter;const start=performance.now();
  try{
    result=await engine.transact(async tx=>{
      const measured={...tx,set:async(key,value)=>{stagedBytes+=Buffer.byteLength(key)+byteLength(value);return tx.set(key,value);},delete:async key=>{stagedBytes+=Buffer.byteLength(key);return tx.delete(key);}};
      try{return await mutateInTransaction(measured,engine.catalog,engine.tenant,changes,options.mutation??{});}
      finally{work=tx.inspect();}
    },{beforeCommit:async event=>{changeBytes=byteLength(event.changes);nativeAdapter=event.metrics;if(options.beforeCommit)await options.beforeCommit(event);}});
    return {accepted:true,result,work,nativeAdapter:nativeAdapter??null,attemptedChangeBytes:stagedBytes,publicationDeltaBytes:changeBytes,committedChangeBytes:changeBytes,latencyMs:performance.now()-start};
  }catch(error){
    return {accepted:false,error:{code:error.code??'ERROR',message:error.message},work,nativeAdapter:nativeAdapter??null,attemptedChangeBytes:stagedBytes,publicationDeltaBytes:changeBytes,committedChangeBytes:0,latencyMs:performance.now()-start};
  }
}

/** Privileged inspection through the engine's active snapshot. No source bodies or
 * hidden IDs are added to authorized result pages by these host functions. */
export async function inspectEngine(engine){return {runtime:runtime(),...await engine.inspect(),adapter:engine.store.capabilities};}

/** Adapt future batch sizes to observed transaction latency and rejection.
 * Each supplied batch is an actual atomic outcome; rejected batches are reported
 * whole and never silently split into transactions with different semantics. */
export async function populateAdaptive(engine,records,{initialBatch=16,minBatch=1,maxBatch=256,targetMs=50,continueOnReject=false,beforeCommit}={}){
  if(!Array.isArray(records)||![initialBatch,minBatch,maxBatch].every(natural)||!minBatch||minBatch>initialBatch||initialBatch>maxBatch||!Number.isFinite(targetMs)||targetMs<=0||typeof continueOnReject!=='boolean')fail('INVALID_WORKLOAD','Invalid population options');
  const resources=resourceStart(),batches=[];let size=initialBatch,offset=0,acceptedRecords=0,rejectedRecords=0;
  while(offset<records.length){
    const values=records.slice(offset,offset+size),outcome=await attempt(engine,values.map(record=>({type:'put',record})),{beforeCommit});
    batches.push({offset,size:values.length,...outcome});offset+=values.length;
    if(outcome.accepted)acceptedRecords+=values.length;else rejectedRecords+=values.length;
    if(!outcome.accepted&&!continueOnReject)break;
    size=outcome.accepted&&outcome.latencyMs<targetMs/2?Math.min(maxBatch,size*2):!outcome.accepted||outcome.latencyMs>targetMs?Math.max(minBatch,Math.floor(size/2)):size;
  }
  const inspection=await inspectEngine(engine);return {runtime:runtime(),requestedRecords:records.length,attemptedRecords:offset,acceptedRecords,rejectedRecords,remainingRecords:records.length-offset,batches,inspection,processResources:resourceReport(resources)};
}

/** Reproducible plan generation over one prepared catalog. Timings and physical
 * work are measured from each actual accepted or rolled-back transaction. The
 * caller's verifier may reconstruct expected records independently after each step.
 */
export async function runSeededWorkload(engine,{seed=1,steps=100,generate,verify,readQuery,readLimits,dataset={}}={}){
  if(!natural(seed)||seed>0xffffffff||!natural(steps)||typeof generate!=='function'||verify!==undefined&&typeof verify!=='function')fail('INVALID_WORKLOAD','Invalid seeded workload options');
  canonical(dataset);const resources=resourceStart(),random=seeded(seed),outcomes=[],before=await inspectEngine(engine),latencies=[];let verificationMs=0,readMs=0;
  for(let step=0;step<steps;step++){
    const plan=await generate({step,random,randomInt:limit=>{if(!natural(limit)||!limit)fail('INVALID_WORKLOAD','Invalid random bound');return random()%limit;}});
    if(!plan||!Array.isArray(plan.changes))fail('INVALID_WORKLOAD','Plan requires changes');
    const outcome=await attempt(engine,plan.changes,{beforeCommit:plan.beforeCommit,mutation:plan.mutation});
    let read;
    if(readQuery){const begin=performance.now(),page=await engine.window(typeof readQuery==='function'?readQuery({step,outcome}):readQuery,readLimits===undefined?{}:{limits:readLimits});read={...page.metrics,latencyMs:performance.now()-begin,limits:page.limits,witness:page.witness};readMs+=read.latencyMs;if(page.cursor)engine.releaseCursor(page.cursor);}
    const entry={step,...outcome,...(read?{read}:{})};outcomes.push(entry);latencies.push(outcome.latencyMs);
    if(verify){const begin=performance.now();await verify({step,plan,outcome,engine});verificationMs+=performance.now()-begin;}
  }
  const sorted=latencies.toSorted((a,b)=>a-b),sum=name=>outcomes.reduce((n,o)=>n+(o.work?.metrics?.[name]??0),0);
  const after=await inspectEngine(engine);return {runtime:runtime(),seed,steps,dataset,before,after,processResources:resourceReport(resources),readLatencyMs:readMs,independentVerificationMs:verificationMs,accepted:outcomes.filter(o=>o.accepted).length,rejected:outcomes.filter(o=>!o.accepted).length,
    work:{gets:sum('gets'),batchReads:sum('getMany'),scans:sum('scans'),scannedKeys:sum('scannedKeys'),stagedWrites:sum('stagedWrites'),touchedEntries:sum('touchedEntries'),
      nativeReadBytes:outcomes.reduce((n,o)=>n+(o.nativeAdapter?.nativeReadBytes??o.work?.metrics?.nativeReadBytes??0),0),attemptedNativeWriteBytes:outcomes.reduce((n,o)=>n+(o.nativeAdapter?.nativeWriteBytes??0),0),committedNativeWriteBytes:outcomes.reduce((n,o)=>n+(o.accepted?(o.nativeAdapter?.nativeWriteBytes??0):0),0),
      attemptedChangeBytes:outcomes.reduce((n,o)=>n+o.attemptedChangeBytes,0),committedChangeBytes:outcomes.reduce((n,o)=>n+o.committedChangeBytes,0),
      readNodeVisits:outcomes.reduce((n,o)=>n+(o.read?.nodes??0),0),readBatches:outcomes.reduce((n,o)=>n+(o.read?.batches??0),0),readBytes:outcomes.reduce((n,o)=>n+(o.read?.readBytes??0),0),
      sourceWitnessBytes:outcomes.reduce((n,o)=>n+(o.read?.witness?.sourceBytes??0),0),sourceWitnessScannedKeys:outcomes.reduce((n,o)=>n+(o.read?.witness?.metrics?.scannedKeys??0),0)},
    latencyMs:{total:latencies.reduce((a,b)=>a+b,0),p50:percentile(sorted,0.5),p95:percentile(sorted,0.95),max:sorted.at(-1)??0},outcomes,
    limits:{read:readLimits??outcomes.find(o=>o.read)?.read.limits??null,steps},measurementLimits:[`Selected adapter: ${engine.store.capabilities.adapter??(engine.store.capabilities.volatile?'volatile memory':'configured storage')}; ${engine.store.capabilities.scans??'scan costs follow its declared implementation'}.`,'Native bytes are client key/value payload counters, including physical namespace and V8 receipt/value encoding; they exclude network framing, disk/WAL amplification and replication. Definite rollback/conflict preserves attempted SDK write bytes but commits none.','Node visits describe measured window reads; mutation KV gets also include routing, validation and tree maintenance.','Attempted bytes sum every logical staged set key/value and delete key, including rolled-back validation attempts; publication bytes encode the final adapter change list.','Byte charges are logical encoded bytes after eager adapter reads; they do not bound heap allocation.','Wall/CPU/RSS cover inspections, workload reads and caller verification; mutation latency excludes those checks. Maximum RSS is the process lifetime high-water mark, not an isolated per-operation allocation.']};
}
