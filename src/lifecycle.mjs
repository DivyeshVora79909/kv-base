import { randomBytes } from 'node:crypto';
import { createMemoryStore } from './memory.mjs';
import { mutate } from './mutate.mjs';
import { mutateAuthorized } from './authorized-mutate.mjs';
import { readAuthorized, readAuthorizedInTransaction } from './authorized-read.mjs';
import { checkStore } from './check.mjs';
import { sourceOracle } from './oracle.mjs';
import { resolveRequestAuthority, permits } from './authority.mjs';
import { treeReadWindow } from './read-window.mjs';
import { normalizeRange, containsRange, compareKey } from './range.mjs';
import { normalizeScope, resolveIndex } from './index-declaration.mjs';
import { sourcePrefix, catalogKey, reservationPrefix } from './keys.mjs';
import { assertJson, canonical, decodeDomain, fingerprint, fail } from './values.mjs';

const bytes=value=>Buffer.byteLength(canonical(value));
const natural=value=>Number.isSafeInteger(value)&&value>=0;
const defaults={maxSources:100000,maxSourceBytes:64*1024*1024,maxSourceScannedKeys:10000000,maxCandidateKeys:1000000,maxCandidateBytes:256*1024*1024,maxCursors:10000};
const windowLimits={maxNodes:100000,maxBatches:10000,maxBytes:16*1024*1024,maxOutput:10000,batchKeys:32};
function fields(input,allowed,required=[],code='INVALID_ENGINE'){
  if(!input||typeof input!=='object'||Array.isArray(input)||![Object.prototype,null].includes(Object.getPrototypeOf(input)))fail(code,'Expected ordinary options');
  for(const key of Reflect.ownKeys(input)){
    const descriptor=Object.getOwnPropertyDescriptor(input,key);
    if(typeof key!=='string'||!allowed.includes(key)||!descriptor.enumerable||!Object.hasOwn(descriptor,'value'))fail(code,'Invalid option descriptor');
  }
  if(required.some(key=>!Object.hasOwn(input,key)))fail(code,'Missing required option');return input;
}
function limits(input={}){
  fields(input,Object.keys(defaults));canonical(input);
  const result={...defaults,...input};for(const value of Object.values(result))if(!natural(value))fail('INVALID_ENGINE','Invalid engine limit');return result;
}
function exact(input,allowed,required=allowed){
  fields(input,allowed,required);canonical(input);
}
const text=(a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b));

/** A serialized lifecycle boundary around the prepared engine. Sources,
 * catalog identity and host policy keys remain shared. Only derived KV namespaces
 * are generation-specific. Raw writes bypassing this boundary can break integrity.
 */
export function createEngine(configuration){
  fields(configuration,['store','catalog','tenant','limits'],['store','catalog','tenant']);
  const {store,catalog,tenant,limits:configured={}}=configuration;
  if(!store?.capabilities?.serialized||typeof store.read!=='function'||typeof store.transact!=='function'||!catalog?.models)fail('INVALID_ENGINE','A serialized store and prepared catalog are required');
  decodeDomain('id',tenant);const caps=limits(configured),tag=canonical(tenant);
  const activeKey=`engine/v1/${tag}/${catalog.identity}/active`,generationBase=`generation/v1/${tag}/${catalog.identity}/`;
  const ids=[catalog.identity,fingerprint(['internal-potential-avl-v1',catalog.identity]),fingerprint(['internal-route-interval-avl-v1',catalog.identity])];
  const owned=[...['root','node','incoming','claim'].flatMap(kind=>ids.map(id=>`${kind}/${tag}/${id}/`)),`reactive/v1/${tag}/${catalog.identity}/`,`routes/v1/${tag}/${catalog.identity}/`];
  const isOwned=key=>typeof key==='string'&&owned.some(prefix=>key.startsWith(prefix));
  const prefix=generation=>`${generationBase}${generation}/`;
  const physical=(key,generation)=>generation&&isOwned(key)?prefix(generation)+key:key;
  const cursors=new Map(),readers=new Map();
  function fixedContext(context){
    fields(context,['tenant','actor'],['tenant','actor']);assertJson(context);
    decodeDomain('id',context.tenant);decodeDomain('id',context.actor);
    if(context.tenant!==tenant)fail('FORBIDDEN','Context belongs to another engine tenant');return structuredClone(context);
  }
  async function state(tx){
    const [value,identity]=await tx.getMany([activeKey,catalogKey(tenant)]);
    if(identity!==undefined&&identity!==catalog.identity)fail('CATALOG_MISMATCH','Store catalog differs');
    if(value===undefined)return {catalog:catalog.identity,generation:0,retired:[]};
    if(Object.keys(value).length!==3||value.catalog!==catalog.identity||!natural(value.generation)||!Array.isArray(value.retired)||new Set(value.retired).size!==value.retired.length||value.retired.some(g=>!natural(g)||g>=value.generation))fail('CORRUPT_STATE','Invalid active generation');
    return value;
  }
  function transaction(tx,generation){
    const mapped={...tx,
      get:key=>tx.get(physical(key,generation)),getMany:keys=>tx.getMany(keys.map(key=>physical(key,generation))),
      set:(key,value)=>tx.set(physical(key,generation),value),delete:key=>tx.delete(physical(key,generation)),
      async scan(options={}){
        fields(options,['prefix','gte','gt','lte','lt','limit'],[],'INVALID_RANGE');
        const fixed={...options};
        const rangeKeys=['gte','gt','lte','lt'];
        if(options.prefix!==undefined&&typeof options.prefix!=='string'||rangeKeys.some(key=>options[key]!==undefined&&typeof options[key]!=='string')||options.limit!==undefined&&options.limit!==Infinity&&!natural(options.limit))fail('INVALID_RANGE','Invalid logical scan range');
        // Engine prefixes are concrete. A broad host scan filters all inactive
        // generations and maps the active derived keys back to logical keys.
        if(options.prefix&&isOwned(options.prefix)&&rangeKeys.every(key=>fixed[key]===undefined||isOwned(fixed[key]))){
          for(const key of ['prefix','gte','gt','lte','lt'])if(fixed[key]!==undefined)fixed[key]=physical(fixed[key],generation);
          return (await tx.scan(fixed)).map(row=>({...row,key:generation?row.key.slice(prefix(generation).length):row.key}));
        }
        const broad=options.prefix===undefined||options.prefix===''||isOwned(options.prefix)||owned.some(prefix=>prefix.startsWith(options.prefix));
        if(!broad)return tx.scan(fixed);
        const rows=await tx.scan(),result=[];
        for(const row of rows){
          if(row.key===activeKey)continue;
          if(row.key.startsWith(generationBase)){
            if(generation&&row.key.startsWith(prefix(generation)))result.push({...row,key:row.key.slice(prefix(generation).length)});
          }else if(!generation||!isOwned(row.key))result.push(row);
        }
        const selected=result.filter(row=>(options.prefix===undefined||row.key.startsWith(options.prefix))
          &&(options.gte===undefined||text(row.key,options.gte)>=0)&&(options.gt===undefined||text(row.key,options.gt)>0)
          &&(options.lte===undefined||text(row.key,options.lte)<=0)&&(options.lt===undefined||text(row.key,options.lt)<0));
        selected.sort((a,b)=>text(a.key,b.key));return selected.slice(0,options.limit??Infinity);
      },
    };return Object.freeze(mapped);
  }
  const activeStore=Object.freeze({capabilities:store.capabilities,
    read:callback=>store.read(async tx=>{
      const current=await state(tx),generation=current.generation;readers.set(generation,(readers.get(generation)??0)+1);
      try{return await callback(transaction(tx,generation),{generation,catalog,tenant});}
      finally{const count=readers.get(generation)-1;if(count)readers.set(generation,count);else readers.delete(generation);}
    }),
    transact:(callback,options={})=>store.transact(async tx=>{
      const current=await state(tx);if(await tx.get(activeKey)===undefined)await tx.set(activeKey,current);
      return callback(transaction(tx,current.generation),{generation:current.generation,catalog,tenant});
    },{...options,...(options.verifyReplay?{verifyReplay:async(tx,receipt)=>{
      const current=await state(tx);return options.verifyReplay(transaction(tx,current.generation),receipt);
    }}:{})}),
    ...(typeof store.getReceipt==='function'?{getReceipt:id=>store.getReceipt(id)}:{}),
  });
  async function sources(tx){
    const before=tx.inspect().metrics.scannedKeys;
    const rows=await tx.scan({prefix:sourcePrefix(tenant),limit:caps.maxSources+1});
    const scannedKeys=tx.inspect().metrics.scannedKeys-before;
    if(scannedKeys>caps.maxSourceScannedKeys)fail('WORK_LIMIT','Source witness scan budget exceeded');
    if(rows.length>caps.maxSources)fail('WORK_LIMIT','Source witness budget exceeded');
    let sourceBytes=0;for(const row of rows)sourceBytes+=Buffer.byteLength(row.key)+bytes(row.value);
    if(sourceBytes>caps.maxSourceBytes)fail('WORK_LIMIT','Source witness byte budget exceeded');
    return {records:rows.map(row=>row.value),sourceBytes,scannedKeys};
  }
  function fixedQuery(query){
    exact(query,['index','partition','range','limit'],['index','partition','limit']);
    if(!natural(query.limit))fail('INVALID_READ','Invalid page limit');
    const index=resolveIndex(catalog,query.index),contract=catalog.views[index.view].contract;
    if(!Array.isArray(query.partition)||query.partition.length!==contract.partition.length)fail('INVALID_SCOPE','Wrong page partition');
    return {index:structuredClone(index.signature),view:index.view,partition:query.partition.map((v,i)=>decodeDomain(contract.partition[i],v)),range:normalizeRange(contract.order,Object.hasOwn(query,'range')?query.range:{}),limit:query.limit};
  }
  function token(current,queryHash,dataHash,authorityHash,position,replacing){
    if(cursors.size-(replacing&&cursors.has(replacing)?1:0)>=caps.maxCursors)fail('WORK_LIMIT','Cursor inventory budget exceeded');
    const id=randomBytes(24).toString('base64url');cursors.set(id,{generation:current.generation,queryHash,dataHash,authorityHash,position});if(replacing)cursors.delete(replacing);return id;
  }
  function resume(id,current,queryHash,dataHash,authorityHash){
    if(id===undefined)return;
    const value=typeof id==='string'&&cursors.get(id);if(!value)fail('INVALID_CURSOR','Unknown or consumed cursor');
    if(value.generation!==current.generation||value.queryHash!==queryHash||value.dataHash!==dataHash||value.authorityHash!==authorityHash)fail('STALE_CURSOR','Cursor query, generation, data or authority changed');
    return value.position;
  }
  async function window(query,options={}){
    exact(options,['cursor','limits'],[]);options=structuredClone(options);
    if(options.limits!==undefined){fields(options.limits,Object.keys(windowLimits),Object.keys(windowLimits));if(!Object.values(options.limits).every(natural)||options.limits.batchKeys===0)fail('INVALID_READ','Invalid window limits');}
    const fixed=fixedQuery(query),contract=catalog.views[fixed.view].contract;
    return activeStore.read(async(tx,current)=>{
      const witness=await sources(tx),records=new Map(witness.records.map(r=>[r.id,r]));
      const tree=sourceOracle(catalog,witness.records).trees.find(t=>t.view===fixed.view&&canonical(t.partition)===canonical(fixed.partition));
      const all=(tree?.members??[]).map((row,rank)=>({...row,rank})).filter(row=>containsRange(contract.order,row,fixed.range));
      const queryHash=fingerprint(['raw',tenant,fixed]),dataHash=fingerprint(all.map(row=>[row.sourceId,row.slot,row.order,records.get(row.sourceId)]));
      const last=resume(options.cursor,current,queryHash,dataHash,null),remaining=last?all.filter(row=>compareKey(contract.order,row,last)>0):all,page=remaining.slice(0,fixed.limit);
      const ranks=page.length?{start:page[0].rank,end:page.at(-1).rank+1}:{start:0,end:0};
      const result=await treeReadWindow(tx,catalog,{tenant,view:fixed.view,partition:fixed.partition},{range:fixed.range,ranks,limits:options.limits??windowLimits});
      const expected=page.map(row=>({rank:row.rank,sourceId:row.sourceId,slot:row.slot,order:row.order}));
      if(canonical(result.occurrences)!==canonical(expected))fail('INTEGRITY','Window differs from source facts');
      const cursor=page.length&&remaining.length>page.length?token(current,queryHash,dataHash,null,expected.at(-1),options.cursor):null;
      if(options.cursor)cursors.delete(options.cursor);
      return {...result,cursor,limits:{...(options.limits??windowLimits)},witness:{sources:witness.records.length,sourceBytes:witness.sourceBytes,scannedKeys:witness.scannedKeys,metrics:tx.inspect().metrics}};
    });
  }
  async function authorizedPage(context,query,options={}){
    context=fixedContext(context);
    exact(query,['model','index','partition','range','limit'],['model','index','partition','limit']);query=structuredClone(query);
    const budgets=['maxDiscovery','maxStreams','maxCandidates','maxRefills','maxNodeVisits','maxBatchCalls','maxReadBytes','maxOutputBytes','maxScannedKeys','batchSize'];
    fields(options,['cursor','getAuthority','onMetrics',...budgets],['getAuthority']);
    if(typeof options.getAuthority!=='function'||options.onMetrics!==undefined&&typeof options.onMetrics!=='function')fail('INVALID_READ','Invalid authority provider');
    const {cursor,getAuthority:provider,onMetrics,...inputLimits}=options;canonical(inputLimits);
    if(!Object.values(inputLimits).every(natural)||inputLimits.batchSize===0)fail('INVALID_READ','Invalid authorized page limits');
    const readOptions={...structuredClone(inputLimits),getAuthority:provider,...(onMetrics===undefined?{}:{onMetrics})};
    return activeStore.read(async(tx,current)=>{
      const prepared=await readOptions.getAuthority?.(tx),getAuthority=async()=>prepared;
      let initialDiscovery=0;const discoveryLimit=readOptions.maxDiscovery??10000;
      const scope=await resolveRequestAuthority(tx,catalog,context,getAuthority,()=>{if(++initialDiscovery>discoveryLimit)fail('WORK_LIMIT','Authorized discovery budget exceeded');});
      const authorityHash=fingerprint(scope),held=cursor===undefined?undefined:typeof cursor==='string'&&cursors.get(cursor);
      if(cursor!==undefined&&!held)fail('INVALID_CURSOR','Unknown or consumed cursor');
      if(held&&(held.generation!==current.generation||held.authorityHash!==authorityHash))fail('STALE_CURSOR','Cursor generation or authority changed');
      if(typeof query.model!=='string'||!Object.hasOwn(catalog.models,query.model)||!permits(scope,{tenant,model:query.model,owner:scope.actor},'read'))fail('FORBIDDEN','Operation is not permitted');
      // R4's reader remains the query/policy authority. This independent inventory
      // supplies continuation state; it never discloses its hidden rows or counts.
      const index=resolveIndex(catalog,query.index),view=index.view,contract=catalog.views[view].contract,model=catalog.models[query.model];
      const eligible=model.memberships.flatMap(m=>{
        if(m.view!==view)return [];
        const positions=m.partition.flatMap((s,i)=>s.header==='owner'&&s.path===undefined?[i]:[]);
        return positions.length===1&&contract.partition[positions[0]]==='id'?[{membership:m,position:positions[0]}]:[];
      });
      const positions=new Set(eligible.map(binding=>binding.position));
      if(positions.size!==1)fail('INVALID_READ','Structural page requires a trusted owner role');
      const ownerPosition=[...positions][0];
      if(!Array.isArray(query.partition)||query.partition.length!==contract.partition.length-1||!natural(query.limit))fail('INVALID_READ','Invalid structural page');
      const slots=new Set(eligible.map(binding=>binding.membership.slot));
      let n=0;const partition=contract.partition.map((d,i)=>i===ownerPosition?null:decodeDomain(d,query.partition[n++])),range=normalizeRange(contract.order,Object.hasOwn(query,'range')?query.range:{});
      const fixed={model:query.model,index:index.signature,partition:partition.filter((_,i)=>i!==ownerPosition),range,limit:query.limit};
      const witness=await sources(tx),records=new Map(witness.records.map(r=>[r.id,r])),all=[];
      for(const tree of sourceOracle(catalog,witness.records).trees){
        if(tree.view!==view||tree.partition.some((v,i)=>i===ownerPosition?!scope.accessible.includes(v):canonical(v)!==canonical(partition[i])))continue;
        for(const row of tree.members){const record=records.get(row.sourceId);if(record.model===query.model&&slots.has(row.slot)&&permits(scope,record,'read')&&containsRange(contract.order,row,range))all.push(row);}
      }
      all.sort((a,b)=>compareKey(contract.order,a,b));
      const queryHash=fingerprint(['authorized',context,fixed]),dataHash=fingerprint(all.map(row=>[row.sourceId,row.slot,row.order,records.get(row.sourceId)]));
      const last=resume(cursor,current,queryHash,dataHash,authorityHash),remaining=last?all.filter(row=>compareKey(contract.order,row,last)>0):all,page=remaining.slice(0,query.limit);
      const narrowed={...query,range:last?{...range,lower:{value:last.order,sourceId:last.sourceId,slot:last.slot,inclusive:false}}:range};
      let readerMetrics;
      const result=await readAuthorizedInTransaction(tx,catalog,context,narrowed,{...readOptions,maxDiscovery:discoveryLimit-initialDiscovery,getAuthority,onMetrics:metrics=>{readerMetrics=metrics;}});
      if(canonical(result.records)!==canonical(page.map(row=>records.get(row.sourceId))))fail('INTEGRITY','Authorized page differs from source facts');
      if(readOptions.onMetrics)await readOptions.onMetrics({...readerMetrics,discovery:readerMetrics.discovery+initialDiscovery,continuationWitness:{authorityDiscovery:initialDiscovery,sources:witness.records.length,sourceBytes:witness.sourceBytes,scannedKeys:witness.scannedKeys}});
      const next=page.length&&remaining.length>page.length?token(current,queryHash,dataHash,authorityHash,{order:page.at(-1).order,sourceId:page.at(-1).sourceId,slot:page.at(-1).slot},cursor):null;
      if(cursor)cursors.delete(cursor);return {records:result.records,cursor:next};
    });
  }
  async function retire(){
    return store.transact(async tx=>{
      const current=await state(tx),ready=current.retired.filter(g=>!readers.has(g));if(!ready.length)return {removed:0,metrics:tx.inspect()};
      const rows=await tx.scan(),keys=rows.filter(row=>ready.some(g=>g===0?isOwned(row.key):row.key.startsWith(prefix(g))));
      for(const row of keys)await tx.delete(row.key);
      await tx.set(activeKey,{...current,retired:current.retired.filter(g=>!ready.includes(g))});return {removed:keys.length,metrics:tx.inspect()};
    });
  }
  async function rebuild(options={}){
    fields(options,['beforeCommit','mutation']);
    if(options.beforeCommit!==undefined&&typeof options.beforeCommit!=='function')fail('INVALID_ENGINE','Invalid publication callback');
    const {beforeCommit,mutation:mutationOptions={}}=options;
    exact(mutationOptions,['maxProducers','maxEdges','maxRouteVisits','maxPotentialVisits'],[]);
    if(!Object.values(mutationOptions).every(natural))fail('INVALID_ENGINE','Invalid rebuild mutation budgets');
    const fixedMutation=structuredClone(mutationOptions);
    const result=await store.transact(async tx=>{
      const current=await state(tx),logical=transaction(tx,current.generation),witness=await sources(logical),scratch=createMemoryStore();
      const ledger=await logical.scan({prefix:reservationPrefix(catalog,tenant)});
      await scratch.transact(async target=>{for(const row of ledger)await target.set(row.key,row.value);});
      const built=await mutate(scratch,catalog,tenant,witness.records.filter(r=>!catalog.models[r.model]?.managed).map(record=>({type:'put',record})),fixedMutation);
      const checked=await checkStore(scratch,catalog,tenant),snapshot=await scratch.read(target=>target.scan({limit:caps.maxCandidateKeys+1}));
      if(snapshot.length>caps.maxCandidateKeys)fail('WORK_LIMIT','Candidate key budget exceeded');
      const candidateBytes=snapshot.reduce((n,row)=>n+Buffer.byteLength(row.key)+bytes(row.value),0);
      if(candidateBytes>caps.maxCandidateBytes)fail('WORK_LIMIT','Candidate byte budget exceeded');
      const original=witness.records.toSorted((a,b)=>text(a.id,b.id)),candidate=snapshot.filter(row=>row.key.startsWith(sourcePrefix(tenant))).map(row=>row.value).sort((a,b)=>text(a.id,b.id));
      if(canonical(original)!==canonical(candidate))fail('REBUILD_SOURCE_MISMATCH','Rebuild changes authoritative source records');
      const generation=current.generation+1;if(!natural(generation))fail('GENERATION_OVERFLOW','Generation counter exhausted');
      if((await tx.scan({prefix:prefix(generation),limit:1})).length)fail('CORRUPT_STATE','Candidate generation namespace is occupied');
      const derived=snapshot.filter(row=>isOwned(row.key));for(const row of derived)await tx.set(prefix(generation)+row.key,row.value);
      const publicationCheck=await checkStore({read:callback=>callback(transaction(tx,generation))},catalog,tenant);
      await tx.set(catalogKey(tenant),catalog.identity);await tx.set(activeKey,{catalog:catalog.identity,generation,retired:[...current.retired,current.generation]});
      return {generation,check:checked,publicationCheck,sourceDigest:fingerprint(original),sources:original.length,sourceBytes:witness.sourceBytes,candidateKeys:derived.length,candidateBytes,build:built.metrics,activation:tx.inspect()};
    },{beforeCommit});
    let retirement;try{retirement=await retire();}catch(error){retirement={pending:true,error:{code:error.code??'ERROR',message:error.message}};}
    return {...result,retirement};
  }
  async function inspect(){
    return activeStore.read(async(tx,current)=>{
      const witness=await sources(tx),derived=(await tx.scan()).filter(row=>isOwned(row.key));
      return {catalog:catalog.identity,tenant,generation:current.generation,limits:{...caps},sources:witness.records.length,sourceBytes:witness.sourceBytes,derivedKeys:derived.length,derivedBytes:derived.reduce((n,row)=>n+Buffer.byteLength(row.key)+bytes(row.value),0),cursors:cursors.size,readers:[...readers.values()].reduce((a,b)=>a+b,0)-1,metrics:tx.inspect()};
    });
  }
  return Object.freeze({catalog,tenant,store:activeStore,read:activeStore.read,transact:activeStore.transact,
    ...(activeStore.getReceipt?{getReceipt:activeStore.getReceipt}:{}),
    mutate:(changes,options)=>mutate(activeStore,catalog,tenant,changes,options),
    mutateAuthorized:async(context,changes,options)=>mutateAuthorized(activeStore,catalog,fixedContext(context),changes,options),
    readAuthorized:async(context,query,options)=>readAuthorized(activeStore,catalog,fixedContext(context),query,options),
    window,authorizedPage,rebuild,retire,inspect,check:()=>checkStore(activeStore,catalog,tenant),
    releaseCursor:id=>cursors.delete(id),
  });
}
