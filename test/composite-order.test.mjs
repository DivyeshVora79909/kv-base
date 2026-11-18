import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog, createMemoryStore, mutate, checkStore, settlementOracle, generatedId, indexId, decodeDomain, compareDomain, treeSelect, treeGet, treeAggregate, treeReadWindow, prepareAuthority, readAuthorized } from '../src/index.mjs';
import { treePut, treeDelete } from '../src/tree.mjs';
import { replaceRoutes, affectedSubscribers } from '../src/routes.mjs';
import { replacePotential, potentialPredecessors } from '../src/potential.mjs';
import { sourcePrefix, sourceKey, allNodesPrefix, catalogKey } from '../src/keys.mjs';
import { standardMonoids } from '../src/monoids.mjs';

// These comparisons and reductions intentionally use only source values, not the
// engine's domain/range/projection functions or tree traversal.
const text = (a,b) => Math.sign(Buffer.compare(Buffer.from(a),Buffer.from(b)));
const tuple = (a,b) => {
  for (let i=0;i<a.length;i++) {
    const c = typeof a[i]==='string' ? text(a[i],b[i]) : a[i]<b[i]?-1:a[i]>b[i]?1:0;
    if(c)return c;
  }
  return 0;
};
const total = (a,b) => tuple(a.order,b.order)||text(a.sourceId,b.sourceId)||text(a.slot,b.slot);
const matches = (row,range) => {
  for(const side of ['lower','upper']) {
    const cut=range[side];if(!cut)continue;
    const c=tuple(row.order,cut.value)||(Object.hasOwn(cut,'sourceId')?text(row.sourceId,cut.sourceId)||text(row.slot,cut.slot):0);
    if(side==='lower'?c<0||c===0&&cut.inclusive===false:c>0||c===0&&cut.inclusive===false)return false;
  }
  return true;
};
const fold = rows => {
  let sum=0n,minPrefix=0n;
  for(const row of rows){sum+=row.contributions.sum;if(sum<minPrefix)minPrefix=sum;}
  return {sum,prefix:{sum,minPrefix}};
};
const limits={maxNodes:10000,maxBatches:10000,maxBytes:10_000_000,maxOutput:10000,batchKeys:7};
const rawCatalog={identity:'independent-tuples-v2',models:{Entry:{schema:{properties:{amount:{}}}}},views:{v:{contract:{partition:['id'],order:['integer','string','boolean'],aggregates:{sum:{implementation:standardMonoids.sum},prefix:{implementation:standardMonoids.prefix}}}}}};
const scope={tenant:'t',view:'v',partition:['book']};

test('composite domains preserve component order, exact integers and strict tuple arity',()=>{
  const domains=['integer','string','boolean','id'],wire=['900719925474099312345','😀',false,'x'];
  const decoded=decodeDomain(domains,wire);
  assert.deepEqual(decoded,[900719925474099312345n,'😀',false,'x']);
  assert.notEqual(decoded,wire);
  assert.equal(compareDomain(domains,decoded,[decoded[0],'😀',true,'a']),-1);
  assert.equal(compareDomain(domains,decoded,[decoded[0],'\uE000',false,'x']),1);
  assert.equal(compareDomain(domains,decoded,[decoded[0]+1n,'a',false,'x']),-1);
  for(const value of [undefined,[],wire.slice(0,3),[...wire,'extra'],['01','a',false,'x'],[1,'a',false,'x'],['1','a',0,'x']])assert.throws(()=>decodeDomain(domains,value),{code:'INVALID_VALUE'});
  const accessor=[...wire];let accessed=false;
  Object.defineProperty(accessor,'0',{enumerable:true,get(){accessed=true;return wire[0];}});
  assert.throws(()=>decodeDomain(domains,accessor),{code:'INVALID_VALUE'});assert.equal(accessed,false);
  const extended=[...wire];extended.extra='unsupported';
  assert.throws(()=>decodeDomain(domains,extended),{code:'INVALID_VALUE'});
});

test('composite trees agree with independent sorting, ranks, ties, noncommutative folds and bounded pages',async()=>{
  const store=createMemoryStore(),rows=[];
  for(const major of [900719925474099312345n,900719925474099312346n])for(const minor of ['a','é','\uE000','😀'])for(const flag of [false,true])for(const slot of ['first','second']){
    const amount=BigInt(rows.length%9)-4n;
    rows.push({...scope,sourceId:`s${String(rows.length>>1).padStart(3,'0')}`,slot,order:[major,minor,flag],contributions:{sum:amount,prefix:{sum:amount,minPrefix:amount<0n?amount:0n}}});
  }
  await store.transact(async tx=>{for(const row of rows.toReversed())await treePut(tx,rawCatalog,row);});
  const cuts=[undefined,{value:[rows[0].order[0],'a',false]},{value:[rows[0].order[0],'é',true],inclusive:false},{value:[rows[0].order[0],'😀',true]},{value:[rows[0].order[0]+1n,'a',false]}];
  const sorted=rows.toSorted(total);
  for(const row of sorted.filter((_,i)=>i%11===0))for(const inclusive of [false,true])cuts.push({value:row.order,sourceId:row.sourceId,slot:row.slot,inclusive});
  await store.read(async tx=>{
    for(let rank=0;rank<sorted.length;rank++) {
      const node=await treeSelect(tx,rawCatalog,scope,rank);
      assert.deepEqual([node.sourceId,node.slot,node.order],[sorted[rank].sourceId,sorted[rank].slot,sorted[rank].order]);
      assert.deepEqual((await treeGet(tx,rawCatalog,scope,sorted[rank])).contributions,sorted[rank].contributions);
    }
    assert.equal(await treeSelect(tx,rawCatalog,scope,sorted.length),undefined);
    for(const lower of cuts)for(const upper of cuts) {
      const range={lower,upper},expected=sorted.filter(row=>matches(row,range));
      assert.deepEqual({...await treeAggregate(tx,rawCatalog,scope,range)},fold(expected));
      const actual=await treeReadWindow(tx,rawCatalog,scope,{range,limits});
      assert.deepEqual(actual.occurrences,expected.map(row=>({rank:sorted.indexOf(row),sourceId:row.sourceId,slot:row.slot,order:row.order})));
    }
  });
  const before=rows[0],moved={...before,order:[before.order[0],'😀',before.order[2]]};
  await store.transact(async tx=>{assert.equal(await treeDelete(tx,rawCatalog,before),true);await treePut(tx,rawCatalog,moved);});
  const final=rows.map(row=>row===before?moved:row).toSorted(total);
  await store.read(async tx=>{
    assert.deepEqual({...await treeAggregate(tx,rawCatalog,scope)},fold(final));
    const result=await treeReadWindow(tx,rawCatalog,scope,{ranks:{start:3,end:17},limits});
    assert.deepEqual(result.occurrences,final.slice(3,17).map((row,i)=>({rank:i+3,sourceId:row.sourceId,slot:row.slot,order:row.order})));
  });
});

test('composite interval routing reaches old and new groups and complete cuts by direct matching',async()=>{
  const store=createMemoryStore(),registrations=new Map(),major=900719925474099312345n;
  const cut=(minor,inclusive=true)=>({value:[major,minor,false],inclusive});
  for(let n=0;n<16;n++){
    const subscriber=`r${String(n).padStart(2,'0')}`,rs=[{slot:'range',kind:'range',view:'v',partition:['book'],aggregate:'sum',...(n%3?{lower:cut(n%2?'é':'a',!!(n%2))}:{}),...(n%4?{upper:cut(n%2?'😀':'é',!!(n%3))}:{})}];
    if(n%5===0)rs.push({slot:'complete',kind:'range',view:'v',partition:['book'],aggregate:'sum',lower:{...cut('é'),sourceId:'source',slot:'second'},upper:cut('é')});
    registrations.set(subscriber,rs);
    await store.transact(tx=>replaceRoutes(tx,rawCatalog,'t',subscriber,rs));
  }
  const old={view:'v',partition:['book'],aggregate:'sum',sourceId:'source',slot:'first',order:[major,'a',false]},next={...old,order:[major,'😀',false]},tie={...old,order:[major,'é',false],slot:'second'};
  await store.read(async tx=>{
    for(const points of [[old],[next],[tie],[old,next,tie]]){
      const expected=[];
      for(const [subscriber,rs] of registrations)for(const r of rs)if(points.some(p=>matches(p,r)))expected.push({subscriber,slot:r.slot});
      expected.sort((a,b)=>text(a.subscriber,b.subscriber)||text(a.slot,b.slot));
      assert.deepEqual((await affectedSubscribers(tx,rawCatalog,'t',{points})).matches,expected);
    }
    await assert.rejects(affectedSubscribers(tx,rawCatalog,'t',{points:[next]},{maxVisits:0}),{code:'ROUTE_BUDGET'});
  });
});

const integer={type:'string',pattern:'^(0|-?[1-9][0-9]*)$'},str={type:'string'},field=name=>({field:name});
const schema=properties=>({type:'object',additionalProperties:false,properties,required:Object.keys(properties)});
const signature=['Ledger',['group'],['major','minor']];
const member=()=>({slot:'main',index:signature,partition:[field('group')],order:[field('major'),field('minor')],inputs:{amount:field('amount')},onMissing:'reject'});
const record=(id,model,data)=>({id,tenant:'t',owner:'o',model,createdAt:'1',data});
const fact=(id,group,minor,amount)=>record(id,'Entry',{group,major:'5',minor:String(minor),amount:String(amount),label:id});
const anchor=(id,data)=>record(id,'Anchor',{mode:'range',source:'none',group:'inputs',outputGroup:id,major:'5',minor:'0',bound:'0',...data});
function fixture(){
  const definition={name:'composite-structural',version:'2',models:{
    Ledger:{schema:schema({}),indexes:[{version:'1',partitionFields:signature[1],orderFields:signature[2],roles:{group:'id',major:'integer',minor:'integer'},inputs:{amount:'integer'},aggregates:{sum:{monoid:'sum',input:'amount'},prefix:{monoid:'prefix',input:'amount'}}}]},
    Entry:{schema:schema({group:str,major:integer,minor:integer,amount:integer,label:str}),memberships:[member()]},
    Managed:{managed:true,schema:schema({group:str,major:integer,minor:integer,amount:integer}),memberships:[member()]},
    Anchor:{schema:schema({mode:str,source:str,group:str,outputGroup:str,major:integer,minor:integer,bound:integer}),calculations:[{slot:'fold',use:'fold'}]},
  }};
  const calculations={fold:{version:'1',resolve({record:r}){
    const d=r.data,reads=d.mode==='direct'?[{slot:'value',kind:'field',sourceId:d.source,selector:field('amount')}]:[{slot:'value',kind:'range',index:signature,partition:[d.group],aggregate:'sum',...(d.mode==='upper'?{upper:{value:[d.major,d.bound]}}:d.mode==='lower'?{lower:{value:[d.major,d.bound]}}:{})}];
    return {parameters:{},reads,outputs:[{role:'value',model:'Managed',owner:r.owner,createdAt:r.createdAt,data:{group:d.outputGroup,major:d.major,minor:d.minor,amount:'0'},computed:['amount']}]};
  },evaluate({values}){const v=values.value;return {outputs:{value:{amount:String(typeof v==='bigint'?v:v.present?BigInt(v.value):0n)}}};}}};
  return {catalog:prepareCatalog(definition,{calculations}),definition,calculations};
}
const put=record=>({type:'put',record});
const output=id=>generatedId('t',id,'fold','value');
async function verify(store,catalog,authored){
  await checkStore(store,catalog,'t');
  const expected=await settlementOracle(catalog,'t',authored);
  const actual=await store.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).map(row=>row.value));
  assert.deepEqual(actual.filter(r=>r.model==='Managed').toSorted((a,b)=>text(a.id,b.id)),expected.generated.toSorted((a,b)=>text(a.id,b.id)));
  // Independent source inventory with manually mapped roles and sort.
  const rows=actual.filter(r=>['Entry','Managed'].includes(r.model)).map(r=>({sourceId:r.id,slot:'main',group:r.data.group,order:[BigInt(r.data.major),BigInt(r.data.minor)],amount:BigInt(r.data.amount)}));
  for(const group of new Set(rows.map(row=>row.group)))await store.read(async tx=>{
    const expectedRows=rows.filter(row=>row.group===group).toSorted(total);
    const page=await treeReadWindow(tx,catalog,{tenant:'t',index:signature,partition:[group]},{limits});
    assert.deepEqual(page.occurrences,expectedRows.map((row,rank)=>({rank,sourceId:row.sourceId,slot:row.slot,order:row.order})));
  });
}

test('second-component-only source and potential-output moves settle both intervals with no-op occurrence writes',async()=>{
  const {catalog}=fixture(),store=createMemoryStore(),records=[fact('f','inputs',2,10),anchor('L',{mode:'upper',bound:'4'}),anchor('H',{mode:'lower',bound:'6'})];
  await mutate(store,catalog,'t',records.map(put));
  const amount=id=>store.read(async tx=>(await tx.get(sourceKey('t',output(id)))).data.amount);
  assert.deepEqual(await Promise.all(['L','H'].map(amount)),['10','0']);
  records[0].data.minor='8';let changes;
  const moved=await mutate(store,catalog,'t',[put(records[0])],{beforeCommit:event=>{changes=event.changes;}});
  assert.equal(moved.settlement.evaluated,2);
  assert.deepEqual(await Promise.all(['L','H'].map(amount)),['0','10']);
  assert.ok(changes.some(change=>change.key.startsWith(allNodesPrefix(catalog,'t'))));
  await verify(store,catalog,records);
  const noop=await mutate(store,catalog,'t',[put(records[0])],{beforeCommit:event=>{assert.deepEqual(event.changes,[]);}});
  assert.equal(noop.changedMemberships,0);assert.equal(noop.metrics.metrics.stagedWrites,0);
  records[0].data.label='payload-only edit';
  const payload=await mutate(store,catalog,'t',[put(records[0])],{beforeCommit:event=>{
    assert.equal(event.changes.length,1);
    assert.equal(event.changes[0].key,sourceKey('t',records[0].id));
  }});
  assert.equal(payload.changedSources,1);assert.equal(payload.changedMemberships,0);
  // Now move only a producer template's second component, with the underlying
  // source unchanged. Its old and final potential intervals drive graph repair.
  const seed=fact('seed','base',1,7),producer=anchor('P',{mode:'direct',source:'seed',outputGroup:'managed-inputs',minor:'2'}),left=anchor('PL',{mode:'upper',group:'managed-inputs',bound:'4'}),right=anchor('PH',{mode:'lower',group:'managed-inputs',bound:'6'});
  records.push(seed,producer,left,right);await mutate(store,catalog,'t',[seed,producer,left,right].map(put));
  assert.deepEqual(await Promise.all(['PL','PH'].map(amount)),['7','0']);
  producer.data.minor='8';const changed=await mutate(store,catalog,'t',[put(producer)]);
  assert.equal(changed.settlement.evaluated,3);
  assert.deepEqual(await Promise.all(['PL','PH'].map(amount)),['0','7']);
  await verify(store,catalog,records);
});

test('potential composite matches and ties agree with independently declared output positions',async()=>{
  const {catalog}=fixture(),store=createMemoryStore(),view=indexId(signature),templates=[];
  for(let n=0;n<8;n++)templates.push({producer:`p${n}`,outputs:[{role:'value',record:record(`out${n}`,'Managed',{group:'g',major:'5',minor:String(n%3),amount:'0'}),computed:['amount']}]});
  await store.transact(async tx=>{for(const t of templates)await replacePotential(tx,catalog,'t',t.producer,[],t.outputs);});
  const cuts=[{}, {lower:{value:[5n,1n]},upper:{value:[5n,1n]}}, {lower:{value:[5n,1n],sourceId:'out4',slot:'main',inclusive:false}}, {upper:{value:[5n,1n],inclusive:false}}];
  await store.read(async tx=>{
    for(const range of cuts){
      const read={slot:'q',kind:'range',view,partition:['g'],aggregate:'sum',...range};
      const expected=templates.filter(t=>matches({order:[5n,BigInt(t.outputs[0].record.data.minor)],sourceId:t.outputs[0].record.id,slot:'main'},range)).map(t=>t.producer).sort(text);
      assert.deepEqual((await potentialPredecessors(tx,catalog,'t',[read])).bySlot,[{slot:'q',producers:expected}]);
    }
  });
});

test('representation identities reject incompatible state atomically',async()=>{
  const {catalog}=fixture(),store=createMemoryStore();
  assert.equal(catalog.representationVersion,2);
  await store.transact(tx=>tx.set(catalogKey('t'),'pre-tuple-v1'));
  const before=await store.read(tx=>tx.scan());
  await assert.rejects(mutate(store,catalog,'t',[put(fact('x','g',1,1))]),{code:'CATALOG_MISMATCH'});
  assert.deepEqual(await store.read(tx=>tx.scan()),before);
});


test('authorized owner heads merge composite order tuples and equal-key ties within budgets',async()=>{
  const structural=['Entry',['group','owner'],['major','minor']],view=indexId(structural);
  const definition={name:'composite-owner-merge',version:'2',models:{Entry:{
    schema:schema({group:str,major:integer,minor:integer,amount:integer,label:str}),
    indexes:[{version:'1',partitionFields:structural[1],orderFields:structural[2],roles:{group:'id',owner:'id',major:'integer',minor:'integer'},inputs:{amount:'integer'},aggregates:{sum:{monoid:'sum',input:'amount'}}}],
    memberships:[{slot:'main',index:structural,partition:[field('group'),{header:'owner'}],order:[field('major'),field('minor')],inputs:{amount:field('amount')},onMissing:'reject'}],
    access:{orders:{tuple:{view,slot:'main',ownerPartition:1}}},
  }}};
  const catalog=prepareCatalog(definition),store=createMemoryStore();
  const records=[['z','u','2'],['a','g','2'],['late','g','9'],['early','u','1'],['hidden','x','0']].map(([id,owner,minor])=>({...fact(id,'g',minor,1),owner}));
  await mutate(store,catalog,'t',records.map(put));
  const principal=(id,kind,parents,resources={})=>({id,kind,parents,active:true,grants:{admin:false,resources}});
  const administrator=principal('admin','user',['t']);administrator.grants.admin=true;
  const authority=prepareAuthority(catalog,{tenant:'t',principals:[principal('t','tenant',[]),administrator,principal('g','group',['t'],{Entry:['read']}),principal('u','user',['g']),principal('x','user',['g'])]});
  const options={getAuthority:async()=>authority,maxStreams:3,maxCandidates:8,maxRefills:8,maxNodeVisits:20,maxReadBytes:100000,maxOutputBytes:100000};
  const expected=records.filter(r=>['u','g'].includes(r.owner)).toSorted((a,b)=>tuple([BigInt(a.data.major),BigInt(a.data.minor)],[BigInt(b.data.major),BigInt(b.data.minor)])||text(a.id,b.id));
  const query={model:'Entry',order:'tuple',partition:['g'],limit:10};
  assert.deepEqual((await readAuthorized(store,catalog,{tenant:'t',actor:'u'},query,options)).records,expected);
  assert.deepEqual((await readAuthorized(store,catalog,{tenant:'t',actor:'u'},{...query,range:{lower:{value:[5n,2n]},upper:{value:[5n,2n]}}},options)).records,expected.filter(r=>r.data.minor==='2'));
  await assert.rejects(readAuthorized(store,catalog,{tenant:'t',actor:'u'},query,{...options,maxCandidates:0}),{code:'WORK_LIMIT'});
});

test('structural range registrations and change points share the exact composite identity',async()=>{
  const {catalog}=fixture(),store=createMemoryStore();
  await store.transact(tx=>replaceRoutes(tx,catalog,'t','subscriber',[{slot:'q',kind:'range',index:signature,partition:['g'],aggregate:'sum',lower:{value:['5','1']},upper:{value:['5','1']}}]));
  await store.read(async tx=>{
    const point={index:signature,partition:['g'],aggregate:'sum',sourceId:'x',slot:'main',order:['5','1']};
    assert.deepEqual((await affectedSubscribers(tx,catalog,'t',{points:[point]})).matches,[{subscriber:'subscriber',slot:'q'}]);
    assert.deepEqual((await affectedSubscribers(tx,catalog,'t',{points:[{...point,order:['5','2']}]})).matches,[]);
  });
});
