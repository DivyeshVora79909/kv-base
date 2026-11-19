import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog, createMemoryStore, indexId, prepareAuthority, treeAggregate, canonical } from '../src/index.mjs';
import { createEngine } from '../src/lifecycle.mjs';
import { sourcePrefix, sourceKey, allNodesPrefix, reservationPrefix } from '../src/keys.mjs';
import { causalDefinition,causalCalculations,causalDiamond,causalOutput } from '../examples/causal.mjs';
const integer={type:'string',pattern:'^(0|-?[1-9][0-9]*)$'},str={type:'string'},field=name=>({field:name});
const schema=properties=>({type:'object',additionalProperties:false,properties,required:Object.keys(properties)});
const business=['Ledger',['group'],['major','minor']],owned=['Ledger',['group','owner'],['major','minor']];
const binding=(slot,index,partition)=>({slot,index,partition,order:[field('major'),field('minor')],inputs:{amount:field('amount')},onMissing:'reject'});
const row=(id,owner,minor,group='hot')=>({id,tenant:'t',owner,model:'Entry',createdAt:'1',data:{group,major:'5',minor:String(minor),amount:'1',label:id}});
function definition(){return {name:'lifecycle',version:'1',models:{
  Ledger:{schema:schema({}),indexes:[business,owned].map(index=>({version:'1',partitionFields:index[1],orderFields:index[2],roles:Object.fromEntries([...index[1],...index[2]].map(role=>[role,['major','minor'].includes(role)?'integer':'id'])),inputs:{amount:'integer'},aggregates:{total:{monoid:'sum',input:'amount'}}}))},
  Entry:{schema:schema({group:str,major:integer,minor:integer,amount:integer,label:str}),memberships:[binding('business',business,[field('group')]),binding('owned',owned,[field('group'),{header:'owner'}])],access:{orders:{}}},
}};}
function graph(){const principal=(id,kind,parents,resources={},admin=false)=>({id,kind,parents,active:true,grants:{admin,resources}});return {tenant:'t',principals:[principal('t','tenant',[]),principal('admin','user',['t'],{},true),principal('g','group',['t'],{Entry:['read','create','update','delete']}),principal('u','user',['g']),principal('x','user',['g'])]};}
async function setup(){
  const catalog=prepareCatalog(definition()),store=createMemoryStore(),engine=createEngine({store,catalog,tenant:'t'}),records=[row('a','u',1),row('b','g',1),row('c','u',2),row('d','g',3),row('hidden','x',0),row('other','u',0,'elsewhere')];
  await engine.mutate(records.map(record=>({type:'put',record})));await engine.transact(tx=>tx.set('fixture-policy/t',graph()));
  const options={getAuthority:async tx=>prepareAuthority(catalog,await tx.get('fixture-policy/t'))};return {catalog,store,engine,records,options};
}
const query={index:business,partition:['hot'],range:{lower:{value:['5','1']}},limit:1};
const authorizedQuery={model:'Entry',index:owned,partition:['hot'],limit:1};
const snapshot=store=>store.read(tx=>tx.scan());
const sources=engine=>engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).map(row=>row.value).sort((a,b)=>a.id.localeCompare(b.id)));

test('raw continuation preserves complete equal-tuple positions and progresses after unrelated edits',async()=>{
  const {engine,records}=await setup(),ids=[];let page=await engine.window(query);
  ids.push(...page.occurrences.map(row=>row.sourceId));assert.equal(typeof page.cursor,'string');assert.equal(page.cursor.length,32);
  records.at(-1).data.label='unrelated payload';await engine.mutate([{type:'put',record:records.at(-1)}]);
  while(page.cursor){page=await engine.window(query,{cursor:page.cursor});ids.push(...page.occurrences.map(row=>row.sourceId));}
  assert.deepEqual(ids,['a','b','c','d']);await engine.check();
});

test('raw cursors reject relevant payload, owner, membership, query and token changes',async()=>{
  for(const change of ['payload','owner','order','insert','query','token']){
    const {engine,records}=await setup(),page=await engine.window(query);
    if(change==='payload')records[2].data.label='changed body';
    if(change==='owner')records[2].owner='g';
    if(change==='order')records[2].data.minor='9';
    if(['payload','owner','order'].includes(change))await engine.mutate([{type:'put',record:records[2]}]);
    if(change==='insert')await engine.mutate([{type:'put',record:row('new','u',2)}]);
    const request=change==='query'?{...query,limit:2}:query,cursor=change==='token'?page.cursor+'tampered':page.cursor;
    await assert.rejects(engine.window(request,{cursor}),{code:change==='token'?'INVALID_CURSOR':'STALE_CURSOR'},change);
  }
});

test('authorized cursors fence transfer and revocation while exposing only permitted rows and opaque handles',async()=>{
  const {engine,records,options}=await setup(),first=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options);
  assert.deepEqual(Object.keys(first).sort(),['cursor','records']);assert.deepEqual(first.records.map(r=>r.id),['a']);
  const resumed=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:first.cursor});assert.deepEqual(resumed.records.map(r=>r.id),['b']);
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:first.cursor}),{code:'INVALID_CURSOR'});
  const beforeTransfer=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options);
  records[2].owner='g';await engine.mutateAuthorized({tenant:'t',actor:'u'},[{type:'put',record:records[2]}],options);
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:beforeTransfer.cursor}),{code:'STALE_CURSOR'});
  const beforeRevoke=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options),beforeSources=await sources(engine);
  await engine.transact(async tx=>{const policy=await tx.get('fixture-policy/t');policy.principals.find(p=>p.id==='g').grants.resources.Entry=[];await tx.set('fixture-policy/t',policy);});
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:beforeRevoke.cursor}),{code:'STALE_CURSOR'});
  assert.deepEqual(await sources(engine),beforeSources);
});

test('authorized continuation allows unrelated partitions and uses one policy/catalog after generation activation',async()=>{
  const {engine,records,options}=await setup(),page=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options);
  records.at(-1).data.label='other partition';await engine.mutate([{type:'put',record:records.at(-1)}]);
  assert.deepEqual((await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:page.cursor})).records.map(r=>r.id),['b']);
  const old=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options),before=await sources(engine),result=await engine.rebuild();
  assert.equal(result.generation,1);assert.deepEqual(await sources(engine),before);
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:old.cursor}),{code:'STALE_CURSOR'});
  assert.deepEqual((await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options)).records.map(r=>r.id),['a']);await engine.check();
});

test('failed candidate publication leaves the complete active generation and its cursor usable',async()=>{
  const {engine,store}=await setup(),page=await engine.window(query),before=await snapshot(store);
  await assert.rejects(engine.rebuild({beforeCommit(){throw Error('activation fault');}}),/activation fault/);
  assert.deepEqual(await snapshot(store),before);assert.equal((await engine.inspect()).generation,0);
  assert.deepEqual((await engine.window(query,{cursor:page.cursor})).occurrences.map(row=>row.sourceId),['b']);await engine.check();
});

test('rebuild drains in-flight reads before activation and retires the old derived generation afterwards',async()=>{
  const {engine,store,catalog}=await setup();let entered,release;
  const ready=new Promise(resolve=>{entered=resolve;}),blocked=new Promise(resolve=>{release=resolve;});
  const reading=engine.read(async(tx,current)=>{assert.equal(current.generation,0);entered();await blocked;assert.ok((await tx.scan({prefix:allNodesPrefix(catalog,'t')})).length);return (await tx.get(sourceKey('t','a'))).data.label;});
  await ready;let activated=false;const rebuilding=engine.rebuild().then(result=>{activated=true;return result;});
  await new Promise(resolve=>setImmediate(resolve));assert.equal(activated,false);
  release();assert.equal(await reading,'a');const result=await rebuilding;
  assert.equal(result.generation,1);assert.ok(result.retirement.removed>0);
  assert.deepEqual(await store.read(tx=>tx.scan({prefix:allNodesPrefix(catalog,'t')})),[]);
  assert.deepEqual((await engine.window(query)).occurrences.map(row=>row.sourceId),['a']);await engine.check();
});

test('candidate reconstructs reactive memberships, routes, potential state and retired reservations from shared sources',async()=>{
  const catalog=prepareCatalog(causalDefinition,{calculations:causalCalculations()}),store=createMemoryStore(),engine=createEngine({store,catalog,tenant:'t'});
  const records=causalDiamond();await engine.mutate(records.map(record=>({type:'put',record})));await engine.mutate([{type:'delete',id:'D'}]);
  const before=await sources(engine),ledger=await engine.read(tx=>tx.scan({prefix:reservationPrefix(catalog,'t')}));assert.equal(ledger.length,3);
  const result=await engine.rebuild();assert.equal(result.check.sources,5);assert.equal(result.sources,5);assert.deepEqual(await sources(engine),before);
  assert.deepEqual(await engine.read(tx=>tx.scan({prefix:reservationPrefix(catalog,'t')})),ledger);
  await engine.mutate([{type:'put',record:records[3]}]);
  assert.equal(await engine.read(async tx=>(await tx.get(sourceKey('t',causalOutput('D')))).data.amount),'50');await engine.check();
});

test('candidate independently repairs index corruption without changing authoritative records',async()=>{
  const {engine,store,catalog}=await setup(),before=await sources(engine);
  await store.transact(async tx=>{const nodes=await tx.scan({prefix:allNodesPrefix(catalog,'t')});assert.ok(nodes.length);await tx.delete(nodes[0].key);});
  await assert.rejects(engine.check());await engine.rebuild();assert.deepEqual(await sources(engine),before);await engine.check();
});

test('continuation and source/candidate budgets fail without publishing partial lifecycle state',async()=>{
  const {engine,store,catalog}=await setup(),before=await snapshot(store);
  const tiny=createEngine({store,catalog,tenant:'t',limits:{maxSources:1}});
  await assert.rejects(tiny.rebuild(),{code:'WORK_LIMIT'});assert.deepEqual(await snapshot(store),before);
  const candidate=createEngine({store,catalog,tenant:'t',limits:{maxCandidateKeys:1}});
  await assert.rejects(candidate.rebuild(),{code:'WORK_LIMIT'});assert.deepEqual(await snapshot(store),before);
  const cursor=createEngine({store,catalog,tenant:'t',limits:{maxCursors:0}});
  await assert.rejects(cursor.window(query),{code:'WORK_LIMIT'});await engine.check();
});

test('generation remapping preserves host policy, shared sources and unrelated catalog namespaces',async()=>{
  const {engine,store,catalog}=await setup(),unrelated='root/other-tenant/unrelated-catalog/partition',host='fixture-host-key';
  await store.transact(async tx=>{await tx.set(unrelated,{head:'outside'});await tx.set(host,{value:'shared'});});
  const before=await sources(engine);await engine.rebuild();await engine.rebuild();
  assert.equal((await engine.inspect()).generation,2);assert.deepEqual(await sources(engine),before);
  assert.deepEqual(await engine.read(tx=>tx.get(unrelated)),{head:'outside'});assert.deepEqual(await engine.read(tx=>tx.get(host)),{value:'shared'});
  assert.deepEqual(await store.read(tx=>tx.get(unrelated)),{head:'outside'});await engine.check();
});

test('authorized continuation rejects visible payload edits, tolerates hidden payload edits and reports witness work privately',async()=>{
  const {engine,records,options}=await setup();let measured;
  const page=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,onMetrics:metrics=>{measured=metrics;}});
  assert.ok(measured.continuationWitness.scannedKeys>0);assert.ok(measured.continuationWitness.sourceBytes>0);
  assert.deepEqual(Object.keys(page).sort(),['cursor','records']);
  records.find(r=>r.id==='hidden').data.label='hidden payload edit';await engine.mutate([{type:'put',record:records.find(r=>r.id==='hidden')}]);
  const resumed=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:page.cursor});assert.deepEqual(resumed.records.map(r=>r.id),['b']);
  const fresh=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options);
  records.find(r=>r.id==='d').data.label='visible payload edit';await engine.mutate([{type:'put',record:records.find(r=>r.id==='d')}]);
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:fresh.cursor}),{code:'STALE_CURSOR'});
});

test('one-slot cursor inventory can advance by replacing consumed handles',async()=>{
  const {store,catalog}=await setup(),engine=createEngine({store,catalog,tenant:'t',limits:{maxCursors:1}}),ids=[];let page=await engine.window(query);
  while(true){ids.push(...page.occurrences.map(row=>row.sourceId));if(!page.cursor)break;page=await engine.window(query,{cursor:page.cursor});}
  assert.deepEqual(ids,['a','b','c','d']);assert.equal((await engine.inspect()).cursors,0);
});

test('mapped ancestor prefixes and logical key ranges preserve ordering and limits after activation',async()=>{
  const {engine,catalog}=await setup();await engine.rebuild();
  await engine.read(async tx=>{
    const all=await tx.scan(),roots=all.filter(row=>row.key.startsWith('root/'));
    assert.ok(roots.length>3);assert.deepEqual(await tx.scan({prefix:'root/'}),roots);
    const compare=(a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b)),low=roots[1].key,high=roots.at(-1).key;
    assert.deepEqual(await tx.scan({prefix:'root/',gte:low,lt:high,limit:2}),roots.filter(row=>compare(row.key,low)>=0&&compare(row.key,high)<0).slice(0,2));
    assert.deepEqual(await tx.scan({gt:low,lte:high,limit:1}),all.filter(row=>compare(row.key,low)>0&&compare(row.key,high)<=0).slice(0,1));
    const ownedPrefix=`root/${canonical('t')}/${catalog.identity}/`;
    assert.deepEqual(await tx.scan({prefix:ownedPrefix,gte:'a',limit:2}),roots.slice(0,2));
  });
});

test('queued windows detach query and limits before awaiting the active snapshot',async()=>{
  const {engine}=await setup();let entered,release;const ready=new Promise(resolve=>{entered=resolve;}),blocked=new Promise(resolve=>{release=resolve;});
  const writer=engine.transact(async()=>{entered();await blocked;});await ready;
  const request=structuredClone(query),options={limits:{maxNodes:100,maxBatches:100,maxBytes:100000,maxOutput:100,batchKeys:8}};
  const reading=engine.window(request,options);request.partition[0]='elsewhere';request.range.lower.value[1]='99';options.limits.maxNodes=0;
  release();await writer;assert.deepEqual((await reading).occurrences.map(row=>row.sourceId),['a']);
});

test('queued authorized pages detach context query and provider/options before awaiting',async()=>{
  const {engine,options}=await setup();let entered,release;const ready=new Promise(resolve=>{entered=resolve;}),blocked=new Promise(resolve=>{release=resolve;});
  const writer=engine.transact(async()=>{entered();await blocked;});await ready;
  const context={tenant:'t',actor:'u'},request=structuredClone(authorizedQuery),input={...options,maxCandidates:100};
  const reading=engine.authorizedPage(context,request,input);context.actor='x';request.partition[0]='elsewhere';input.maxCandidates=0;input.getAuthority=()=>{throw Error('late replacement');};
  release();await writer;assert.deepEqual((await reading).records.map(row=>row.id),['a']);
});

test('public lifecycle requests reject input getters without invoking them and failed resumes keep cursors usable',async()=>{
  const {engine,options}=await setup();let accessed=false;
  const getAuthority={};Object.defineProperty(getAuthority,'getAuthority',{enumerable:true,get(){accessed=true;return options.getAuthority;}});
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,getAuthority));assert.equal(accessed,false);
  const request={};Object.defineProperty(request,'index',{enumerable:true,get(){accessed=true;return business;}});
  await assert.rejects(engine.window(request));assert.equal(accessed,false);
  const rebuilt={};Object.defineProperty(rebuilt,'beforeCommit',{enumerable:true,get(){accessed=true;return ()=>{};}});
  await assert.rejects(engine.rebuild(rebuilt));assert.equal(accessed,false);
  await assert.rejects(engine.window({...query,range:undefined}));
  const page=await engine.window(query);
  await assert.rejects(engine.window(query,{cursor:page.cursor,limits:{maxNodes:0,maxBatches:100,maxBytes:100000,maxOutput:100,batchKeys:8}}),{code:'WORK_LIMIT'});
  assert.deepEqual((await engine.window(query,{cursor:page.cursor})).occurrences.map(row=>row.sourceId),['b']);
});

test('fixed-tenant contextual wrappers reject other tenants before provider or storage access after activation',async()=>{
  const {engine:other,store,catalog,records,options}=await setup();await other.rebuild();
  let io=0,providers=0;
  const isolated=createEngine({catalog,tenant:'isolated',store:{...store,read:callback=>{io++;return store.read(callback);},transact:(callback,options)=>{io++;return store.transact(callback,options);}}});
  const input={...options,getAuthority:async tx=>{providers++;return options.getAuthority(tx);}},before=await snapshot(store),changed=structuredClone(records[0]);changed.data.minor='99';
  await assert.rejects(isolated.mutateAuthorized({tenant:'t',actor:'u'},[{type:'put',record:changed}],input),{code:'FORBIDDEN'});
  await assert.rejects(isolated.readAuthorized({tenant:'t',actor:'u'},authorizedQuery,input),{code:'FORBIDDEN'});
  await assert.rejects(isolated.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,input),{code:'FORBIDDEN'});
  assert.equal(io,0);assert.equal(providers,0);assert.deepEqual(await snapshot(store),before);await other.check();
});

test('authorized continuation excludes string-domain owner bindings from its independent position inventory',async()=>{
  const signature=['Entry',['owner','stringRole'],['major','minor']],definition={name:'owner-domain-witness',version:'1',models:{Entry:{
    schema:schema({group:str,major:integer,minor:integer,amount:integer,label:str}),
    indexes:[{version:'1',partitionFields:signature[1],orderFields:signature[2],roles:{owner:'id',stringRole:'string',major:'integer',minor:'integer'},inputs:{amount:'integer'},aggregates:{total:{monoid:'sum',input:'amount'}}}],
    memberships:[binding('good',signature,[{header:'owner'},{constant:'u'}]),binding('bad',signature,[{constant:'u'},{header:'owner'}])],access:{orders:{}},
  }}};
  const catalog=prepareCatalog(definition),engine=createEngine({catalog,store:createMemoryStore(),tenant:'t'});
  await engine.mutate([row('a','u',1),row('b','u',1)].map(record=>({type:'put',record})));
  const options={getAuthority:async()=>prepareAuthority(catalog,graph())},query={model:'Entry',index:signature,partition:['u'],limit:1};
  const first=await engine.authorizedPage({tenant:'t',actor:'u'},query,options);assert.deepEqual(first.records.map(r=>r.id),['a']);
  const second=await engine.authorizedPage({tenant:'t',actor:'u'},query,{...options,cursor:first.cursor});assert.deepEqual(second.records.map(r=>r.id),['b']);assert.equal(second.cursor,null);
});

test('initial authorized pages enforce Read before structural diagnostics or source witness budgets',async()=>{
  const {store,catalog,options}=await setup();await store.transact(async tx=>{const policy=await tx.get('fixture-policy/t');policy.principals.find(p=>p.id==='g').grants.resources.Entry=[];await tx.set('fixture-policy/t',policy);});
  let scans=0;const engine=createEngine({catalog,tenant:'t',limits:{maxSources:0},store:{...store,read:callback=>store.read(tx=>callback({...tx,scan:options=>{scans++;return tx.scan(options);}}))}});
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},{...authorizedQuery,index:['Unknown',['secret'],['at']]},options),{code:'FORBIDDEN'});assert.equal(scans,0);
  const privateDefinition=definition();delete privateDefinition.models.Entry.access;
  const privateCatalog=prepareCatalog(privateDefinition),privateEngine=createEngine({catalog:privateCatalog,store:createMemoryStore(),tenant:'t',limits:{maxSources:0}});
  await assert.rejects(privateEngine.authorizedPage({tenant:'t',actor:'u'},{...authorizedQuery,index:['Unknown',['secret'],['at']]},{getAuthority:async()=>prepareAuthority(privateCatalog,graph())}),{code:'FORBIDDEN'});
});

test('revoked cursor authority fences before an over-budget source witness and null ranges reject',async()=>{
  const {store,catalog,options}=await setup(),engine=createEngine({store,catalog,tenant:'t',limits:{maxSources:6}}),page=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,options);
  await engine.transact(async tx=>{const policy=await tx.get('fixture-policy/t');policy.principals.find(p=>p.id==='g').grants.resources.Entry=[];await tx.set('fixture-policy/t',policy);});
  await engine.mutate([{type:'put',record:row('extra','u',0,'elsewhere')}]);
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,cursor:page.cursor}),{code:'STALE_CURSOR'});
  const fresh=await setup();await assert.rejects(fresh.engine.window({...query,range:null}),{code:'INVALID_RANGE'});
  await assert.rejects(fresh.engine.authorizedPage({tenant:'t',actor:'u'},{...authorizedQuery,range:null},fresh.options),{code:'INVALID_RANGE'});
});

test('authority preflight and owner-stream reader share one total discovery budget',async()=>{
  const {engine,options}=await setup();let measured;
  await assert.rejects(engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,maxDiscovery:5}),{code:'WORK_LIMIT'});
  const page=await engine.authorizedPage({tenant:'t',actor:'u'},authorizedQuery,{...options,maxDiscovery:6,onMetrics:metrics=>{measured=metrics;}});
  assert.deepEqual(page.records.map(r=>r.id),['a']);assert.equal(measured.discovery,6);assert.equal(measured.continuationWitness.authorityDiscovery,2);
});

test('retirement failure reports pending cleanup after successful activation and permits an explicit retry',async()=>{
  const {store,catalog}=await setup();let failCleanup=true;
  const wrapped={...store,transact:(callback,options={})=>store.transact(callback,{beforeCommit:async event=>{
    if(failCleanup&&event.changes.some(change=>change.type==='delete'&&change.key.startsWith('root/')))throw Object.assign(Error('cleanup fault'),{code:'RETIREMENT_FAULT'});
    if(options.beforeCommit)await options.beforeCommit(event);
  }})};
  const engine=createEngine({store:wrapped,catalog,tenant:'t'}),before=await sources(engine),result=await engine.rebuild();
  assert.equal(result.generation,1);assert.equal(result.retirement.pending,true);assert.equal(result.retirement.error.code,'RETIREMENT_FAULT');
  assert.equal((await engine.inspect()).generation,1);assert.deepEqual(await sources(engine),before);await engine.check();
  assert.ok((await store.read(tx=>tx.scan({prefix:allNodesPrefix(catalog,'t')}))).length);
  failCleanup=false;assert.ok((await engine.retire()).removed>0);assert.deepEqual(await store.read(tx=>tx.scan({prefix:allNodesPrefix(catalog,'t')})),[]);await engine.check();
});
