import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog } from '../src/catalog.mjs';
import { prepareAuthority, resolveAuthority, permits } from '../src/authority.mjs';

const schema={type:'object',properties:{},additionalProperties:false};
const catalog=prepareCatalog({name:'authority-test',version:'1',contracts:{},views:{},models:{item:{schema,access:{orders:{}}},secret:{schema}}});
const grants=(actions=[],admin=false)=>({admin,resources:{item:actions}});
const principal=(id,kind,parents,actions=[],active=true,admin=false)=>({id,kind,parents,active,grants:grants(actions,admin)});
function fixture() {
  return {tenant:'t',principals:[
    principal('t','tenant',[],['read']),principal('admin','user',['t'],[],true,true),
    principal('g0','group',['t'],['create']),principal('g1','group',['g0'],['update']),
    principal('gi','group',['t'],['delete'],false),principal('p','user',['t'],['use']),
    principal('u','user',['g1','gi','p','t']),principal('child','user',['u']),principal('leaf','group',['child']),
    principal('peer','user',['g1']),principal('other','user',['t']),
  ]};
}
const record=(owner='u',model='item',tenant='t')=>({id:'r',tenant,owner,model,createdAt:'0',data:{}});
const sorted=values=>[...values].sort();
const throws=(fn,code='INVALID_AUTHORITY')=>assert.throws(fn,{code});

test('hand-drawn owner DAG separates direct group scope, descendants and action grants',()=>{
  const prepared=prepareAuthority(catalog,fixture()), scope=resolveAuthority(prepared,{tenant:'t',actor:'u'});
  assert.deepEqual(sorted(scope.accessible),['child','g1','gi','leaf','u']);
  assert.deepEqual(sorted(scope.descendants),['child','leaf']);
  for(const owner of ['u','child','leaf','g1','gi']) {
    assert.equal(permits(scope,record(owner),'read'),true,owner);
    assert.equal(permits(scope,record(owner),'update'),true,owner);
    assert.equal(permits(scope,record(owner),'create'),false,'grandparent grant must not inherit');
    assert.equal(permits(scope,record(owner),'delete'),false,'inactive parent grant must not inherit');
    assert.equal(permits(scope,record(owner),'use'),false,'User-parent grant must not inherit');
  }
  for(const owner of ['t','g0','p','peer','other','absent']) assert.equal(permits(scope,record(owner),'read'),false,owner);
  assert.equal(permits(scope,record('u','item','different'),'read'),false);
});

test('delete excludes group parents, admin preserves owner scope, and Use requires effective explicit grant',()=>{
  const graph=fixture(), user=graph.principals.find(p=>p.id==='u');
  user.grants=grants(['delete']);
  let scope=resolveAuthority(prepareAuthority(catalog,graph),{tenant:'t',actor:'u'});
  for(const owner of ['u','child','leaf']) assert.equal(permits(scope,record(owner),'delete'),true);
  for(const owner of ['g1','gi','t','peer']) assert.equal(permits(scope,record(owner),'delete'),false);
  user.grants=grants([],true);
  scope=resolveAuthority(prepareAuthority(catalog,graph),{tenant:'t',actor:'u'});
  assert.equal(permits(scope,record('g1'),'create'),true);
  assert.equal(permits(scope,record('peer'),'read'),false);
  assert.equal(permits(scope,record(),'use'),false);
  graph.principals.find(p=>p.id==='g1').grants.resources.item.push('use');
  scope=resolveAuthority(prepareAuthority(catalog,graph),{tenant:'t',actor:'u'});
  assert.equal(permits(scope,record(),'use'),true,'raw explicit direct Group grant inherits');
  assert.equal(permits(scope,record('u','secret'),'read'),false,'private model remains private for admin');
});

test('groups do not inherit grants; inactive owners remain structural scope',()=>{
  const graph=fixture(); graph.principals.find(p=>p.id==='child').active=false;
  const prepared=prepareAuthority(catalog,graph), group=resolveAuthority(prepared,{tenant:'t',actor:'g1'}), user=resolveAuthority(prepared,{tenant:'t',actor:'u'});
  assert.equal(permits(group,record('g1'),'create'),false);
  assert.equal(permits(group,record('g1'),'read'),false);
  assert.equal(permits(user,record('child'),'read'),true);
  throws(()=>resolveAuthority(prepared,{tenant:'t',actor:'child'}),'FORBIDDEN');
  throws(()=>resolveAuthority(prepared,{tenant:'t',actor:'missing'}),'FORBIDDEN');
  throws(()=>resolveAuthority(prepared,{tenant:'elsewhere',actor:'u'}),'FORBIDDEN');
});

test('principal and parent input permutations preserve the truth table',()=>{
  const graph=fixture(), shuffled=structuredClone(graph);
  shuffled.principals.reverse(); for(const p of shuffled.principals)p.parents.reverse();
  const preparedA=prepareAuthority(catalog,graph),preparedB=prepareAuthority(catalog,shuffled);
  assert.equal(preparedA.identity,preparedB.identity);
  const a=resolveAuthority(preparedA,{tenant:'t',actor:'u'}),b=resolveAuthority(preparedB,{tenant:'t',actor:'u'});
  assert.deepEqual(sorted(a.accessible),sorted(b.accessible));
  for(const owner of graph.principals.map(p=>p.id)) for(const action of ['read','create','update','delete','use']) assert.equal(permits(a,record(owner),action),permits(b,record(owner),action));
});

test('authority validates malformed graph controls and retains an active effective administrator',()=>{
  const changes=[g=>g.principals.push(structuredClone(g.principals[0])),g=>g.principals[0].active=false,g=>g.principals[1].kind='tenant',g=>g.principals[1].parents=[],g=>g.principals[1].parents=['missing'],g=>g.principals[1].parents=['t','t'],g=>g.principals[1].parents=['admin'],g=>g.principals.find(p=>p.id==='g0').parents=['g1'],g=>g.principals.find(p=>p.id==='admin').active=false,g=>g.principals[1].grants.resources.unknown=['read'],g=>g.principals[1].grants.resources.item=['execute'],g=>delete g.principals[1].active,g=>g.extra=true,g=>g.principals[1].extra=true];
  for(const change of changes){const graph=fixture();change(graph);throws(()=>prepareAuthority(catalog,graph));}
  const inherited=fixture(); inherited.principals.find(p=>p.id==='admin').grants.admin=false; inherited.principals[0].grants.admin=true;
  assert.ok(prepareAuthority(catalog,inherited),'effective inherited admin suffices');
});

test('sparse populations exceed native historical cap with explicit optional budgets',()=>{
  const graph={tenant:'t',principals:[principal('t','tenant',[]),principal('admin','user',['t'],[],true,true)]};
  for(let i=0;i<260;i++)graph.principals.push(principal(`n${i}`,'group',[i?`n${i-1}`:'t']));
  const edges=261;
  const prepared=prepareAuthority(catalog,graph,{maxPrincipals:262,maxEdges:edges});
  assert.equal(resolveAuthority(prepared,{tenant:'t',actor:'n0'}).descendants.length,259);
  throws(()=>prepareAuthority(catalog,graph,{maxPrincipals:261}),'AUTHORITY_BUDGET');
  throws(()=>prepareAuthority(catalog,graph,{maxEdges:edges-1}),'AUTHORITY_BUDGET');
  for(const options of [{maxEdges:-1},{maxPrincipals:1.5},{maxPrincipals:NaN},{typo:1}])throws(()=>prepareAuthority(catalog,graph,options));
});

test('opaque immutable handles resist fabricated authority and detached input mutation',()=>{
  const input=fixture(), prepared=prepareAuthority(catalog,input);
  const scope=resolveAuthority(prepared,{tenant:'t',actor:'u'});
  assert.ok(Object.isFrozen(prepared)); assert.ok(Object.isFrozen(scope)); assert.ok(Object.isFrozen(scope.accessible)); assert.ok(Object.isFrozen(scope.grants));
  input.principals.find(p=>p.id==='u').grants.admin=true;
  input.principals.find(p=>p.id==='u').parents=['other'];
  assert.equal(permits(scope,record('other'),'read'),false);
  assert.deepEqual(sorted(resolveAuthority(prepared,{tenant:'t',actor:'u'}).accessible),['child','g1','gi','leaf','u']);
  throws(()=>resolveAuthority({...prepared},{tenant:'t',actor:'u'}));
  throws(()=>permits({...scope},record(),'read'));
  throws(()=>permits(scope,record(),'execute'));
  for(const bad of [null,{},record('u','missing'),{...record(),owner:null}])assert.equal(permits(scope,bad,'read'),false);
});


test('option descriptors reject getters without executing them and enforce plain typed limits',()=>{
  let calls=0;
  const accessor={get maxEdges(){calls++;throw new Error('getter executed');}};
  throws(()=>prepareAuthority(catalog,fixture(),accessor));
  assert.equal(calls,0);
  const hidden=Object.defineProperty({},'maxEdges',{value:Infinity,enumerable:false});
  const symbol={[Symbol('maxEdges')]:1};
  for(const options of [null,[],new Date(),Object.create({maxEdges:Infinity}),hidden,symbol,{maxEdges:'1'},{maxEdges:true},{maxEdges:undefined},{maxPrincipals:2n}])throws(()=>prepareAuthority(catalog,fixture(),options));
  assert.ok(prepareAuthority(catalog,fixture(),{maxEdges:Infinity,maxPrincipals:Infinity}));
  for(const input of [null,[],{}, {tenant:'t',principals:null}])throws(()=>prepareAuthority(catalog,input));
  const prepared=prepareAuthority(catalog,fixture());
  for(const context of [null,{}, {tenant:'t',actor:1},{tenant:'t',actor:'u',extra:true}])throws(()=>resolveAuthority(prepared,context));
});

test('diamond descendants appear once without including cousins through a shared parent',()=>{
  const graph=fixture();
  graph.principals.push(principal('left','group',['u']),principal('right','group',['u']),principal('diamond','user',['left','right']),principal('tail','group',['diamond']));
  const scope=resolveAuthority(prepareAuthority(catalog,graph),{tenant:'t',actor:'u'});
  assert.deepEqual(sorted(scope.descendants),['child','diamond','leaf','left','right','tail']);
  assert.equal(scope.descendants.filter(id=>id==='diamond').length,1);
  assert.equal(permits(scope,record('tail'),'read'),true);
  assert.equal(permits(scope,record('peer'),'read'),false);
});
