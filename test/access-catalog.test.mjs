import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog } from '../src/catalog.mjs';
import { fingerprint } from '../src/values.mjs';
function definition() {
  return { name:'access', version:'1', contracts:{ordered:{version:'1',partition:['id'],order:'integer',inputs:{},aggregates:{}}}, views:{created:{contract:'ordered'},effective:{contract:'ordered'}}, models:{
    Entry:{schema:{type:'object',properties:{owner:{type:'string'},account:{type:'string'}},additionalProperties:false},memberships:[{slot:'created',view:'created',partition:[{header:'owner'}],order:{header:'createdAt'},inputs:{},onMissing:'skip'}],access:{orders:{created:{view:'created',slot:'created',ownerPartition:0}}}},
    Private:{managed:true,schema:{type:'object',properties:{},additionalProperties:false}}
  }};
}
const rejected = change => { const d=definition(); change(d); assert.throws(()=>prepareCatalog(d)); };
test('access metadata is immutable and shared contracts support dedicated views',()=>{
  const d=definition();
  d.models.Entry.memberships.push({...structuredClone(d.models.Entry.memberships[0]),slot:'effective',view:'effective'});
  d.models.Entry.access.orders.effective={view:'effective',slot:'effective',ownerPartition:0};
  const p=prepareCatalog(d);
  assert.equal(p.models.Private.access,null);
  assert.deepEqual(p.models.Entry.access,d.models.Entry.access);
  assert.ok(Object.isFrozen(p.models.Entry.access.orders.created));
  d.models.Entry.access.orders.created.ownerPartition=8;
  assert.equal(p.models.Entry.access.orders.created.ownerPartition,0);
  assert.equal(p.models.Entry.memberships[0].onMissing,'skip');
});
test('composite partition preserves other typed roles',()=>{
  const d=definition(); d.contracts.ordered.partition=['string','id','boolean'];
  d.models.Entry.memberships[0].partition=[{field:'account'},{header:'owner'},{constant:true}];
  d.models.Entry.access.orders.created.ownerPartition=1;
  assert.equal(prepareCatalog(d).models.Entry.access.orders.created.ownerPartition,1);
});
test('private default, point-only opt-in and managed read opt-in are distinct',()=>{
  const d=definition(); delete d.models.Entry.access;
  const p=prepareCatalog(d); assert.equal(p.models.Entry.access,null);
  assert.equal(p.identity,fingerprint({declaration:d,monoids:{},validators:{}}));
  assert.equal(Object.hasOwn(p.definition.models.Entry,'access'),false);
  d.models.Entry.access={orders:{}};
  assert.notEqual(prepareCatalog(d).identity,p.identity);
  d.models.Private.access={orders:{}};
  assert.deepEqual(prepareCatalog(d).models.Private.access,{orders:{}});
  d.models.Entry.managed=true; d.models.Entry.access=definition().models.Entry.access;
  assert.ok(prepareCatalog(d).models.Entry.managed);
});
test('access identity is stable under key order and pins query names',()=>{
  const a=definition(),b=definition();
  b.models.Entry.access.orders.created={ownerPartition:0,slot:'created',view:'created'};
  assert.equal(prepareCatalog(a).identity,prepareCatalog(b).identity);
  b.models.Entry.access.orders={renamed:b.models.Entry.access.orders.created};
  assert.notEqual(prepareCatalog(a).identity,prepareCatalog(b).identity);
});
test('body owner, constant owner, duplicate owner roles and wrong domain reject',()=>{
  rejected(d=>{d.models.Entry.memberships[0].partition=[{field:'owner'}];});
  rejected(d=>{d.models.Entry.memberships[0].partition=[{constant:'owner'}];});
  rejected(d=>{d.contracts.ordered.partition=['string'];});
  rejected(d=>{d.contracts.ordered.partition.push('id');d.models.Entry.memberships[0].partition.push({header:'owner'});});
});
test('mixed models, extra slots and duplicate order aliases reject',()=>{
  rejected(d=>{d.models.Private.memberships=structuredClone(d.models.Entry.memberships);});
  rejected(d=>{d.models.Entry.memberships.push({...d.models.Entry.memberships[0],slot:'second'});});
  rejected(d=>{d.models.Entry.access.orders.alias={...d.models.Entry.access.orders.created};});
});
test('malformed access metadata and missing bindings reject',()=>{
  for(const change of [
    ...[null,[],{}].map(value=>d=>{d.models.Entry.access=value;}),
    d=>{d.models.Entry.access.extra=true;},
    ...[null,[],'x'].map(value=>d=>{d.models.Entry.access.orders=value;}),
    d=>{d.models.Entry.access.orders={'':{view:'created',slot:'created',ownerPartition:0}};},
    ...[null,[],'x'].map(value=>d=>{d.models.Entry.access.orders.created=value;}),
    d=>{d.models.Entry.access.orders.created.extra=true;},
    ...['view','slot','ownerPartition'].map(key=>d=>{delete d.models.Entry.access.orders.created[key];}),
    d=>{d.models.Entry.access.orders.created.view='missing';},
    d=>{d.models.Entry.access.orders.created.slot='missing';},
    d=>{d.models.Entry.access.orders.created.view='effective';},
    ...[-1,1,0.5,'0',null,true].map(value=>d=>{d.models.Entry.access.orders.created.ownerPartition=value;}),
    d=>{d.models.Entry.access.orders.created.view={};},
    d=>{d.models.Entry.access.orders.created.slot='';}
  ]) rejected(change);
});
