import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCatalog, projectRecord, mutate, createMemoryStore, checkStore, readRoot, treeSelect, treeAggregate, generatedId, settlementOracle } from '../src/index.mjs';
import { sourcePrefix, sourceKey, incomingPrefix, reservationPrefix } from '../src/keys.mjs';
import { loadPathSources } from '../src/reference-path.mjs';
import { createMutation } from '../src/mutate.mjs';

const business = ['Invoice', ['invoice'], ['day', 'priority']];
const owned = ['Invoice', ['invoice', 'owner'], ['day', 'priority']];
const hop = (reference, target) => ({ reference, target });
const invoicePath = [hop('issue','Issue'),hop('invoice','Invoice')];
const schema = (properties, required = properties) => ({ type:'object', properties:Object.fromEntries(properties.map(name => [name,{type:'string'}])), required, additionalProperties:false });
const record = (id, model, data = {}, owner = 'visible') => ({ id, model, data, owner, tenant:'tenant', createdAt:'1' });
const put = record => ({type:'put',record});
function fixture({ validator = () => true } = {}) {
  const index = (partitionFields) => ({ version:'1',partitionFields,orderFields:['day','priority'],roles:Object.fromEntries([...partitionFields,'day','priority'].map(name => [name,name==='day'?'integer':name==='priority'?'string':'id'])),inputs:{amount:'integer'},aggregates:{balance:{monoid:'sum',input:'amount'}},validators:[{scope:'root',use:'final'}] });
  const membership = (slot,index,owner) => ({slot,index,partition:[{path:invoicePath,header:'id'},...(owner?[{header:'owner'}]:[])],order:[{path:[hop('issue','Issue')],field:'day'},{field:'priority'}],inputs:{amount:{field:'amount'}},onMissing:'skip'});
  const definition = {name:'reference-paths',version:'1',models:{
    Invoice:{schema:schema([]),indexes:[index(['invoice']),index(['invoice','owner'])],calculations:[{slot:'balance',use:'balance'}]},
    Issue:{schema:schema(['invoiceId','day'],['day']),references:[{slot:'invoice',field:'invoiceId',targets:['Invoice'],many:false,onDelete:'reject'}]},
    Tax:{schema:schema(['issueId','amount','priority']),references:[{slot:'issue',field:'issueId',targets:['Issue'],many:false,onDelete:'reject'}],memberships:[membership('business',business,false),membership('owned',owned,true)]},
    Balance:{managed:true,schema:schema(['invoiceId','amount'])}
  }};
  const catalog = prepareCatalog(definition,{validators:{final:{version:'1',validate:validator}},calculations:{balance:{version:'1',resolve:({record})=>({parameters:{},reads:[{slot:'total',kind:'range',index:business,partition:[record.id],aggregate:'balance'}],outputs:[{role:'balance',model:'Balance',owner:record.owner,createdAt:record.createdAt,data:{invoiceId:record.id,amount:'0'},computed:['amount']}]}),evaluate:({values})=>({outputs:{balance:{amount:String(values.total)}}})}}});
  return {definition,catalog,store:createMemoryStore()};
}
const initial = () => [record('A','Invoice'),record('B','Invoice'),record('issue','Issue',{invoiceId:'A',day:'9007199254740993'}),record('tax1','Tax',{issueId:'issue',amount:'100',priority:'same'}),record('tax2','Tax',{issueId:'issue',amount:'-80',priority:'same'},'hidden')];
const snapshot = store => store.read(tx=>tx.scan());
const sources = store => store.read(async tx=>(await tx.scan({prefix:sourcePrefix('tenant')})).map(row=>row.value));
const output = (store,invoice) => store.read(async tx=>(await tx.get(sourceKey('tenant',generatedId('tenant',invoice,'balance','balance')))).data.amount);

// Expected occurrences come directly from source facts, without any projection,
// reverse routing, sourceOracle or tree traversal helper.
function enumerate(records,index) {
  const byId = new Map(records.map(record=>[record.id,record]));
  const groups = new Map();
  for(const tax of records.filter(record=>record.model==='Tax')) {
    const issue = byId.get(tax.data.issueId),invoice = byId.get(issue?.data.invoiceId);
    if(!invoice) continue;
    const partition = index===business?[invoice.id]:[invoice.id,tax.owner],key=JSON.stringify(partition);
    if(!groups.has(key)) groups.set(key,{partition,rows:[]});
    groups.get(key).rows.push({sourceId:tax.id,slot:index===business?'business':'owned',order:[BigInt(issue.data.day),tax.data.priority],amount:BigInt(tax.data.amount)});
  }
  for(const group of groups.values()) group.rows.sort((a,b)=>a.order[0]<b.order[0]?-1:a.order[0]>b.order[0]?1:a.order[1]<b.order[1]?-1:a.order[1]>b.order[1]?1:a.sourceId<b.sourceId?-1:a.sourceId>b.sourceId?1:0);
  return groups;
}
async function verify(store,catalog) {
  const facts=await sources(store);
  for(const index of [business,owned]) {
    const expected=enumerate(facts,index);
    for(const invoice of ['A','B']) for(const owner of index===business?[undefined]:['visible','hidden']) {
      const partition=owner===undefined?[invoice]:[invoice,owner],scope={tenant:'tenant',index,partition},rows=expected.get(JSON.stringify(partition))?.rows??[];
      await store.read(async tx=>{
        const root=await readRoot(tx,catalog,scope);
        assert.equal(root?.size??0,rows.length);
        assert.equal((await treeAggregate(tx,catalog,scope)).balance,rows.reduce((sum,row)=>sum+row.amount,0n));
        for(let i=0;i<rows.length;i++) {
          const actual=await treeSelect(tx,catalog,scope,i);
          assert.deepEqual([actual.sourceId,actual.slot,actual.order],[rows[i].sourceId,rows[i].slot,rows[i].order]);
        }
      });
    }
  }
  assert.equal(await output(store,'A'),String((enumerate(facts,business).get('["A"]')?.rows??[]).reduce((sum,row)=>sum+row.amount,0n)));
  assert.equal(await output(store,'B'),String((enumerate(facts,business).get('["B"]')?.rows??[]).reduce((sum,row)=>sum+row.amount,0n)));
  await checkStore(store,catalog,'tenant');
}

test('R3 ancestor retarget moves untouched tax business and source-owner occurrences and downstream empty balances',async()=>{
  for(const reverse of [false,true]) {
    const {store,catalog}=fixture(),records=initial();
    await mutate(store,catalog,'tenant',(reverse?records.toReversed():records).map(put));
    await verify(store,catalog);assert.equal(await output(store,'A'),'20');assert.equal(await output(store,'B'),'0');
    const taxes=await store.read(tx=>tx.getMany(['tax1','tax2'].map(id=>sourceKey('tenant',id))));
    records[2].data.invoiceId='B';
    const result=await mutate(store,catalog,'tenant',[put(records[2])]);
    assert.equal(result.changedSources,3,'one issue and two managed balances; untouched taxes are not rewritten');
    assert.ok(result.referenceWork.reverseVisits>0);assert.ok(result.referenceWork.pathReads>0);
    assert.deepEqual(await store.read(tx=>tx.getMany(['tax1','tax2'].map(id=>sourceKey('tenant',id)))),taxes);
    assert.equal(await output(store,'A'),'0');assert.equal(await output(store,'B'),'20');await verify(store,catalog);
    let writes;const noop=await mutate(store,catalog,'tenant',[put(records[2])],{beforeCommit:result=>{writes=result.changes;}});
    assert.deepEqual(writes,[]);assert.equal(noop.metrics.metrics.stagedWrites,0);assert.equal(noop.changedMemberships,0);
  }
});

test('R3 terminal field edit and joint edge changes use final source graph in either command order',async()=>{
  for(const reverse of [false,true]) {
    const {store,catalog}=fixture(),records=initial();await mutate(store,catalog,'tenant',records.map(put));
    const nextIssue=record('next','Issue',{invoiceId:'B',day:'9007199254740994'});
    records[3].data.issueId='next';records[2].data.day='9007199254740999';
    const changes=[nextIssue,records[2],records[3]];
    await mutate(store,catalog,'tenant',(reverse?changes.toReversed():changes).map(put));await verify(store,catalog);
    assert.equal(await output(store,'A'),'-80');assert.equal(await output(store,'B'),'100');
    nextIssue.data.day='-9007199254740995';await mutate(store,catalog,'tenant',[put(nextIssue)]);await verify(store,catalog);
  }
});

test('R3 missing optional path skips then re-enters, and missing terminal rejects according to membership policy',async()=>{
  const {store,catalog,definition}=fixture(),records=initial();delete records[2].data.invoiceId;
  await mutate(store,catalog,'tenant',records.map(put));assert.equal(await output(store,'A'),'0');await verify(store,catalog);
  records[2].data.invoiceId='A';await mutate(store,catalog,'tenant',[put(records[2])]);assert.equal(await output(store,'A'),'20');await verify(store,catalog);
  definition.models.Tax.memberships[0].onMissing='reject';
  const strict=prepareCatalog(definition,{validators:{final:{version:'1',validate:()=>true}},calculations:{balance:{version:'1',resolve:()=>({parameters:{},reads:[],outputs:[]}),evaluate:()=>({outputs:{}})}}});
  const isolated=createMemoryStore();delete records[2].data.invoiceId;
  await assert.rejects(mutate(isolated,strict,'tenant',records.map(put)),{code:'MISSING_PROJECTION'});assert.deepEqual(await snapshot(isolated),[]);
});

test('R3 final validator failure restores source bodies reverse edges trees aggregates routes and outputs',async()=>{
  let reject=false;
  const {store,catalog}=fixture({validator:(_summary,{scope})=>!(reject&&scope.partition[0]==='B')});
  const records=initial();await mutate(store,catalog,'tenant',records.map(put));const before=await snapshot(store);
  reject=true;records[2].data.invoiceId='B';await assert.rejects(mutate(store,catalog,'tenant',[put(records[2])]),{code:'VALIDATION_FAILED'});
  assert.deepEqual(await snapshot(store),before);reject=false;await verify(store,catalog);
});

test('R3 preparation rejects nonfinite undeclared nonscalar wrong-target and terminal-domain paths',()=>{
  const {definition}=fixture();
  delete definition.models.Invoice.calculations;
  for(const index of definition.models.Invoice.indexes) index.validators=[];
  for(const change of [
    d=>d.models.Tax.memberships[0].partition[0].path=[],
    d=>d.models.Tax.memberships[0].partition[0].path[0].reference='missing',
    d=>d.models.Tax.memberships[0].partition[0].path[0].target='Invoice',
    d=>d.models.Tax.references[0].many=true,
    d=>d.models.Tax.memberships[0].order[0].field='missing',
    d=>d.models.Issue.schema.properties.day={type:'boolean'},
    d=>d.models.Tax.memberships[0].partition[0].extra=true,
    d=>d.models.Issue.managed=true,
  ]) {
    const candidate=structuredClone(definition);change(candidate);
    assert.throws(()=>prepareCatalog(candidate),{code:'INVALID_CATALOG'});
  }
});

test('R3 explicit path target narrowing rejects an otherwise valid alternate target and leaves the state unchanged',async()=>{
  const {definition}=fixture();delete definition.models.Invoice.calculations;
  for(const index of definition.models.Invoice.indexes) index.validators=[];
  definition.models.Alternate={schema:schema([])};
  definition.models.Issue.references[0].targets.push('Alternate');
  const catalog=prepareCatalog(definition),store=createMemoryStore(),records=initial();
  await mutate(store,catalog,'tenant',records.map(put));
  await store.read(async tx=>{
    const context=await loadPathSources(tx,catalog,'tenant',[records[3]]);
    assert.deepEqual(projectRecord(catalog,records[3],context).map(row=>row.partition),[['A'],['A','visible']]);
  });
  const before=await snapshot(store),alternate=record('alternate','Alternate');records[2].data.invoiceId='alternate';
  await assert.rejects(mutate(store,catalog,'tenant',[put(alternate),put(records[2])]),{code:'INVALID_REFERENCE'});
  assert.deepEqual(await snapshot(store),before);
});

test('R3 repeated stages discover new attempt-local reverse edges and preserve original-to-final occurrence counts',async()=>{
  const {definition}=fixture();delete definition.models.Invoice.calculations;
  for(const index of definition.models.Invoice.indexes) index.validators=[];
  const catalog=prepareCatalog(definition),store=createMemoryStore(),records=initial();
  await mutate(store,catalog,'tenant',records.map(put));
  await store.transact(async tx=>{
    const workspace=await createMutation(tx,catalog,'tenant');
    records[2].data.invoiceId='B';await workspace.stage([put(records[2])]);
    await workspace.stage([put(record('newTax','Tax',{issueId:'issue',amount:'7',priority:'same'}))]);
    records[2].data.invoiceId='A';await workspace.stage([put(records[2])]);
    const result=await workspace.finish();
    assert.equal(result.changedSources,1);assert.equal(result.changedMemberships,2);
    assert.equal((await treeAggregate(tx,catalog,{tenant:'tenant',index:business,partition:['A']})).balance,27n);
    assert.equal((await treeAggregate(tx,catalog,{tenant:'tenant',index:business,partition:['B']})).balance,0n);
  });
  await checkStore(store,catalog,'tenant');
});

test('R3 optional terminal policy skips absent values, re-enters on presence and rejects invalid exact values',async()=>{
  const {definition}=fixture();delete definition.models.Invoice.calculations;
  for(const index of definition.models.Invoice.indexes) index.validators=[];
  definition.models.Issue.schema.properties.optionalDay={type:'string'};
  for(const membership of definition.models.Tax.memberships) membership.order[0].field='optionalDay';
  const catalog=prepareCatalog(definition),store=createMemoryStore(),records=initial();
  await mutate(store,catalog,'tenant',records.map(put));
  const scope={tenant:'tenant',index:business,partition:['A']};
  assert.equal(await store.read(tx=>readRoot(tx,catalog,scope)),undefined);
  records[2].data.optionalDay='9007199254740997';await mutate(store,catalog,'tenant',[put(records[2])]);
  assert.equal((await store.read(tx=>treeAggregate(tx,catalog,scope))).balance,20n);
  const before=await snapshot(store);records[2].data.optionalDay='01';
  await assert.rejects(mutate(store,catalog,'tenant',[put(records[2])]));assert.deepEqual(await snapshot(store),before);
  const strict=structuredClone(definition);for(const membership of strict.models.Tax.memberships) membership.onMissing='reject';
  const isolated=createMemoryStore();delete records[2].data.optionalDay;
  await assert.rejects(mutate(isolated,prepareCatalog(strict),'tenant',records.map(put)),{code:'MISSING_PROJECTION'});
  assert.deepEqual(await snapshot(isolated),[]);
});

test('R3 malformed incoming discovery aborts atomically and an omitted reverse edge is detected independently',async()=>{
  for(const kind of ['malformed','missing']) {
    const {definition}=fixture();delete definition.models.Invoice.calculations;
    for(const index of definition.models.Invoice.indexes) index.validators=[];
    const catalog=prepareCatalog(definition),store=createMemoryStore(),records=initial();
    await mutate(store,catalog,'tenant',records.map(put));
    await store.transact(async tx=>{
      const rows=await tx.scan({prefix:incomingPrefix(catalog,'tenant','issue')});
      const row=rows.find(row=>row.value.sourceId==='tax1');assert.ok(row);
      if(kind==='malformed') {row.value.position=-1;await tx.set(row.key,row.value);}
      else await tx.delete(row.key);
    });
    const before=await snapshot(store);records[2].data.day='9';
    if(kind==='malformed') {
      await assert.rejects(mutate(store,catalog,'tenant',[put(records[2])]),{code:'CORRUPT_STATE'});
      assert.deepEqual(await snapshot(store),before);
    } else await mutate(store,catalog,'tenant',[put(records[2])]);
    await assert.rejects(checkStore(store,catalog,'tenant'),{code:'INTEGRITY'});
  }
});

test('R3 managed contributors project authored paths and move actual and zero-output potential inventories together',async()=>{
  const {definition}=fixture();
  definition.models.Generator={schema:{...schema(['issueId']),properties:{issueId:{type:'string'},enabled:{type:'boolean'}}},references:[{slot:'issue',field:'issueId',targets:['Issue'],many:false,onDelete:'reject'}],calculations:[{slot:'tax',use:'tax'}]};
  definition.models.ComputedTax={managed:true,schema:structuredClone(definition.models.Tax.schema),references:structuredClone(definition.models.Tax.references),memberships:structuredClone(definition.models.Tax.memberships)};
  const catalog=prepareCatalog(definition,{validators:{final:{version:'1',validate:()=>true}},calculations:{
    balance:{version:'1',resolve:({record})=>({parameters:{},reads:[{slot:'total',kind:'range',index:business,partition:[record.id],aggregate:'balance'}],outputs:[{role:'balance',model:'Balance',owner:record.owner,createdAt:record.createdAt,data:{invoiceId:record.id,amount:'0'},computed:['amount']}]}),evaluate:({values})=>({outputs:{balance:{amount:String(values.total)}}})},
    tax:{version:'1',resolve:({record})=>({parameters:{enabled:record.data.enabled},reads:[],outputs:[{role:'tax',model:'ComputedTax',owner:record.owner,createdAt:record.createdAt,data:{issueId:record.data.issueId,amount:'0',priority:'same'},computed:['amount']}]}),evaluate:({parameters})=>({outputs:{tax:parameters.enabled?{amount:'7'}:null}})}
  }});
  const store=createMemoryStore(),records=initial().slice(0,3),issue=records[2],generator=record('generator','Generator',{issueId:'issue',enabled:false});records.push(generator);
  await mutate(store,catalog,'tenant',records.map(put));
  const computedId=generatedId('tenant','generator','tax','tax');
  const verifyComputed=async(expectedA,expectedB,present)=>{
    assert.equal(await output(store,'A'),expectedA);assert.equal(await output(store,'B'),expectedB);
    assert.equal(Boolean(await store.read(tx=>tx.get(sourceKey('tenant',computedId)))),present);
    await checkStore(store,catalog,'tenant');
    const facts=await sources(store),authored=facts.filter(record=>!catalog.models[record.model].managed);
    const reservations=await store.read(async tx=>(await tx.scan({prefix:reservationPrefix(catalog,'tenant')})).map(row=>row.value));
    const independent=await settlementOracle(catalog,'tenant',authored,{reservations});
    assert.deepEqual(facts.filter(record=>catalog.models[record.model].managed).toSorted((a,b)=>a.id.localeCompare(b.id)),independent.generated.toSorted((a,b)=>a.id.localeCompare(b.id)));
  };
  await verifyComputed('0','0',false);
  issue.data.invoiceId='B';await mutate(store,catalog,'tenant',[put(issue)]);await verifyComputed('0','0',false);
  assert.deepEqual(await store.read(tx=>tx.get(sourceKey('tenant','generator'))),generator);
  issue.data.day='9007199254740999';await mutate(store,catalog,'tenant',[put(issue)]);await verifyComputed('0','0',false);
  generator.data.enabled=true;await mutate(store,catalog,'tenant',[put(generator)]);await verifyComputed('0','7',true);
  issue.data.day='-9007199254740999';await mutate(store,catalog,'tenant',[put(issue)]);await verifyComputed('0','7',true);
  issue.data.invoiceId='A';await mutate(store,catalog,'tenant',[put(issue)]);await verifyComputed('7','0',true);
  generator.data.enabled=false;issue.data.invoiceId='B';await mutate(store,catalog,'tenant',[put(generator),put(issue)]);await verifyComputed('0','0',false);
});
