import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { fork } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { openSqliteStore } from '../src/sqlite.mjs';
async function filename(t){const dir=await mkdtemp(join(tmpdir(),'kv-base-sqlite-'));t.after(()=>rm(dir,{recursive:true,force:true}));return join(dir,'state.sqlite');}
const snapshot=store=>store.read(tx=>tx.scan());
const binary=(a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b));
function child(filename,mode){
  const process=fork(fileURLToPath(new URL('./fixtures/sqlite-process.mjs',import.meta.url)),[filename,mode],{stdio:['ignore','pipe','pipe','ipc']});
  const messages=[],waiters=[];let stderr='';process.stderr.on('data',data=>{stderr+=data;});process.on('message',value=>{if(waiters.length)waiters.shift()(value);else messages.push(value);});
  const exited=new Promise((resolve,reject)=>{process.on('error',reject);process.on('exit',(code,signal)=>resolve({code,signal,stderr}));});
  return {process,exited,message:()=>messages.length?Promise.resolve(messages.shift()):new Promise(resolve=>waiters.push(resolve))};
}

test('SQLite persistence roundtrips exact cloned values, safe cycles and typed bytes after reopen',async t=>{
  const path=await filename(t);let store=openSqliteStore(path);const cycle={value:9007199254740993123456789n};cycle.self=cycle;
  const data={cycle,bytes:Uint8Array.from([0,255,7]),map:new Map([['x',4n]]),marker:{integer:'not a tag'}};
  await store.transact(tx=>tx.set('data',data));data.bytes[0]=3;
  const read=await store.read(tx=>tx.get('data'));assert.equal(read.cycle.self,read.cycle);read.bytes[1]=0;await store.close();store=openSqliteStore(path);
  const persisted=await store.read(tx=>tx.get('data'));assert.equal(persisted.cycle.self,persisted.cycle);assert.equal(persisted.cycle.value,9007199254740993123456789n);assert.deepEqual([...persisted.bytes],[0,255,7]);assert.deepEqual(persisted.map,new Map([['x',4n]]));await store.close();
});

test('SQLite callback/hook failure rolls back sources and no-op writes retain guards',async t=>{
  const store=openSqliteStore(await filename(t));await store.transact(tx=>tx.set('a',{n:1n}));const original=await snapshot(store);
  for(const phase of ['callback','hook']){
    await assert.rejects(store.transact(async tx=>{await tx.set('a',2n);await tx.set('b',3n);if(phase==='callback')throw Error('abort');},{beforeCommit:()=>{throw Error('abort');}}),/abort/);assert.deepEqual(await snapshot(store),original);
  }
  await store.transact(async tx=>{await tx.set('a',{n:1n});await tx.delete('missing');assert.equal(await tx.get('missing'),undefined);assert.deepEqual(await tx.scan({prefix:'empty/'}),[]);assert.ok(tx.inspect().guards.some(g=>g.key==='missing'&&!g.exists));assert.ok(tx.inspect().guards.some(g=>g.type==='range'&&g.prefix==='empty/'));},{beforeCommit:({changes})=>assert.deepEqual(changes,[])});
  await store.transact(tx=>tx.set('progress',true));await store.close();
});

test('SQLite BLOB scan bounds preserve binary UTF8, embedded NUL, overlay compensation and limits',async t=>{
  const store=openSqliteStore(await filename(t)),keys=['a','a\0','a\0x','a1','a2','z','\uE000','😀'];
  await store.transact(async tx=>{for(const key of keys)await tx.set(key,key);});
  const sorted=keys.toSorted(binary);assert.deepEqual((await snapshot(store)).map(row=>row.key),sorted);
  const ranges=[{prefix:'a\0'},{prefix:'a',gt:'a',lte:'a2',limit:2},{gte:'a\0',lt:'z'},{gt:'z',limit:1},{prefix:'missing',limit:0}];
  await store.transact(async tx=>{
    await tx.delete('a\0');await tx.delete('a1');await tx.set('a0','a0');const expected=sorted.filter(k=>!['a\0','a1'].includes(k)).concat('a0').sort(binary);
    for(const range of ranges){const matches=expected.filter(k=>(range.prefix===undefined||k.startsWith(range.prefix))&&(range.gt===undefined||binary(k,range.gt)>0)&&(range.gte===undefined||binary(k,range.gte)>=0)&&(range.lt===undefined||binary(k,range.lt)<0)&&(range.lte===undefined||binary(k,range.lte)<=0)).slice(0,range.limit??Infinity);assert.deepEqual((await tx.scan(range)).map(row=>row.key),matches);}
  });await store.close();
});

test('SQLite value rejection, closed/nested facade checks and read-only handles preserve state',async t=>{
  const path=await filename(t),store=openSqliteStore(path),shared=new SharedArrayBuffer(4);
  for(const value of [shared,new Uint8Array(shared),new Map([['x',shared]]),()=>{}]){await assert.rejects(store.transact(async tx=>{await tx.set('partial',true);await tx.set('invalid',value);}));assert.equal(await store.read(tx=>tx.get('partial')),undefined);}
  let saved;await store.read(async tx=>{saved=tx;await assert.rejects(tx.set('x',1),{code:'READ_ONLY'});await assert.rejects(store.read(()=>{}),{code:'NESTED_TRANSACTION'});await assert.rejects(store.close(),{code:'NESTED_TRANSACTION'});});await assert.rejects(saved.get('x'),{code:'TRANSACTION_CLOSED'});
  await store.transact(tx=>tx.set('x',4n));const readonly=openSqliteStore(path,{readOnly:true});assert.equal(readonly.capabilities.serialized,false);await assert.rejects(readonly.transact(()=>{}),{code:'READ_ONLY'});assert.equal(await readonly.read(tx=>tx.get('x')),4n);await readonly.close();await store.close();await assert.rejects(store.read(()=>{}),{code:'STORAGE_CLOSED'});
});

test('same SQLite handle serializes callbacks and different handles conflict before callback entry',async t=>{
  const path=await filename(t),a=openSqliteStore(path),b=openSqliteStore(path);await a.transact(tx=>tx.set('n',0n));let entered,release;
  const ready=new Promise(r=>{entered=r;}),blocked=new Promise(r=>{release=r;});const order=[];
  const first=a.read(async tx=>{order.push('read');assert.equal(await tx.get('n'),0n);entered();await blocked;assert.equal(await tx.get('n'),0n);});await ready;
  let called=false;await assert.rejects(b.transact(()=>{called=true;}),{code:'STORAGE_CONFLICT'});assert.equal(called,false);
  const queued=a.transact(async tx=>{order.push('write');await tx.set('n',1n);});await Promise.resolve();assert.deepEqual(order,['read']);release();await Promise.all([first,queued]);
  assert.deepEqual(order,['read','write']);assert.equal(await b.read(tx=>tx.get('n')),1n);await a.close();await b.close();
});

test('receipts persist original results, skip duplicate callbacks and require matching digest plus replay verification',async t=>{
  const path=await filename(t);let store=openSqliteStore(path),calls=0;const callback=async tx=>{calls++;await tx.set('counter',1n);return {number:1n};};
  const options={receipt:{id:'op',digest:'request'}};assert.deepEqual(await store.transact(callback,options),{number:1n});await store.close();store=openSqliteStore(path);
  assert.deepEqual(await store.getReceipt('op'),{id:'op',digest:'request',result:{number:1n}});assert.deepEqual(await store.transact(callback,options),{number:1n});assert.equal(calls,1);
  await assert.rejects(store.transact(callback,{receipt:{id:'op',digest:'other'}}),{code:'RECEIPT_MISMATCH'});
  await assert.rejects(store.transact(callback,{...options,verifyReplay:async(tx,receipt)=>{assert.equal(await tx.get('counter'),1n);assert.deepEqual(receipt.result,{number:1n});await assert.rejects(tx.set('forbidden',true),{code:'READ_ONLY'});throw Object.assign(Error('revoked'),{code:'FORBIDDEN'});}}),{code:'FORBIDDEN'});
  assert.equal(calls,1);assert.equal(await store.read(tx=>tx.get('forbidden')),undefined);await store.close();
});

test('lost acknowledgement reports COMMIT_UNKNOWN and original receipt prevents replay under that identity',async t=>{
  const path=await filename(t);let store=openSqliteStore(path),calls=0;const options={receipt:{id:'original',digest:'once'},afterCommit(){throw Error('lost acknowledgement');}};
  await assert.rejects(store.transact(async tx=>{calls++;await tx.set('n',1n);return {n:1n};},options),{code:'COMMIT_UNKNOWN',receiptId:'original'});await store.close();store=openSqliteStore(path);
  const receipt=await store.getReceipt('original');assert.deepEqual(receipt.result,{n:1n});assert.deepEqual(await store.transact(()=>{calls++;return {n:2n};},{receipt:{id:'original',digest:'once'}}),{n:1n});assert.equal(calls,1);assert.equal(await store.read(tx=>tx.get('n')),1n);await store.close();
});

test('cross-process writer locking protects point absence and empty range facts',async t=>{
  const path=await filename(t),store=openSqliteStore(path);await store.transact(tx=>tx.set('counter',0n));await store.close();
  const holding=child(path,'hold');assert.deepEqual(await holding.message(),{event:'ready'});
  const attempt=child(path,'attempt');assert.deepEqual(await attempt.message(),{event:'attempt',called:false,code:'STORAGE_CONFLICT'});assert.equal((await attempt.exited).code,0);
  holding.process.send('release');assert.deepEqual(await holding.message(),{event:'done'});assert.equal((await holding.exited).code,0);
  const reopened=openSqliteStore(path);assert.equal(await reopened.read(tx=>tx.get('counter')),1n);assert.deepEqual(await reopened.read(tx=>tx.scan({prefix:'phantom/'})),[]);await reopened.close();
});

for(const phase of ['before','after'])test(`SIGKILL ${phase} commit reopens with an atomic source and original receipt inventory`,async t=>{
  const path=await filename(t),store=openSqliteStore(path);await store.transact(tx=>tx.set('counter',0n));await store.close();
  const crashing=child(path,phase);assert.deepEqual(await crashing.message(),{event:'ready'});crashing.process.kill('SIGKILL');assert.equal((await crashing.exited).signal,'SIGKILL');
  const reopened=openSqliteStore(path),committed=phase==='after';assert.equal(await reopened.read(tx=>tx.get('counter')),committed?1n:0n);assert.deepEqual(await reopened.read(tx=>tx.get('new')),committed?{value:9n}:undefined);
  assert.deepEqual(await reopened.getReceipt('original-operation'),committed?{id:'original-operation',digest:'request-one',result:{value:1n}}:undefined);await reopened.close();
});

test('unknown persisted storage versions reject rather than opening incompatible state',async t=>{
  const path=await filename(t),store=openSqliteStore(path);await store.close();const db=new DatabaseSync(path);db.exec('PRAGMA user_version=99');db.close();assert.throws(()=>openSqliteStore(path),{code:'STORAGE_FORMAT_MISMATCH'});
});
