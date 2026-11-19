import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {serialize} from 'node:v8';
import {openFoundationDbStore} from '../src/foundationdb.mjs';
const clusterFile=process.env.KV_BASE_FDB_CLUSTER_FILE,skip=clusterFile?false:'Selected real cluster required';
async function fixture(t){
  const prefixes=[];let native;const stores=[];
  const open=async()=>{const prefix='checkpoint-'+randomUUID();prefixes.push(prefix);const store=await openFoundationDbStore({clusterFile,prefix});stores.push(store);return{store,prefix};};
  const capture=async prefix=>{const {default:fdb}=await import('foundationdb');native??=fdb.open(clusterFile);const root=fdb.tuple.pack(['kv-base',prefix]),end=Buffer.from(root);let upper=end;for(let i=end.length-1;i>=0;i--)if(end[i]<255){end[i]++;upper=end.subarray(0,i+1);break;}const tx=native.rawCreateTransaction({timeout:4500});try{return{root,upper,rows:(await tx.getRangeAll(root,upper)).map(([key,value])=>[Buffer.from(key).subarray(root.length),Buffer.from(value)])};}finally{tx.rawCancel();}};
  t.after(async()=>{for(const store of stores)await store.close();for(const prefix of prefixes){const{root,upper}=await capture(prefix),tx=native.rawCreateTransaction({timeout:4500});try{tx.clearRange(root,upper);await tx.rawCommit();}finally{tx.rawCancel();}}native?.close();});
  return{open,capture};
}
function gate(){let entered,release;return{entered:new Promise(resolve=>{entered=resolve;}),blocked:new Promise(resolve=>{release=resolve;}),enter:()=>entered(),release:()=>release()};}

test('D5 FDB checkpoint atomically preserves exact physical data and original receipts in a fresh namespace',{skip},async t=>{
  const f=await fixture(t),source=await f.open(),target=await f.open();
  await source.store.transact(async tx=>{await tx.set('zero\0unicode😀',{integer:2n**110n,negative:-11n,buffer:Buffer.from([0,255]),array:[undefined,null]});await tx.set('other',{at:'final'});return{original:'result',exact:91n};},{receipt:{id:'operation\0id',digest:'original-digest'}});
  const expected=await f.capture(source.prefix),snapshot=await source.store.captureCheckpoint();assert.deepEqual(snapshot.rows,expected.rows);
  assert.deepEqual(await target.store.restoreCheckpoint(snapshot),{restored:true,rows:snapshot.rows.length,bytes:snapshot.rows.reduce((sum,[key,value])=>sum+key.length+value.length,0)});
  assert.deepEqual((await f.capture(target.prefix)).rows,expected.rows);assert.deepEqual(await target.store.getReceipt('operation\0id'),await source.store.getReceipt('operation\0id'));
  assert.deepEqual(await target.store.transact(()=>{throw Error('Must not repeat restored operation');},{receipt:{id:'operation\0id',digest:'original-digest'}}),{original:'result',exact:91n});
});

test('D5 FDB checkpoint rejects corrupt incomplete oversized or active-target restore without modifying target',{skip},async t=>{
  const f=await fixture(t),source=await f.open(),target=await f.open();await source.store.transact(tx=>tx.set('a',{value:'source'}));const snapshot=await source.store.captureCheckpoint(),before=(await f.capture(target.prefix)).rows;
  const missing={...snapshot,rows:snapshot.rows.filter(([key])=>key[0]!==2)};await assert.rejects(target.store.restoreCheckpoint(missing),{code:'BACKUP_INVALID'});
  const malformed={...snapshot,rows:snapshot.rows.map(([key,value])=>[key,key[0]===0?Buffer.from('not v8'):value])};await assert.rejects(target.store.restoreCheckpoint(malformed),{code:'CORRUPT_STATE'});
  const duplicate={...snapshot,rows:[...snapshot.rows,snapshot.rows[0]]};await assert.rejects(target.store.restoreCheckpoint(duplicate),{code:'BACKUP_INVALID'});
  await assert.rejects(target.store.restoreCheckpoint(snapshot,{maxBytes:1}),{code:'BACKUP_LIMIT'});assert.deepEqual((await f.capture(target.prefix)).rows,before);
  await target.store.transact(tx=>tx.set('live',{untouched:true}));const active=(await f.capture(target.prefix)).rows;await assert.rejects(target.store.restoreCheckpoint(snapshot),{code:'RESTORE_TARGET_NOT_EMPTY'});assert.deepEqual((await f.capture(target.prefix)).rows,active);
  await assert.rejects(source.store.captureCheckpoint({maxRows:1}),{code:'BACKUP_INVALID'});await assert.rejects(source.store.captureCheckpoint({maxBytes:1}),{code:'BACKUP_LIMIT'});assert.deepEqual((await f.capture(target.prefix)).rows,active);
});

test('D5 FDB checkpoint rollback conflict and lost acknowledgment never partially replace a candidate',{skip},async t=>{
  const f=await fixture(t),source=await f.open(),target=await f.open();await source.store.transact(tx=>tx.set('a',{value:1}),{receipt:{id:'one',digest:'digest'}});const snapshot=await source.store.captureCheckpoint(),before=(await f.capture(target.prefix)).rows;
  await assert.rejects(target.store.restoreCheckpoint(snapshot,{beforeCommit(){throw Error('rollback candidate');}}),/rollback candidate/);assert.deepEqual((await f.capture(target.prefix)).rows,before);
  const g=gate(),restore=target.store.restoreCheckpoint(snapshot,{beforeCommit:async()=>{g.enter();await g.blocked;}}),failed=assert.rejects(restore,{code:'STORAGE_CONFLICT'});await g.entered;await target.store.transact(tx=>tx.set('concurrent',{winner:true}));g.release();await failed;assert.deepEqual(await target.store.read(tx=>tx.scan()),[{key:'concurrent',value:{winner:true}}]);
  const fresh=await f.open();await assert.rejects(fresh.store.restoreCheckpoint(snapshot,{afterCommit(){throw Error('lost restored response');}}),{code:'COMMIT_UNKNOWN'});assert.deepEqual((await f.capture(fresh.prefix)).rows,snapshot.rows);assert.deepEqual(await fresh.store.getReceipt('one'),await source.store.getReceipt('one'));
});
