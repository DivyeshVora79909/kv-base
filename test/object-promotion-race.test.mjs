import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {syncBuiltinESMExports} from 'node:module';
import {join} from 'node:path';
import {fixture,chunks,collect,digest} from './helpers/objects-fixture.mjs';
import {sourceKey} from '../src/keys.mjs';
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
function gate(){let release;return{promise:new Promise(resolve=>{release=resolve;}),release:()=>release()};}

for(const first of ['finalize','reconcile'])test(`object promotion preserves bytes when delayed ${first} observes another driver moving its stage`,{skip,timeout:15000},async t=>{
 const f=await fixture(t),bytes=Buffer.from('one immutable promoted object'),grant=await f.begin(bytes);await f.objects.upload(f.token,grant.uploadToken,chunks(bytes));
 const missing=gate(),resumeMissing=gate(),promoted=gate(),resumePromoted=gate();let reserve=true,firstPromotion=true;
 await f.store.close();await f.open({onPhase:async phase=>{if(reserve&&phase==='finalize:intent')throw Error('persist intent only');if(phase==='finalize:promoted'&&firstPromotion){firstPromotion=false;promoted.release();await resumePromoted.promise;}}});
 const input={operationId:'original-finalize',id:'blob',uploadToken:grant.uploadToken};await assert.rejects(f.objects.finalize(f.token,input),/persist intent only/);reserve=false;
 const persisted=(await f.engine.read(tx=>tx.scan({prefix:'objects/'}))).find(entry=>entry.value.protocol==='local-object-1').value,target=join(f.directory,'objects',persisted.file),originalOpen=fs.open;let delayed=true,a,b;
 fs.open=async(path,...arguments_)=>{try{return await originalOpen(path,...arguments_);}catch(error){if(path===target&&error.code==='ENOENT'&&delayed){delayed=false;missing.release();await resumeMissing.promise;}throw error;}};syncBuiltinESMExports();
 try{
  a=first==='finalize'?f.objects.finalize(f.token,input):f.objects.reconcile();void a.catch(()=>{});await missing.promise;
  b=first==='finalize'?f.objects.reconcile():f.objects.finalize(f.token,input);void b.catch(()=>{});await promoted.promise;resumeMissing.release();const firstResult=await a;
  if(first==='reconcile'){assert.equal(firstResult.cleaned,0);assert.equal(firstResult.activated,1);assert.deepEqual(firstResult.errors,[]);}else assert.deepEqual(firstResult,{ready:true,bytes:bytes.length,sha256:digest(bytes)});
  resumePromoted.release();await b;assert.equal((await f.engine.read(tx=>tx.get(sourceKey('t','blob')))).data.state,'ready');assert.deepEqual(await collect((await f.objects.download(f.token,'blob')).stream),bytes);
  const files=await f.objects.inspectFiles();assert.deepEqual(files.stages,[]);assert.deepEqual(files.objects,[persisted.file]);assert.equal((await f.objects.checkFiles()).ready,1);assert.deepEqual((await f.engine.getReceipt(persisted.finalize.receiptId)).result,{ready:true,bytes:bytes.length,sha256:digest(bytes)});await f.engine.check();
 }finally{fs.open=originalOpen;syncBuiltinESMExports();resumeMissing.release();resumePromoted.release();await Promise.allSettled([a,b].filter(Boolean));}
});
