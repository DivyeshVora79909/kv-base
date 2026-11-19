import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture} from './helpers/operations-fixture.mjs';
import {createEmailProvider} from '../src/providers.mjs';
import {sourceKey,sourcePrefix} from '../src/keys.mjs';
const skip=process.env.KV_BASE_FDB_CLUSTER_FILE?false:'Selected real FDB cluster required';
const source=(runtime,id)=>runtime.engine.read(tx=>tx.get(sourceKey('t',id)));
const attempts=runtime=>runtime.engine.read(async tx=>(await tx.scan({prefix:sourcePrefix('t')})).map(row=>row.value).filter(row=>row.model==='EffectAttempt'));

test('D5 restoring pending intent after post-backup non-idempotent delivery looks up original identity without resending',{skip},async t=>{
  const f=await fixture(t);await f.accept(f.runtime,'job');const original=await source(f.runtime,'effect:job'),backup=await f.backup();
  await f.runtime.worker().runOne(original.id);assert.equal(f.provider.stats().starts,1);assert.equal((await source(f.runtime,'job')).data.state,'delivered');
  const prepared=await f.prepare(backup),runtime=prepared.runtime;assert.equal((await source(runtime,'effect:job')).data.state,'pending');assert.equal((await attempts(runtime)).length,0);
  await runtime.worker().runOne(original.id);assert.equal((await source(runtime,'job')).data.state,'delivered');assert.equal((await source(runtime,original.id)).data.providerOperationId,original.data.providerOperationId);assert.equal(f.provider.stats().starts,1);
  assert.equal((await f.provider.ledger())[0].value.deliveries,1);const restored=await attempts(runtime);assert.equal(restored.length,1);assert.equal(restored[0].data.startedAt,String(f.time));assert.equal(restored[0].data.providerOperationId,original.data.providerOperationId);await runtime.engine.check();
});

test('D5 restored pending preflight adopts a higher durable external fence before a safe absent send',{skip},async t=>{
  const f=await fixture(t);await f.accept(f.runtime,'floor');const original=await source(f.runtime,'effect:floor'),backup=await f.backup();
  const prior=await f.providers.brevo.lookup({operationId:original.data.providerOperationId,secret:Buffer.from('external-secret'),fence:'9'});assert.equal(prior.status,'absent');assert.equal(prior.fence,'9');
  const prepared=await f.prepare(backup),runtime=prepared.runtime;await runtime.worker().runOne(original.id);const fenced=await source(runtime,original.id);assert.equal(fenced.data.state,'pending');assert.equal(fenced.data.epoch,'10');assert.equal(f.provider.stats().starts,0);
  await runtime.worker().runOne(original.id);const admitted=await attempts(runtime);assert.deepEqual(admitted.map(row=>row.data.epoch).sort((a,b)=>BigInt(a)<BigInt(b)?-1:1),['1','11']);assert.equal((await source(runtime,'floor')).data.state,'delivered');assert.equal((await f.provider.ledger())[0].value.minimumFence,'11');assert.equal((await f.provider.ledger())[0].value.deliveries,1);assert.equal(f.provider.stats().starts,1);await runtime.engine.check();
});

test('D5 restored unfenced non-idempotent pending isolates while freshly accepted current-incarnation work can execute',{skip},async t=>{
  const f=await fixture(t);await f.accept(f.runtime,'old');const original=await source(f.runtime,'effect:old'),backup=await f.backup();await f.runtime.worker().runOne(original.id);
  const prepared=await f.prepare(backup),runtime=prepared.runtime,registry={brevo:createEmailProvider({protocol:'brevo',baseUrl:f.provider.baseUrl})},worker=runtime.worker(registry);
  const outcome=await worker.runOne(original.id);assert.equal(outcome.error,'RESTORED_EFFECT_UNSAFE');assert.equal((await source(runtime,original.id)).data.state,'isolated');assert.equal((await attempts(runtime)).length,0);assert.equal(f.provider.stats().starts,1);
  await f.accept(runtime,'fresh');assert.equal((await source(runtime,'effect:fresh')).data.incarnation,prepared.descriptor.incarnation);await worker.runOne('effect:fresh');assert.equal((await source(runtime,'fresh')).data.state,'delivered');assert.equal(f.provider.stats().starts,2);const ledger=await f.provider.ledger();assert.equal(ledger.length,2);for(const row of ledger)assert.equal(row.value.deliveries,1);await runtime.engine.check();
});
