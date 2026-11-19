import { prepareCatalog, createMemoryStore, createEngine, populateAdaptive, runSeededWorkload } from '../src/index.mjs';
import { structuralActivityDefinition } from './structural.mjs';
import { sourcePrefix } from '../src/keys.mjs';

export const workloadIndex = ['resource', ['id'], ['effective', 'tie']];

/** Small reproducible workload using the activity schemas and real transaction
 * outcomes. Importing this module does no work. All diagnostics are privileged. */
export async function runWorkloadExample({ seed = 20261119, steps = 80, population = 64 } = {}) {
  const definition = structuredClone(structuralActivityDefinition);
  definition.version = 'workload-1';
  for (const index of definition.models.resource.indexes) {
    if (index.orderFields[0] !== 'effective') continue;
    index.orderFields.push('tie'); index.roles.tie = 'integer';
  }
  for (const model of Object.values(definition.models)) for (const binding of model.memberships ?? []) {
    if (binding.index[2][0] !== 'effective') continue;
    binding.index = [binding.index[0], [...binding.index[1]], ['effective', 'tie']];
    binding.order.push({ constant: '0' });
  }
  for (const index of definition.models.resource.indexes) index.validators = [{ scope: 'root', use: 'nonnegative' }];
  const catalog = prepareCatalog(definition, { validators: { nonnegative: { version: '1', validate: summary => summary.total >= 0n } } });
  const engine = createEngine({ catalog, store: createMemoryStore(), tenant: 'tenant' });
  const source = (id, model, data, owner = 'team') => ({ id, model, data, owner, tenant: 'tenant', createdAt: '0' });
  const expected = new Map(['hot', 'warm', 'cold'].map(id => [id, source(id, 'resource', { name: id })]));
  await engine.mutate([...expected.values()].map(record => ({ type: 'put', record })));
  const records = Array.from({ length: population }, (_, n) => source(`row-${n}`, 'movement', {
    resource: n % 10 < 8 ? 'hot' : n % 10 === 8 ? 'warm' : 'cold',
    effective: String(n % 3), units: '1', label: `row-${n}`,
  }, n % 2 ? 'other-team' : 'team'));
  const populationReport = await populateAdaptive(engine, records, { initialBatch: 8, maxBatch: 32 });
  for (const batch of populationReport.batches) if (batch.accepted) for (const record of records.slice(batch.offset, batch.offset + batch.size)) expected.set(record.id, record);
  const readLimits = { maxNodes: 1000, maxBatches: 1000, maxBytes: 2_000_000, maxOutput: 1000, batchKeys: 16 };
  const report = await runSeededWorkload(engine, {
    seed, steps, dataset: { population, partitions: 3, partitionRows: Object.fromEntries(['hot','warm','cold'].map(partition=>[partition,records.filter(record=>record.data.resource===partition).length])), order: 'two exact integer components', equalKeys: 'three groups with constant second component', validation: 'nonnegative root totals', adapter: 'serialized volatile memory' },
    readQuery: { index: workloadIndex, partition: ['hot'], limit: 1000 }, readLimits,
    generate: ({ step, randomInt }) => {
      const n = randomInt(population), record = structuredClone(expected.get(`row-${n}`));
      record.data.effective = String(randomInt(3)); record.data.units = String(1 + randomInt(5));
      record.data.label = `row-${n}-payload-${step}`;
      if (step % 17 === 0) record.data.units = '-1000000';
      else if (step % 11 === 0 && population > 1) record.data.label = expected.get(`row-${(n + 1) % population}`).data.label;
      return { changes: [{ type: 'put', record }], ...(step % 13 === 0 && step % 17 !== 0 ? { beforeCommit() { throw Object.assign(new Error('Injected publication rollback'), { code: 'INJECTED_ROLLBACK' }); } } : {}) };
    },
    verify: async ({ step, plan, outcome }) => {
      if (outcome.accepted) for (const change of plan.changes) expected.set(change.record.id, structuredClone(change.record));
      const actual = await engine.read(async tx => (await tx.scan({ prefix: sourcePrefix('tenant') })).map(row => row.value).sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
      const facts = [...expected.values()].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
      if (JSON.stringify(actual) !== JSON.stringify(facts)) throw new Error('Independent committed source inventory differs');
      if (step % 16 === 0) await engine.check();
    },
  });
  return { population: populationReport, workload: report, check: await engine.check(), rebuild: await engine.rebuild() };
}
