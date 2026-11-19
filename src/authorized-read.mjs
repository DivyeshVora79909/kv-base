import { resolveRequestAuthority, permits } from './authority.mjs';
import { discloseSource } from './disclosure.mjs';
import { createOwnerStream } from './owner-stream.mjs';
import { catalogKey, incomingPrefix, incomingKey, sourceKey } from './keys.mjs';
import { validateRecord } from './catalog.mjs';
import { resolveIndex } from './index-declaration.mjs';
import { treeAggregate } from './tree.mjs';
import { normalizeRange, compareKey } from './range.mjs';
import { assertJson, canonical, compareDomain, decodeDomain, fail } from './values.mjs';

const invalid = () => fail('INVALID_READ', 'Invalid authorized read options');
const forbidden = () => fail('FORBIDDEN', 'Operation is not permitted');
const corrupt = () => fail('CORRUPT_STATE', 'Invalid discovery metadata');
const own = (value, key) => Object.hasOwn(value, key);
const natural = value => Number.isSafeInteger(value) && value >= 0;
const defaults = {
  maxDiscovery: 10000, maxStreams: 256, maxCandidates: 10000, maxRefills: 10000,
  maxNodeVisits: 100000, maxBatchCalls: 10000, maxReadBytes: 16 * 1024 * 1024,
  maxOutputBytes: 4 * 1024 * 1024, maxScannedKeys: 1000000, batchSize: 32,
};
function fields(value, allowed) {
  if (!value || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) invalid();
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!allowed.includes(key) || !descriptor.enumerable || !own(descriptor, 'value')) invalid();
  }
}
function prepare(context, query, options, allowed) {
  fields(options, [...Object.keys(defaults), 'getAuthority', 'onMetrics']);
  if (typeof options.getAuthority !== 'function' || (options.onMetrics !== undefined && typeof options.onMetrics !== 'function')) invalid();
  const limits = { ...defaults, ...options };
  for (const key of Object.keys(defaults)) if (!natural(limits[key])) invalid();
  if (limits.batchSize === 0) invalid();
  fields(query, allowed);
  if (allowed.includes('limit') && !natural(query.limit)) invalid();
  // Validate ordinary data before detaching, without executing user accessors.
  assertJson(context); canonical(query);
  return { context: structuredClone(context), query: structuredClone(query), limits };
}
function work(tx, limits) {
  const metrics = { discovery: 0, streams: 0, candidates: 0, refills: 0, nodes: 0,
    batches: 0, readBytes: 0, outputBytes: 0, outputs: 0, scans: 0, scannedKeys: 0 };
  const caps = { discovery: 'maxDiscovery', streams: 'maxStreams', candidates: 'maxCandidates',
    refills: 'maxRefills', nodes: 'maxNodeVisits', batches: 'maxBatchCalls',
    readBytes: 'maxReadBytes', outputBytes: 'maxOutputBytes', scannedKeys: 'maxScannedKeys' };
  function charge(name, amount = 1) {
    metrics[name] += amount;
    if (!Number.isSafeInteger(metrics[name]) || metrics[name] > limits[caps[name]]) fail('WORK_LIMIT', 'Authorized read budget exceeded');
  }
  function readBytes(key, value) {
    let count = Buffer.byteLength(key);
    if (value !== undefined) { try { count += Buffer.byteLength(canonical(value)); } catch { corrupt(); } }
    charge('readBytes', count);
  }
  async function fetch(keys, nodes = false) {
    const result = [];
    for (let start = 0; start < keys.length; start += limits.batchSize) {
      const part = keys.slice(start, start + limits.batchSize);
      charge('batches'); if (nodes) charge('nodes', part.length);
      const values = await tx.getMany(part);
      if (!Array.isArray(values) || values.length !== part.length) corrupt();
      values.forEach((value, index) => { readBytes(part[index], value); result.push(value); });
    }
    return result;
  }
  function output(record) { charge('outputBytes', Buffer.byteLength(canonical(record))); metrics.outputs++; }
  return { metrics, charge, fetch, readBytes, output };
}
async function authority(tx, catalog, context, limits, meter) {
  const scope = await resolveRequestAuthority(tx, catalog, context, limits.getAuthority, amount => meter.charge('discovery', amount));
  const [identity] = await meter.fetch([catalogKey(scope.tenant)]);
  if (identity !== undefined && identity !== catalog.identity) fail('CATALOG_MISMATCH', 'Store catalog identity differs');
  return scope;
}
function resource(catalog, scope, model) {
  if (typeof model !== 'string' || !own(catalog.models, model)
    || !permits(scope, { tenant: scope.tenant, model, owner: scope.actor }, 'read')) forbidden();
  return catalog.models[model];
}
async function result(records, meter, limits) {
  // Diagnostics are privileged host callbacks, separate from the disclosed page.
  if (limits.onMetrics) await limits.onMetrics(structuredClone(meter.metrics));
  return { records };
}

function ownerPlan(catalog, model, query) {
  if (query.index !== undefined) {
    if (query.order !== undefined) invalid();
    const index = resolveIndex(catalog, query.index), contract = index.contract;
    const compatible = model.memberships.flatMap(membership => {
      if (membership.view !== index.view) return [];
      const positions = membership.partition.flatMap((selector, position) => selector.header === 'owner' && selector.path === undefined ? [position] : []);
      return positions.length === 1 && contract.partition[positions[0]] === 'id' ? [{ membership, position: positions[0] }] : [];
    });
    if (!compatible.length || new Set(compatible.map(binding => binding.position)).size !== 1) invalid();
    return { view: index.view, ownerPartition: compatible[0].position, slots: new Set(compatible.map(binding => binding.membership.slot)) };
  }
  if (typeof query.order !== 'string' || !own(model.access.orders, query.order)) invalid();
  const plan = model.access.orders[query.order];
  return { ...plan, slots: new Set([plan.slot]) };
}

/** Point lookup uses the same tenant, source validation and record policy as
 * indexed and reverse reads. Absent and unreadable points return an empty page. */
export async function readPointAuthorized(store, catalog, context, query, options = {}) {
  const fixed = prepare(context, query, options, ['id']);
  decodeDomain('id', fixed.query.id);
  return store.read(async tx => {
    const { context, query, limits } = fixed, meter = work(tx, limits);
    const scope = await authority(tx, catalog, context, limits, meter);
    meter.charge('candidates');
    const record = await discloseSource(catalog, scope, query.id, meter.fetch);
    const records = record ? [record] : [];
    records.forEach(meter.output);
    return result(records, meter, limits);
  });
}

/** Explicit publication reads the complete anchor partition. Only declared
 * aggregate fields are returned; membership counts and identities stay internal. */
export async function readAnchorSummaryAuthorized(store, catalog, context, query, options = {}) {
  const fixed = prepare(context, query, options, ['index', 'partition', 'anchorId']);
  return store.read(tx => readAnchorSummaryAuthorizedInTransaction(tx, catalog, fixed.context, fixed.query, fixed.limits));
}

export async function readAnchorSummaryAuthorizedInTransaction(tx, catalog, requestContext, requestQuery, options = {}) {
  const fixed = prepare(requestContext, requestQuery, options, ['index', 'partition', 'anchorId']);
  const index = resolveIndex(catalog, fixed.query.index), publication = index.publication;
  if (!publication || !Array.isArray(fixed.query.partition) || fixed.query.partition.length !== index.contract.partition.length) invalid();
  const partition = fixed.query.partition.map((value, position) => decodeDomain(index.contract.partition[position], value));
  const anchorId = partition[publication.anchorPartition];
  if (fixed.query.anchorId !== undefined && fixed.query.anchorId !== anchorId) invalid();
  const { context, limits } = fixed, meter = work(tx, limits);
  const scope = await authority(tx, catalog, context, limits, meter);
  meter.charge('candidates');
  const anchor = await discloseSource(catalog, scope, anchorId, meter.fetch, { model: index.signature[0] });
  if (!anchor) forbidden();
  const readTx = { get: async key => (await meter.fetch([key], key.startsWith('node/')))[0] };
  const full = await treeAggregate(readTx, catalog, { tenant: scope.tenant, index: index.signature, partition });
  const summary = Object.fromEntries(publication.aggregates.map(name => [name, full[name]]));
  meter.output(summary);
  if (limits.onMetrics) await limits.onMetrics(structuredClone(meter.metrics));
  return { summary };
}

/** Same-snapshot owner merge. Heads and iterator paths survive each heap pop;
 * no cross-request continuation or visible rank is implied. */
export async function readAuthorized(store, catalog, context, query, options = {}) {
  const fixed = prepare(context, query, options, ['model', 'order', 'index', 'partition', 'range', 'limit']);
  return store.read(tx => readAuthorizedInTransaction(tx, catalog, fixed.context, fixed.query, fixed.limits));
}

export async function readAuthorizedInTransaction(tx, catalog, requestContext, requestQuery, options = {}) {
  const fixed = prepare(requestContext, requestQuery, options, ['model', 'order', 'index', 'partition', 'range', 'limit']);
  const { context, query, limits } = fixed, meter = work(tx, limits);
  const scope = await authority(tx, catalog, context, limits, meter);
  const model = resource(catalog, scope, query.model);
  const plan = ownerPlan(catalog, model, query), contract = catalog.views[plan.view].contract;
  if (!Array.isArray(query.partition) || query.partition.length !== contract.partition.length - 1) invalid();
  const partition = []; let position = 0;
  contract.partition.forEach((domain, index) => { partition.push(index === plan.ownerPartition ? null : decodeDomain(domain, query.partition[position++])); });
  if (own(query, 'range') && query.range === undefined) invalid();
  const range = own(query, 'range') ? normalizeRange(contract.order, query.range) : {};
  const records = [], heap = [];
  const compare = (a, b) => compareKey(contract.order, a.head, b.head);
  function push(item) {
    let index = heap.length; heap.push(item);
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (compare(heap[parent], item) <= 0) break;
      heap[index] = heap[parent]; index = parent;
    }
    heap[index] = item;
  }
  function pop() {
    const first = heap[0], last = heap.pop();
    if (heap.length) {
      let index = 0;
      while (index * 2 + 1 < heap.length) {
        let child = index * 2 + 1;
        if (child + 1 < heap.length && compare(heap[child + 1], heap[child]) < 0) child++;
        if (compare(last, heap[child]) <= 0) break;
        heap[index] = heap[child]; index = child;
      }
      heap[index] = last;
    }
    return first;
  }
  async function advance(item, refill) {
    if (refill) meter.charge('refills');
    for (;;) {
      const node = await item.stream.next();
      if (!node) return;
      meter.charge('candidates');
      let validatedSource;
      const fetchSource = async keys => {
        const values = await meter.fetch(keys, false);
        const sourcePosition = keys.indexOf(sourceKey(scope.tenant, node.sourceId));
        if (sourcePosition !== -1) validatedSource = values[sourcePosition];
        return values;
      };
      const record = await discloseSource(catalog, scope, node.sourceId, fetchSource, {
        occurrence: { ...node, view: plan.view, partition: item.partition },
      });
      // Every candidate is validated against complete source facts before model,
      // membership and permission filters. Other contributors share this stream.
      if (validatedSource.model !== query.model || !plan.slots.has(node.slot)) continue;
      if (validatedSource.owner !== item.partition[plan.ownerPartition]) corrupt();
      if (!record) corrupt();
      push({ ...item, head: node, record });
      return;
    }
  }
  if (query.limit > 0) {
    for (const owner of scope.accessible) {
      meter.charge('discovery'); meter.charge('streams');
      const streamPartition = [...partition]; streamPartition[plan.ownerPartition] = owner;
      const stream = await createOwnerStream(catalog, { tenant: scope.tenant, view: plan.view, partition: streamPartition }, range, meter.fetch);
      await advance({ stream, partition: streamPartition }, false);
    }
    while (heap.length && records.length < query.limit) {
      const item = pop(); meter.output(item.record); records.push(item.record);
      if (records.length < query.limit) await advance(item, true);
    }
  }
  return result(records, meter, limits);
}

/** Reverse-reference discovery composes existing metadata and source disclosure.
 * Scan accounting is post-fetch; the memory adapter physically enumerates its Map. */
export async function readIncomingAuthorized(store, catalog, context, query, options = {}) {
  const fixed = prepare(context, query, options, ['targetId', 'model', 'slot', 'limit']);
  decodeDomain('id', fixed.query.targetId); decodeDomain('string', fixed.query.slot);
  return store.read(async tx => {
    const { context, query, limits } = fixed, meter = work(tx, limits);
    const scope = await authority(tx, catalog, context, limits, meter);
    const model = resource(catalog, scope, query.model);
    const spec = model.references.find(item => item.slot === query.slot);
    if (!spec) invalid();
    const [storedTarget] = await meter.fetch([sourceKey(scope.tenant, query.targetId)]);
    if (storedTarget === undefined) forbidden();
    let target;
    try {
      target = validateRecord(catalog, storedTarget);
      if (target.tenant !== scope.tenant || target.id !== query.targetId) corrupt();
    } catch { corrupt(); }
    if (!spec.targets.includes(target.model)) invalid();
    const records = new Map();
    if (query.limit > 0) {
      // Read one beyond the remaining discovery budget to detect truncation.
      const before = tx.inspect().metrics.scannedKeys;
      const entries = await tx.scan({ prefix: incomingPrefix(catalog, scope.tenant, target.id), limit: limits.maxDiscovery - meter.metrics.discovery + 1 });
      meter.metrics.scans++;
      meter.charge('scannedKeys', tx.inspect().metrics.scannedKeys - before);
      for (const { key, value: ref } of entries) {
        meter.charge('discovery'); meter.charge('candidates'); meter.readBytes(key, ref);
        try {
          decodeDomain('id', ref?.sourceId);
          if (!ref || ref.targetId !== target.id || !ref.targets.includes(target.model)
            || key !== incomingKey(catalog, scope.tenant, target.id, ref)) corrupt();
        } catch { corrupt(); }
        const record = await discloseSource(catalog, scope, ref.sourceId, meter.fetch, { reference: ref });
        if (record?.model === query.model && ref.slot === query.slot) records.set(record.id, record);
      }
    }
    const page = [...records.values()].sort((a, b) => compareDomain('id', a.id, b.id)).slice(0, query.limit);
    page.forEach(meter.output);
    return result(page, meter, limits);
  });
}
