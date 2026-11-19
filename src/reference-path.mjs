import { assertJson, canonical, decodeDomain, fail, selectValue } from './values.mjs';
import { sourceKey, incomingPrefix, incomingKey } from './keys.mjs';

const own = (value, key) => Object.hasOwn(value, key);
const invalid = message => fail('INVALID_CATALOG', message);
const bad = message => fail('INVALID_REFERENCE', message);

/** Validate a finite literal path and return its terminal schema model. */
export function validatePath(models, startingModel, selector) {
  if (!selector || typeof selector !== 'object' || Array.isArray(selector)
    || !Array.isArray(selector.path) || selector.path.length === 0
    || Object.keys(selector).length !== 2
    || own(selector, 'field') === own(selector, 'header')) invalid('Path requires finite hops and one terminal selector');
  let model = startingModel;
  for (const hop of selector.path) {
    if (!hop || typeof hop !== 'object' || Array.isArray(hop)
      || Object.keys(hop).length !== 2 || typeof hop.reference !== 'string'
      || typeof hop.target !== 'string' || !own(models, hop.target)) invalid('Path requires an explicit reference slot and target model');
    const references = model.references ?? [];
    if (!Array.isArray(references)) invalid('Path model references must be an array');
    const reference = references.find(entry => entry.slot === hop.reference);
    if (!reference || reference.many !== false || !Array.isArray(reference.targets)
      || !reference.targets.includes(hop.target)) invalid('Path hop requires a declared scalar reference and allowed target');
    if (models[hop.target].managed) invalid('Path target topology must be authored');
    model = models[hop.target];
  }
  return model;
}

export function pathsForModel(catalog, modelId) {
  const paths = [];
  for (const membership of catalog.models[modelId].memberships) {
    for (const selector of [...membership.partition,
      ...(Array.isArray(membership.order) ? membership.order : [membership.order]),
      ...Object.values(membership.inputs)]) if (selector.path) paths.push(selector);
  }
  return paths;
}

/** Synchronous projection reads an explicit, tenant-local source context. */
export function selectPath(catalog, record, selector, sources) {
  if (!selector.path) return selectValue(record, selector);
  let current = record;
  for (const hop of selector.path) {
    const reference = catalog.models[current.model].references.find(entry => entry.slot === hop.reference);
    if (!reference || reference.many || !reference.targets.includes(hop.target)) bad('Invalid compiled path hop');
    if (!own(current.data, reference.field)) return { present: false };
    const id = decodeDomain('id', current.data[reference.field]);
    if (!(sources instanceof Map)) bad('Path projection requires a source context');
    const next = sources.get(id);
    if (!next) return { present: false };
    if (next.id !== id || next.tenant !== record.tenant || next.model !== hop.target) bad('Path target identity or type mismatch');
    current = next;
  }
  return selectValue(current, own(selector, 'field') ? { field: selector.field } : { header: selector.header });
}

function storedRecord(catalog, tenant, id, value) {
  if (value === undefined) return undefined;
  try {
    assertJson(value);
    const headers = ['id','tenant','owner','model','createdAt','data'];
    if (!value || Object.keys(value).length !== headers.length || headers.some(key => !own(value,key))
      || value.id !== id || value.tenant !== tenant || !catalog.models[value.model]
      || !catalog.models[value.model].validate(value.data)) throw new Error('Invalid source');
    for (const key of ['id', 'tenant', 'owner', 'model']) decodeDomain('id', value[key]);
    decodeDomain('integer', value.createdAt);
  } catch { fail('CORRUPT_STATE', 'Invalid path source'); }
  return value;
}

/** Point-load only the finite path dependencies of the supplied records. */
export async function loadPathSources(tx, catalog, tenant, records, metrics = {}) {
  const sources = new Map(records.filter(Boolean).map(record => [record.id, record]));
  const cached = new Map(sources);
  const fetch = async id => {
    if (!cached.has(id)) {
      metrics.pathReads = (metrics.pathReads ?? 0) + 1;
      cached.set(id, storedRecord(catalog, tenant, id, await tx.get(sourceKey(tenant, id))));
    }
    return cached.get(id);
  };
  for (const record of records.filter(Boolean)) for (const selector of pathsForModel(catalog, record.model)) {
    let current = record;
    for (const hop of selector.path) {
      const reference = catalog.models[current.model].references.find(entry => entry.slot === hop.reference);
      if (!own(current.data, reference.field)) break;
      const id = decodeDomain('id', current.data[reference.field]), target = await fetch(id);
      if (!target) break;
      if (target.model !== hop.target) bad('Path target type mismatch');
      sources.set(id, target); current = target;
    }
  }
  return sources;
}

/** Find contributors through both current and staged-final typed reverse edges.
 * Stored incoming entries describe the committed graph. Attempt-local source
 * entries supply every replacement/new edge across repeated settlement stages.
 */
export async function affectedPathContributors(tx, catalog, tenant, changes, staged, metrics = {}) {
  const result = new Set(changes.map(change => change.id));
  if (!catalog.hasReferencePaths || changes.length === 0) return result;
  const finals = new Map(changes.map(change => [change.id, change.next]));
  const points = new Map(), incoming = new Map();
  async function current(id) {
    if (!points.has(id)) {
      metrics.pathReads = (metrics.pathReads ?? 0) + 1;
      points.set(id, storedRecord(catalog, tenant, id, await tx.get(sourceKey(tenant, id))));
    }
    return points.get(id);
  }
  async function reverse(targetId, sourceModel, hop, final) {
    if (!incoming.has(targetId)) {
      const before = tx.inspect().metrics.scannedKeys;
      incoming.set(targetId, await tx.scan({ prefix: incomingPrefix(catalog, tenant, targetId) }));
      metrics.scannedKeys = (metrics.scannedKeys ?? 0) + tx.inspect().metrics.scannedKeys - before;
    }
    const ids = new Set();
    for (const { key, value } of incoming.get(targetId)) {
      metrics.reverseVisits = (metrics.reverseVisits ?? 0) + 1;
      try {
        const names = ['tenant','sourceId','slot','position','targetId','targets'];
        if (!value || Object.keys(value).length !== names.length || names.some(name => !own(value,name))
          || value.targetId !== targetId || value.tenant !== tenant
          || !Number.isSafeInteger(value.position) || value.position < 0
          || !Array.isArray(value.targets) || !value.targets.length
          || key !== incomingKey(catalog, tenant, targetId, value)) throw new Error('Invalid incoming identity');
        for (const id of [value.sourceId,value.targetId,...value.targets]) decodeDomain('id',id);
        decodeDomain('string',value.slot);
        const source = await current(value.sourceId), spec = source && catalog.models[source.model].references.find(entry => entry.slot === value.slot);
        // Attempt-local deletion can leave committed reverse metadata until finish.
        if (source && (!spec || canonical(value.targets) !== canonical(spec.targets)
          || (!spec.many && value.position !== 0))) throw new Error('Invalid incoming declaration');
      } catch { fail('CORRUPT_STATE', 'Invalid path incoming entry'); }
      if (value.slot === hop.reference && value.position === 0) ids.add(value.sourceId);
    }
    for (const [id, record] of staged) {
      metrics.reverseVisits = (metrics.reverseVisits ?? 0) + 1;
      if (record?.model === sourceModel) ids.add(id);
    }
    if (final) for (const [id, record] of finals) if (record?.model === sourceModel) ids.add(id);
    const found = [];
    for (const id of ids) {
      const record = final && finals.has(id) ? finals.get(id) : await current(id);
      if (!record || record.model !== sourceModel) continue;
      const reference = catalog.models[sourceModel].references.find(entry => entry.slot === hop.reference);
      if (record.data[reference.field] === targetId) found.push(id);
    }
    return found;
  }
  for (const [modelId] of Object.entries(catalog.models)) for (const selector of pathsForModel(catalog, modelId)) {
    const models = [modelId, ...selector.path.map(hop => hop.target)];
    for (const change of changes) for (let depth = 1; depth < models.length; depth++) {
      if (![change.old?.model, change.next?.model].includes(models[depth])) continue;
      for (const final of [false, true]) {
        let ids = [change.id];
        for (let level = depth - 1; level >= 0; level--) {
          const next = new Set();
          for (const id of ids) for (const source of await reverse(id, models[level], selector.path[level], final)) next.add(source);
          ids = [...next];
        }
        ids.forEach(id => result.add(id));
      }
    }
  }
  return result;
}
