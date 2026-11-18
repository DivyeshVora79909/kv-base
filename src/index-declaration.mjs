import { canonical, cloneData, fail } from './values.mjs';

const invalid = message => fail('INVALID_CATALOG', message);
const domains = new Set(['id', 'string', 'integer', 'boolean']);
export function indexId(signature) {
  if (!Array.isArray(signature) || signature.length !== 3 || typeof signature[0] !== 'string' || !signature[0].length
    || signature.slice(1).some(fields => !Array.isArray(fields) || fields.some(field => typeof field !== 'string' || !field.length))) invalid('Invalid structural index signature');
  return `@index/${canonical(signature)}`;
}
export function resolveIndex(catalog, signature) {
  const id = indexId(signature), index = catalog.indexes?.[id];
  if (!index) fail('INVALID_SCOPE', 'Unknown structural index');
  return index;
}
export function normalizeScope(catalog, scope) {
  if (scope?.index !== undefined) {
    if (scope.view !== undefined) fail('INVALID_SCOPE', 'Specify one index identity');
    const { index, ...rest } = scope;
    return { ...rest, view: resolveIndex(catalog, index).view };
  }
  return scope;
}

/** Lower model-owned declarations once into the engine's compiled contracts.
 * The generated identifier retains both ordered arrays; it is not a public alias.
 */
export function lowerIndexes(definition) {
  const declaration = cloneData(definition), indexes = Object.create(null);
  declaration.contracts ??= {}; declaration.views ??= {};
  for (const [modelId, model] of Object.entries(declaration.models ?? {})) {
    if (model.indexes === undefined) continue;
    if (!Array.isArray(model.indexes)) invalid('Model indexes must be an array');
    for (const spec of model.indexes) {
      if (!spec || typeof spec !== 'object' || Array.isArray(spec)
        || Object.keys(spec).some(key => !['version','partitionFields','orderFields','roles','inputs','aggregates','validators'].includes(key))) invalid('Invalid index declaration');
      const signature = [modelId, spec.partitionFields, spec.orderFields], id = indexId(signature);
      const fields = [...spec.partitionFields, ...spec.orderFields];
      if (new Set(fields).size !== fields.length || !spec.orderFields.length) invalid('Index roles must be distinct with at least one order role');
      if (!spec.roles || Array.isArray(spec.roles) || typeof spec.roles !== 'object'
        || Object.keys(spec.roles).length !== fields.length || fields.some(field => !domains.has(spec.roles[field]))) invalid('Index role domains must match its signature');
      if (indexes[id]) invalid('Duplicate or conflicting structural index definition');
      if (Object.hasOwn(declaration.views, id) || Object.hasOwn(declaration.contracts, id)) invalid('Conflicting structural index definition');
      const order = spec.orderFields.map(field => spec.roles[field]);
      declaration.contracts[id] = { version: spec.version, partition: spec.partitionFields.map(field => spec.roles[field]), order: order.length === 1 ? order[0] : order, inputs: spec.inputs, aggregates: spec.aggregates, ...(spec.validators === undefined ? {} : { validators: spec.validators }) };
      declaration.views[id] = { contract: id };
      indexes[id] = { signature, view: id, ...spec };
    }
  }
  for (const model of Object.values(declaration.models ?? {})) {
    for (const binding of model.memberships ?? []) {
      if (binding.index === undefined) continue;
      if (binding.view !== undefined) invalid('Membership specifies two index identities');
      const id = indexId(binding.index);
      if (!indexes[id]) invalid('Unknown structural index declaration');
      if (!Array.isArray(binding.order) || binding.order.length !== indexes[id].orderFields.length) invalid('Wrong order binding arity');
      binding.view = id; binding.order = binding.order.length === 1 ? binding.order[0] : binding.order; delete binding.index;
    }
  }
  return { declaration, indexes };
}
