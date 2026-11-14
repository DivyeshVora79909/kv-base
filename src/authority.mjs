import { assertJson, cloneData, freezeDeep, fingerprint, decodeDomain } from './values.mjs';

// Brands protect handle authenticity and keep mutable graph indexes private. They
// are not a security boundary against trusted JavaScript executing in this process.
const preparedData = new WeakMap(), scopeData = new WeakMap();
const actions = ['read', 'create', 'update', 'delete', 'use'];
const own = (value, key) => Object.hasOwn(value, key);
const fail = (message, code = 'INVALID_AUTHORITY') => { throw Object.assign(new Error(message), { code }); };
function json(value) { try { assertJson(value); } catch { fail('Authority requires JSON data'); } }
function object(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${label} must be an object`);
}
function exact(value, keys, label) {
  object(value, label);
  if (Object.keys(value).length !== keys.length || keys.some(key => !own(value, key))) fail(`Invalid ${label} keys`);
}
function id(value) { try { return decodeDomain('id', value); } catch { fail('Expected a nonempty well-formed identifier'); } }
function effective(node, nodes) {
  const result = { admin: node.grants.admin, resources: Object.assign(Object.create(null), cloneData(node.grants.resources)) };
  if (node.kind === 'user') for (const parentId of node.parents) {
    const parent = nodes.get(parentId);
    if (!parent.active || !['group', 'tenant'].includes(parent.kind)) continue;
    result.admin ||= parent.grants.admin;
    for (const [model, granted] of Object.entries(parent.grants.resources)) {
      result.resources[model] = [...new Set([...(own(result.resources, model) ? result.resources[model] : []), ...granted])].sort();
    }
  }
  return result;
}

/** Prepare fixture authority. Graph work is O(V+E); canonical sorting adds
 * O(V log V + sum(degree log degree) + grant sorting). No all-pairs closure. */
export function prepareAuthority(catalog, definition, options = {}) {
  if (!catalog || typeof catalog.identity !== 'string' || !catalog.models || typeof catalog.models !== 'object') fail('Expected a prepared catalog');
  object(options, 'Authority options');
  const prototype = Object.getPrototypeOf(options);
  if (prototype !== Object.prototype && prototype !== null) fail('Authority options must be plain data');
  for (const key of Reflect.ownKeys(options)) {
    if (typeof key !== 'string' || !['maxPrincipals', 'maxEdges'].includes(key)) fail('Unknown authority option');
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (!descriptor.enumerable || !own(descriptor, 'value')) fail('Authority options must be enumerable data');
  }
  const caps = {};
  for (const key of ['maxPrincipals', 'maxEdges']) {
    const value = own(options, key) ? options[key] : Infinity;
    if (value !== Infinity && (!Number.isSafeInteger(value) || value < 0)) fail(`Invalid ${key}`);
    caps[key] = value;
  }
  json(definition);
  const declaration = cloneData(definition);
  exact(declaration, ['tenant', 'principals'], 'authority'); id(declaration.tenant);
  if (!Array.isArray(declaration.principals)) fail('Principals must be an array');
  if (declaration.principals.length > caps.maxPrincipals) fail('Principal budget exceeded', 'AUTHORITY_BUDGET');
  const nodes = new Map(), children = new Map(), pending = new Map();
  let edges = 0;
  for (const node of declaration.principals) {
    exact(node, ['id', 'kind', 'parents', 'active', 'grants'], 'principal'); id(node.id);
    if (nodes.has(node.id)) fail('Duplicate principal');
    if (!['tenant', 'group', 'user'].includes(node.kind) || typeof node.active !== 'boolean') fail('Invalid principal kind or active flag');
    if (!Array.isArray(node.parents)) fail('Parents must be an array');
    node.parents.forEach(id);
    if (new Set(node.parents).size !== node.parents.length) fail('Duplicate parent');
    edges += node.parents.length;
    if (edges > caps.maxEdges) fail('Edge budget exceeded', 'AUTHORITY_BUDGET');
    exact(node.grants, ['admin', 'resources'], 'grants');
    if (typeof node.grants.admin !== 'boolean') fail('Admin must be boolean');
    object(node.grants.resources, 'Resources');
    for (const [model, granted] of Object.entries(node.grants.resources)) {
      if (!own(catalog.models, model)) fail('Unknown resource model');
      if (!Array.isArray(granted) || granted.some(action => !actions.includes(action))) fail('Invalid resource actions');
      node.grants.resources[model] = [...new Set(granted)].sort();
    }
    node.parents.sort(); nodes.set(node.id, node); children.set(node.id, []); pending.set(node.id, node.parents.length);
  }
  const root = nodes.get(declaration.tenant);
  if (!root || root.kind !== 'tenant' || !root.active || root.parents.length) fail('One active tenant root is required');
  for (const node of nodes.values()) {
    if (node !== root && (node.kind === 'tenant' || !node.parents.length)) fail('Invalid non-root principal');
    for (const parent of node.parents) {
      if (!nodes.has(parent)) fail('Unknown parent');
      children.get(parent).push(node.id);
    }
  }
  const ready = [root.id];
  for (let index = 0; index < ready.length; index++) for (const child of children.get(ready[index])) {
    const count = pending.get(child) - 1; pending.set(child, count);
    if (count === 0) ready.push(child);
  }
  if (ready.length !== nodes.size) fail('Principal cycle');
  const hasAdministrator = [...nodes.values()].some(node => node.kind === 'user' && node.active
    && (node.grants.admin || node.parents.some(parentId => {
      const parent = nodes.get(parentId);
      return parent.active && ['group', 'tenant'].includes(parent.kind) && parent.grants.admin;
    })));
  if (!hasAdministrator) fail('Retain an active user administrator');
  declaration.principals.sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  const handle = freezeDeep({ tenant: declaration.tenant, identity: fingerprint({ policy: 'fixture-owner-dag-1', catalog: catalog.identity, declaration }), metrics: { principals: nodes.size, edges } });
  preparedData.set(handle, { catalog, nodes, children });
  return handle;
}

/** Descendant traversal is O(reached vertices + reached edges). Resolution also
 * visits direct parents, copies/merges own and inherited grant contents, and
 * freezes returned arrays/grants; resource unions have at most five actions. */
export function resolveAuthority(prepared, context) {
  const data = preparedData.get(prepared);
  if (!data) fail('Expected a prepared authority handle');
  json(context); exact(context, ['tenant', 'actor'], 'authority context'); id(context.tenant); id(context.actor);
  const node = data.nodes.get(context.actor);
  if (context.tenant !== prepared.tenant || !node?.active) fail('Operation is not permitted', 'FORBIDDEN');
  const descendants = new Set(), queue = [node.id];
  for (let index = 0; index < queue.length; index++) for (const child of data.children.get(queue[index])) {
    if (!descendants.has(child)) { descendants.add(child); queue.push(child); }
  }
  const accessible = new Set([node.id, ...descendants]);
  for (const parent of node.parents) if (data.nodes.get(parent).kind === 'group') accessible.add(parent);
  const grants = effective(node, data.nodes);
  const scope = freezeDeep({ tenant: prepared.tenant, actor: node.id, active: true, accessible: [...accessible], descendants: [...descendants], grants });
  scopeData.set(scope, { catalog: data.catalog, accessible, descendants });
  return scope;
}

/** Model access is explicit opt-in; admin never widens owners or grants use. */
export function permits(scope, record, action) {
  const data = scopeData.get(scope);
  if (!data) fail('Expected a resolved authority scope');
  if (!actions.includes(action)) fail('Unsupported authority action');
  if (!record || typeof record !== 'object'
    || typeof record.tenant !== 'string' || typeof record.model !== 'string' || typeof record.owner !== 'string'
    || record.tenant !== scope.tenant || !own(data.catalog.models, record.model)) return false;
  if (!data.catalog.models[record.model].access) return false;
  if (!(scope.grants.admin && action !== 'use') && !(own(scope.grants.resources, record.model) && scope.grants.resources[record.model].includes(action))) return false;
  return action === 'delete' ? record.owner === scope.actor || data.descendants.has(record.owner) : data.accessible.has(record.owner);
}
