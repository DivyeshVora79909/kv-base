import { permits, resolveRequestAuthority } from './authority.mjs';
import { validateRecord } from './catalog.mjs';
import { mutateInTransaction } from './mutate.mjs';
import { sourceKey } from './keys.mjs';
import { GENERATED_PREFIX } from './calculation-identity.mjs';
import { assertJson, cloneData, decodeDomain, fail } from './values.mjs';

const forbidden = () => fail('FORBIDDEN', 'Operation is not permitted');
const permit = (scope, record, action) => { if (!permits(scope, record, action)) forbidden(); };

function mutationOptions(options) {
  if (!options || typeof options !== 'object' || Array.isArray(options)
      || ![Object.prototype, null].includes(Object.getPrototypeOf(options))) fail('INVALID_MUTATION', 'Expected mutation options');
  const allowed = ['getAuthority', 'beforeCommit', 'maxProducers', 'maxEdges', 'maxRouteVisits', 'maxPotentialVisits'];
  for (const key of Reflect.ownKeys(options)) {
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (!allowed.includes(key) || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) fail('INVALID_MUTATION', 'Unknown mutation option');
  }
  if (typeof options.getAuthority !== 'function') fail('INVALID_AUTHORITY', 'Expected transaction authority provider');
  if (options.beforeCommit !== undefined && typeof options.beforeCommit !== 'function') fail('INVALID_MUTATION', 'Expected commit hook');
  for (const key of allowed.slice(2)) if (options[key] !== undefined && options[key] !== Infinity
      && (!Number.isSafeInteger(options[key]) || options[key] < 0)) fail('CALCULATION_BUDGET', 'Invalid settlement budget');
  return { ...options };
}

function parseChanges(changes) {
  assertJson(changes);
  if (!Array.isArray(changes)) fail('INVALID_MUTATION', 'Expected changes array');
  const detached = cloneData(changes), seen = new Set();
  for (const change of detached) {
    if (!change || typeof change !== 'object' || Array.isArray(change)) fail('INVALID_MUTATION', 'Expected command');
    const allowed = change.type === 'put' ? ['type', 'record'] : change.type === 'delete' ? ['type', 'id'] : [];
    if (Object.keys(change).length !== allowed.length || allowed.some(key => !Object.hasOwn(change, key))) fail('INVALID_MUTATION', 'Invalid command');
    const id = change.type === 'put' ? change.record?.id : change.id;
    decodeDomain('id', id);
    if (seen.has(id)) fail('DUPLICATE_SOURCE', 'Duplicate source command');
    seen.add(id);
  }
  return detached;
}

function references(catalog, record) {
  const result = new Map();
  if (record) for (const spec of catalog.models[record.model].references) {
    if (!Object.hasOwn(record.data, spec.field)) continue;
    const value = record.data[spec.field];
    if (spec.many && !Array.isArray(value)) fail('INVALID_REFERENCE', 'Expected reference array');
    (spec.many ? value : [value]).forEach((targetId, position) => {
      decodeDomain('id', targetId);
      result.set(JSON.stringify([spec.slot, position]), { targetId, targets: spec.targets });
    });
  }
  return result;
}

/** Fixture-authorized authored commands, including privileged settlement, publish once.
 * getAuthority is trusted host code and must read current policy through this tx. */
export async function mutateAuthorized(store, catalog, context, changes, options) {
  const { getAuthority, beforeCommit, ...settlementOptions } = mutationOptions(options);
  assertJson(context);
  const request = cloneData(context), commands = parseChanges(changes);
  return store.transact(async tx => {
    const initialScope = await resolveRequestAuthority(tx, catalog, request, getAuthority);
    const originals = new Map(), entries = new Map(), uses = [];
    async function readSource(id) {
      const record = await tx.get(sourceKey(request.tenant, id));
      if (record !== undefined) {
        try { validateRecord(catalog, record); } catch { fail('CORRUPT_STATE', 'Invalid stored source'); }
        if (record.id !== id || record.tenant !== request.tenant) fail('CORRUPT_STATE', 'Source key mismatch');
      }
      return record;
    }
    async function original(id) {
      if (!originals.has(id)) originals.set(id, await readSource(id));
      return originals.get(id);
    }
    function authorize(scope, entry, next = entry.next) {
      if (entry.old) permit(scope, entry.old, entry.action);
      if (next) permit(scope, next, entry.action);
    }
    for (const command of commands) {
      const id = command.type === 'put' ? command.record.id : command.id;
      const old = await original(id), next = command.type === 'put' ? command.record : undefined;
      if (!old && !next) forbidden();
      const entry = { old, next, action: next ? old ? 'update' : 'create' : 'delete' };
      // Scope precedes immutable-header, reference, uniqueness and business diagnostics.
      authorize(initialScope, entry);
      if (next) validateRecord(catalog, next);
      entries.set(id, entry);
    }
    for (const entry of entries.values()) {
      const oldReferences = references(catalog, entry.old);
      for (const [position, ref] of references(catalog, entry.next)) {
        if (oldReferences.get(position)?.targetId === ref.targetId) continue;
        const old = await original(ref.targetId);
        const next = entries.has(ref.targetId) ? entries.get(ref.targetId).next : old;
        if (old) permit(initialScope, old, 'use');
        if (next) {
          if (!ref.targets.includes(next.model)) forbidden();
          permit(initialScope, next, 'use');
        } else if (!ref.targetId.startsWith(GENERATED_PREFIX) || entries.has(ref.targetId)) forbidden();
        // A newly generated target can first exist after settlement. Its identity
        // and Use authorization are checked below before any publication.
        uses.push({ ...ref, old });
      }
    }
    await mutateInTransaction(tx, catalog, request.tenant, commands, settlementOptions);
    const finalScope = await resolveRequestAuthority(tx, catalog, request, getAuthority);
    for (const [id, entry] of entries) {
      authorize(finalScope, entry);
      const next = await readSource(id);
      if (next) permit(finalScope, next, entry.action);
    }
    for (const ref of uses) {
      if (ref.old) permit(finalScope, ref.old, 'use');
      const next = await readSource(ref.targetId);
      if (!next || !ref.targets.includes(next.model)) forbidden();
      permit(initialScope, next, 'use');
      permit(finalScope, next, 'use');
    }
    // Raw mutation results contain privileged derived scopes and guard keys.
    return { applied: commands.length };
  }, { beforeCommit });
}
