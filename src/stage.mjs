import { validateRecord, projectRecord } from './catalog.mjs';
import { canonical, decodeDomain, selectValue } from './values.mjs';
import { sourceKey, catalogKey, incomingPrefix, incomingKey, claimKey } from './keys.mjs';
import { treePut, treeDelete, readRoot, treeGet } from './tree.mjs';
import { affectedPathContributors, loadPathSources } from './reference-path.mjs';

const fail = (code, message) => { throw Object.assign(new Error(message), { code }); };
const equal = (a, b) => canonical(a) === canonical(b);
function relations(catalog, record) {
  const references = [], claims = [];
  const model = catalog.models[record.model];
  for (const spec of model.references) {
    if (!Object.hasOwn(record.data, spec.field)) continue;
    const value = record.data[spec.field];
    if (spec.many && !Array.isArray(value)) fail('INVALID_REFERENCE', 'Expected reference array');
    (spec.many ? value : [value]).forEach((id, position) => {
      decodeDomain('id', id);
      references.push({ tenant: record.tenant, sourceId: record.id, slot: spec.slot, position, targetId: id, targets: [...spec.targets] });
    });
  }
  for (const spec of model.uniqueness) {
    const values = spec.fields.map(selector => {
      const selected = selectValue(record, selector);
      if (!selected.present || selected.value === undefined) fail('MISSING_CLAIM', `Missing claim value ${spec.slot}`);
      return selected.value;
    });
    claims.push({ tenant: record.tenant, model: record.model, slot: spec.slot, values, sourceId: record.id });
  }
  return { references, claims };
}
const scopeOf = occurrence => ({ tenant: occurrence.tenant, view: occurrence.view, partition: occurrence.partition });

function commandsFor(catalog, tenant, changes) {
  if (!Array.isArray(changes)) fail('INVALID_MUTATION', 'Expected changes array');
  const seen = new Set();
  const commands = changes.map(change => {
    if (!change || typeof change !== 'object' || Array.isArray(change)) fail('INVALID_MUTATION', 'Expected command');
    let record, id;
    if (change.type === 'put') {
      if (Object.keys(change).some(key => !['type', 'record'].includes(key))) fail('INVALID_MUTATION', 'Unknown command field');
      record = validateRecord(catalog, change.record); id = record.id;
      if (record.tenant !== tenant) fail('NAMESPACE_MISMATCH', 'Record belongs to another tenant');
    } else if (change.type === 'delete') {
      if (Object.keys(change).some(key => !['type', 'id'].includes(key))) fail('INVALID_MUTATION', 'Unknown command field');
      id = decodeDomain('id', change.id);
    } else fail('INVALID_MUTATION', 'Unknown command');
    if (seen.has(id)) fail('DUPLICATE_SOURCE', `Duplicate command for ${id}`);
    seen.add(id);
    return { id, record };
  });
  return commands;
}

function occurrencesBySlot(occurrences) {
  return new Map(occurrences.map(o => [canonical([o.view, o.slot]), o]));
}

async function reconcileMemberships(tx, catalog, oldRows, nextRows) {
  const old = occurrencesBySlot(oldRows);
  const next = occurrencesBySlot(nextRows);
  for (const key of new Set([...old.keys(), ...next.keys()])) {
    const a = old.get(key), b = next.get(key);
    const samePosition = a && b && equal(a.partition, b.partition) && equal(a.order, b.order);
    if (samePosition) {
      const node = await treeGet(tx, catalog, a, a);
      if (!node || node.sourceId !== a.sourceId || node.slot !== a.slot || !equal(node.order, a.order) || !equal(node.contributions, a.contributions)) fail('CORRUPT_STATE', 'Missing or invalid old membership');
      if (equal(a, b)) continue;
      await treePut(tx, catalog, b);
      continue;
    }
    if (a) {
      if (!await treeDelete(tx, catalog, a)) fail('CORRUPT_STATE', 'Missing old membership');
    }
    if (b) await treePut(tx, catalog, b);
  }
}

function membershipTransitions(oldRows, nextRows) {
  const old = occurrencesBySlot(oldRows);
  const next = occurrencesBySlot(nextRows);
  return [...new Set([...old.keys(), ...next.keys()])].filter(key => !old.has(key) || !next.has(key) || !equal(old.get(key), next.get(key)))
    .map(key => [old.get(key), next.get(key)]);
}

/** Attempt-local internal workspace. Caller must propagate errors to abort its transaction. */
export async function createMutation(tx, catalog, tenant) {
  decodeDomain('id', tenant);
  const identityKey = catalogKey(tenant), identity = await tx.get(identityKey);
  if (identity === undefined) await tx.set(identityKey, catalog.identity);
  else if (identity !== catalog.identity) fail('CATALOG_MISMATCH', 'Store catalog identity differs');
  const entries = new Map();
  const referenceWork = { pathReads: 0, reverseVisits: 0, scannedKeys: 0 };
  let state = 'open';
  function assertOpen() {
    if (state !== 'open') fail('MUTATION_CLOSED', `Mutation workspace is ${state}`);
  }
  async function stage(changes) {
    assertOpen();
    state = 'staging';
    try {
      const commands = commandsFor(catalog, tenant, changes), changed = [];
      for (const command of commands) {
        const key = sourceKey(tenant, command.id), current = await tx.get(key), next = command.record;
        command.old = current; command.next = next;
        if (current !== undefined) {
          validateRecord(catalog, current);
          if (current.id !== command.id || current.tenant !== tenant) fail('CORRUPT_STATE', 'Source key mismatch');
        }
        const entry = entries.get(command.id);
        for (const prior of [entry?.old, current, entry?.header]) {
          if (prior !== undefined && next && (prior.model !== next.model || prior.createdAt !== next.createdAt)) fail('IMMUTABLE_HEADER', 'Model and createdAt are immutable');
        }
        // Parse relation shapes now; existence and claims belong to the final graph.
        if (next) relations(catalog, next);
        if (current === undefined ? next !== undefined : next === undefined || !equal(current, next)) changed.push(command);
      }
      const affected = await affectedPathContributors(tx, catalog, tenant, changed,
        new Map([...entries].map(([id, entry]) => [id, entry.next])), referenceWork);
      // Ordinary no-op commands still protect/check their old memberships.
      for (const command of commands) affected.add(command.id);
      const originals = new Map();
      for (const id of affected) {
        const command = commands.find(entry => entry.id === id);
        originals.set(id, command ? command.old : await tx.get(sourceKey(tenant, id)));
      }
      const oldContext = await loadPathSources(tx, catalog, tenant, [...originals.values()], referenceWork);
      const before = new Map();
      for (const [id, record] of originals) {
        if (record) validateRecord(catalog, record);
        const rows = record ? projectRecord(catalog, record, oldContext) : [];
        before.set(id, rows);
        if (!entries.has(id)) entries.set(id, { old: record, next: record, header: record,
          oldOccurrences: rows, nextOccurrences: rows });
      }
      // All authored source writes precede path projection of the final graph.
      for (const command of changed) {
        const key = sourceKey(tenant, command.id);
        if (command.next) await tx.set(key, command.next); else await tx.delete(key);
      }
      const finals = new Map();
      for (const id of affected) finals.set(id, await tx.get(sourceKey(tenant, id)));
      const finalContext = await loadPathSources(tx, catalog, tenant, [...finals.values()], referenceWork);
      const projections = [];
      for (const [id, record] of finals) {
        const rows = record ? projectRecord(catalog, record, finalContext) : [];
        await reconcileMemberships(tx, catalog, before.get(id), rows);
        const entry = entries.get(id);
        entry.next = record; entry.header ??= record; entry.nextOccurrences = rows;
        projections.push({ sourceId: id, old: before.get(id), next: rows });
      }
      state = 'open';
      return { sources: commands.map(command => ({ old: command.old, next: command.next })), projections, oldContext, finalContext };
    } catch (error) { state = 'failed'; throw error; }
  }
  async function finish({ scopes: extraScopes = [] } = {}) {
    assertOpen();
    state = 'finishing';
    try {
      const touched = [...entries.values()].map(entry => ({
        ...entry,
        changed: entry.old === undefined ? entry.next !== undefined : entry.next === undefined || !equal(entry.old, entry.next),
        oldRelations: entry.old !== undefined ? relations(catalog, entry.old) : { references: [], claims: [] },
        nextRelations: entry.next !== undefined ? relations(catalog, entry.next) : { references: [], claims: [] },
      }));
      // Guard and release every old claim/reference before any acquisition.
      for (const entry of touched) {
        for (const ref of entry.oldRelations.references) {
          const key = incomingKey(catalog, tenant, ref.targetId, ref), stored = await tx.get(key);
          if (stored === undefined || !equal(stored, ref)) fail('CORRUPT_STATE', 'Missing or invalid incoming reference');
          if (entry.changed) await tx.delete(key);
        }
        for (const claim of entry.oldRelations.claims) {
          const key = claimKey(catalog, tenant, claim.model, claim.slot, claim.values), stored = await tx.get(key);
          if (stored === undefined || !equal(stored, claim)) fail('CORRUPT_STATE', 'Missing or invalid uniqueness claim');
          if (entry.changed) await tx.delete(key);
        }
      }
      for (const entry of touched) {
        for (const ref of entry.nextRelations.references) {
          const target = await tx.get(sourceKey(tenant, ref.targetId));
          if (!target || target.tenant !== tenant || target.id !== ref.targetId || !ref.targets.includes(target.model)) fail('INVALID_REFERENCE', `Invalid final reference ${ref.targetId}`);
          if (entry.changed) await tx.set(incomingKey(catalog, tenant, ref.targetId, ref), ref);
        }
        for (const claim of entry.nextRelations.claims) {
          const key = claimKey(catalog, tenant, claim.model, claim.slot, claim.values), stored = await tx.get(key);
          if (stored !== undefined && !equal(stored, claim)) fail('DUPLICATE_CLAIM', `Claim already held: ${claim.slot}`);
          if (entry.changed) await tx.set(key, claim);
        }
      }
      for (const entry of touched) if (entry.old && !entry.next) {
        if ((await tx.scan({ prefix: incomingPrefix(catalog, tenant, entry.old.id), limit: 1 })).length) fail('DELETE_RESTRICTED', `Source ${entry.old.id} still referenced`);
      }
      // Validator obligations follow committed old-to-final transitions. A scope
      // visited only by transient stages is not part of the final mutation.
      const scopes = new Map();
      let changedMemberships = 0;
      for (const entry of touched) {
        const transitions = membershipTransitions(entry.oldOccurrences, entry.nextOccurrences);
        changedMemberships += transitions.length;
        for (const transition of transitions) for (const occurrence of transition) {
          if (!occurrence) continue;
          const scope = scopeOf(occurrence);
          scopes.set(canonical(scope), scope);
        }
      }
      if (!Array.isArray(extraScopes)) fail('INVALID_SCOPE', 'Expected final scope list');
      for (const scope of extraScopes) {
        if (scope?.tenant !== tenant) fail('NAMESPACE_MISMATCH', 'Scope belongs to another tenant');
        scopes.set(canonical(scope), scope);
      }
      for (const scope of scopes.values()) {
        const contract = catalog.views[scope.view].contract, root = await readRoot(tx, catalog, scope);
        const aggregates = root?.aggregates ?? Object.fromEntries(Object.entries(contract.aggregates).map(([name, spec]) => [name, spec.implementation.identity()]));
        for (const rule of contract.validators) {
          const result = rule.implementation.validate(structuredClone(aggregates), { scope: structuredClone(scope) });
          if (result && typeof result.then === 'function') Promise.resolve(result).catch(() => {});
          if (result !== true) fail('VALIDATION_FAILED', `Summary validator ${rule.use} rejected scope`);
        }
      }
      // Counts describe committed old-to-final differences, not transient staging work.
      const changedSources = touched.filter(entry => entry.changed).length;
      state = 'finished';
      return { changedSources, changedMemberships, trees: [...scopes.values()], metrics: tx.inspect(), referenceWork };
    } catch (error) { state = 'failed'; throw error; }
  }
  return { stage, finish };
}
