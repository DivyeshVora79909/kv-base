import { validateRecord, projectRecord } from './catalog.mjs';
import { permits } from './authority.mjs';
import { sourceKey } from './keys.mjs';
import { canonical, fail } from './values.mjs';

const corrupt = () => fail('CORRUPT_STATE', 'Source and discovery identity disagree');

/** One hydration/disclosure primitive for tree, point and incoming candidates.
 * Validation precedes disclosure. An absent ordinary point is allowed; a dangling
 * occurrence/reference is corruption. fetch is the request's charged batch reader. */
export async function discloseSource(catalog, scope, id, fetch, { model, occurrence, reference } = {}) {
  const [stored] = await fetch([sourceKey(scope.tenant, id)], false);
  if (stored === undefined) {
    if (occurrence || reference) corrupt();
    return undefined;
  }
  let record;
  try {
    record = validateRecord(catalog, stored);
    if (record.tenant !== scope.tenant || record.id !== id || (model !== undefined && record.model !== model)) corrupt();
    if (occurrence) {
      const projected = projectRecord(catalog, record).find(item => item.view === occurrence.view && item.slot === occurrence.slot);
      if (!projected || canonical(projected.partition) !== canonical(occurrence.partition)
        || canonical(projected.order) !== canonical(occurrence.order)
        || canonical(projected.contributions) !== canonical(occurrence.contributions)) corrupt();
    }
    if (reference) {
      const spec = catalog.models[record.model].references.find(item => item.slot === reference.slot);
      if (!spec || reference.tenant !== scope.tenant || reference.sourceId !== id
        || !Number.isSafeInteger(reference.position) || reference.position < 0
        || canonical(reference.targets) !== canonical(spec.targets)) corrupt();
      const value = record.data[spec.field];
      const target = spec.many ? value?.[reference.position] : reference.position === 0 ? value : undefined;
      if (target !== reference.targetId) corrupt();
    }
  } catch { corrupt(); }
  return permits(scope, record, 'read') ? record : undefined;
}
