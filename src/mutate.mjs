import { createMutation } from './stage.mjs';
import { settle } from './settlement.mjs';
import { GENERATED_PREFIX } from './calculation-identity.mjs';
import { fail } from './values.mjs';
export { createMutation } from './stage.mjs';

/** Internal composition point; the caller owns the transaction and must abort on error. */
export async function mutateInTransaction(tx, catalog, tenant, changes, options = {}) {
  if (Array.isArray(changes)) for (const change of changes) {
    const id = change?.type === 'put' ? change.record?.id : change?.id;
    if (typeof id === 'string' && id.startsWith(GENERATED_PREFIX)) {
      fail('INVALID_CALCULATION', 'Client cannot mutate reserved generated identities');
    }
  }
  if (Object.values(catalog.models).some(model => model.managed || model.calculations?.length)) {
    return settle(tx, catalog, tenant, changes, options);
  }
  const mutation = await createMutation(tx, catalog, tenant);
  await mutation.stage(changes);
  return mutation.finish();
}

/** Trusted mutation boundary. Client authorization remains a separate layer. */
export async function mutate(store, catalog, tenant, changes, options = {}) {
  if (!options || typeof options !== 'object' || Array.isArray(options) ||
      Object.keys(options).some(key => !['beforeCommit', 'maxProducers', 'maxEdges', 'maxRouteVisits', 'maxPotentialVisits'].includes(key))) {
    fail('INVALID_MUTATION', 'Unknown mutation options');
  }
  const { beforeCommit, ...settlementOptions } = options;
  return store.transact(tx => mutateInTransaction(tx, catalog, tenant, changes, settlementOptions), { beforeCommit });
}
