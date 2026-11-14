import { activityDefinition, activityRecords } from './activity.mjs';

// Extend the existing catalog; access needs a dedicated resource/slot view.
// No I/O runs on import. The correction model deliberately remains private.
export function authorizedDefinition() {
  const definition = structuredClone(activityDefinition);
  definition.name = 'authorized-activity';
  definition.views.movementOwner = { contract: 'owner' };
  definition.models.movement.memberships.push({
    ...structuredClone(definition.models.movement.memberships[1]),
    slot: 'access', view: 'movementOwner',
  });
  definition.models.movement.access = {
    orders: { effective: { view: 'movementOwner', slot: 'access', ownerPartition: 1 } },
  };
  definition.models.resource.access = { orders: {} };
  return definition;
}

export { activityRecords as authorizedRecords };

export function authorityDefinition() {
  const principal = (id, kind, parents, resources = {}, admin = false) => ({
    id, kind, parents, active: true, grants: { admin, resources },
  });
  return { tenant: 'tenant', principals: [
    principal('tenant', 'tenant', []),
    principal('administrator', 'user', ['tenant'], {}, true),
    principal('team', 'group', ['tenant'], {
      movement: ['read', 'create', 'update'], resource: ['read', 'use'],
    }),
    principal('alice', 'user', ['team']),
    principal('other-team', 'group', ['tenant']),
  ] };
}
