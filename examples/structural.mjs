import { activityDefinition, activityRecords } from './activity.mjs';

export const activityIndexes = {
  resourceEffective: ['resource', ['id'], ['effective']],
  ownerEffective: ['resource', ['id', 'owner'], ['effective']],
  ownerCreated: ['resource', ['id', 'owner'], ['createdAt']],
};
// Reuse the source schemas and fixture facts. New declarations have no view aliases.
export const structuralActivityDefinition = (() => {
  const definition = structuredClone(activityDefinition);
  definition.version = 'structural-1';
  definition.models.resource.indexes = Object.entries(activityIndexes).map(([view, [, partitionFields, orderFields]]) => {
    const contract = definition.contracts[definition.views[view].contract];
    return { version: '1', partitionFields, orderFields,
      roles: Object.fromEntries([...partitionFields.map((field, index) => [field, contract.partition[index]]), [orderFields[0], contract.order]]),
      inputs: contract.inputs, aggregates: contract.aggregates };
  });
  for (const model of Object.values(definition.models)) for (const membership of model.memberships ?? []) {
    membership.index = activityIndexes[membership.view]; delete membership.view;
    membership.order = [membership.order];
  }
  delete definition.contracts; delete definition.views;
  return definition;
})();
export { activityRecords };
