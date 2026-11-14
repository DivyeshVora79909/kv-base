// Ordinary reusable declarations and fixture data. Importing performs no I/O.
const field = name => ({ field: name });
const reference = (name, target) => ({ slot: name, field: name, targets: [target], many: false, onDelete: 'reject' });
const schema = names => ({
  type: 'object', additionalProperties: false,
  properties: Object.fromEntries(names.map(name => [name, { type: 'string' }])),
  required: names,
});

export const relationsDefinition = {
  name: 'relations', version: '1', contracts: {}, views: {},
  models: {
    team: { schema: schema(['name']) },
    person: { schema: schema(['name', 'team']), references: [reference('team', 'team')] },
    project: { schema: schema(['name']) },
    profile: {
      schema: schema(['person', 'bio']), references: [reference('person', 'person')],
      uniqueness: [{ slot: 'person', fields: [field('person')] }],
    },
    participation: {
      schema: schema(['person', 'project']),
      references: [reference('person', 'person'), reference('project', 'project')],
      uniqueness: [{ slot: 'pair', fields: [field('person'), field('project')] }],
    },
  },
};

export function relationsRecords(tenant = 'tenant') {
  const record = (id, model, data) => ({ id, model, tenant, owner: 'application', createdAt: '0', data });
  return [
    record('team-a', 'team', { name: 'Research' }),
    record('team-b', 'team', { name: 'Engineering' }),
    record('alice', 'person', { name: 'Alice', team: 'team-a' }),
    record('bob', 'person', { name: 'Bob', team: 'team-a' }),
    record('project', 'project', { name: 'KV adapter' }),
    record('alice-profile', 'profile', { person: 'alice', bio: 'Researcher' }),
    record('alice-project', 'participation', { person: 'alice', project: 'project' }),
    record('bob-project', 'participation', { person: 'bob', project: 'project' }),
  ];
}
