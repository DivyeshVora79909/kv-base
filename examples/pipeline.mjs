import { objectModel } from '../src/objects.mjs';
import { effectModels } from '../src/effects.mjs';

export const ledgerIndex = ['Ledger', ['ledger'], ['at', 'sequence']];
export const ownerIndex = ['Ledger', ['ledger', 'owner'], ['at', 'sequence']];
const integer = { type: 'string', pattern: '^(0|-?[1-9][0-9]*)$' };
const field = field => ({ field });
const reference = (slot, name, target) => ({ slot, field: name, targets: [target], many: false, onDelete: 'reject' });
const index = (partitionFields, publication) => ({ version: '1', partitionFields, orderFields: ['at', 'sequence'],
  roles: { ledger: 'id', ...(partitionFields.includes('owner') ? { owner: 'id' } : {}), at: 'integer', sequence: 'integer' },
  inputs: { amount: 'integer' }, aggregates: { balance: { monoid: 'sum', input: 'amount' } }, ...(publication ? { publication } : {}),
});

/** One ordinary prepared catalog serves business, object and private delivery work. */
export function pipelineDefinition() {
  return { name: 'ledger-object-proof', version: '1', models: {
    Ledger: { schema: { type: 'object', additionalProperties: false, properties: { name: { type: 'string' } }, required: ['name'] }, access: { orders: {} },
      indexes: [index(['ledger'], { anchorPartition: 0, aggregates: ['balance'] }), index(['ledger', 'owner'])] },
    Entry: { schema: { type: 'object', additionalProperties: false, properties: {
      ledgerId: { type: 'string', minLength: 1 }, at: integer, sequence: integer, amount: integer, attachment: { type: 'string', minLength: 1 },
    }, required: ['ledgerId', 'at', 'sequence', 'amount'] }, access: { orders: {} },
      references: [reference('ledger', 'ledgerId', 'Ledger'), reference('attachment', 'attachment', 'Object')], memberships: [
        { slot: 'balance', index: ledgerIndex, partition: [field('ledgerId')], order: [field('at'), field('sequence')], inputs: { amount: field('amount') }, onMissing: 'reject' },
        { slot: 'owner', index: ownerIndex, partition: [field('ledgerId'), { header: 'owner' }], order: [field('at'), field('sequence')], inputs: { amount: field('amount') }, onMissing: 'reject' },
      ] },
    Object: objectModel(), ...effectModels(),
  } };
}

export function pipelinePolicy(tenant = 't') {
  const node = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  const business = { Ledger: ['read', 'create', 'update', 'delete', 'use'], Entry: ['read', 'create', 'update', 'delete'], Object: ['read', 'create', 'update', 'delete', 'use'] };
  return { tenant, principals: [node(tenant, 'tenant', []), node('admin', 'user', [tenant], { Credential: ['use'], Ledger: ['use'], Object: ['use'] }, true),
    node('visible', 'group', ['admin'], structuredClone(business)), node('hidden', 'group', ['admin'], structuredClone(business)), node('reader', 'user', ['visible']), node('other', 'user', ['hidden']),
  ] };
}

export function pipelineSources(tenant = 't') {
  const record = (id, model, owner, data) => ({ id, tenant, model, owner, createdAt: '0', data });
  return [record('ledger', 'Ledger', 'visible', { name: 'Published balance' }), record('visible-entry', 'Entry', 'visible', { ledgerId: 'ledger', at: '1', sequence: '1', amount: '100' }), record('hidden-entry', 'Entry', 'hidden', { ledgerId: 'ledger', at: '1', sequence: '1', amount: '-80' })];
}
