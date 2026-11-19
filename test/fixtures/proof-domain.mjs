import { effectModels } from '../../src/effects.mjs';
export function proofDefinition() {
  return { name: 'proof-witness', version: '1', models: { Document: { access: { orders: {} }, schema: { type: 'object', additionalProperties: false, properties: { body: { type: 'string' } }, required: ['body'] } }, ...effectModels() } };
}
