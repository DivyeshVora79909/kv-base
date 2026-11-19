import test from 'node:test';
import assert from 'node:assert/strict';
import { createKeyring } from '../src/keyring.mjs';
test('keyring copies external keys, uses fresh nonces and authenticates tenant purpose subject revision and fence', () => {
  const root = Buffer.alloc(32, 3), keyring = createKeyring({ keys: { one: root }, activeKey: 'one' }), binding = { tenant: 't', purpose: 'credential', subject: 'id', revision: '1', fence: 'fence' };
  const a = keyring.seal({ secret: 'provider secret' }, binding), b = keyring.seal({ secret: 'provider secret' }, binding);
  assert.notEqual(a.nonce, b.nonce); root.fill(0);
  assert.deepEqual(keyring.open(a, binding), { secret: 'provider secret' });
  for (const change of [{ tenant: 'other' }, { purpose: 'proof' }, { subject: 'other' }, { revision: '2' }, { fence: 'new' }]) assert.throws(() => keyring.open(a, { ...binding, ...change }), { code: 'SECRET_INTEGRITY' });
  const modified = { ...a, ciphertext: Buffer.alloc(Buffer.from(a.ciphertext, 'base64url').length).toString('base64url') };
  assert.throws(() => keyring.open(modified, binding), { code: 'SECRET_INTEGRITY' });
  assert.deepEqual(Object.keys(keyring), ['activeKey', 'seal', 'open', 'digest']);
});

