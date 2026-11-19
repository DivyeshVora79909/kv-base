import { createCipheriv, createDecipheriv, createHmac, hkdfSync, randomBytes } from 'node:crypto';
import { assertJson, canonical, decodeDomain, fail } from './values.mjs';

const protocol = 'aes256gcm-hkdf-sha256-1';
const own = (value, key) => Object.hasOwn(value, key);
function object(value, allowed, required = allowed) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) fail('INVALID_KEYRING', 'Expected ordinary keyring data');
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!allowed.includes(key) || !descriptor.enumerable || !own(descriptor, 'value')) fail('INVALID_KEYRING', 'Invalid keyring fields');
  }
  if (required.some(key => !own(value, key))) fail('INVALID_KEYRING', 'Missing keyring field');
}
function keyName(value) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{1,64}$/.test(value)) fail('INVALID_KEYRING', 'Invalid key identifier');
  return value;
}
function binding(value) {
  object(value, ['tenant', 'purpose', 'subject', 'revision', 'fence'], ['tenant', 'purpose', 'subject', 'revision']);
  const result = {};
  for (const key of ['tenant', 'purpose', 'subject']) result[key] = decodeDomain('id', value[key]);
  result.revision = String(decodeDomain('integer', value.revision));
  if (own(value, 'fence')) result.fence = decodeDomain('id', value.fence);
  return result;
}
function binary(value, length) {
  if (typeof value !== 'string' || !/^[A-Za-z0-9_-]*$/.test(value)) fail('SECRET_INTEGRITY', 'Malformed encrypted payload');
  const result = Buffer.from(value, 'base64url');
  if (result.toString('base64url') !== value || (length !== undefined && result.length !== length)) fail('SECRET_INTEGRITY', 'Malformed encrypted payload');
  return result;
}

/** Trusted host custody. Root keys are copied into a private closure and never
 * returned, serialized or stored in the engine. Bindings authenticate ciphertext
 * identity; a fence is supplied for restore-sensitive results, sessions and proofs. */
export function createKeyring(configuration) {
  object(configuration, ['keys', 'activeKey', 'maxBytes'], ['keys', 'activeKey']);
  const activeKey = keyName(configuration.activeKey), supplied = configuration.keys;
  object(supplied, Object.keys(supplied ?? {}));
  const keys = new Map();
  for (const [id, key] of Object.entries(supplied)) {
    keyName(id);
    if (!Buffer.isBuffer(key) || key.length !== 32 || key.buffer instanceof SharedArrayBuffer) fail('INVALID_KEYRING', 'Root keys require 32 private bytes');
    keys.set(id, Buffer.from(key));
  }
  if (!keys.has(activeKey)) fail('INVALID_KEYRING', 'Active key is unavailable');
  const maxBytes = configuration.maxBytes ?? 1024 * 1024;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) fail('INVALID_KEYRING', 'Invalid custody byte limit');
  function derive(keyId, scope) {
    const root = keys.get(keyName(keyId));
    if (!root) fail('UNSUPPORTED_SECRET', 'Required custody key is unavailable');
    return Buffer.from(hkdfSync('sha256', root, Buffer.from('kv-base custody v1'), Buffer.from(canonical([scope.tenant, scope.purpose])), 32));
  }
  function seal(value, input) {
    assertJson(value);
    const scope = binding(input), plaintext = Buffer.from(JSON.stringify(value));
    if (plaintext.length > maxBytes) fail('WORK_LIMIT', 'Custody byte limit exceeded');
    const nonce = randomBytes(12), key = derive(activeKey, scope);
    try {
      const cipher = createCipheriv('aes-256-gcm', key, nonce);
      cipher.setAAD(Buffer.from(canonical([protocol, activeKey, scope])));
      const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      return { version: protocol, keyId: activeKey, nonce: nonce.toString('base64url'), ciphertext: ciphertext.toString('base64url'), tag: cipher.getAuthTag().toString('base64url') };
    } finally { key.fill(0); plaintext.fill(0); }
  }
  function open(envelope, input) {
    object(envelope, ['version', 'keyId', 'nonce', 'ciphertext', 'tag']);
    if (envelope.version !== protocol) fail('UNSUPPORTED_SECRET', 'Unsupported custody protocol');
    const scope = binding(input);
    if (typeof envelope.ciphertext !== 'string' || envelope.ciphertext.length > Math.ceil(maxBytes * 4 / 3) + 4) fail('WORK_LIMIT', 'Custody byte limit exceeded');
    const nonce = binary(envelope.nonce, 12), tag = binary(envelope.tag, 16), ciphertext = binary(envelope.ciphertext), key = derive(envelope.keyId, scope);
    let plaintext;
    try {
      const decipher = createDecipheriv('aes-256-gcm', key, nonce);
      decipher.setAAD(Buffer.from(canonical([protocol, envelope.keyId, scope]))); decipher.setAuthTag(tag);
      plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
      const result = JSON.parse(plaintext.toString('utf8')); assertJson(result); return result;
    } catch { fail('SECRET_INTEGRITY', 'Encrypted payload authentication failed'); }
    finally { key.fill(0); plaintext?.fill(0); }
  }
  function digest(value, input, keyId = activeKey) {
    const scope = binding(input), encoded = canonical([scope, value]);
    if (Buffer.byteLength(encoded) > maxBytes) fail('WORK_LIMIT', 'Custody byte limit exceeded');
    const key = derive(keyId, scope);
    try { return `${keyId}:${createHmac('sha256', key).update(encoded).digest('base64url')}`; }
    finally { key.fill(0); }
  }
  return Object.freeze({ activeKey, seal, open, digest });
}
