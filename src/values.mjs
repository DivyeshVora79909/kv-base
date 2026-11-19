import { createHash } from 'node:crypto';
import { Serializer, Deserializer, deserialize } from 'node:v8';
import { isDeepStrictEqual } from 'node:util';
import { copyKvData } from './kv-data.mjs';

export function fail(code, message) {
  throw Object.assign(new Error(message), { code });
}

function text(value) {
  if (typeof value !== 'string' || !value.isWellFormed()) fail('INVALID_VALUE', 'Expected well-formed Unicode');
  return value;
}

/** Explicit numeric wire domain. No implicit Number conversion or locale rules. */
export function decodeDomain(domain, value) {
  if (Array.isArray(domain)) {
    if (!Array.isArray(value) || value.length !== domain.length
      || Reflect.ownKeys(value).length !== value.length + 1) fail('INVALID_VALUE', 'Expected exact order tuple');
    return domain.map((component, index) => {
      const entry = Object.getOwnPropertyDescriptor(value, String(index));
      if (!entry?.enumerable || !Object.hasOwn(entry, 'value')) fail('INVALID_VALUE', 'Expected own order tuple components');
      return decodeDomain(component, entry.value);
    });
  }
  switch (domain) {
    case 'integer':
      if (typeof value === 'bigint') return value;
      if (typeof value !== 'string' || !/^(0|-?[1-9][0-9]*)$/.test(value)) fail('INVALID_VALUE', 'Expected canonical integer string');
      return BigInt(value);
    case 'id':
      if (!text(value).length) fail('INVALID_VALUE', 'Expected nonempty identifier');
      return value;
    case 'string': return text(value);
    case 'boolean':
      if (typeof value !== 'boolean') fail('INVALID_VALUE', 'Expected boolean');
      return value;
    default: fail('INVALID_DOMAIN', `Unsupported domain ${domain}`);
  }
}

export function compareDomain(domain, left, right) {
  const a = decodeDomain(domain, left), b = decodeDomain(domain, right);
  if (Array.isArray(domain)) {
    for (let index = 0; index < domain.length; index++) {
      const comparison = compareDomain(domain[index], a[index], b[index]);
      if (comparison) return comparison;
    }
    return 0;
  }
  if (domain === 'id' || domain === 'string') return Math.sign(Buffer.compare(Buffer.from(a), Buffer.from(b)));
  return a < b ? -1 : a > b ? 1 : 0;
}

// Canonical tagged encoding: data objects cannot collide with integer/type markers.
function encode(value, seen, jsonOnly) {
  if (value === null) return ['null'];
  if (typeof value === 'string') return ['string', text(value)];
  if (typeof value === 'boolean') return ['boolean', value];
  if (typeof value === 'bigint') {
    if (jsonOnly) fail('INVALID_JSON', 'BigInt requires a canonical string at the JSON boundary');
    return ['integer', value.toString()];
  }
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || Object.is(value, -0)) fail('INVALID_JSON', 'Numbers must be safe integers; exact quantities use strings');
    return ['number', value];
  }
  if (!value || typeof value !== 'object') fail('INVALID_VALUE', 'Expected plain data');
  if (seen.has(value)) fail('INVALID_VALUE', 'Cyclic data is unsupported');
  const prototype = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) fail('INVALID_VALUE', 'Expected plain object');
  seen.add(value);
  let result;
  if (Array.isArray(value)) {
    if (Reflect.ownKeys(value).length !== value.length + 1) fail('INVALID_VALUE', 'Sparse or extended arrays are unsupported');
    const items = [];
    for (let index = 0; index < value.length; index++) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      if (!descriptor?.enumerable || !Object.hasOwn(descriptor, 'value')) fail('INVALID_VALUE', 'Array entries must be own enumerable data');
      items.push(encode(descriptor.value, seen, jsonOnly));
    }
    result = ['array', items];
  } else {
    const keys = Reflect.ownKeys(value);
    if (keys.some(key => typeof key !== 'string')) fail('INVALID_VALUE', 'Symbol keys are unsupported');
    keys.sort((a, b) => compareDomain('string', a, b));
    result = ['object', keys.map(key => {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) fail('INVALID_VALUE', 'Accessors and hidden properties are unsupported');
      return [text(key), encode(descriptor.value, seen, jsonOnly)];
    })];
  }
  seen.delete(value);
  return result;
}

export function assertJson(value) { encode(value, new Set(), true); }
export function canonical(value) { return JSON.stringify(encode(value, new Set(), false)); }
export function fingerprint(value) { return createHash('sha256').update(canonical(value)).digest('hex'); }
export function cloneData(value) { return structuredClone(value); }
export function freezeDeep(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freezeDeep(child);
    Object.freeze(value);
  }
  return value;
}

export function selectValue(record, selector) {
  if (Object.hasOwn(selector, 'constant')) return { present: true, value: selector.constant };
  const field = Object.hasOwn(selector, 'field');
  const object = field ? record.data : record;
  const key = field ? selector.field : selector.header;
  return { present: Object.hasOwn(object, key), value: object[key] };
}

const valueHeader = Buffer.from('kv-base-value-2\0');
const viewNames = ['Int8Array', 'Uint8Array', 'Uint8ClampedArray', 'Int16Array', 'Uint16Array', 'Int32Array', 'Uint32Array', 'Float16Array', 'Float32Array', 'Float64Array', 'BigInt64Array', 'BigUint64Array', 'DataView'];
class ValueSerializer extends Serializer {
  constructor() { super(); this._setTreatArrayBufferViewsAsHostObjects(true); }
  _writeHostObject(view) {
    const type = ArrayBuffer.isView(view) ? viewNames.indexOf(view.constructor.name) : -1;
    if (type < 0 || view.byteOffset > 0xffffffff || view.byteLength > 0xffffffff) fail('INVALID_VALUE', 'Unsupported binary view');
    this.writeUint32(type); this.writeUint32(view.byteOffset); this.writeUint32(view.byteLength); this.writeValue(view.buffer);
  }
}
class ValueDeserializer extends Deserializer {
  _readHostObject() {
    const type = this.readUint32(), offset = this.readUint32(), bytes = this.readUint32(), buffer = this.readValue(), name = viewNames[type], constructor = globalThis[name];
    if (typeof constructor !== 'function' || !(buffer instanceof ArrayBuffer) || buffer.resizable || offset > buffer.byteLength || bytes > buffer.byteLength - offset) fail('INVALID_VALUE', 'Invalid encoded binary view');
    if (name === 'DataView') return new DataView(buffer, offset, bytes);
    if (offset % constructor.BYTES_PER_ELEMENT || bytes % constructor.BYTES_PER_ELEMENT) fail('INVALID_VALUE', 'Misaligned encoded binary view');
    return new constructor(buffer, offset, bytes / constructor.BYTES_PER_ELEMENT);
  }
}
/** V8 keeps object identities; view hooks retain the actual backing reference
 * instead of V8 DefaultSerializer's separately copied byte slices. */
export function encodeKvData(value) {
  const serializer = new ValueSerializer(); serializer.writeHeader(); serializer.writeValue(copyKvData(value));
  return Buffer.concat([valueHeader, serializer.releaseBuffer()]);
}
export function decodeKvData(bytes) {
  if (!(bytes instanceof Uint8Array)) fail('INVALID_VALUE', 'Expected encoded bytes');
  const encoded = Buffer.from(bytes);
  if (!encoded.subarray(0, valueHeader.length).equals(valueHeader)) return copyKvData(deserialize(encoded));
  const decoder = new ValueDeserializer(encoded.subarray(valueHeader.length)); decoder.readHeader(); return copyKvData(decoder.readValue());
}

function hasBinaryBacking(value, seen = new WeakSet()) {
  if (!value || typeof value !== 'object' || seen.has(value)) return false;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return true;
  seen.add(value);
  if (value instanceof Map) for (const [key, entry] of value) if (hasBinaryBacking(key, seen) || hasBinaryBacking(entry, seen)) return true;
  if (value instanceof Set) for (const entry of value) if (hasBinaryBacking(entry, seen)) return true;
  return Object.values(value).some(entry => hasBinaryBacking(entry, seen));
}
/** Retain ordinary no-op semantics; binary graphs also compare their observable
 * aliases, offsets and full backing bytes. Codec traversal order can stage writes. */
export function sameKvData(left, right) {
  if (!isDeepStrictEqual(left, right)) return false;
  if (!hasBinaryBacking(left) && !hasBinaryBacking(right)) return true;
  return encodeKvData(left).equals(encodeKvData(right));
}
