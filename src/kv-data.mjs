/** Detach adapter values; only fixed private ArrayBuffers are supported. */
export function copyKvData(value) {
  const detached = structuredClone(value), seen = new WeakSet();
  function check(item) {
    if (item === null || typeof item !== 'object' || seen.has(item)) return;
    seen.add(item);
    if (item instanceof SharedArrayBuffer || (ArrayBuffer.isView(item) && item.buffer instanceof SharedArrayBuffer)) {
      throw Object.assign(new Error('Shared memory is not supported in engine values'), { code: 'INVALID_VALUE' });
    }
    if (ArrayBuffer.isView(item)) { check(item.buffer); return; }
    if (item instanceof ArrayBuffer) {
      if (item.resizable) throw Object.assign(new Error('Resizable backing is not supported in engine values'), { code: 'INVALID_VALUE' });
      return;
    }
    if (item instanceof Map) for (const [key, entry] of item) { check(key); check(entry); }
    if (item instanceof Set) for (const entry of item) check(entry);
    for (const key of Object.getOwnPropertyNames(item)) check(item[key]);
  }
  check(detached); return detached;
}

export function checkKvKey(key) {
  if (typeof key !== 'string' || key.length === 0 || !key.isWellFormed()) {
    throw Object.assign(new Error('Keys must be nonempty well-formed Unicode strings'), { code: 'INVALID_KEY' });
  }
  return key;
}

export const compareKvKeys = (left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right));
