import { resolve } from 'node:path';
import { serialize, deserialize } from 'node:v8';
import { AsyncLocalStorage } from 'node:async_hooks';
import { isDeepStrictEqual } from 'node:util';
import { copyKvData as copy, checkKvKey, compareKvKeys as compare } from './kv-data.mjs';

const context = new AsyncLocalStorage(), format = 'kv-base-node-v8-1', apiVersion = 720;
const error = (code, message, cause) => Object.assign(new Error(message, cause ? { cause } : undefined), { code });
let sdk;
async function loadSdk() {
  sdk ??= Promise.all([import('foundationdb'), import('foundationdb/dist/lib/native.js')]).then(([binding, native]) => {
    const fdb = binding.default; fdb.setAPIVersion(apiVersion);
    return { fdb, predicate: native.default.default.errorPredicate };
  }); return sdk;
}
function fields(value, allowed) {
  if (!value || typeof value !== 'object' || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) throw error('INVALID_OPTIONS', 'Expected ordinary options');
  for (const name of Reflect.ownKeys(value)) { const d = Object.getOwnPropertyDescriptor(value, name); if (typeof name !== 'string' || !allowed.includes(name) || !d.enumerable || !Object.hasOwn(d, 'value')) throw error('INVALID_OPTIONS', 'Invalid option descriptor'); }
}
function range(options = {}) {
  fields(options, ['prefix', 'gte', 'gt', 'lte', 'lt', 'limit']); const result = {};
  for (const name of ['prefix', 'gte', 'gt', 'lte', 'lt']) if (options[name] !== undefined) result[name] = name === 'prefix' && options[name] === '' ? '' : checkKvKey(options[name]);
  const limit = options.limit ?? Infinity; if (limit !== Infinity && (!Number.isSafeInteger(limit) || limit < 0)) throw error('INVALID_RANGE', 'Invalid scan limit'); return { ...result, limit };
}
function prefixEnd(bytes) { const end = Buffer.from(bytes); for (let i = end.length - 1; i >= 0; i--) if (end[i] < 255) { end[i]++; return end.subarray(0, i + 1); } throw error('INVALID_RANGE', 'Unbounded physical prefix'); }
const after = bytes => Buffer.concat([bytes, Buffer.from([0])]);
function receiptSpec(value) { fields(value, ['id', 'digest']); if (Object.keys(value).length !== 2) throw error('INVALID_RECEIPT', 'Receipt requires id and digest'); return { id: checkKvKey(value.id), digest: checkKvKey(value.digest) }; }
function decode(bytes) { try { if (!(bytes instanceof Uint8Array)) throw Error('Expected bytes'); return copy(deserialize(bytes)); } catch (cause) { throw error('CORRUPT_STATE', 'Invalid persisted value', cause); } }
function storedReceipt(bytes, id) { const value = decode(bytes); if (!value || Object.keys(value).length !== 3 || value.id !== id || typeof value.digest !== 'string' || !Object.hasOwn(value, 'result')) throw error('CORRUPT_STATE', 'Invalid receipt'); return value; }

/** Concurrent native serializable transactions. Application callbacks execute
 * once; native unknown outcomes require recovery using their original receipt. */
export async function openFoundationDbStore(options = {}) {
  fields(options, ['clusterFile', 'prefix', 'timeoutMs', 'transactionBytes', 'readOnly']);
  const { clusterFile, prefix, timeoutMs = 4500, transactionBytes = 10_000_000, readOnly = false } = options;
  if (typeof clusterFile !== 'string' || !clusterFile.length || clusterFile.includes('\0') || !clusterFile.isWellFormed()) throw error('INVALID_STORAGE', 'Explicit cluster file required');
  checkKvKey(prefix);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 4500 || !Number.isSafeInteger(transactionBytes) || transactionBytes < 32 || transactionBytes > 10_000_000 || typeof readOnly !== 'boolean') throw error('INVALID_STORAGE', 'Invalid physical transaction limits');
  const { fdb, predicate } = await loadSdk(), db = fdb.open(clusterFile), identity = Symbol('foundationdb store');
  const root = fdb.tuple.pack(['kv-base', prefix]), dataPrefix = Buffer.concat([root, Buffer.from([0])]), receiptPrefix = Buffer.concat([root, Buffer.from([1])]), formatKey = Buffer.concat([root, Buffer.from([2]), Buffer.from('format')]);
  if (formatKey.length > 10_000) { db.close(); throw error('STORAGE_LIMIT', 'Namespace exceeds native key limit'); }
  const nativeOptions = { timeout: timeoutMs, size_limit: transactionBytes };
  const nativeError = cause => cause instanceof fdb.FDBError && Number.isInteger(cause.code);
  function classify(cause, attempted, receiptId) {
    if (nativeError(cause)) {
      if ([2101, 2102, 2103, 1031].includes(cause.code) && !predicate(50001, cause.code)) return error('STORAGE_LIMIT', 'Native transaction physical limit exceeded', cause);
      if (predicate(50002, cause.code)) return error('STORAGE_CONFLICT', 'Native transaction definitely did not commit', cause);
      if (attempted && predicate(50001, cause.code)) { const failure = error('COMMIT_UNKNOWN', 'Commit outcome requires original receipt recovery', cause); if (receiptId) failure.receiptId = receiptId; return failure; }
      return error('STORAGE_ERROR', 'Native storage operation failed', cause);
    }
    return cause;
  }
  // Only internal, idempotent format initialization retries a definite native
  // conflict. Application callbacks and commit-unknown outcomes never retry.
  try {
    for (let attempt = 0; ; attempt++) {
      const tn = db.rawCreateTransaction(nativeOptions);
      try {
        const prior = await tn.get(formatKey);
        if (prior === undefined) {
          if (readOnly || (await tn.getRangeAll(root, prefixEnd(root), { limit: 1 })).length) throw error('STORAGE_FORMAT_MISMATCH', 'Namespace is not initialized or contains unrecognized state');
          tn.set(formatKey, Buffer.from(format));
        }
        else if (!Buffer.from(prior).equals(Buffer.from(format))) throw error('STORAGE_FORMAT_MISMATCH', 'Unsupported value codec');
        await tn.rawCommit(); break;
      } catch (cause) { if (attempt < 2 && nativeError(cause) && predicate(50002, cause.code)) continue; throw classify(cause, true); }
      finally { tn.rawCancel(); }
    }
  } catch (cause) { db.close(); throw cause; }
  let closing = false, closed = false, closePromise; const active = new Set();
  const location=Object.freeze({adapter:'foundationdb',clusterFile:resolve(clusterFile),prefix});
  const capabilities = Object.freeze({ adapter: 'foundationdb', volatile: false, durable: true, serialized: false, serializable: true, readOnly, format, apiVersion, keyBytes: 10_000, valueBytes: 100_000, transactionBytes, timeoutMs, scans: 'native ordered range batches plus touched overlay merge', guards: 'native non-snapshot point/absence reads and explicit full requested read conflict ranges', callbackRetries: 0 });
  const key = (name, namespace = dataPrefix) => { const value = Buffer.concat([namespace, Buffer.from(checkKvKey(name))]); if (value.length > 10_000) throw error('STORAGE_LIMIT', 'Physical key exceeds 10000 bytes'); return value; };
  const encode = value => { const bytes = serialize(copy(value)); if (bytes.length > 100_000) throw error('STORAGE_LIMIT', 'Encoded value exceeds 100000 bytes'); return bytes; };
  function run(callback, options, readonly, receiptOnly) {
    if (context.getStore()?.has(identity)) return Promise.reject(error('NESTED_TRANSACTION', 'Same-store nested transactions are unsupported'));
    if (closing || closed) return Promise.reject(error('STORAGE_CLOSED', 'Store is closing or closed'));
    if (typeof callback !== 'function') return Promise.reject(error('INVALID_CALLBACK', 'Expected callback'));
    let fixed;
    try {
      fields(options, ['beforeCommit', 'afterCommit', 'receipt', 'verifyReplay']);
      for (const name of ['beforeCommit', 'afterCommit', 'verifyReplay']) if (options[name] !== undefined && typeof options[name] !== 'function') throw error('INVALID_OPTIONS', 'Expected callback');
      fixed = { ...options, ...(options.receipt === undefined ? {} : { receipt: receiptSpec(options.receipt) }) };
      if (fixed.receipt) key(fixed.receipt.id, receiptPrefix);
      if (receiptOnly !== undefined) key(receiptOnly, receiptPrefix);
    } catch (cause) { return Promise.reject(cause); }
    if (readOnly && !readonly) return Promise.reject(error('READ_ONLY', 'Store is read-only'));
    const parents = context.getStore() ?? new Set();
    const task = context.run(new Set([...parents, identity]), async () => {
      const tn = db.rawCreateTransaction(nativeOptions), overlay = new Map(), base = new Map(), points = new Map(), ranges = [];
      const metrics = { gets: 0, getMany: 0, scans: 0, scannedKeys: 0, stagedWrites: 0, nativeReads: 0, nativeWrites: 0, nativeReadBytes: 0, nativeWriteBytes: 0 };
      let open = true, attempted = false, committed = false;
      const assertOpen = () => { if (!open) throw error('TRANSACTION_CLOSED', 'Transaction is closed'); };
      function load(name) {
        if (!base.has(name)) {
          const physical = key(name); metrics.nativeReads++;
          base.set(name, tn.get(physical).then(bytes => { metrics.nativeReadBytes += physical.length + (bytes?.length ?? 0); const entry = bytes === undefined ? { exists: false } : { exists: true, value: decode(bytes) }; points.set(name, { type: 'point', key: name, exists: entry.exists }); return entry; }));
        } return base.get(name);
      }
      async function read(name) { checkKvKey(name); const original = await load(name), current = overlay.get(name) ?? original; return current.exists ? copy(current.value) : undefined; }
      function facade(forceReadonly = false) { return Object.freeze({
        async get(name) { assertOpen(); metrics.gets++; const value = await read(name); assertOpen(); return value; },
        async getMany(names) { assertOpen(); if (!Array.isArray(names)) throw error('INVALID_KEYS', 'Expected keys'); names.forEach(checkKvKey); metrics.getMany++; const values = await Promise.all(names.map(read)); assertOpen(); return values; },
        async set(name, value) { assertOpen(); if (readonly || forceReadonly) throw error('READ_ONLY', 'Cannot write in a read transaction'); if (value === undefined) throw error('INVALID_VALUE', 'Undefined denotes absence'); const detached = copy(value); key(name); encode(detached); await load(name); assertOpen(); overlay.set(name, { exists: true, value: detached }); metrics.stagedWrites++; },
        async delete(name) { assertOpen(); if (readonly || forceReadonly) throw error('READ_ONLY', 'Cannot write in a read transaction'); await load(name); assertOpen(); overlay.set(name, { exists: false }); metrics.stagedWrites++; },
        async scan(options = {}) {
          assertOpen(); const r = range(options); ranges.push({ type: 'range', ...r }); metrics.scans++;
          let start = dataPrefix, end = prefixEnd(dataPrefix);
          const lower = candidate => { if (Buffer.compare(candidate, start) > 0) start = candidate; }, upper = candidate => { if (Buffer.compare(candidate, end) < 0) end = candidate; };
          if (r.prefix) { const p = key(r.prefix); lower(p); upper(prefixEnd(p)); }
          if (r.gte !== undefined) lower(key(r.gte)); if (r.gt !== undefined) lower(after(key(r.gt)));
          if (r.lt !== undefined) upper(key(r.lt)); if (r.lte !== undefined) upper(after(key(r.lte)));
          if (start.length > 10_000 || end.length > 10_000) throw error('STORAGE_LIMIT', 'Physical range endpoint exceeds native key limit');
          const rows = new Map();
          if (Buffer.compare(start, end) < 0) {
            // FDB's limited reads protect their native result footprint. The
            // adapter contract protects the complete declared range, even when
            // the caller requests zero rows or only the first page.
            tn.addReadConflictRange(start, end);
            if (r.limit === 0) await tn.getReadVersion();
            if (r.limit !== 0) {
              const amount = r.limit === Infinity ? undefined : Math.min(2_147_483_647, r.limit + overlay.size);
              metrics.nativeReads++; const stored = await tn.getRangeAll(start, end, amount === undefined ? {} : { limit: amount });
              metrics.scannedKeys += stored.length;
              for (const [physical, bytes] of stored) { metrics.nativeReadBytes += physical.length + bytes.length; const name = Buffer.from(physical).subarray(dataPrefix.length).toString(); rows.set(name, decode(bytes)); }
            }
          }
          metrics.scannedKeys += overlay.size;
          for (const [name, entry] of overlay) { if (entry.exists) rows.set(name, entry.value); else rows.delete(name); }
          assertOpen(); return [...rows].filter(([name]) => (r.prefix === undefined || name.startsWith(r.prefix)) && (r.gte === undefined || compare(name, r.gte) >= 0) && (r.gt === undefined || compare(name, r.gt) > 0) && (r.lte === undefined || compare(name, r.lte) <= 0) && (r.lt === undefined || compare(name, r.lt) < 0)).sort(([a], [b]) => compare(a, b)).slice(0, r.limit).map(([name, value]) => ({ key: name, value: copy(value) }));
        },
        inspect() { assertOpen(); return copy({ metrics: { ...metrics, touchedEntries: overlay.size }, guards: [...points.values(), ...ranges] }); },
      }); }
      async function loadReceipt(id) { const physical = key(id, receiptPrefix); metrics.nativeReads++; const bytes = await tn.get(physical); metrics.nativeReadBytes += physical.length + (bytes?.length ?? 0); return bytes; }
      try {
        if (receiptOnly !== undefined) { const bytes = await loadReceipt(receiptOnly); return bytes === undefined ? undefined : storedReceipt(bytes, receiptOnly); }
        if (fixed.receipt) {
          const bytes = await loadReceipt(fixed.receipt.id);
          if (bytes !== undefined) { const value = storedReceipt(bytes, fixed.receipt.id); if (value.digest !== fixed.receipt.digest) throw error('RECEIPT_MISMATCH', 'Operation identity has another digest'); if (fixed.verifyReplay) await fixed.verifyReplay(facade(true), copy(value)); return copy(value.result); }
        }
        const result = await callback(facade()), changes = [];
        for (const [name, entry] of overlay) { const previous = await load(name); if (entry.exists) { if (!previous.exists || !isDeepStrictEqual(previous.value, entry.value)) changes.push({ type: 'set', key: name, value: entry.value }); } else if (previous.exists) changes.push({ type: 'delete', key: name }); }
        const receipt = fixed.receipt ? { ...fixed.receipt, result: copy(result) } : undefined, receiptBytes = receipt ? encode(receipt) : undefined;
        open = false;
        for (const change of changes) { const physical = key(change.key); if (change.type === 'set') { const bytes = encode(change.value); tn.set(physical, bytes); metrics.nativeWriteBytes += physical.length + bytes.length; } else { tn.clear(physical); metrics.nativeWriteBytes += physical.length; } metrics.nativeWrites++; }
        if (receipt) { const physical = key(receipt.id, receiptPrefix); tn.set(physical, receiptBytes); metrics.nativeWriteBytes += physical.length + receiptBytes.length; metrics.nativeWrites++; }
        if (fixed.beforeCommit) await fixed.beforeCommit({ changes: copy(changes), metrics: copy(metrics) });
        attempted = true; await tn.rawCommit(); committed = true;
        if (fixed.afterCommit) await fixed.afterCommit({ receipt: receipt ? copy(receipt) : undefined, metrics: copy(metrics) });
        return result;
      } catch (cause) {
        if (committed) { const failure = error('COMMIT_UNKNOWN', 'Committed operation lost acknowledgement; recover original receipt', cause); if (fixed.receipt) failure.receiptId = fixed.receipt.id; throw failure; }
        throw classify(cause, attempted, fixed.receipt?.id);
      } finally { open = false; tn.rawCancel(); }
    });
    active.add(task); task.then(() => active.delete(task), () => active.delete(task)); return task;
  }
  function checkpointTask(callback) {
    if (context.getStore()?.has(identity)) return Promise.reject(error('NESTED_TRANSACTION', 'Same-store nested checkpoint unsupported'));
    if (closing || closed) return Promise.reject(error('STORAGE_CLOSED', 'Store is closing or closed'));
    const parents = context.getStore() ?? new Set(), task = context.run(new Set([...parents, identity]), callback);
    active.add(task); task.then(() => active.delete(task), () => active.delete(task)); return task;
  }
  function checkpointLimits(options) {
    const maxRows = options.maxRows ?? 100000, maxBytes = options.maxBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(maxRows) || maxRows < 1 || maxRows >= 2147483647 || !Number.isSafeInteger(maxBytes) || maxBytes < 1) throw error('INVALID_OPTIONS', 'Finite positive checkpoint limits required');
    return { maxRows, maxBytes };
  }
  function validateCheckpoint(snapshot, limits) {
    fields(snapshot, ['protocol', 'format', 'rows']);
    if (snapshot.protocol !== 'kv-base-fdb-checkpoint-1' || snapshot.format !== format || !Array.isArray(snapshot.rows) || snapshot.rows.length > limits.maxRows) throw error('BACKUP_INVALID', 'Unsupported or oversized checkpoint');
    let previous, bytes = 0, formats = 0;
    const rows = snapshot.rows.map(row => {
      if (!Array.isArray(row) || row.length !== 2 || !Buffer.isBuffer(row[0]) || !Buffer.isBuffer(row[1])) throw error('BACKUP_INVALID', 'Exact binary checkpoint rows required');
      const [suffix, value] = row.map(Buffer.from);
      if (!suffix.length || suffix.length + root.length > 10000 || value.length > 100000 || previous && Buffer.compare(previous, suffix) >= 0) throw error('BACKUP_INVALID', 'Invalid checkpoint key/value order');
      if (suffix[0] === 2 && suffix.equals(Buffer.concat([Buffer.from([2]), Buffer.from('format')]))) { if (!value.equals(Buffer.from(format))) throw error('BACKUP_INVALID', 'Unsupported checkpoint format'); formats++; }
      else if ([0, 1].includes(suffix[0])) {
        const nameBytes = suffix.subarray(1), name = checkKvKey(nameBytes.toString('utf8'));
        if (!Buffer.from(name).equals(nameBytes)) throw error('BACKUP_INVALID', 'Invalid checkpoint key bytes');
        if (suffix[0] === 0) decode(value); else storedReceipt(value, name);
      } else throw error('BACKUP_INVALID', 'Unknown checkpoint namespace');
      previous = suffix; bytes += suffix.length + value.length;
      if (bytes > limits.maxBytes) throw error('BACKUP_LIMIT', 'Checkpoint byte limit exceeded');
      return [suffix, value];
    });
    if (formats !== 1) throw error('BACKUP_INVALID', 'Checkpoint format row missing');
    return { rows, bytes };
  }
  // Privileged paused-writer operations. Physical suffixes preserve exact values
  // and original receipt bytes without a second engine/schema serialization.
  function captureCheckpoint(options = {}) { return checkpointTask(async () => {
    fields(options, ['maxRows', 'maxBytes']); const limits = checkpointLimits(options), tn = db.rawCreateTransaction(nativeOptions);
    try {
      tn.addReadConflictRange(root, prefixEnd(root));
      const physical = await tn.getRangeAll(root, prefixEnd(root), { limit: limits.maxRows + 1 });
      const snapshot = { protocol: 'kv-base-fdb-checkpoint-1', format, rows: physical.map(([key, value]) => [Buffer.from(key).subarray(root.length), Buffer.from(value)]) };
      validateCheckpoint(snapshot, limits); await tn.rawCommit(); return snapshot;
    } catch (cause) { throw classify(cause, true); } finally { tn.rawCancel(); }
  }); }
  function restoreCheckpoint(snapshot, options = {}) { return checkpointTask(async () => {
    if (readOnly) throw error('READ_ONLY', 'Cannot restore through a read-only handle');
    fields(options, ['maxRows', 'maxBytes', 'beforeCommit', 'afterCommit']);
    for (const name of ['beforeCommit', 'afterCommit']) if (options[name] !== undefined && typeof options[name] !== 'function') throw error('INVALID_OPTIONS', 'Expected checkpoint hook');
    const limits = checkpointLimits(options), checked = validateCheckpoint(snapshot, limits), tn = db.rawCreateTransaction(nativeOptions); let attempted = false, committed = false;
    try {
      tn.addReadConflictRange(root, prefixEnd(root));
      const existing = await tn.getRangeAll(root, prefixEnd(root), { limit: 2 });
      if (existing.length !== 1 || !Buffer.from(existing[0][0]).equals(formatKey) || !Buffer.from(existing[0][1]).equals(Buffer.from(format))) throw error('RESTORE_TARGET_NOT_EMPTY', 'Restore requires an empty initialized candidate namespace');
      for (const [suffix, value] of checked.rows) tn.set(Buffer.concat([root, suffix]), value);
      if (options.beforeCommit) await options.beforeCommit({ rows: checked.rows.length, bytes: checked.bytes });
      attempted = true; await tn.rawCommit(); committed = true;
      if (options.afterCommit) await options.afterCommit({ rows: checked.rows.length, bytes: checked.bytes });
      return { restored: true, rows: checked.rows.length, bytes: checked.bytes };
    } catch (cause) { if (committed) throw error('COMMIT_UNKNOWN', 'Restored candidate lost acknowledgement; compare its original checkpoint', cause); throw classify(cause, attempted); }
    finally { tn.rawCancel(); }
  }); }
  async function close() {
    if (context.getStore()?.has(identity)) throw error('NESTED_TRANSACTION', 'Cannot close inside its transaction');
    closePromise ??= (async () => { closing = true; await Promise.allSettled([...active]); db.close(); closed = true; })(); return closePromise;
  }
  return Object.freeze({ capabilities, location, transact: (callback, options = {}) => run(callback, options, false), read: callback => run(callback, {}, true), getReceipt: id => run(() => {}, {}, true, checkKvKey(id)), captureCheckpoint, restoreCheckpoint, close });
}
