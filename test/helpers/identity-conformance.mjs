import test from 'node:test';
import assert from 'node:assert/strict';
import { createDecipheriv, hkdfSync, scryptSync } from 'node:crypto';
import { prepareCatalog, canonical, readAuthorizedInTransaction, resolveAuthority } from '../../src/index.mjs';
import { createEngine } from '../../src/lifecycle.mjs';
import { createKeyring } from '../../src/keyring.mjs';
import { createIdentity } from '../../src/identity.mjs';
import { mutateAuthorizedInTransaction } from '../../src/authorized-mutate.mjs';
import { sourcePrefix } from '../../src/keys.mjs';

export const password = 'a long reader password', adminPassword = 'a long admin password', replacement = 'another reader password';
export const index = ['Ledger', ['anchor', 'owner'], ['at']];
const field = field => ({ field });
const schema = properties => ({ type: 'object', additionalProperties: false, properties, required: Object.keys(properties) });
export function catalogDefinition() {
  return { name: 'identity-witness', version: '1', models: {
    Ledger: { schema: schema({ name: { type: 'string' } }), access: { orders: {} }, indexes: [{ version: '1', partitionFields: ['anchor', 'owner'], orderFields: ['at'], roles: { anchor: 'id', owner: 'id', at: 'integer' }, inputs: { amount: 'integer' }, aggregates: { total: { monoid: 'sum', input: 'amount' } } }] },
    Entry: { schema: schema({ anchor: { type: 'string' }, at: { type: 'string' }, amount: { type: 'string' } }), references: [{ slot: 'anchor', field: 'anchor', targets: ['Ledger'], many: false, onDelete: 'reject' }], access: { orders: {} }, memberships: [{ slot: 'main', index, partition: [field('anchor'), { header: 'owner' }], order: [field('at')], inputs: { amount: field('amount') }, onMissing: 'reject' }] },
  } };
}
export function graph() {
  const node = (id, kind, parents, resources = {}, admin = false) => ({ id, kind, parents, active: true, grants: { admin, resources } });
  return { tenant: 't', principals: [node('t', 'tenant', []), node('admin', 'user', ['t'], { Ledger: ['use'] }, true), node('visible', 'group', ['admin'], { Entry: ['read', 'create', 'update', 'delete'], Ledger: ['read', 'use'] }), node('hidden', 'group', ['admin']), node('reader', 'user', ['visible'])] };
}
export const record = (id, model, owner, data) => ({ id, tenant: 't', model, owner, createdAt: '0', data });
export const sources = () => [record('ledger', 'Ledger', 'visible', { name: 'Anchor' }), record('a', 'Entry', 'visible', { anchor: 'ledger', at: '1', amount: '100' }), record('b', 'Entry', 'hidden', { anchor: 'ledger', at: '1', amount: '-80' })];
export const login = (f, operationId = 'login-reader') => f.identity.login({ actor: 'reader', password, operationId });
export const query = { model: 'Entry', index, partition: ['ledger'], limit: 20 };
export const rows = (f, token) => f.identity.read(token, (tx, context, getAuthority) => readAuthorizedInTransaction(tx, f.catalog, context, query, { getAuthority }));
export const rawSources = f => f.engine.read(tx => tx.scan({ prefix: sourcePrefix('t') }));
// Independent AES decryption follows the declared public crypto contract, not
// production keyring.open or any identity writer/cache.
export function independentlyOpen(envelope, binding, root) {
  const key = Buffer.from(hkdfSync('sha256', root, Buffer.from('kv-base custody v1'), Buffer.from(canonical([binding.tenant, binding.purpose])), 32));
  const cipher = createDecipheriv('aes-256-gcm', key, Buffer.from(envelope.nonce, 'base64url'));
  cipher.setAAD(Buffer.from(canonical([envelope.version, envelope.keyId, binding]))); cipher.setAuthTag(Buffer.from(envelope.tag, 'base64url'));
  return JSON.parse(Buffer.concat([cipher.update(Buffer.from(envelope.ciphertext, 'base64url')), cipher.final()]).toString('utf8'));
}
export const credentialScope = (actor, revision = '1') => ({ tenant: 't', purpose: 'identity-password', subject: actor, revision });


export async function prepareIdentityFixture(t, options, backend) {
  const keys = { old: Buffer.alloc(32, 7), next: Buffer.alloc(32, 8) }, catalog = prepareCatalog(catalogDefinition());
  const f = { ...backend, keys, catalog, time: 1000, fence: 'incarnation-A', activeKey: 'old' };
  f.open = async () => {
    f.store = await backend.open(); f.engine = createEngine({ store: f.store, catalog, tenant: 't' });
    f.keyring = createKeyring({ keys, activeKey: f.activeKey });
    f.identity = createIdentity({ engine: f.engine, keyring: f.keyring, getRestoreFence: () => f.fence, clock: () => f.time, passwordPolicy: { N: 1024, maxN: 262144, sessionMs: 10000, maxFailures: 3, failureWindowMs: 100, lockMs: 100, ...options } });
  };
  await f.open(); t.after(async () => { await f.store.close().catch(() => {}); await backend.cleanup(); });
  await f.engine.mutate(sources().map(record => ({ type: 'put', record })));
  await f.identity.initialize({ policy: graph(), credentials: [{ actor: 'admin', password: adminPassword }, { actor: 'reader', password }] });
  return f;
}

export function registerIdentityConformance(name, fixture) {
  const witness = (label, callback) => test(name + ' identity: ' + label, callback);
  witness('private host providers use the same current graph and enrolled credential revision without a bearer', async t => {
    const f = await fixture(t);
    const prepared = await f.engine.read(tx => f.identity.getAuthorityInTransaction(tx));
    assert.equal(resolveAuthority(prepared, { tenant: 't', actor: 'admin' }).grants.admin, true);
    assert.deepEqual(await f.engine.read(tx => f.identity.inspectActorInTransaction(tx, 'reader')), { tenant: 't', actor: 'reader', credentialRevision: '1' });
    await assert.rejects(f.engine.read(tx => f.identity.inspectActorInTransaction(tx, 'unknown')), { code: 'UNAUTHENTICATED' });
    await assert.rejects(f.engine.read(tx => f.identity.inspectActorInTransaction(tx, 'visible')), { code: 'UNAUTHENTICATED' });
    const revoked = graph(); revoked.principals.find(node => node.id === 'reader').active = false; await f.identity.replacePolicy(revoked);
    await assert.rejects(f.engine.read(tx => f.identity.inspectActorInTransaction(tx, 'reader')), { code: 'UNAUTHENTICATED' });
    const current = await f.engine.read(tx => f.identity.getAuthorityInTransaction(tx));
    assert.throws(() => resolveAuthority(current, { tenant: 't', actor: 'reader' }), { code: 'FORBIDDEN' });
    await f.identity.replacePolicy(graph()); const session = await login(f);
    await f.identity.changePassword(session.token, { oldPassword: password, newPassword: replacement, operationId: 'private-provider-password' });
    assert.deepEqual(await f.engine.read(tx => f.identity.inspectActorInTransaction(tx, 'reader')), { tenant: 't', actor: 'reader', credentialRevision: '2' });
  });
  witness('original successful login password guesses consume bounded durable attempts once and lock result release', async t => {
    const f = await fixture(t), original = await login(f), guesses = ['a first guessed reader password', 'a second guessed reader password', 'a third guessed reader password'];
    const request = guessed => ({ actor: 'reader', password: guessed, operationId: 'login-reader' });
    for (let n = 0; n < 2; n++) await assert.rejects(f.identity.login(request(guesses[0])), { code: 'RECEIPT_MISMATCH' });
    assert.equal((await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/failure/')).value.count, 1);
    for (const guess of guesses.slice(1)) await assert.rejects(f.identity.login(request(guess)), { code: 'RECEIPT_MISMATCH' });
    await f.store.close(); await f.open(); const before = await f.engine.read(tx => tx.scan());
    await assert.rejects(f.identity.login(request('a fourth guessed reader password')), { code: 'UNAUTHENTICATED' }); await assert.rejects(login(f), { code: 'UNAUTHENTICATED' });
    assert.deepEqual(await f.engine.read(tx => tx.scan()), before); assert.equal((await rows(f, original.token)).records[0].id, 'a');
    const counter = before.find(row => row.key.includes('/failure/')).value; f.time = counter.lockedUntil + 1;
    assert.deepEqual(await login(f), original); assert.equal((await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length, 1);
  });
  witness('password envelopes independently verify scrypt and durable state contains no password root key or original bearer plaintext', async t => {
    const f = await fixture(t), issued = await login(f), inventory = await f.engine.read(tx => tx.scan());
    const stored = inventory.find(row => row.key.includes('/credential/') && row.value.actor === 'reader').value;
    const specification = independentlyOpen(stored.envelope, credentialScope('reader'), f.keys.old);
    const expected = scryptSync(password, Buffer.from(specification.salt, 'base64url'), 32, { N: specification.N, r: specification.r, p: specification.p });
    assert.equal(expected.toString('base64url'), specification.hash);
    const other = inventory.find(row => row.key.includes('/credential/') && row.value.actor === 'admin').value;
    assert.notEqual(independentlyOpen(other.envelope, credentialScope('admin'), f.keys.old).salt, specification.salt);
    const encoded = JSON.stringify(inventory, (_, value) => typeof value === 'bigint' ? String(value) : value);
    for (const secret of [password, adminPassword, issued.token, specification.hash, specification.salt]) assert.equal(encoded.includes(secret), false, secret);
    await f.store.close(); const physical = await f.physical();
    for (const secret of [password, issued.token, f.keys.old.toString('hex')]) assert.equal(physical.includes(Buffer.from(secret)), false);
    await f.open(); assert.deepEqual((await rows(f, issued.token)).records.map(row => row.id), ['a']);
  });

  witness('unknown actors add no state; known failed password proofs are durable bounded and count each operation once', async t => {
    const f = await fixture(t), before = await f.engine.read(tx => tx.scan());
    for (let n = 0; n < 6; n++) await assert.rejects(f.identity.login({ actor: `unknown-${n}`, password, operationId: `unknown-${n}` }), { code: 'UNAUTHENTICATED' });
    assert.deepEqual(await f.engine.read(tx => tx.scan()), before);
    const failure = { actor: 'reader', password: 'wrong reader password', operationId: 'failure-one' };
    for (let n = 0; n < 2; n++) await assert.rejects(f.identity.login(failure), { code: 'UNAUTHENTICATED' });
    let counter = (await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/failure/')).value;
    assert.equal(counter.count, 1);
    for (const operationId of ['failure-two', 'failure-three']) await assert.rejects(f.identity.login({ ...failure, operationId }), { code: 'UNAUTHENTICATED' });
    await f.store.close(); await f.open();
    await assert.rejects(login(f), { code: 'UNAUTHENTICATED' });
    counter = (await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/failure/')).value;
    assert.equal(counter.count, 3); f.time = counter.lockedUntil + 1;
    assert.ok((await login(f, 'after-lock')).token);
  });

  witness('lost session acknowledgement recovers the original encrypted result after reopen without minting another session', async t => {
    const f = await fixture(t); let receiptId;
    await assert.rejects(f.identity.login({ actor: 'reader', password, operationId: 'uncertain-login' }, { afterCommit() { throw Error('lost reply'); } }), error => { receiptId = error.receiptId; return error.code === 'COMMIT_UNKNOWN'; });
    const receipt = await f.engine.getReceipt(receiptId), original = independentlyOpen(receipt.result.envelope, receipt.result.binding, f.keys.old);
    assert.equal(JSON.stringify(receipt).includes(original.token), false); assert.equal(JSON.stringify(receipt).includes(password), false);
    const count = (await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length;
    await f.store.close(); await f.open();
    assert.deepEqual(await login(f, 'uncertain-login'), { token: original.token, expiresAt: original.expiresAt });
    assert.equal((await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length, count);
    await assert.rejects(f.identity.login({ actor: 'reader', password: 'changed request password', operationId: 'uncertain-login' }), { code: 'RECEIPT_MISMATCH' });
  });

  witness('session expiry is exact and external restore fencing rejects restored sessions/results while permitting a fresh password login', async t => {
    const f = await fixture(t), issued = await login(f);
    f.time = Number(issued.expiresAt); await assert.rejects(rows(f, issued.token), { code: 'UNAUTHENTICATED' });
    f.time = 1000; await f.store.close(); const backup = await f.capture(); await f.open();
    await f.identity.logout(issued.token, { operationId: 'logout-before-restore' }); await f.store.close();
    await f.restore(backup); f.fence = 'incarnation-B'; await f.open();
    await assert.rejects(rows(f, issued.token), { code: 'UNAUTHENTICATED' });
    await assert.rejects(login(f), { code: 'RESTORE_FENCED' });
    const fresh = await login(f, 'login-after-restore'); assert.notEqual(fresh.token, issued.token);
    assert.deepEqual((await rows(f, fresh.token)).records.map(row => row.id), ['a']);
  });

  witness('password change invalidates every old session and original changed result recovers after lost acknowledgement', async t => {
    const f = await fixture(t), a = await login(f, 'session-a'), b = await login(f, 'session-b'), request = { oldPassword: password, newPassword: replacement, operationId: 'password-change' };
    await assert.rejects(f.identity.changePassword(a.token, request, { afterCommit() { throw Error('lost reply'); } }), { code: 'COMMIT_UNKNOWN' });
    for (const token of [a.token, b.token]) await assert.rejects(rows(f, token), { code: 'UNAUTHENTICATED' });
    await f.store.close(); await f.open(); assert.deepEqual(await f.identity.changePassword(a.token, request), { changed: true });
    const stored = (await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/credential/') && row.value.actor === 'reader').value;
    assert.equal(stored.revision, '2');
    await assert.rejects(login(f, 'old-password-after-change'), { code: 'UNAUTHENTICATED' });
    const fresh = await f.identity.login({ actor: 'reader', password: replacement, operationId: 'new-password-login' });
    assert.deepEqual((await rows(f, fresh.token)).records.map(row => row.id), ['a']);
  });

  witness('old password verification attempts commit bounded failure state and never alter credentials or sources', async t => {
    const f = await fixture(t), issued = await login(f), before = await rawSources(f), wrong = { oldPassword: 'wrong old password', newPassword: replacement, operationId: 'old-proof-1' };
    await assert.rejects(f.identity.changePassword(issued.token, wrong), { code: 'UNAUTHENTICATED' });
    await assert.rejects(f.identity.changePassword(issued.token, wrong), { code: 'UNAUTHENTICATED' });
    for (const operationId of ['old-proof-2', 'old-proof-3']) await assert.rejects(f.identity.changePassword(issued.token, { ...wrong, operationId }), { code: 'UNAUTHENTICATED' });
    const counter = (await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/failure/')).value;
    assert.equal(counter.count, 3);
    await assert.rejects(f.identity.changePassword(issued.token, { oldPassword: password, newPassword: replacement, operationId: 'locked-correct-proof' }), { code: 'UNAUTHENTICATED' });
    assert.deepEqual(await rawSources(f), before);
    f.time = counter.lockedUntil + 1;
    assert.deepEqual(await f.identity.changePassword(issued.token, { oldPassword: password, newPassword: replacement, operationId: 'after-lock-change' }), { changed: true });
  });

  witness('logout remains revoked after reopen and receipt recovery returns only the original logout outcome', async t => {
    const f = await fixture(t), issued = await login(f);
    await assert.rejects(f.identity.logout(issued.token, { operationId: 'logout-once' }, { afterCommit() { throw Error('lost reply'); } }), { code: 'COMMIT_UNKNOWN' });
    await f.store.close(); await f.open(); await assert.rejects(rows(f, issued.token), { code: 'UNAUTHENTICATED' });
    assert.deepEqual(await f.identity.logout(issued.token, { operationId: 'logout-once' }), { loggedOut: true });
    assert.equal((await f.engine.read(tx => tx.scan())).filter(row => row.key.includes('/session/')).length, 0);
  });

  witness('authenticated mutations retain both-owner checks and request receipts do not repeat changed source writes', async t => {
    const f = await fixture(t), issued = await login(f), moved = sources()[1]; moved.owner = 'hidden';
    const before = await rawSources(f);
    await assert.rejects(f.identity.mutate(issued.token, [{ type: 'put', record: moved }], { operationId: 'forbidden-transfer' }), { code: 'FORBIDDEN' });
    assert.deepEqual(await rawSources(f), before);
    const admin = await f.identity.login({ actor: 'admin', password: adminPassword, operationId: 'login-admin' });
    assert.deepEqual(await f.identity.mutate(admin.token, [{ type: 'put', record: moved }], { operationId: 'admin-transfer' }), { applied: 1 });
    assert.deepEqual(await rows(f, issued.token), { records: [] });
    const again = await f.identity.mutate(admin.token, [{ type: 'put', record: moved }], { operationId: 'admin-transfer' }); assert.deepEqual(again, { applied: 1 });
    await f.engine.check();
  });

  witness('current graph revocation changes authorization without rewriting sources and final auth failure rolls back application policy and receipt together', async t => {
    const f = await fixture(t), issued = await login(f), before = await rawSources(f), policyKey = `identity/v1/${canonical('t')}/policy`;
    const next = graph(); next.principals.find(node => node.id === 'visible').grants.resources.Entry = [];
    await f.identity.replacePolicy(next); await assert.rejects(rows(f, issued.token), { code: 'FORBIDDEN' }); assert.deepEqual(await rawSources(f), before);
    await f.identity.replacePolicy(graph());
    const changed = sources()[1]; changed.data.amount = '200';
    await assert.rejects(f.identity.transact(issued.token, { operationId: 'final-revoke', request: { amount: '200' } }, async (tx, context, getAuthority) => {
      const result = await mutateAuthorizedInTransaction(tx, f.catalog, context, [{ type: 'put', record: changed }], { getAuthority });
      const graph = await tx.get(policyKey); graph.principals.find(node => node.id === 'reader').active = false; await tx.set(policyKey, graph); return result;
    }), { code: 'UNAUTHENTICATED' });
    assert.deepEqual(await rawSources(f), before); assert.deepEqual((await rows(f, issued.token)).records.map(row => row.id), ['a']);
  });

  witness('private session bindings support worker recovery and reject changes revocation expiry and restore without a bearer', async t => {
    const f = await fixture(t), issued = await login(f), authenticated = await f.engine.read(tx => f.identity.authenticateInTransaction(tx, issued.token)), binding = authenticated.binding;
    assert.equal(JSON.stringify(binding).includes(issued.token), false);
    assert.deepEqual((await f.engine.read(tx => f.identity.authenticateBindingInTransaction(tx, binding))).context, { tenant: 't', actor: 'reader' });
    for (const change of [{ actor: 'admin' }, { tenant: 'other' }, { credentialRevision: '2' }, { expiresAt: '99999' }]) await assert.rejects(f.engine.read(tx => f.identity.authenticateBindingInTransaction(tx, { ...binding, ...change })), { code: 'UNAUTHENTICATED' });
    f.fence = 'new-incarnation'; await assert.rejects(f.engine.read(tx => f.identity.authenticateBindingInTransaction(tx, binding)), { code: 'UNAUTHENTICATED' });
    f.fence = 'incarnation-A'; f.time = Number(binding.expiresAt); await assert.rejects(f.engine.read(tx => f.identity.authenticateBindingInTransaction(tx, binding)), { code: 'UNAUTHENTICATED' });
    f.time = 1000; await f.identity.logout(issued.token, { operationId: 'logout-binding' }); await assert.rejects(f.engine.read(tx => f.identity.authenticateBindingInTransaction(tx, binding)), { code: 'UNAUTHENTICATED' });
  });

  witness('retained key rotation reopens credentials sessions and exact original receipts while active key mints new envelopes', async t => {
    const f = await fixture(t), old = await login(f); await f.store.close(); f.activeKey = 'next'; await f.open();
    assert.deepEqual((await rows(f, old.token)).records.map(row => row.id), ['a']); assert.deepEqual(await login(f), old);
    const fresh = await login(f, 'new-key-session'); assert.ok(fresh.token.startsWith('s1.next.'));
    const envelope = f.keyring.seal({ secret: 'new' }, { tenant: 't', purpose: 'credential', subject: 'id', revision: '1' }); assert.equal(envelope.keyId, 'next');
  });

  witness('malformed controls reject without executing accessors and proof issuance requires a live enrolled actor', async t => {
    const f = await fixture(t); let touched = false;
    const bad = { actor: 'reader', operationId: 'getter' }; Object.defineProperty(bad, 'password', { enumerable: true, get() { touched = true; return password; } });
    await assert.rejects(f.identity.login(bad), { code: 'INVALID_IDENTITY' }); assert.equal(touched, false);
    await assert.rejects(f.identity.login({ actor: 'reader', password, operationId: 'bad-hook' }, { beforeCommit: 'not a function' }), { code: 'INVALID_IDENTITY' });
    await assert.rejects(f.identity.mutate('invalid-token', [], { operationId: 'bad-hook', beforeCommit: false }), { code: 'INVALID_IDENTITY' });
    for (const token of [undefined, null, '', 's1.missing.' + Buffer.alloc(32, 1).toString('base64url'), 's1.old.' + Buffer.alloc(32, 2).toString('base64url')]) await assert.rejects(rows(f, token), { code: 'UNAUTHENTICATED' });
    await assert.rejects(f.engine.transact(tx => f.identity.issueSessionInTransaction(tx, 'unknown')), { code: 'UNAUTHENTICATED' });
    const token = await f.engine.transact(tx => f.identity.issueSessionInTransaction(tx, 'reader'));
    assert.deepEqual((await rows(f, token.token)).records.map(row => row.id), ['a']);
  });

  witness('default password enrollment uses the declared scrypt work factor and remains independently verifiable', async t => {
    const f = await fixture(t, { N: 131072 });
    const value = (await f.engine.read(tx => tx.scan())).find(row => row.key.includes('/credential/') && row.value.actor === 'admin').value;
    const specification = independentlyOpen(value.envelope, credentialScope('admin'), f.keys.old);
    assert.deepEqual([specification.N, specification.r, specification.p], [131072, 8, 1]);
    const expected = scryptSync(adminPassword, Buffer.from(specification.salt, 'base64url'), 32, { N: specification.N, r: 8, p: 1, maxmem: 160 * 1024 * 1024 });
    assert.equal(expected.toString('base64url'), specification.hash);
  });

  witness('live authenticated cursors bind session credential revision and external incarnation while unrelated writes preserve progress', async t => {
    const f = await fixture(t), added = record('c', 'Entry', 'visible', { anchor: 'ledger', at: '2', amount: '3' });
    await f.engine.mutate([{ type: 'put', record: added }]);
    const request = { ...query, limit: 1 }, a = await login(f, 'cursor-a'), b = await login(f, 'cursor-b');
    const first = await f.identity.authorizedPage(a.token, request); assert.equal(first.records[0].id, 'a'); assert.equal(typeof first.cursor, 'string');
    await assert.rejects(f.identity.authorizedPage(b.token, request, { cursor: first.cursor }), { code: 'STALE_CURSOR' });
    const other = record('other-ledger', 'Ledger', 'visible', { name: 'Unrelated' }); await f.engine.mutate([{ type: 'put', record: other }]);
    assert.deepEqual((await f.identity.authorizedPage(a.token, request, { cursor: first.cursor })).records.map(row => row.id), ['c']);
    const beforePassword = await f.identity.authorizedPage(a.token, request);
    await f.identity.changePassword(a.token, { oldPassword: password, newPassword: replacement, operationId: 'cursor-password' });
    const next = await f.identity.login({ actor: 'reader', password: replacement, operationId: 'cursor-new-password' });
    await assert.rejects(f.identity.authorizedPage(next.token, request, { cursor: beforePassword.cursor }), { code: 'STALE_CURSOR' });
    const beforeFence = await f.identity.authorizedPage(next.token, request); f.fence = 'cursor-new-incarnation';
    const refreshed = await f.identity.login({ actor: 'reader', password: replacement, operationId: 'cursor-after-fence' });
    await assert.rejects(f.identity.authorizedPage(refreshed.token, request, { cursor: beforeFence.cursor }), { code: 'STALE_CURSOR' });
    await assert.rejects(f.identity.authorizedPage(refreshed.token, { ...request, getContinuationFence: 'wire-value' }), { code: 'INVALID_IDENTITY' });
  });
}
