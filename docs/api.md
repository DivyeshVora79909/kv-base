# Engine API guide

Function contracts below describe the extracted Node implementation. Numeric test
counts and evidence links in historical checkpoint paragraphs describe the pinned
predecessor package, not fresh kv-base acceptance. Current acceptance belongs to
[CONTEXT](../CONTEXT.md); remaining work belongs to [checkpoints](checkpoints.md).

JS-P1 and JS-P2 supply prepared index contracts/bindings, an independent flat source
oracle, exact monoids, a transactional memory adapter and augmented AVL mutation/read
functions. JS-P3a internal staging/routing primitives are accepted; JS-P3b.1 full-snapshot reference is accepted; JS-P3b.2 incremental reactive
settlement and JS-P4a raw batched windows are accepted for the local volatile adapter.
This package is not a replacement server.

Use Node 24+ and `npm ci` at the repository root. There is no custom command parser;
`npm test` uses the standard Node test runner. From the repository-root Node REPL:

```js
const engine = await import('./src/index.mjs');
const example = await import('./examples/structural.mjs');
const catalog = engine.prepareCatalog(example.structuralActivityDefinition);
const records = example.activityRecords();
const store = engine.createMemoryStore();
await engine.mutate(store, catalog, 'tenant',
  records.map(record => ({ type: 'put', record })));
await engine.checkStore(store, catalog, 'tenant');
// { sources: 4, trees: 5, occurrences: 9, references: 3, claims: 3 }
const scope = { tenant: 'tenant', index: example.activityIndexes.resourceEffective, partition: ['resource'] };
await store.read(async tx => ({
  first: await engine.treeSelect(tx, catalog, scope, 0),
  totals: await engine.treeAggregate(tx, catalog, scope),
  throughTwo: await engine.treeAggregate(tx, catalog, scope,
    { upper: { value: ['2'] } }),
}));
await engine.mutate(store, catalog, 'tenant', [{ type: 'delete', id: 'fix' }]);
await engine.checkStore(store, catalog, 'tenant');
```

The example declares two source models mapping different amount fields into shared
declarations and three structural indexes. Effective and created indexes have distinct identities
and folds; three contributing records produce nine occurrences. `sourceOracle` sorts
and folds complete source facts; it never reads maintained indexes. It also checks
final reference targets and unique claims. This deliberate whole-source operation is
an inspector/test oracle, not a mutation algorithm.

## Structural declarations (R1)

Models declare `indexes: [{ version, partitionFields, orderFields, roles, inputs,
aggregates, validators? }]`. Role domains are `id`, `string`, `integer` or
`boolean`. The signature is `[declaringModel, partitionFields[], orderFields[]]`;
both ordered arrays and their boundary enter identity. Roles need not be fields on
the declaring model: contributors bind local selectors in `memberships: [{ slot,
index: signature, partition: selectors[], order: selectors[], inputs, onMissing }]`.
Unknown declarations, duplicate/conflicting signatures, role/domain mismatches and
binding arity errors reject during preparation. The same prepared catalog compiles
schemas and all summary implementations.

`resolveIndex(catalog, signature)` returns frozen declaration/contract metadata.
Raw scopes accept `{ tenant, index: signature, partition: values[] }`. The signature
contains a model name, never a record ID; an anchor is an explicit partition value.
Unanchored country/year partitions work identically. Each populated partition owns
its rotating AVL head. Stable source/slot identities distinguish repeated memberships.
An anchor creates no automatic opening: only explicitly authored memberships enter
the fold. See `examples/structural.mjs` for an importable example.

R2 carries typed lexicographic tuples through all structural indexes, including
singleton orders as `[value]`. Comparison appends source ID and stable slot after
the tuple. Bounds use `{value: [componentValues], inclusive?}` for an entire equal
tuple group; adding both `sourceId` and `slot` cuts at one complete occurrence.
Range calculations, reverse subscription intervals, potential inventories, rank/
select, raw pages and owner merges preserve this order. Identical projections and
payload-only edits perform no occurrence writes. Prepared catalogs expose
`representationVersion: 2`, included in identity; mutations and authorized requests
reject incompatible catalog state. Privileged transaction tree primitives assume
caller-established catalog identity. R5 supplies explicit lifecycle opening/rebuild.
The older named-view scalar fixtures remain regression witnesses. Reference paths,
structural owner publication and the lifecycle are described in the accepted R3–R5 contracts.

## Reference-path participation (R3)

A membership selector may use `{path:[{reference:'issue',target:'Issue'},
{reference:'invoice',target:'Invoice'}], header:'id'}` or a terminal `field`.
Preparation checks every declared scalar reference slot, explicit allowed target
model and terminal domain. Paths are finite literal arrays. Many-valued hops and
managed target topology reject; managed contributors may follow authored targets.
Membership `onMissing: 'skip' | 'reject'` governs absent edges/terminal values;
invalid present values and mismatched target types always reject.

`projectRecord(catalog,record,sourcesMap?)` accepts an explicit tenant-local Map
keyed by source ID for path dependencies. The transactional writer point-loads the
finite dependencies, stages all source commands before projecting the final graph,
and finds untouched contributors through old incoming entries and staged final
edges. It retains original/final occurrences across repeated settlement stages.
Their changes route to subscribers within the same atomic transaction. Managed
potential templates register authored path dependencies, including target existence
while actual outputs are absent. Hydration charges path reads to the request budget.
The source, calculation and maintained-state checkers independently traverse source
facts without the writer's path/projection helpers. See `examples/reference-path.mjs`.

Mutation diagnostics add `referenceWork: {pathReads,reverseVisits,scannedKeys}`.
Incoming prefix scans and attempt-local graph enumeration remain real work; no
whole-transaction logarithmic claim follows from an AVL path. Independent full
checking detects omitted reverse edges; every mutation does not detect every possible
pre-existing omission. Durable graph storage and concurrency remain deferred.

## Current functions

- `prepareCatalog(definition, { monoids, validators, calculations })`: compile JSON Schema and
  check binding/reference/view declarations, returning frozen metadata and identity.
- `validateRecord(catalog, record)`: validate and detach one source record. It does
  not check database references, authorize callers, or execute summary validators.
- `projectRecord(catalog, record)`: derive zero/one/many typed membership contributions.
- `sourceOracle(catalog, records)`: independent expected trees, references and claims
  for a complete supplied snapshot. Partial data is not proof of full-store integrity.
- `mutate(store, catalog, tenant, changes, options?)`: atomically apply
  `{ type: 'put', record }` / `{ type: 'delete', id }` commands, one per source ID.
  Reconcile memberships, reverse references and unique claims against the final graph;
  release old claims before acquisition so joint swaps work. `model` and `createdAt`
  are immutable on update. Return changed-source/membership counts, touched tree scopes
  and adapter metrics. Reactive catalogs also return `settlement` metrics; see below.
  The optional `beforeCommit` fault hook runs before publication.
- `readAuthorized`, `readIncomingAuthorized`, `mutateAuthorized`: transaction-owned
  fixture-policy enforcement with current authority, safe result shapes and protected
  disclosure; see [JS-P4b.2](#js-p4b2-authorized-request-composition) for exact contracts.
- `checkStore(store, catalog, tenant)`: independently reconstruct complete active-generation
  inventories from source facts, then check ordering, links, AVL balance, heights,
  structural sizes, subtree folds, references, claims and business validators.
  Return source/tree/occurrence/reference/claim counts; throw on disagreement.
- `readRoot(tx, catalog, scope)`: return the current head node, or `undefined`.
  Scope is `{ tenant, view, partition }`; the stable anchor is independent of its head.
- `treeGet(tx, catalog, scope, { order, sourceId, slot })`: exact occurrence lookup.
- `treeSelect(tx, catalog, scope, rank)`: zero-based structural occurrence selection.
  Missing keys/out-of-range ranks return `undefined`; invalid ranks reject.
- `treeAggregate(tx, catalog, scope, { lower, upper }?)`: ordered aggregate map.
  Bounds are `{ value, inclusive? }` on the declared order domain (inclusive by default),
  applying equally to all record/slot ties. A complete occurrence bound adds both
  `sourceId` and `slot`: `{ value, sourceId, slot, inclusive? }`, cutting within a tie
  group in order/source/slot order. Empty ranges return monoid identities.
- `treeReadWindow(tx, catalog, scope, { ranks?, range?, limits })`: batched raw
  occurrences in order, with absolute ranks and work metrics. Rank bounds are
  half-open `{ start, end }`; key cuts use the same syntax as `treeAggregate`.
  Both constraints intersect. Counts and member IDs are privileged; see below.
- `createMemoryStore()`: caller-owned `transact` and `read` callbacks; transaction
  methods are `get`, `getMany`, `set`, `delete`, `scan`, and `inspect` for work/guards.
  A `beforeCommit({ changes })` fault hook receives detached changes before publication.

Declarations are JSON data; trusted ordinary monoid/validator functions are supplied
separately with explicit versions. Function source is not a semantic fingerprint.
Authors must update versions when behavior changes. `standardMonoids` supplies
exact integer sum, minimum, maximum and ordered minimum-prefix state. Validators are
executed synchronously against final touched-root summaries in JS-P2. An `allPrefixes`
rule requires a suitable witness monoid (for example, `prefix.minPrefix >= 0n`);
the engine does not quantify an arbitrary predicate over every prefix. Validators must
return exactly `true`. Deleting the last occurrence validates identity summaries before
removing the anchor; persistent live empty-anchor lifetimes remain deferred.

## Batched raw windows

Compose windows inside one `store.read` callback to keep the same snapshot. All
limits below are caller-selected budgets, not fixed engine capacity ceilings:

```js
const limits = {
  maxNodes: 2000, maxBatches: 200, maxBytes: 4_000_000,
  maxOutput: 100, batchKeys: 64,
};
await store.read(async tx => {
  const first = await engine.treeReadWindow(tx, catalog, scope, {
    ranks: { start: 0, end: 2 }, limits,
  });
  const second = await engine.treeReadWindow(tx, catalog, scope, {
    ranks: { start: 2, end: 4 }, limits,
  });
  return { first, second };
});
```

The result is `{ total, occurrences, metrics }`. Each occurrence is
`{ rank, sourceId, slot, order }`: two slots from one source are two occurrences.
`total` is the complete raw tree count; ranks always refer to that tree, even when
`range` removes some rows. Without `ranks`, read the whole requested key range;
without `range`, use only ranks. Ends beyond the tree count are clipped. An
exclusive complete lower cut also composes continuation within the same snapshot.
These calls are not serialized cursors; another read callback may see changed data.

All five limits are required finite safe integers, nonnegative except positive
`batchKeys`. Misspelled options reject. Exhausting a budget throws `WORK_LIMIT`
without returning a partial page. `maxOutput` rejects oversized results rather
than truncating a breadth-first frontier into an incorrectly ordered page.

`metrics.nodes` counts distinct node keys fetched, including child-root count
probes; `batches` counts every `getMany`, including the root header. `readBytes`
counts UTF-8 requested keys plus canonical encoded present values; `outputBytes`
counts encoded returned occurrences. Their sum is capped by `maxBytes`. This check
happens **after** the memory adapter fetches/clones a batch; it does not bound
earlier transfer, cloning or heap allocation. `waves` counts traversal levels
after header/head setup, `maxFrontier` counts the largest processing frontier,
and `outputs` counts occurrences. Empty windows still read header/head for `total`.

For valid AVL trees, traversal plus child metadata probes takes `O(log(n+1)+k)`
node work; cached nodes are fetched once, and rank scattering orders results in
`O(k)` without sorting them. Adequate batch capacity gives logarithmic dependent
tree levels, not guaranteed logarithmic wall time or bytes. Finite batches add
storage calls; a batch is not a measured network round trip. The reader checks
loaded identities and expanded local structure but trusts summaries in pruned
subtrees. Use `checkStore` for independent full integrity checking.

This raw API neither hydrates sources nor authorizes disclosure. Use the separate
JS-P4b.2 functions below for fixture-authorized reads. Cross-request cursor fencing
remains a later gate. Fixture authority alone does not authorize this raw API.
JS-P4a acceptance: 152 tests passed on Node 24.22.1, zero failed/skipped; see
[evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p4a.json).

## JS-P3a internal primitives

The staging and routing primitives below are internal imports, not exports from
`src/index.mjs`. The extended `treeAggregate` remains public.
`createMutation(tx, catalog, tenant)` in `src/stage.mjs` (re-exported internally by
`src/mutate.mjs`) returns asynchronous
`stage(changes)` and `finish()`. Repeated stages update sources/trees in the local
transaction overlay; finish checks final references, unique claims and root validators,
allowing joint repairs before publication. Call finish exactly once and propagate any
error to abort the surrounding transaction. Final validator scopes derive from the
original-to-final transition, excluding transient-only scopes. Intermediate staging has no independently
valid graph or managed/private-output guarantees. Public `mutate` wraps plain catalogs in one stage/finish transaction and dispatches
calculation/managed-model catalogs through incremental settlement. Internal
`finish({ scopes })` can add final live potential scopes for identity validation.

`src/routes.mjs` supplies `replaceRoutes`, `readRoutes` and `affectedSubscribers` for
explicit direct-field and typed aggregate-range registrations. Callers must supply
changed fields and old/new occurrence points; the JS-P3b.2 writer now supplies these
signals automatically.
Registrations may persist when the selected range is empty; unchanged registrations
retain guards without delete/reinsert work. The router reuses AVL
nodes with maximum-upper summaries and conservative pruning; it reports actual
interval-node/direct-entry visits and matches. `maxVisits` exhaustion rejects rather
than returning an incomplete answer. Worst-case interval routing is O(R) for R
registrations, not a demonstrated logarithmic-plus-matches bound. Memory prefix scans
still enumerate the Map, outside the logical route-visit budget.

Acceptance: 81 Node tests passed on Node 24.22.1, zero failed/skipped.
JS-P3b.1 adds prepared calculation contracts and a full-snapshot reference below.
Incremental routing/staging integration and managed storage are accepted locally
in JS-P3b.2.

## JS-P3b.1 calculation reference

From the repository-root Node REPL:

```js
const engine = await import('./src/index.mjs');
const causal = await import('./examples/causal.mjs');
const catalog = engine.prepareCatalog(causal.causalDefinition,
  { calculations: causal.causalCalculations() });
const result = await engine.settlementOracle(catalog, 't', causal.causalDiamond());
Object.fromEntries(['A', 'B', 'D'].map(id =>
  [id, result.generated.find(r => r.id === causal.causalOutput(id)).data.amount]));
// { A: '20', B: '30', D: '50' }
```

`settlementOracle(catalog, tenant, authored, { reservations, maxProducers, maxEdges }?)`
is a full recomputation reference, not a transactional writer. Supply the complete tenant
**authored** snapshot and complete historical reservation ledger, including retired roles;
these are caller preconditions, not facts checked against a database. It returns final
`records`, `generated`, retained `reservations`, producers, graph order/edges, routes,
static inventory and work metrics. `generatedId(tenant, anchorId, calculationSlot, role)`
is public; `producerId` remains an internal module function.

Authored models declare `calculations: [{ slot, use }]`. Output-only models declare
`managed: true` and must not declare calculations. The trusted calculation registry
supplies a version and ordinary `resolve`/`evaluate` functions.
Resolve may be synchronous or asynchronous. Its `readField(id, selector)` is synchronous
(and await-compatible), returns `{ exists, present, value? }`, reads authored records
only and records implicit field/existence routes. Dynamic topology may depend only on
authored facts. Explicit reads are direct fields or typed aggregate ranges with the
same complete cuts as tree reads.

Resolve returns parameters, reads and complete schema-valid output templates with a
role, managed model, owner, creation time, data and exact `computed` field names.
Template values establish potential membership topology; placeholders never serve as
computed values. Potential edges include outputs whose evaluation is null. The oracle
orders/checks the full potential graph for cycles before evaluation. Synchronous evaluate
returns `{ outputs: { role: patchOrNull } }`; each patch contains exactly its declared
computed fields and must preserve membership topology. Null emits no current record.
The first potential declaration reserves output model and creation time even if null;
retired reservations remain, and a later changed model/time rejects rather than being
automatically replaced with historical metadata.

Callbacks are trusted ordinary JavaScript with a developer purity obligation, not a
sandbox. The reference deliberately scans/sorts complete snapshots, compares potential
edges pairwise and repeatedly scans during topological ordering; work may be quadratic
or higher with query costs. Producer/edge caps do not bound all CPU, bytes or callback
work. No incremental complexity, durability or production capacity follows.

The JS-P3b.2 writer now accepts calculation/managed-model catalogs through public
`mutate`; direct caller commands still reject managed models and reserved derived IDs.
`sourceOracle` inspects static final-record inventories inside the reference, while
`checkStore` independently compares reactive and public inventories in one snapshot.
Acceptance: 102 Node tests passed on Node 24.22.1, zero failed/skipped.
See [JS-P3b.1 evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p3b-oracle.json).

## JS-P3b.2 transactional settlement

Using `engine`, `causal` and `catalog` from the reference example above:

```js
const store = engine.createMemoryStore();
const mutation = await engine.mutate(store, catalog, 't',
  causal.causalDiamond().map(record => ({ type: 'put', record })),
  { maxProducers: 100, maxEdges: 1000,
    maxRouteVisits: 10000, maxPotentialVisits: 10000 });
const checked = await engine.checkStore(store, catalog, 't');
const d = await store.read(tx => engine.treeGet(tx, catalog,
  { tenant: 't', view: 'effective', partition: ['D'] },
  { order: '1', sourceId: engine.generatedId('t', 'D', 'main', 'value'), slot: 'main' }));
d.contributions.total; // 50n
mutation.settlement; // resolved, evaluated, routeVisits, potentialVisits, edges, pending
```

One transaction stages authored and generated rows, producer state, retained identity
reservations, reverse routes and potential-output AVL indexes. The writer follows
changed routes and potential predecessors without scanning all tenant sources or
producers. It resolves authored-only topology, closes the affected final graph and
rejects cycles before evaluating. It withdraws all pending producers' old actual
outputs before evaluation, preventing moved stale points from entering range reads.
References, uniqueness and final validators—including live potential scopes with no
actual output—validate jointly before publication. Failure rolls back private and
public changes together. The existing `beforeCommit` hook remains available.

Runtime resolver `readField` is asynchronous; await its result. Outstanding reads are
drained and failures latched even if a callback ignores their promises. The batch
reference facade remains synchronous and await-compatible. Both enforce authored-only
resolver topology and exact synchronous evaluation patches. Reservations retain first
potential model/time; caller commands cannot write managed models or reserved IDs.

Options `maxProducers`, `maxEdges`, `maxRouteVisits`, `maxPotentialVisits` default to
`Infinity`. They bound logical counters, not all callback CPU, bytes or physical scans;
callbacks remain trusted ordinary JavaScript. Route reporting can be O(R); memory
prefix scans still enumerate the Map. `pending` counts the affected cohort processed, not remaining unsettled jobs.
Counts of changed sources include authored and
generated rows. No authentication/owner-policy, durability or production-capacity claim
follows from this local serialized volatile adapter.

`checkStore` runs the independent full reference in the same snapshot as public-tree
checking and verifies active managed records, producers, routes, potential forests and
retained reservations. Its full scans are deliberate. A missing retired reservation
cannot always be inferred from present facts alone; historical ledger completeness
remains a precondition. See [JS-P3b.2 evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p3b-settlement.json)
Acceptance: 140 Node tests passed on Node 24.22.1, zero failed/skipped.
Subsequent checkpoints added raw windows and fixture-authorized owner-stream reads.
Cross-request continuations and generation lifecycle remain open under R5.

## Explicit boundaries

Sources contain exactly `id`, `tenant`, `owner`, `model`, `createdAt`, `data`.
IDs are nonempty well-formed Unicode strings in this prototype, not a claim of UUID
authentication. Authored inputs cannot use the reserved `@next/derived/` prefix,
including through plain static-catalog mutations; generated identities are engine-owned. Integers in ordered/input roles and creation metadata use canonical
decimal strings; computations use BigInt. Ordinary JSON numbers must be safe integers.
Strings compare by UTF-8 bytes, not locale. Generic decimal/floating domains are not
implemented. Structural counts are distinct from business sums.

Schemas use synchronous JSON Schema 2020-12 via Ajv 8.20.0 (MIT), with strict mode,
no coercion/defaults/property removal, no remote schema loading and no async schemas.
Binding selectors require explicitly declared top-level properties, trusted header
selectors or constants. Supported role domains are `integer`, `id`, `string`,
`boolean`; unknown configuration keys reject. Missing values skip a membership only
under `onMissing: 'skip'`; malformed present values still fail. Scalar/array/polymorphic
references and unique tuples are declared once. Static distinct membership slots can
repeat a source in the same view. Array-expanded membership binding is not yet supplied.
Nonempty contract-level calculation/dependency declarations remain unsupported; model
calculation contracts use the reference and incremental paths described above.

The memory adapter serializes **both readers and writers**, retains protected-read
evidence and publishes a touched-key overlay without copying the complete store.
Point operations use Map lookup; prefix scans enumerate the entire Map and sort matches,
so reference guards using scans are not logarithmic index queries. AVL point/select/range
reads and per-membership repairs take O(h) logical path work for bounded summaries;
a whole-root fold takes O(1) logical reads. Unchanged projections still require O(h)
guarded lookup but cause zero physical occurrence writes; a complete no-op on an
initialized valid store causes zero physical writes. Variable-width BigInt cost,
multiple memberships, cascades and adapter scans add work. Values are detached; undefined is the absence sentinel; shared memory is
rejected. Reentry into the same store and transaction use after closure reject.
This memory adapter supplies no crash durability. Direct adapter calls are trusted
internal operations, not authorized RPCs.

`openSqliteStore(filename, {readOnly: false})` supplies persistent transactions
through Node's built-in SQLite binding. Writable handles use WAL, FULL synchronization,
zero busy timeout and `BEGIN IMMEDIATE` before both read and write callbacks. Callbacks
on one handle queue; another handle/process receives `STORAGE_CONFLICT` before entry
when the writer reservation is held. Reads, absent points and empty ranges stay
protected across awaits. UTF-8 BLOB keys support embedded NUL and exact binary bounds;
scans use SQLite bounds and merge the touched overlay. Values use a versioned V8
codec, preserving BigInts, cycles and bytes. Unknown formats reject; cross-runtime
format migration is not provided. `readOnly: true` supplies coherent inspection
snapshots with `serialized: false` and cannot back the engine facade.

`store.transact(callback, {receipt: {id, digest}, verifyReplay, beforeCommit,
afterCommit})` commits the original result with the KV changes. An existing ID with
another digest rejects `RECEIPT_MISMATCH`; a match skips the callback and optionally
runs `verifyReplay(tx, receipt)` with a readonly snapshot before disclosure.
`afterCommit` failure reports `COMMIT_UNKNOWN` and `receiptId`; recover via
`getReceipt(id)` or retry the original ID/digest, never invent another operation.
Receipts are privileged and may contain secrets unless the caller encrypts them.
The engine forwards receipt options and maps replay checking to the active generation;
`engine.getReceipt(id)` is privileged original-receipt access. `store.close()` drains
queued operations. Process-kill witnesses prove local SQLite recovery, not power-loss,
distributed concurrency, storage-device guarantees or automatic retries.

`openFoundationDbStore({clusterFile, prefix, timeoutMs: 4500,
transactionBytes: 10000000, readOnly: false})` is the primary durable target. An
explicit binary tuple namespace isolates KV rows, original receipts and codec
metadata. One Node service may execute multiple native transactions concurrently:
the adapter advertises `serializable: true`, `serialized: false`. Normal point and
batch reads protect present/missing keys. Scans add a native conflict range over
the complete requested interval, including keys beyond a limited page or a
zero-output request. Touched overlays preserve the same ordering/value API.

Application callbacks run once. A definite native conflict returns
`STORAGE_CONFLICT`; keys/values/transaction size and timeout controls return
`STORAGE_LIMIT` when FoundationDB confirms no commit. Native maybe-committed errors
and failed post-commit acknowledgement return `COMMIT_UNKNOWN`. Use the same
receipt ID/digest for recovery. Only internal idempotent codec initialization may
retry a definite conflict; client mutation callbacks never silently retry.
Encoded physical keys include namespace overhead and must fit 10000 bytes; V8
values and receipts must fit 100000 bytes. The native transaction limit includes
write and conflict-range overhead. Engine budgets remain additional limits.

Fresh D1F verification uses the Node `foundationdb` 2.0.1 N-API binding, compatible
API720, and native client/server7.3.81. The host supplies `libfdb_c.so` and an explicit
cluster file; the SDK loads lazily, so memory/SQLite need no installed native library.
`close()` drains active callbacks and closes that database handle. Tests select an
owned target with `KV_BASE_FDB_CLUSTER_FILE` and library search configuration.
Without a selected cluster, the real-target witnesses report skipped acceptance.
The local single-server witnesses do not certify replicated/server failures,
disk/power loss, native commit-response fault injection or general capacity.

The checker covers the requested tenant and current catalog generation, not unknown
inactive generations or lifecycle activation. Raw mutation/read functions are trusted
internal APIs: tenant partitioning is not caller authentication or authorization.

See [JS-P3a evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p3a.json),
[JS-P2 evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p2.json),
[historical JS-P1 evidence](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/checkpoints/js-p1.json) and
[remaining gates](checkpoints.md).

## JS-P4b.1 fixture authority

`prepareAuthority(catalog, { tenant, principals }, { maxPrincipals?, maxEdges? }?)`
prepares an acyclic fixture principal graph with optional work budgets. Both caps
default to `Infinity`; finite caps must be nonnegative safe integers. They bound
principal/edge counts, not earlier input cloning or total heap allocation.
Each principal has exactly
`id`, `kind` (`tenant`, `group`, `user`), `parents`, `active`, and
`grants: { admin, resources: { model: [actions] } }`. Actions are lowercase
`read`, `create`, `update`, `delete`, `use`. Require one active tenant root and retain
an active User administrator. `resolveAuthority(prepared, { tenant, actor })` returns
a branded scope for an active actor. `permits(scope, record, action)` checks an
opted-in model, tenant, action grant and owner scope; it does not validate the source.

Models are private by default. Optional declaration:

```js
access: { orders: { effective: { view: 'ownerEffective', slot: 'main', ownerPartition: 0 } } }
```

Legacy named orders select a dedicated model/slot view; structural owner queries
derive compatible memberships directly as described in R4 below. The owner position must be domain
`id` and project the trusted `owner` header exactly once. Other partition positions
may use arbitrary supported typed roles; do not assume a fixed two-role layout.
Unknown options, ambiguous views/slots, body-field owner substitutes and malformed
owner positions reject at preparation.

Read/Create/Update/Use owners are self, strict descendants and immediate Group
parents; Delete owners are self/strict descendants. Immediate Tenant parents are
excluded from ordinary owner scope. Only Users inherit raw grants from active
immediate Group/Tenant parents; grants do not transitively propagate. Admin satisfies
ordinary action checks except Use, without widening owners. Both-owner update checks
and reference authorization are enforced by the separate JS-P4b.2 functions below.
These pure fixtures do not change `mutate` or raw reads. WeakMap branding is handle authenticity,
not isolation from trusted JavaScript in the process. No login, session, network or
production authentication is supplied.

Preparation builds adjacency and checks cycles without an all-pairs descendant
table. Graph work is `O(V+E)` plus grant validation and canonical sorting. Resolving
one actor walks its reachable descendants/edges and direct parents, and copies its
effective grants. The prepared graph is immutable fixture input: request
wrappers must obtain current authority within their transaction; a long-lived
handle is not a revocation or current-session guarantee. Adding access declarations
changes catalog identity; use an explicit fresh store until lifecycle activation
is implemented.

## JS-P4b.2 authorized request composition

`readAuthorized`, `readIncomingAuthorized` and `mutateAuthorized` each own one adapter
transaction. They require `{ tenant, actor }` and a trusted host option
`getAuthority: async tx => preparedAuthority`. The provider must return current
fixture policy for that transaction, prepared against the same catalog identity.
Load mutable policy through `tx`; a constant provider is appropriate only for an
explicitly immutable fixture. The provider and diagnostic/fault callbacks are trusted
application code, never executable request input. These APIs enforce fixture policy;
they do not authenticate a claimed actor or create sessions.

Executable composition from a repository-root Node REPL:

```js
const engine = await import('./src/index.mjs');
const example = await import('./examples/authorized.mjs');
const catalog = engine.prepareCatalog(example.authorizedDefinition());
const store = engine.createMemoryStore();
await engine.mutate(store, catalog, 'tenant',
  example.authorizedRecords().map(record => ({ type: 'put', record })));
// Host-owned fixture data; this key is not a production policy persistence API.
await store.transact(tx => tx.set('fixture-authority/tenant', example.authorityDefinition()));
const getAuthority = async tx => engine.prepareAuthority(catalog,
  await tx.get('fixture-authority/tenant'));
const context = { tenant: 'tenant', actor: 'alice' };
const page = await engine.readAuthorized(store, catalog, context,
  { model: 'movement', order: 'effective', partition: ['resource'], limit: 10 },
  { getAuthority });
page.records.map(record => record.id); // ['add', 'use']
const changed = structuredClone(page.records[0]);
changed.data.label = 'renamed';
await engine.mutateAuthorized(store, catalog, context,
  [{ type: 'put', record: changed }], { getAuthority }); // { applied: 1 }
await engine.readIncomingAuthorized(store, catalog, context,
  { targetId: 'resource', model: 'movement', slot: 'resource', limit: 10 },
  { getAuthority }); // { records: [changed, the use record] }
await engine.checkStore(store, catalog, 'tenant');
```

`readAuthorized(store, catalog, context, query, options)` accepts exactly
`{ model, order, partition, range?, limit }` for legacy named orders, or the
structural `index` alternative described in R4 below. `order` names the model's prepared
access order, and `partition` supplies the **non-owner** roles in their declared
order. The request cannot supply an owner partition. `range` uses the scalar or
complete typed cuts of `treeAggregate`; explicit null/undefined ranges reject.
`limit` is a finite nonnegative safe integer. There is no filtered rank parameter.
Private models or missing Read grants reject with `FORBIDDEN`.

Resolve owner scope once, open each permitted owner stream, hydrate its head, and
merge with a heap using exact order/source/slot comparison. Advancing a popped stream
retains its traversal stack; it does not seek from the root for each row. Source
hydration validates the schema, tenant, ID, model, projected partition, occurrence
slot/order and contribution before disclosure. Dangling/mismatched candidates reject
with `CORRUPT_STATE`. A successful result is only `{ records }`; it contains no raw
ranks, totals, summaries, hidden IDs or continuation token. Zero limit returns an
empty page after request/authority validation, without discovering streams.

`readIncomingAuthorized(store, catalog, context, query, options)` accepts exactly
`{ targetId, model, slot, limit }`, where `slot` names a declared reference of the
requested source model. The target's identity and type are validated; it requires
no Read grant. Absent targets reject with `FORBIDDEN`. Incoming entries are validated against their
key, target and hydrated source's declared reference slot/array position before the
same disclosure check. Other models/slots and hidden owners are filtered. Return
distinct source records sorted by UTF-8 ID, clipped to `limit` **after** bounded
discovery. Repeated array references do not duplicate records. This query uses the
existing incoming metadata, without another relation inventory. It scans all bounded
target candidates, even for a small output limit.

Both readers support these finite options; unknown options and invalid limits reject.
Every exhausted limit throws `WORK_LIMIT` and returns no partial page:

| Option | Default | Charged work |
| --- | ---: | --- |
| `maxDiscovery` | 10000 | Actor visit, traversed descendant edges, direct parents, owner stream candidates or incoming entries |
| `maxStreams` | 256 | Owner stream setups, including absent roots |
| `maxCandidates` | 10000 | Hydrated candidate occurrences/references, including filtered incoming entries |
| `maxRefills` | 10000 | Stream advances after an output is popped, including exhausted streams |
| `maxNodeVisits` | 100000 | Loaded tree nodes |
| `maxBatchCalls` | 10000 | Reader `getMany` calls, including catalog/root/source reads |
| `maxReadBytes` | 16777216 | UTF-8 keys plus canonical encoded fetched values and incoming entries |
| `maxOutputBytes` | 4194304 | Canonical encoded disclosed records |
| `maxScannedKeys` | 1000000 | Physical enumerated keys reported by memory `tx.inspect()` |
| `batchSize` | 32 | Positive maximum keys per fetch call; it does not promise full batches |

An optional trusted `onMetrics(metrics)` receives `discovery`, `streams`, `candidates`,
`refills`, `nodes`, `batches`, `readBytes`, `outputBytes`, `outputs`, `scans` and
`scannedKeys` on success. Keep these diagnostics in the host: incoming work counters
can reveal hidden population. Callback failure rejects the read. Runtime/work-limit
behavior is not a timing noninterference guarantee.

For valid trees and bounded comparison, owner traversal loads
`O(sum(log(n_owner+1)) + candidates)` nodes; heap comparisons add
`O(candidates log(streams+1))`. Setup and authority discovery also cost work.
The current stream traversal can use single-key batches; no network RTT bound follows.
Cached paths use memory proportional to loaded nodes. Budgeting is logical and often
post-fetch: encoding, source validation/projection, authority provider preparation,
grant copying and trusted callback CPU are not hard heap/transport/CPU caps. Memory
incoming scans still enumerate the store and sort matches; the physical scan cap is
checked after scanning. No logarithmic reverse query or transaction claim follows.

`mutateAuthorized(store, catalog, context, changes, options)` accepts the same authored
put/delete commands as `mutate`, with required `getAuthority` plus the existing
settlement caps and optional `beforeCommit` fault hook. It authorizes existing and
requested owners before immutable-header, reference, uniqueness and business checks.
Create requires Create on the requested owner, update requires Update on both owners,
and delete requires the narrower Delete scope. Missing and inaccessible deletes both
reject with `FORBIDDEN`. No CAS protocol is introduced by this gate.

New/retargeted reference positions require explicit Use on the target; Admin does not
substitute for Use. Check original and proposed target owners, then settled final
targets. A fresh generated target may first appear during settlement and is checked
before publication. Unchanged reference positions do not acquire new caller checks.
Managed repairs remain privileged; direct managed/reserved-identity commands reject.
After settlement, reload authority through the **same** transaction and recheck the
original/requested/final owners and changed reference targets. Denial, settlement
failure or commit-hook failure rolls back sources, indexes, claims, routes, generated
state and policy effects together. Results contain only `{ applied: changes.length }`,
counting accepted authored commands, including no-ops; raw derived counts and guards
are not returned. Raw `mutate`, adapter calls and low-level tree APIs remain trusted.

Acceptance evidence is [JS-P4b.2](evidence/2026-11-14-js-p4b2.json). Explicit published
anchor-summary policy, cross-request continuation fencing, persistent adapters and
production identity/session enforcement remain outside this checkpoint.

## Structural owner queries and explicit anchor publication (R4)

Models still require explicit `access: { orders: {} }` to opt in to ordinary
fixture actions. Structural reads do not require an access-order alias or a
separate view. The requested model's membership must bind exactly one `id`
partition role to its own trusted `{ header: 'owner' }`. Bindings to a body field
or a related record's owner do not qualify. All qualifying membership slots must
place that header in the same role position. Other typed partition values remain
ordinary query inputs.

```js
const ownerIndex = ['Invoice', ['anchor', 'owner'], ['day', 'sequence']];
const page = await engine.readAuthorized(store, catalog, context, {
  model: 'Entry', index: ownerIndex, partition: ['invoice'], limit: 10,
  range: { lower: { value: ['1', '2'], inclusive: true } },
}, { getAuthority });
```

The `partition` query array excludes the trusted owner role. The reader substitutes
each accessible owner and merges cached stream heads by the complete typed order
and stable source/slot tie-breakers. Different models and repeated memberships can
share an ordinary structural index. Every encountered occurrence is checked against
its source facts, then filtered to the requested model and qualifying slots.
Repeated qualifying slots remain repeated rows. The source owner must match the
stream owner. Returned bodies pass the same record policy used by direct and
reverse reads. Setup, candidates, refills, nodes, batches and logical bytes retain
the finite reader budgets; diagnostics stay in the privileged `onMetrics` callback.

`readPointAuthorized(store, catalog, context, { id }, options)` returns
`{ records: [record] }` for a readable point, or `{ records: [] }` for an absent or
unreadable point. Reverse traversal validates the target's identity and type but
requires no Read grant on it. The source's own policy controls every returned row.
An absent reverse target still rejects with `FORBIDDEN`; a dangling or mismatched
source/reference rejects with `CORRUPT_STATE`. Neither traversal nor an intermediate
record grants access to a source.

A model-owned declaration can explicitly publish a selected full-anchor summary:

```js
{
  version: '1', partitionFields: ['anchor'], orderFields: ['day', 'sequence'],
  roles: { anchor: 'id', day: 'integer', sequence: 'integer' },
  inputs: { amount: 'integer' },
  aggregates: { total: { monoid: 'sum', input: 'amount' } },
  publication: { anchorPartition: 0, aggregates: ['total'] },
}

await engine.readAnchorSummaryAuthorized(store, catalog, context, {
  index: ['Invoice', ['anchor'], ['day', 'sequence']], partition: ['invoice'],
}, { getAuthority }); // { summary: { total: 20n } }
```

`anchorPartition` names an `id` partition position. It must resolve to a record of
the declaring model that passes Read in the same tenant and snapshot. The optional
query `anchorId` must equal that partition value. The query uses the entire
partition and has no range or filtered-owner inputs. Only explicitly selected
aggregate names are returned; source identities, member bodies, ranks, counts and
undeclared summaries are absent. A visible `+100` contribution and a hidden `-80`
contribution therefore yield a published full balance of `20`, while row reads
return only permitted occurrences. Exact summary values retain their ordinary
Node `BigInt` representation. An empty authorized anchor fold uses the declared
monoid identity and protects its absent root.

Trusted hosts can compose `readAuthorizedInTransaction(tx, catalog, context,
query, options)` and `readAnchorSummaryAuthorizedInTransaction(tx, catalog,
context, query, options)` within one `store.read` callback. The host owns the
read-only transaction lifetime; each operation still loads current fixture
authority through that transaction and applies all disclosure checks. This permits
public rows, a declared full summary and independent source-derived inspection to
be checked in one snapshot.

## Continuations, generations and developer tools (R5)

`createEngine({store,catalog,tenant,limits?})` supplies one fixed-tenant lifecycle
boundary over a serialized or explicitly serializable store. Configurable lifecycle
limits are `maxSources`,
`maxSourceBytes`, `maxSourceScannedKeys`, `maxCandidateKeys`, `maxCandidateBytes` and
`maxCursors`, all nonnegative safe integers. Use its `read`, `transact`, `mutate`,
`mutateAuthorized`, `readAuthorized` and `check` methods after activation.
The prepared catalog, authoritative sources and host policy keys stay shared;
only derived index/reference/claim/route/potential/reservation namespaces rotate.
Generation zero adopts existing compatible state. Every operation checks catalog
identity. Authorized contexts for another tenant reject before provider or data I/O.
Raw adapter writes bypass this lifecycle boundary and remain privileged.

`engine.window({index,partition,range?,limit},{cursor?,limits?})` returns a raw
window, an opaque cursor or `null`, selected limits and privileged source-witness
metrics. `engine.authorizedPage(context,{model,index,partition,range?,limit},
{getAuthority,cursor?,onMetrics?,...readBudgets})` returns only `{records,cursor}`.
Its partition omits the local owner role as in R4. Each request loads authority and
source facts inside its active snapshot. Query identity includes both structural
arrays, normalized bounds and page size. Continuations fence the complete tuple/
source/slot position, catalog/generation, relevant source bodies and resolved
fixture scope. Payload edits, owner transfers, revocation and query changes reject;
unrelated partitions and hidden payload edits can allow authorized progress.
No global source revision counter is used.

Cursors are bounded random process-local handles whose positions and authority
state remain private. Successful resume consumes the old handle; failed reads do
not consume it. `engine.releaseCursor(handle)` frees an unused handle. Restart,
another engine instance, tampering or reuse rejects. Old-generation handles reject
after activation. This is volatile continuation, with no persistent/session-restore
promise. A continuation independently enumerates source facts; its row, logical-byte
and physical scanned-key budgets are separate from the tree and authority budgets.
Initial and reader authority traversal share the total discovery limit. Those
witness metrics are returned only by raw reads or privileged host callbacks.

`engine.rebuild({beforeCommit?,mutation?})` pauses writers through the serialized
transaction boundary. It reconstructs a scratch candidate from authored facts and
the retained reservation ledger, checks it independently, verifies exact shared
source preservation, stages the new derived namespace and independently checks
that staged state again. Activation is atomic. Candidate/validator/publication
failure leaves the old active generation and its cursors usable. In-flight reads
finish before activation; old derived entries retire afterwards. A cleanup failure
returns successful activation with `retirement.pending` and an error; call
`engine.retire()` to retry. It never turns a known successful activation into an
apparent rebuild failure. Rebuild currently uses the same prepared catalog;
definition migration, changed generated sources, online catch-up, persistent crash
recovery and durable adapter concurrency remain deferred.

`inspectEngine(engine)` reports active population/generation, derived keys/bytes,
work counters and runtime. `populateAdaptive(engine,records,options?)` changes
future batch size from measured latency/rejection and reports each actual atomic
batch outcome. It never silently splits a rejected batch into different semantics.
`runSeededWorkload(engine,{seed,steps,generate,verify?,readQuery?,readLimits?,dataset?})`
runs reproducible caller-authored plans and records accepted/rejected transactions,
KV gets/batches/writes, scans, logical bytes, optional window visits and latency.
`attemptedChangeBytes` sums every measured staged mutation set key/value and delete
key, including validation rollback; `publicationDeltaBytes` encodes the final
adapter change list and `committedChangeBytes` is zero for rejection. These are
different measurements. Mutation latency excludes subsequent verification and
window work. Source-witness enumeration, stream setup and AVL visits remain separate;
encoded byte caps do not bound eager adapter cloning or heap allocation.

`examples/workload.mjs` exports `runWorkloadExample()` without work on import.
It uses the activity schemas, skewed partitions, equal tuple groups, exact values,
constraint rejection and injected rollback, independently checks source outcomes,
and rebuilds the final inventory. Its evidence describes this small fixture and
machine/runtime, with no production capacity or whole-transaction logarithmic claim.

## Password custody and authenticated sessions (D2a)

`createKeyring({ keys: { keyId: Buffer32 }, activeKey })` copies externally supplied
root keys into a private closure. The host keeps those keys outside database files
and backups. Its ordinary import API is `seal(JSONvalue, binding)`,
`open(envelope, binding)` and `digest(value, binding, keyId?)`. Bindings contain
`{ tenant, purpose, subject, revision, fence? }`. Revision is an exact decimal
integer string. AES-256-GCM uses a fresh 12-byte nonce and authenticates the binding,
key identifier and protocol version. HKDF-SHA256 separates tenant/purpose keys;
request digests are keyed HMACs. Envelopes retain their key identifier, so old keys
must remain available while retained envelopes or sessions use them. Unknown
protocol/key versions reject as unsupported; altered ciphertext/bindings reject
integrity. Keys are never an enumerable or serialized handle field.

`createIdentity({ engine, keyring, getRestoreFence, clock?, passwordPolicy? })`
requires a durable engine with receipt recovery. `getRestoreFence()` reads trusted
external incarnation state, distinct from the database being restored. The default
password work is scrypt `N=131072, r=8, p=1`, with a fresh 16-byte salt and 32-byte
digest, and at most two concurrent derivations per identity instance. Passwords
use their exact UTF-8 bytes. Default enrollment minimum/maximum is 12/4096 bytes;
trusted host configuration can select explicit limits. Credential envelopes are
bound to tenant, actor and revision and remain decryptable after a managed restore.
No plaintext password, password digest, salt or bearer is stored in engine state.
Original bearer results are retained only as authenticated ciphertext in receipts.
JavaScript string inputs remain transient host memory; this is not a memory-erasure
or timing-noninterference claim.

`initialize({ policy, credentials: [{ actor, password }] })` and
`replacePolicy(policy)` are privileged operator functions. They persist the exact
existing `prepareAuthority` principal graph; there is no second grant/schema DSL.
Enrollment requires active User principals and at least one usable administrator.
Policy replacement is not delegated principal CRUD. The credential/session keys
are opaque private protocol metadata, outside source memberships and generation
rotation. A model still requires declared access, the appropriate action grant and
the existing source-owner scope.

`login({ actor, password, operationId }, hooks?)` returns
`{ token, expiresAt }`. The opaque token contains a custody key identifier and 32
random secret bytes. Session state stores its keyed digest, actor, credential
revision, issue/expiry times and external restore fence. Invalid/unknown/inactive
actors receive the same authentication failure; unknown actors allocate no durable
failure/receipt rows. Known failed password proofs commit one bounded actor failure
counter per original operation. The default is five failures per fifteen-minute
window followed by a fifteen-minute lock. Wrong old-password proofs use the same
counter. Password derivation runs outside the write transaction; final credential
revision, active actor and restore incarnation are checked inside issuance.

Each successful session operation retains its encrypted original result under the
request identity and a keyed digest of the exact request, including any password
proof. A lost acknowledgement reports the adapter's `COMMIT_UNKNOWN`; repeating
that identity recovers the original result and does not mint another session or
repeat source writes. Changed requests reject. Changed-password guesses against an
existing successful login operation consume the bounded failure allowance; exact
mismatch retries do not recount. A lock prevents release of the original successful
result until expiry. Recovered login results require the
original session to remain valid. An old incarnation's result cannot be released
after restore; a fresh operation can authenticate against restored credentials.

`authenticateInTransaction(tx, token)` supplies trusted
`{ context: { tenant, actor }, getAuthority, binding }`. The private binding contains
`tenant`, `actor`, keyed `digest`, `keyId`, `credentialRevision`, `fence` and
`expiresAt`; it contains no bearer. `authenticateBindingInTransaction(tx, binding)`
is a host-worker composition point that reloads the session, current credential,
active graph, expiry and external fence. It permits recovery without storing a
bearer in worker intent. Public request handlers must accept tokens rather than
arbitrary bindings or caller-supplied actor context.

`read(token, callback(tx, context, getAuthority))` and
`transact(token, { operationId, request }, callback, hooks?)` own authenticated
engine transactions. The provider checks the session and current persisted graph
on every authority load; the transaction rechecks authentication before completion.
`mutate(token, changes, { operationId, ...settlementCaps })` composes
`mutateAuthorizedInTransaction` with that provider. It preserves both-owner checks,
explicit Use on changed references, hidden atomic settlement, and final authority.
The direct `mutateAuthorized` wrapper still detaches its context/commands before
awaiting transaction admission. Its in-transaction counterpart leaves commit hooks
to the transaction owner.

`logout(token, { operationId }, hooks?)` durably revokes that session.
`changePassword(token, { oldPassword, newPassword, operationId }, hooks?)` increments
the encrypted credential revision and invalidates all previous sessions. Their
original results remain recoverable after a lost acknowledgement, including after
the old token was intentionally invalidated. Receipt/failure metadata stays private.
`issueSessionInTransaction(tx, actor)` is a privileged verified-proof handoff for an
already enrolled active actor; its caller must seal the returned bearer into the
same original-result receipt. Delivered email/phone proof issuance follows in D2b
and is not supplied merely by this handoff.

The service selects one tenant, maps its admitted username to an actor, and supplies
clock, custody keys, fences and work budgets as host configuration. Public handlers
expose authorized operations only. Raw engine transactions, policy replacement,
credentials, diagnostic callbacks, inspection, generation controls and secret/worker
helpers remain privileged imports. KDF/read/receipt budgets are explicit work
limits, not whole-process heap or deployment-capacity evidence.

`identity.authorizedPage(token, query, options)` composes the R5 page reader with
current authenticated context. Its private trusted `getContinuationFence(tx,
context)` callback includes the exact session digest, credential revision and
restore incarnation in the cursor's authority fence, within the page transaction.
A replacement session cannot resume an old session's cursor even if actor/grants
and source data are unchanged. Password changes and live incarnation rotation
also reject old cursors. Unrelated source writes still allow progress with the
same session. The callback and fence values are never wire query/options fields.

## Durable effects and provider custody (D3)

Merge `...effectModels()` into the application's one `prepareCatalog` definition.
`Credential`, `CredentialRevision`, `Effect`, `EffectAttempt` and `EffectFact` are
ordinary private source models. `EFFECT_QUEUE` and `EFFECT_STARTS` are structural
signatures with tuple order bounds. Explicit Credential Use is required; Admin
does not grant Use. Retained revisions keep their original owners and encryption keys.

`createCredentialCustody({catalog,keyring,getAuthority,clock,grantMs})` supplies
privileged `storeRevision(tx,context,{id,owner?,provider,secret})`,
`revoke(tx,context,{id,revision?})` and request-bound grants. Revision secrets and
accepted requests persist as authenticated ciphertext. Request identity uses keyed
HMAC bound to tenant, effect and credential revision, protecting low-entropy proof
codes from offline guesses using database contents. Initial and final authority
checks include both current and retained-revision owner scope.

`acceptEffectInTransaction(tx,catalog,context,intent,{custody,providers,clock})`
accepts `{id,owner?,provider,protocolVersion:'1',operation,credentialId,
credentialRevision,request,rate:{limit,windowMs}}`. Email requests are
`{from,to:[email],subject,text}`; SMS requests are `{to:E164,text}`. Unsupported
protocol/operation/schema/byte limits reject before acceptance. Compose this call
with domain mutations and the original operation receipt in one `engine.transact`.
It returns `{effectId,providerOperationId}` to the host; the operation identity is
immutable. No public route accepts custody/context/provider configuration.

`createEffectWorker({engine,custody,providers,workerId,clock,leaseMs,apply})`
exposes privileged `claim`, `start`, `recordResult`, `runOne`, `reconcile`, `apply`,
`drain` and `cancel`. Start commits a monotone lease epoch, exact request/revision
proof and admission charge before network I/O. The rate interval is
`(now-windowMs,now]`, including time zero when in range. A committed admission can
retain its charge after death before dispatch. Callbacks do not silently retry.
Existing starts require recovery and never send again directly.

Provider calls execute outside transactions. Immutable accepted/rejected/unknown/
fenced-absent facts retain the original operation, attempt and observer epoch.
Stale observations cannot select application. `apply(tx,{effect,fact})` returns
source commands; domain changes and the applied marker commit atomically. Failed
validation retains the accepted fact for application retry without a new send.
`drain` includes facts awaiting application and has finite job/read budgets.

Unknown outcomes require lookup under the original key and retained credential.
Absent lookup alone cannot make an obsolete delayed start safe. Fake transports
provide an explicit durable fence-and-lookup protocol; live Brevo has no inferred
lookup/idempotency/fencing. Unreconcilable work remains unknown or isolated.
Cancellation before admission prevents a send; cancellation after admission cannot
promise suppression of external success, and accepted work may still apply.

`createEmailProvider({protocol:'resend'|'brevo',baseUrl,...})` implements distinct
HTTP payloads and authentication headers. `createSmsProvider({protocol:'brevo',...})`
uses an E164 recipient and host-selected sender. `createFakeEmailClient`,
`createFakeSmsClient` and `createFakeProviderServer` are test/demo transports with
an independently durable external ledger. Fresh fake delivery/crash evidence does
not certify live provider acceptance. External keys are host supplied; transient
JavaScript secret strings are not memory-erasure evidence.

`identity.getAuthorityInTransaction(tx)` and
`identity.inspectActorInTransaction(tx,actor)` are privileged host composition
helpers. The latter requires an active enrolled User and authenticates stored
credential integrity, returning `{tenant,actor,credentialRevision}` without a secret.

## Delivered email and phone proofs (D2b)

`identity.getAuthorityInTransaction(tx)` is a privileged current-policy provider
for host components such as credential custody. It uses the existing identity graph.
`identity.inspectActorInTransaction(tx, actor)` privately checks enrollment, current
credential integrity and active User membership and returns `{ tenant, actor,
credentialRevision }`. Neither is a public request entry point.

`createIdentityProofs({ engine, identity, keyring, getRestoreFence, custody,
providers, deliveryContext, contacts, channels, clock?, proofPolicy? })` uses the
same prepared catalog, the D2a identity graph and the D3 effects pipeline. Contacts
are operator-selected `{ actor: { email?, phone? } }` for already enrolled users;
this does not enroll or manage principals. Channels bind each delivery kind to its
provider registry key, credential ID and exact revision. Email also declares `from`.
The fixed delivery principal requires current owner scope and explicit Credential
Use. Recipient actors and delivery authority have separate roles.

`begin({ actor, channel: 'email' | 'phone', operationId }, hooks?)` privately admits
one delivery and returns only `{ proofId, expiresAt }`. The service maps admitted
usernames to actors; it does not accept actor IDs or contact replacements from
clients. One transaction records the bounded actor admission, private proof state,
encrypted provider intent and sealed original admission result. Email sends the
canonical email request; phone sends the canonical `sms.send` E164 request. No code
is returned by admission or generated by the client. Fake transports exercise local
HTTP protocols; live delivery requires operator credentials and provider acceptance.

Default codes have eight cryptographically selected decimal digits, expire after
five minutes, allow five distinct failed proof operations and permit three proof
starts per actor per fifteen minutes. Trusted configuration can raise the code length
to ten digits and select finite limits. The actor admission counter survives restart.
Unknown/missing-contact actors create no durable proof or receipt. Failed completion
records one bounded attempt and encrypted original denial per request identity;
retries do not recount. Changed requests against a committed completion also consume
the same bounded allowance through sealed HMAC-identified mismatch receipts; exact
mismatch retries do not recount. The cap blocks further original bearer release.
Locked, undelivered, expired and consumed proofs deny new completion identities.

`applyDelivery(tx, { effect, fact })` is a private worker planner. It marks delivery
only from the effect's current accepted provider fact and returns no source commands.
The worker commits that metadata with applied effect state. Unknown external outcomes
keep proofs undelivered until original-key reconciliation establishes acceptance.
The planner never issues a session. Expired, restored, contact-changed, credential-
changed or revoked proof targets remain unusable even if an old delivery completes.

`complete({ proofId, code, operationId }, hooks?)` checks current enrollment,
credential revision, operator contact, exact expiry, delivery and restore incarnation,
then consumes the proof and issues one D2a session within the same original-result
transaction. It returns `{ token, expiresAt }`; the durable result is AES-sealed.
Different or repeated concurrent operation identities cannot mint duplicate sessions.
A committed original completion can recover after the proof deadline while its
issued session and target binding remain valid and recovery attempts are available. Changed requests reject, and restored receipts cannot
release pre-restore bearers. Key rotation retains keys needed by encrypted delivery
payloads, keyed request/proof verifiers and original receipts. No plaintext password,
proof code, root/provider secret or bearer is stored in application KV or receipts.

Proofs are private versioned identity protocol metadata, not a second application
schema or grant DSL. Public handlers must apply request byte limits and expose only
the two sanitized results; contacts, private delivery state, effect IDs, key custody,
worker callbacks and raw transactions remain host imports. Logical limits do not
establish whole-process memory, live-provider delivery or deployment capacity.

## Local objects and fixed authenticated HTTP (D4)

`objectModel()` supplies an ordinary Object source declaration; merge it with domain
and effect models before preparing the one catalog. Its fields are
`{state:'pending'|'ready',bytes,sha256}`, with exact safe integer byte sizes up to
256MiB, a lowercase SHA256 and no additional fields. Optional typed attachments
require explicit Object Use when added or retargeted. Reference deletion policy
still applies; remove attachments before tombstoning a referenced Object.

`await openObjectStore({engine,identity,keyring,getRestoreFence,directory,model,
clock?,limits?,onPhase?})` opens private absolute `stages/` and `objects/`
directories. It requires durable storage and matching tenant identity. Defaults are
16MiB per object, five-minute grant,30s upload,4 concurrent uploads,8 attempts and
100 reconciliation rows. `onPhase` is a privileged fault/observation callback.
Only server-selected random names enter paths. Source records and opaque lifecycle
metadata commit together; files are streamed, hashed, fsynced and reconciled outside
transactions. File/source checking derives expected ready bytes from source facts.

The dedicated APIs are `begin(token,{operationId,id,owner,bytes,sha256})`,
`upload(token,uploadToken,asyncByteStream)`,
`finalize(token,{operationId,id,uploadToken})`,
`remove(token,{operationId,id})`, `transfer(token,{operationId,id,owner})`,
`status(token,id)` and `download(token,id)`. Begin returns an opaque upload grant,
bound to exact issuing session, incarnation, owner, size, digest and expiry. Upload
persists bounded attempt evidence; finalize persists intent before file promotion,
then current authority, ready source and original result commit atomically. Original
finalize recovery cannot select another grant/version. Downloads require current
Read and return `{bytes,sha256,stream}`. Transfers require both owners; pending
transfers abort old grants. Tombstones commit with source deletion, while cleanup
preserves later versions. Original begin recovery rechecks session and Update.

Privileged `reconcile({maxJobs?,after?})` returns bounded progress and an opaque
internal row cursor. It completes valid finalizations and cleans aborted/tombstoned
versions. Unsupported/corrupt bytes cannot activate. `checkFiles({maxFiles?,
maxBytes?,maxSources?,maxRows?})` checks complete source/private-row/file identity,
hashes ready/staged files and detects concurrent inventory changes; defaults are
10000 files/sources/rows and256MiB total file bytes. `inspectFiles()` remains private.
These scans and per-download hashing are proportional work, not logarithmic queries.

`createService({engine,identity,logins,publicModels,writableModels?,objects?,
operations?,...limits})` exposes fixed HTTP handlers. Host usernames map to existing
actors; all other authenticated routes derive actor/tenant from live Bearer tokens.
Writable models are a public-model subset and exclude Object lifecycle models.
The trusted host supplies operations as functions or `{authenticated,handle}`;
authenticated extensions receive a validated bearer and must own final transactional
authority checks. Anonymous proof begin/complete handlers map usernames themselves.
No client supplies an actor, tenant, authority provider, secret key, transport or
raw engine command. Generic mutation sets trusted tenant/creation time/default owner.

| Route | Request / result |
| --- | --- |
| `GET /health` | Sanitized status only |
| `POST /v1/login` | `{username,password,operationId}` → opaque session |
| `POST /v1/logout`, `/v1/password` | Existing identity inputs; Bearer required |
| `POST /v1/mutate` | `{operationId,changes}`; puts accept only id/model/owner?/data |
| `POST /v1/query` | `{kind,query,model?,cursor?}`; kind point/index/page/incoming/summary |
| `POST /v1/objects/begin`, `/finalize`, `/remove`, `/transfer` | Dedicated object inputs above |
| `PUT /v1/objects/upload` | Bearer, `X-Upload-Grant`, octet-stream bytes |
| `GET /v1/objects/:id`, `/:id/data` | Current-authority metadata or streamed bytes |

Query contracts are the existing authorized contracts; only a page accepts a cursor.
Summary output contains solely declared published aggregates. JSON routes default to
256KiB input,4MiB output,100 page/change items and32 active requests. Upload/download
defaults16MiB; request/body/download deadlines default30s. Strict UTF8/JSON and
field validation reject malformed input, query strings and browser Origin. Errors
contain a sanitized code; hidden IDs, secrets, counters, ranks and raw diagnostics
are not wire fields. `listen({host,port})` defaults loopback/ephemeral and `close()`
stops admission and drains admitted requests. TLS/proxy and browser integration are
operator deployment choices. Logical byte limits do not prove a heap/capacity bound.

## Linux runtime, recovery and release (D5)

The [deployment guide](deployment.md) owns operator recipes. `readDeployment(path)`
and `readKeyringFile(path)` read bounded private regular files; symlinks, public
permissions and unsupported formats reject. `writeDeployment({deploymentFile,
descriptor})` creates the initial fsynced descriptor without replacing an existing
installation. A descriptor binds version, explicit cluster file/prefix, object root,
tenant, catalog identity and external incarnation; restored descriptors also retain
known previous incarnations and original backup/activation identity. Keys stay external.

`openSelfHosted({descriptor?,deploymentFile?,definition?|catalog?,keys?|keyring?|
keyringFile?,activeKey?,logins,contacts,channels,providers,deliveryContext?,...})`
opens the selected Linux FoundationDB installation. Select one catalog and one
custody source. It validates actual adapter location against the descriptor.
Engine, identity, proofs, custody, effects, objects and HTTP share that catalog and
current identity policy. Candidate open starts no listener or worker. Service and
worker tuning fields cannot replace those fixed components. Host clock, password,
proof, engine/object limits and private callbacks remain trusted configuration.

`runtime.bootstrap({operationId,policy,credentials,sources,providerCredentials})`
is privileged initial enrollment. A private HMAC claim binds the entire original
request and catalog. Interrupted claim/identity/seed phases resume under that exact
identity; final seed/provider changes and original receipt commit together. Completed
bootstrap never reseeds or rotates provider revisions on replay. A different request
cannot take over the namespace. Bootstrap and current complete source/file checks
must pass before ingress. Credentials and provider secrets never persist as plaintext.

`runtime.start({host?,port?,workerIntervalMs?,maxJobs?})` owns one listener and a
bounded worker schedule. Defaults are loopback, ephemeral port,250ms interval and
one job per tick; zero interval disables automatic worker admission. HTTP health
uses runtime readiness:200 for ready/ok,503 for unavailable/stopped/uninitialized.
It contains no target, secret or diagnostic fields. `runtime.health()` is also an
importable sanitized status. A changed selected descriptor/incarnation makes the
old runtime unavailable; reopen the newly selected installation explicitly.

`runtime.drainEffects(options)` and the returned identity/proof/object/engine/worker
facades register work synchronously before async admission. Nested work is tracked;
escaped async contexts cannot reuse an expired admission. `runtime.quiesce(callback)`
stops new ingress/workers and waits for admitted HTTP/uploads/provider calls and
application. Its callback admits only maintenance read/check/cleanup work; it then
closes the runtime. `close()` drains before storage close. Nested lifecycle calls
reject instead of waiting on themselves. Raw `runtime.storage`, raw underlying
facades and independently retained privileged helpers can bypass admission; they
must not write during quiescence.

`store.captureCheckpoint({maxRows?,maxBytes?})` is a privileged FoundationDB
snapshot of the exact selected native suffix/value bytes, including data, receipts
and format. Defaults are100000 rows and8MiB. The full native namespace range is
protected. `restoreCheckpoint(snapshot,{...limits,beforeCommit?,afterCommit?})`
requires an initialized empty candidate namespace, validates binary ordering/format,
and imports once atomically. Definite failures and lost acknowledgement remain
distinct. Native key/value/transaction/lifetime limits additionally apply. This
versioned Node/V8 artifact is not a cross-runtime migration format.

`writeBackup({runtime,objectRoot,directory,keyring,incarnation,sourceIdentity,
backupId?,quiesce,limits?,checkpointLimits?,after?})` requires a trusted complete
pause. It checks the engine/files, captures the native checkpoint, hashes/copies
ready/staged bytes, rechecks the inventory and publishes a signed manifest plus
`snapshot.bin` and `files/` by directory rename. `sourceIdentity` is the checked
release's `{revision,sourceHash,lockHash,catalogIdentity}`. External keys/descriptor
are not backup contents. Default bounds are10000 files,256MiB file bytes,1MiB
manifest and16MiB serialized snapshot. Actual native backup size remains bounded
by the adapter's smaller checkpoint/transaction caps.

`readBackup({directory,keyring,limits?})` authenticates before V8 decoding and rejects
missing/unexpected artifacts and inconsistent inventories. `prepareRestoration({
backupDirectory,keyring,target:{store,prefix,clusterFile,objectRoot},catalog,tenant,
openCandidate,previousIncarnations?,...limits})` verifies actual target identity,
imports into empty state and opens an offline candidate with a fresh incarnation.
It checks complete source/derived/receipt/file state and returns a verified
process-local handle. The opener must retain that exact raw storage handle and
supply the quiescence boundary. Partial candidates never replace active state.

`activateRestoration({handle,deploymentFile,operationId,stopPrevious,after?})`
stops the previous runtime and quiesces the candidate for the entire final check
and descriptor publication. Exact native bytes and file inventory/hash/modes must
still match the authenticated backup. Candidate facade writes are denied during
publication. Activation serializes admission per deployment file within the selected
process. The descriptor commits by fsynced rename; lost acknowledgement recovers
the original descriptor and different backups cannot share that operation identity.
Activation closes the candidate; reopen/start the selected descriptor. Separate
operator processes must coordinate; distributed activation is not accepted.

Custody configuration `getRestoreFence` and `getPreviousRestoreFence` binds effects
to external incarnation history. Accepted request/grant identity remains immutable;
claims, starts, completion tokens and publication checks bind current incarnation.
Restored pending intent first requires original-key provider status. Fake providers
implement `reserveRestore` with a fixed known predecessor list, atomic incarnation
takeover and strict external epoch advance. Old-incarnation lookup/send rejects even
when its numeric epoch is higher. Reservation facts and dispatch markers persist
before a newly admitted send. Stale restore CAS cannot revive after a later restore.
Unsupported/no-lookup restored work is isolated, including live Resend without
qualified restore fencing. Live idempotency retention is not inferred from headers.

`prepareRelease({repository,directory,sourceIdentity:revision,catalogIdentity,
maxFiles?,maxBytes?})` produces a deterministic checked source directory and
`RELEASE.json`, excluding installed dependencies, keys and application state.
`checkRelease({directory,...limits})` verifies its source/lock/catalog identity and
complete selected inventory. Defaults are10000 files/128MiB. Ordinary `npm ci` and
`npm pack` supply install/package mechanics. `RELEASE-LOCK.json` is generated
byte-for-byte from the authoritative `package-lock.json`, since npm can omit its
reserved lock filenames from archives. The derived file is required by the checker;
when the original lock exists both must match. Extract an archive, check it, copy
the derived lock to `package-lock.json`, then run `npm ci` and check again. Installing
into another application's dependency tree does not enforce this release's lock.
Source inventory determinism does not
certify arbitrary native build identity or remote deployment.

Workload functions also retain `nativeAdapter` counters per outcome and aggregate
native read/attempted/committed write bytes separately from logical staged/change
bytes and source witness scans. These native counters include namespace/V8 encoding,
but exclude network framing, disk amplification and replication. RSS/CPU and latency
are measured local observations, not a general capacity or heap guarantee.
