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
boundary over a serialized store. Configurable lifecycle limits are `maxSources`,
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
