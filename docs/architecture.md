# Engine architecture and retained design obligations

Current implementation and next work are recorded in [CONTEXT](../CONTEXT.md) and
[checkpoints](checkpoints.md). Sections below distinguish implemented core mechanisms
from retained reader, lifecycle and production obligations. Rust references describe
the predecessor only; this repository runs Node directly.

## 1. Index declarations and bindings

**Shipped baseline (JS-P4b.2).** The prepared catalog compiles JSON Schema
2020-12 with Ajv, names each view, and gives its contract a typed partition tuple
and one ordered value. Models contribute through stable membership slots.
Access orders select dedicated model/slot views. The executable contracts and
examples are in [the API guide](api.md).

**Structural declarations, ordering, authored paths and owner publication accepted (R1–R4).** The declaring model owns each index's role
domains, contribution types, summaries and validators. Its public identity is
`[declaringModel, partitionFields[], orderFields[]]`. For example, the following
three signatures address distinct indexes on the Invoice model.

```js
['Invoice', ['id'],          ['effectiveAt']]
['Invoice', ['id', 'owner'], ['effectiveAt']]
['Invoice', ['id'],          ['owner', 'effectiveAt']]
```

The prepared catalog preserves component order and both array boundaries. A
contributor maps its own fields, trusted headers or explicitly declared reference
paths into the roles. For the second signature a Tax can supply the invoice ID
through its Issue, its own owner header, and its effective date. Other models can
use different field names for the same roles. Preparation checks arity, domains,
and binding provenance, and rejects conflicting definitions of one signature.
The signature identifies the declaration across callers. The catalog fingerprint
also covers behavior such as aggregate and validator versions so changed semantics
can require a new generation. Trusted function implementations run locally.

The declaring model is a schema. An optional semantic anchor is a record such as
Invoice I. Each populated partition has its own AVL head, which can rotate. An
anchor contributes an opening amount only through an explicit binding with a
defined scope and stable slot. Creating another owner partition cannot duplicate
an invoice-wide opening contribution. A partition key identifies a complete tuple.
Queries spanning several partitions must discover the applicable tuples and merge
streams whose order is compatible with the requested order.

## 2. Occurrences, tree structure and arithmetic

R1–R2 retain generation-scoped membership and tree identities while changing the
index and order components. The following keys describe the target semantics.

```text
tree       = (tenantNamespace, catalogGeneration, indexSignature, partitionTuple)
membership = (tenantNamespace, catalogGeneration, sourceId, indexSignature, stableBindingOccurrence)
orderKey   = (projectedOrderTuple, sourceId, stableBindingOccurrence)
```

Within a partition, compare order tuple components lexicographically by their
declared domains, then source ID and stable slot. A cut on the full tuple bounds
the entire tie group. Adding source ID and slot locates one occurrence in that
group. Insert, delete, rank, folds, windows, interval routing, potential-output
discovery and merged reads must agree on these comparisons and open/closed bounds.
Preserve exact integer arithmetic and deterministic UTF-8 string order. Check the
maintained order and subscriber matches against an independent source-derived
reference. A store with an incompatible key format requires explicit rejection or
rebuild. Candidate generation keys must be disjoint from the active generation.

Each occurrence owns left/right links, height or balance metadata, structural size
and summaries. Root headers locate roots; parent/previous/next links are optional
optimizations requiring evidence, not prerequisites. Keep source bodies separate
from engine-owned membership entries so unrelated index-node changes do not all
rewrite the source body. All entries still share one atomic transaction. Logical
segment objects need not be serialized as one giant source record.

`size(v)=size(L)+1+size(R)`; rank is derived, not stored globally. The engine reconciles
old/final membership multisets: remove old positions, insert new, repair changed
contributions, skip unchanged projections. Retargets affect old and new partitions;
deleting the final occurrence changes that partition to the monoid identity.
Repeated references/participation require stable occurrence IDs and defined duplicate
semantics. Structural occurrence count and distinct-source count are different.

For each ordered monoid: `A(v)=A(L) combine lift(v) combine A(R)` with identity.
Rotations preserve inorder; associativity permits regrouping, not operand permutation.
No subtraction/inverse is required. Update descendants before recomputing ancestors.
Expose `finalize(summary)` for average/SD/etc; do not average averages. Prototype
exact quantities use BigInt in memory with explicit units/scale and canonical decimal
strings in JSON. No JSON BigInt or unsafe Number round-trip. Arbitrary rationals and
floating statistics need their own declared arithmetic contract before admission.
Floating regrouping is approximate unless proved otherwise. Monoid laws are developer
obligations backed by algebraic witnesses; a schema validator cannot prove them.

Counts/sums/min/max/prefix summaries are initial modules. Variable-size monoids may
be supported but must account for combine/storage/output bytes. String length can
use sum-of-lengths; materialized concatenation has no constant-size guarantee simply
because inputs are often empty. No claim every associative operation is cheap.

## 3. Validation, references and publication

Separate record shape, record/parent predicates, tree structure, cached-summary
correctness and business invariants. Business validators name whole partition,
all-prefix, or explicit range scopes. Do not apply a root-balance rule blindly to
each rotation-dependent subtree. All-prefix nonnegativity uses a proved prefix
monoid; arbitrary quantified rules need suitable augmentation or explicit extra work.
Linear predicates over summaries check conditions; they do not solve constraints or
generate valid records automatically. A validator's inspected fields/ranges are
dependencies just like a calculation's inputs.
Validator lifetime follows its declared scope/anchor: a live empty scope validates
the identity summary. Removing the final member alone cannot erase that obligation;
retiring the anchor and all its dependents may retire the scope explicitly.

Relations declare source field/array slots, target model/domain, optionality and
`onDelete: reject` initially. The engine derives protected reverse-reference entries;
models/tools do not maintain a second relationship inventory. 1:1 requires tuple
uniqueness; n:m is a junction with two 1:n references and optional pair uniqueness.
Final-state uniqueness releases old claims before acquiring final claims, permitting
valid swaps. Protect missing keys as well as existing keys. Stage all authored rows
before checking forward references; reject unresolved final references. Reference
cycles and forbidden causal cycles are distinct. Generic cascade/nullify is deferred.
Cross-parent equality or chronological predicates are explicit rules, not inferred
from matching reference types. R3 compiles finite, scalar reference paths from these declarations. A change to
an intermediate relation must find contributors through the original and final
edges and reread affected fields in the transaction. Moving an Issue to a new
Invoice reprojects its Taxes even when no Tax body changed. Both invoices' trees,
owner partitions, and downstream outputs settle before final validation. Hidden
dependents and previously empty targets participate in the same rollback boundary.

Conceptual mutation pipeline (ordinary functions, one transaction owner):

```text
authenticate/authorize command scope
  -> validate/stage authored final inputs
  -> resolve final reference/unique/membership changes
  -> settle dependent computations and generated occurrences
  -> validate final scopes and final authority
  -> publish source + indexes + routes + revisions + receipt + effect intent once
```

Intermediate structural assertions may fail early; business validation must allow
jointly repairing commands whose settled final state is valid. Unresolved computed
values are pending/errors, never zero. Reconcile generated output creation, update,
absence and reactivation with stable producer/role identities and first-creation
metadata. Direct client writes cannot modify these internal rows. External provider
calls occur after a committed intent, never in a replayable calculation/validator.

## 4. Reactive semantics

Two dependency types share one explicit registration interface:

- Direct: `(sourceIdentity, field)` including declared relationship/authority inputs.
- Ordered: `(viewId, partition, summaryField, keyBounds)` with explicit open/closed
  endpoints and optional infinities. Numeric/string/time orders use the same engine.

Resolved declared selectors supply pure-function inputs. Dynamic bounds/calculations
may use an explicit transaction-bound tracked read facade which records successful,
absent and empty reads before returning values or pending results. Raw store access,
ambient clock/random/I/O and undeclared mutable closure inputs are unavailable to
replayable model calculations. Ordinary JavaScript is not automatically dependency-
tracked; there is no source-code analysis that infers hidden reads.

JS-P3b uses two ordinary callbacks, preserving the existing native
`ReactiveBinding::resolve/evaluate` separation ([causal.rs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/causal.rs), [dependency.rs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/dependency.rs)):
**resolve final authored topology, then evaluate settled values**. Resolution can
follow tracked authored fields and compute dynamic bounds/partitions. It declares
all potential emission roles and positions before amounts or presence are evaluated.
Calculated publications may be explicit evaluation inputs; they cannot control this
first implementation's topology. Reject unsupported computed-dependent topology;
do not discover edges by evaluating arbitrary callbacks against stale computed rows.
Trusted callbacks are not sandboxed: ordinary JS can access ambient globals, so
purity is an author obligation, not an isolation claim.

Keep potential emissions separate from actual memberships, including roles currently
emitting nothing. Resolve all affected authored shapes before numerical settlement;
route old/new positions through original and final registrations, close causal
descendants, and test the final graph for cycles, including paths through unchanged
producers. Explicit adjacency traversal is acceptable with charged edge work; graph
compression is an optimization, not a prerequisite. Evaluate predecessors first;
unresolved contributions cannot be read as the identity. A moved query may remove
an old edge in the same transaction that creates its reverse. Two zero-output
producers can still form an invalid potential cycle. Both are required witnesses.

Incremental integration must finish resolving and installing final potential positions
before evaluating values. In the transaction overlay, withdraw old actual outputs
belonging to the pending closure before reevaluation: an output moving out of a query
must not leave its stale contribution visible until its producer happens to run.
Topological readiness distinguishes a settled absent output from an unresolved one.
Maintain an indexed potential-output inventory for finding predecessors of new/moved
queries; actual memberships alone cannot provide it. No tenant-wide source scan belongs
in the ordinary writer. The independent full-snapshot oracle intentionally scans instead.
Both resolver and evaluation field reads protect existence as well as the requested
field. An asynchronous KV read facade must retain/await outstanding reads and latch
failures before accepting a resolver result; discarding a returned promise cannot
discard its failure. The synchronous reference facade is not that adapter proof.

Staging may temporarily visit a partition that never existed initially or finally.
Final business-scope obligations follow original-to-final membership transitions,
including an emptied original root, rather than transient staging paths. Final
references and unique acquisitions wait for all authored and managed rows. Stable
producer/role reservations outlive output absence. JS-P3b.1 reserves model and canonical
creation time at the first **potential declaration**, including one that emits nothing;
reactivation must redeclare those same values, otherwise it rejects. Source commands
cannot take over managed identities. These integrated managed/causal guarantees are implemented through JS-P3b.2.

A range fold computes a value. Reverse interval stabbing discovers subscribers of
a changed point. These are separate algorithms. Route contribution changes, old/new
positions, old/new partitions, changed bounds and partition-empty transitions; replace
old registrations atomically. Empty ranges and producers currently emitting nothing
remain registered. Final-graph causality governs diamonds and moved queries, not
command order or the historical graph. Use an explicit pending worklist/evaluation
stack with pending-item coalescing and input-version invalidation; a permanent visited
set would miss changes after an earlier evaluation. Recursive helpers are fine;
unbounded JavaScript call-stack depth and hidden repeated scans are not contracts.

Reject causal feedback cycles initially; do not silently iterate to an unproved fixed
point. A calculation sees settled predecessors or remains pending. Read evidence and
registrations must reconcile even when the visible result is unchanged. Final-state
equality/no-op elimination cannot discard conflict guards.

An upper-endpoint AVL with lower-bound pruning does not automatically prove
`O(log n + matches)` reporting. Select a proved reporter or charge actual visited
registrations; worst-case linear discovery is possible. Keep this distinct from
`O(log n)` range folds. One-sided, fixed-orientation subscriptions can reduce to
threshold ranges. Rank reads are supported, but reactive rank-window subscriptions
are deferred: insertion before a rank window can change it without stabbing it.
Time passing without a stored change requires explicit clock/wakeup input; the
ordering index alone is not a scheduler or rate-limit admission protocol.

## 5. Read, authority and work contracts

Assuming bounded comparison and fixed-size summary arithmetic, logical tree work for
point/rank/select and range folds is `O(log(n+1))`. Enumerate a
contiguous window in `O(log(n+1)+k)` node work; count/bound-pruned BFS batches independent
frontiers and hydration on one snapshot. Finite concurrency, buffering and bytes
remain costs. A batch is not necessarily one wire RTT. Do not call select separately
for every rank. Residual filters can inspect many candidates for little output;
raw occurrence ranks are not visible, filtered or distinct-record ranks.

Owner-only baseline: same tenant and action; Read/Create/Update scope is self, strict
descendants and direct Group parents; Delete is self/strict descendants. Direct Tenant
parent is not ordinary data scope; tenant-grant inheritance is separate. Admin satisfies ordinary action grants except Use, and does not widen ordinary data scope. Create checks requested owner; update checks both
old and new owners before CAS diagnostics and against final authority. Keep engine
repairs privileged while authorizing client commands and retargeted references.
Owner scope comes from an acyclic, same-tenant principal DAG.
Prototype uses explicit fixture authority; this is not a production authentication API.

JS-P4b.2 supplies separate request functions that obtain fixture authority inside
their read/write transaction. The authorized writer checks original and requested
owners, settles through the existing transaction engine, reloads final authority,
then checks final owners and newly used reference targets before publication. Policy
effects in that transaction therefore cannot bypass final checks. Raw writer and
repair primitives remain privileged. No new CAS, session or credential protocol is
implied. Authorized results omit raw derived scopes and guard inventories.

The shipped reader opens one partition per accessible owner and merges streams in
order with bounded initialization and candidate work. In the baseline catalog the
dedicated view must bind its owner partition role to the trusted source owner;
a similarly named arbitrary body field is insufficient. Preparation checks that
binding and the selected resource/action plan, with final per-record disclosure.
K-way merge is a general multi-stream algorithm, not an authorization primitive alone.
Full-parent ordered integrity can require a separate owner-independent membership;
noncommutative summaries from owner streams cannot simply be concatenated in owner
order. Shared business roots may contend legitimately. Tenant namespace alone does
not require a mutable tenant-wide aggregate/counter.

The implemented authorized reader retains one inorder iterator and hydrated head
per owner stream and merges using a heap. Source/schema/key/partition/slot/order and
contribution agreement is checked before disclosure. Declared reverse-reference
discovery composes that same source validation and disclosure with existing incoming
entries; it adds no business edge inventory. Records are the request result; raw
totals, summaries and work counters are excluded. A separate trusted diagnostic
callback records budgeted work, including hidden candidates and the memory adapter's
physical scan enumeration. This is not a timing confidentiality guarantee. Cached
paths, authority discovery, schema/projection work and post-fetch byte/scan accounting
remain distinct costs; see the exact limits in [the API](api.md#js-p4b2-authorized-request-composition).

R4 binds the existing owner decision to ordinary structural indexes using the
contributor's trusted source owner. A record reached directly, through an indexed
list, or through reverse references must satisfy the same tenant, action and
owner checks. Knowing the traversal path neither widens access nor requires a
separate Read grant on a record used only as a lookup key. The accepted reverse reader validates the lookup target without an extra Read grant.
Authority changes alter the eligible streams without rewriting business rows.
Transfers check both owners and settle affected memberships atomically.

R4 also lets a readable anchor publish a declared FULL summary. An invoice can
publish a balance of 20 from a visible +100 and a hidden −80 while its member
page discloses only the permitted source records. A summary grants no contributor
identities, bodies or undeclared counts. Publishing an aggregate from an unanchored
partition requires an explicit disclosure policy.

R5 cursors bind the structural query, catalog, authority and relevant data state,
including payload edits and owner transfers. Changed dependencies reject the cursor
or trigger an explicit restart. Scope-local fencing allows unrelated writes to proceed
without updating a global revision counter.

## 6. Storage, index lifecycle and limits

Adapter obligations: coherent point/batch reads, protected absence/ranges used by
invariants, staged put/delete, one atomic commit, rollback and an explicit commit
outcome. A generic KV `get/put` interface without these semantics is insufficient.
Async Node functions await native adapter operations; no Surreal dependency or Rust
host required for the prototype. Snapshot isolation alone does not prevent write
skew. Later adapters must prove read/write invariant protection or explicit guards.

Memory baseline may serialize writers and use an attempt overlay over a stable base;
avoid deep-cloning the complete store per mutation while claiming logarithmic work.
Serialization is a labeled test-adapter property, not a production global mutation
counter or distributed acceptance. After known conflicts, reject cleanly initially;
unknown commit outcomes require original receipt/status recovery, never blind replay
with a new operation identity. Prototype atomic rollback is not crash durability.

D1 accepts optional embedded SQLite. A writable adapter
reserves the writer before every callback, protects absence/ranges for its entire
snapshot, and publishes touched KV rows plus the original receipt in one transaction.
Contention rejects before callback execution without blocking the event loop. The
adapter has process-kill/reopen evidence with WAL/FULL; neither synchronous settings
nor these witnesses certify power-loss or multi-host storage. Readonly inspection
handles provide snapshots without claiming writer exclusion. Codec identity rejects
unknown persisted formats; source/catalog evolution requires separate acceptance.

D1F selects FoundationDB as the durable primary. One writable Node service means
one service owns write ingress; it does not mean one globally serialized transaction.
The adapter protects normal point/absence reads and explicit complete range predicates
with native conflict ranges, then publishes data and original receipts atomically.
Disjoint engine source mutations can overlap and both commit. Conflicting work aborts
definitively without replaying application callbacks. Startup metadata initialization
precedes ingress, avoiding an accidental shared write on the disjoint path.
Native transactions retain their finite lifetime/key/value/byte limits. One local
FoundationDB process is the conformance target, not replicated-failure acceptance.

D2a loads the existing prepared owner graph from the transaction. Password envelopes
use independent salts and scrypt outside the writer transaction, with bounded work
and durable admitted failure counters. The final transaction rechecks credential
revision, actor activity, authority and the external incarnation. Sessions persist
only keyed bearer digests; original issuance receipts hold authenticated ciphertext.
Password changes revoke old sessions by credential revision. Restore retains stable
credential encryption bindings and changes the external incarnation, allowing fresh
login while rejecting old sessions/results. Authorized cursors include the private
session binding in their authority fence. Enrollment and graph replacement are
operator functions, not delegated principal CRUD.

Accepted R5 uses a fixed-tenant engine facade and the same prepared catalog. Sources
and host policy stay shared; only derived namespaces rotate. A serializable rebuild
checks source-derived scratch state, preserves source records exactly, stages and
checks the candidate namespace, then atomically activates it. Optimistic adapters
protect active-generation metadata and the complete source inventory; conflicting
publication aborts. Writers are paused for the operational rebuild procedure.
Old reads drain and
retirement follows. Cleanup errors report pending retirement after successful
activation. Process-local opaque cursors fence query, complete position, generation,
relevant source bodies and fixture scope, with explicitly metered source inventories.
Definition migration and changed source outputs are not accepted by this rebuild.
The lifecycle obligations below continue to constrain future migration/durable work.

Lifecycle: `declared -> building -> checked -> active -> retired`. Initially pause
all writers, including workers, while building a candidate generation; readers retain
the old catalog/snapshot. Build/fold/check from source, activate once, retain old data
until admitted in-flight reads finish. Activation invalidates old-generation cursors;
idle serialized cursors do not pin storage forever. Failed validation leaves the old active
generation intact. Changing order/mapping/monoid can require rebuilding; changing only
a whole-partition validator may reuse verified summaries for revalidation. Removal
rejects live references to the view or replaces dependencies in the same migration.
Rebind/rebuild affected routes, computations, claims and validator evidence as part of
the candidate generation and settle them before activation. If a changed definition
changes generated source records, stage those changes too; switching only root pointers
is insufficient. Initial support may reject a migration class it cannot settle safely,
with explicit recreation as the alternative, instead of leaving stale derived state.
Physical cleanup is proportional to entries. Persistent chunking, crash recovery and
online catch-up are separate gates, not hidden in the memory prototype.

Unsorted bulk input generally requires `O(n log n)` sorting; sorted bulk tree/summary
construction is `O(n)` plus projection/validation. New index construction has an
input-size lower bound. Full independent inventories are explicitly batch/stream
operations; normal writes/open must not acquire hidden population scans or arbitrary
4096-record ceilings. Work/byte/time budgets remain explicit and configurable;
exhaustion returns rejection or incomplete evidence, never a false integrity pass.

## 7. Composition, dependencies and unresolved gates

Public surface should represent real capabilities: prepare catalog, transact/read,
source-derived checking and the R5 lifecycle, population and workload tools.
Use the same prepared structural catalog for those tools and future RPC handlers. Internal balancing/secret/worker helpers are not remotely callable.
No wrapper around each filesystem operation, loop or process command; no bespoke CLI.
Keep population candidates caller-owned, seeded and proposal-local; commit/reject
updates available reference space/uniqueness/fanout only after authoritative outcome.
Optional solvers supply candidates, not a validity or performance guarantee.

Choose one validator: JSON Schema 2020-12 + Ajv 8.20.0 is selected. JS-P1 checked supported dialect, exact-value schemas, license/security
and compatible maintained versions; compile/cache once. Avoid parallel Zod/Valibot
authority or a portability wrapper before a real requirement. Use Node built-ins for
test runner, async orchestration and resource measurement. Authoring is trusted ESM
plus standard schemas; arbitrary closures are not a portable wire configuration.

Persistent adapter/client selection (FoundationDB/redb/SurrealKV or another engine)
is **unresolved** for Node until maintenance, licensing, native bindings and transaction
conformance are checked. Existing Rust adapters are references, not ready Node APIs.
TiKV, B/B+ page rewrites, canonical exact-median trees and native-rank-skip assumptions
are not selected. No promise JIT matches Rust CPU/memory behavior. Model/system
metadata should remain opaque typed data so measured hotspots can later move behind
the same semantic boundary; speculative FFI frameworks are unnecessary now.

After prototype gates: persistent crash/concurrency/receipt acceptance; full
email/phone identity and session fencing; credentials/key custody and durable provider
adapters; object lifecycle/full fake pipeline; retention/recovery/capacity/deployment.
Maintain distinct provider implementations (e.g. Resend/Brevo), without requiring a
versioned operation naming convention. Optional live feeds, recurrence, frontend,
domain accounting/CRM/HRM models and TiKV remain separate. See the capability map for
retained guarantees and missing behaviors; omission from the prototype is not deletion.

| Decision still requiring evidence | Gate / disposition |
| --- | --- |
| Exact JSON Schema subset, codecs and supported Node/Ajv versions | JS-P1 compatibility/dependency check; explicit integer/string order baseline above |
| Reverse-interval reporter and its actual adversarial bound | JS-P3 algorithm witness; never substitute rank-window enumeration or claim a proof from AVL balance |
| Reuse of current population/workload code versus replacement | R5 demonstrate equivalent constraints with the same prepared catalog; compare maintenance surface |
| Native Node persistent client and concurrency mapping | After R5; choose only after client support and real adapter conformance review |
| Online migration, custom floating/variable-size algebras, reactive rank windows | Outside initial acceptance; require explicit semantics and cost evidence before adding |
