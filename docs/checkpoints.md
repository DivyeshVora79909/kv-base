# Checkpoints and acceptance gates

**D2b is the first unmet gate. R1–R5, optional SQLite D1, D1F, D2a and D3 are accepted.**
The durable primary is FoundationDB. One Node service accepts writes and may
execute disjoint transactions concurrently through the ordered transactional KV
adapter. SQLite remains an optional serialized adapter. This roadmap starts from `8f337cd`, which includes
fixture-authorized reads and mutations. R1–R5 supersede the pending named-view,
single-order-field and dedicated-access-view plan. Accept a gate only after its
witnesses run against the implementation. The [API guide](api.md) records executable
contracts and the [handoff](../CONTEXT.md) records the last accepted revision.

## Baseline

| Historical gate | Verified capability to carry forward |
| --- | --- |
| JS-P1–P2 | One Ajv-prepared catalog, serialized memory transactions, guarded absent/empty reads, lawful ordered summaries, AVL sizes/ranks, stable source/slot identities, atomic memberships, typed references and final uniqueness checks. |
| JS-P3 | Old/new interval routing, zero-output producer discovery, cycle rejection and incremental settlement compared with a separate full-snapshot computation. |
| JS-P4a | Budgeted batched rank/key windows and complete occurrence cuts within one snapshot. |
| JS-P4b.1 | Private-by-default fixture policy, owner-DAG scope and action grants. Migration verification at `f67e6d5` recorded 171 passing tests. |
| JS-P4b.2 | Source hydration, same-snapshot owner-stream merge, incoming reads and atomic authorized mutations. Verification at `8f337cd` recorded 206 passing tests. |

The earlier counts describe their original test runs. The baseline uses public view
names, scalar order domains and dedicated per-model access orders. See the
[migration evidence](evidence/2026-11-14-migration.json),
[JS-P4b.2 evidence](evidence/2026-11-14-js-p4b2.json) and
[checkpoint history at 8f337cd](https://github.com/DivyeshVora79909/kv-base/blob/8f337cdfe0ef0c33c0aac175b84eb22e4932fdf9/docs/checkpoints.md).

## R1 — Structural declarations

**Accepted 2026-11-18.** Fresh R1 verification: 213 tests passed on Node 26.12.0, including seven independent structural witnesses. See [R1 evidence](evidence/2026-11-18-r1.json). Composite declaration execution continues at R2.

`prepareCatalog` already compiles model schemas and checks
membership arity, domains, input bindings and unique slots. Replace public view
names with a declaration owned by the model. Its signature is
`[declaringModel, partitionFields[], orderFields[]]`. Preserve element order and
the boundary between arrays in lookup, equality and catalog identity. The model
owns role types, ordered summaries and validators. Contributors bind their local
fields or headers to those roles. R3 extends these bindings to explicit reference
paths. Keep one prepared catalog and the existing Ajv validator.

**Exit.** Two different contributing models bind different field names to the
same signature. Moving a role from the partition array to the order array selects
a different declaration. Reject incompatible types, arity and conflicting
definitions at preparation. The declaring model identifies a schema. An optional
anchor identifies a record. Each populated partition has its own rotating AVL head.
An unanchored declaration can partition by ordinary typed fields such as country
and year. Verify lookup without a record ID in that tuple. Only a declared opening
contribution enters the fold. Check two owner partitions
and repeated participation by distinct stable slots against independently enumerated
occurrences, including the number and scope of any opening contributions.

## R2 — Composite ordering throughout the engine

**Accepted 2026-11-18.** Fresh R2 verification: 221 tests passed on Node 26.12.0, including eight independent composite-order witnesses. See [R2 evidence](evidence/2026-11-18-r2.json).

Existing AVL comparisons, bounds, range routing and owner-stream
merge use a scalar order followed by source ID and slot. Carry the typed
lexicographic order tuple followed by those same tie-breakers through projection,
CRUD, rank/select, folds, batched reads, merge heads, subscription intervals and
potential outputs. Define inclusive/exclusive bounds both for a whole equal-tuple
group and for one complete occurrence. Version the representation and reject opening
incompatible state unless an explicit validated rebuild is available.

**Exit.** Hold the first component constant and edit only the second. Prove the
row moved, both old and new intervals reached the right subscribers, and no-op
projections caused no occurrence writes. Compare ties, ranks, folded values,
bounded pages, potential matches and settled outputs with independent sorting,
direct interval matching and full recomputation from source facts. Preserve exact
domains and noncommutative fold order.

## R3 — Reference-path participation

**Accepted 2026-11-19.** Fresh R3 verification: 231 tests passed on Node 26.12.0, including ten independent reference-path witnesses. See [R3 evidence](evidence/2026-11-19-r3.json). Finite paths target authored scalar topology; managed contributors are supported.

Current membership projection reads the contributing record's fields
and headers. Compile finite, explicitly declared scalar reference paths, including
each hop's allowed target type and policy for missing values. Use existing typed
forward/reverse references and the old and staged final graph to find affected
contributors when an intermediate edge or terminal field changes. Reconcile their
old and final projections, summaries, routes and derived outputs within the same
transaction.

**Exit.** Move an issue from invoice A to invoice B while leaving its tax records
untouched. Check removal and insertion in both business indexes and the corresponding
owner partitions, including empty results and downstream calculations. Compare with
a full source traversal and recomputation. A failing final validator must restore
sources, reverse edges, occurrences, aggregates and outputs together.

## R4 — Owner-constrained queries and mutations

**Accepted 2026-11-19.** Fresh R4 verification: 245 tests passed on Node 26.12.0, including fourteen independent structural-authority witnesses and the joint R3/R4 snapshot. See [R4 evidence](evidence/2026-11-19-r4.json).

Keep the existing tenant, action and owner-DAG checks,
hydration, same-snapshot merge and both-owner/final-authority mutation checks. Bind
accessible owners to a structural partition role backed by the **contributing
record's trusted owner header**. A related record's owner has a different meaning.
Plan ordinary compatible partitions and merge cached stream heads by the R2 order.
Direct, indexed and reverse-reference results use the same record policy. The
traversal path grants no permission and requires no extra read grant on an
intermediate record. Keep internal integrity checks over complete business state.
Selected full-anchor summaries use explicit publication declarations.

**Exit.** One invoice has a visible +100 entry and a hidden −80 entry. A reader
authorized for the declared full balance sees 20 and only their permitted rows.
Keep hidden IDs, bodies, ranks and undeclared counts private. Transfer a row
between owners with checks on both. Revoke group access and verify visibility
changes without rewriting source rows. Compare results to an independently
filtered, ordered source inventory. Exercise tenant boundaries, repeated
memberships, equal keys and discovery, candidate, refill, node and byte budgets.

R3 and R4 can proceed independently once R1 and R2 establish the representation.
Their joint witness moves an ancestor and transfers a contributor between owners
in the presence of tied order keys. Check the committed inventory, public rows
and summary in one snapshot.

## R5 — Continuations, generation lifecycle and developer tools

**Accepted 2026-11-19.** Fresh R5 verification: 272 tests passed on Node 26.12.0, including 27 independent lifecycle/workload witnesses. Seed20261119 exercised 64 contributors/three skewed partitions through 80 steps: 65 accepted, 15 intentional rejections/rollbacks. Source checking and generation rebuild both passed. See [R5 evidence](evidence/2026-11-19-r5.json). Cursors are process-local; generations use the same prepared catalog and preserve shared sources.

Raw windows and fixture reads already share a transaction snapshot.
Cross-request continuation requires fencing on the exact structural query, total
position, catalog/generation, authority and relevant data state. Test payload-only
edits, owner transfers and revocation. An unrelated edit must allow progress without
a global mutation counter.

**Exit.** Exercise resume/rejection across requests. With writers paused, build a
candidate generation from source facts, independently check its memberships, routes
and derived state, then activate atomically. A failed rebuild leaves the active
generation usable. Fence old cursors and retire the old generation after in-flight
reads finish. Expose inspection, adaptive population and workload functions through
ordinary Node imports, using the same prepared catalog and actual transaction
outcomes. Run reproducible seeded workloads with skew, ties, rejection and rollback.
Report runtime, dataset, budgets, visits, reads/writes, bytes and latency. Account for
memory scans and stream setup separately from AVL path length.

## Verification and subsequent gates

## D1 — Durable adapter and original receipts

**Accepted 2026-11-19.** 287 tests passed on Node 26.12.0, including 15 new
adapter/engine witnesses. Real child-process writer conflicts and SIGKILL before
and after SQL commit protect source/receipt atomicity. Reopened structural paths,
reactive recomputation, generation activation rollback and active-generation replay
checking passed. See [D1 evidence](evidence/2026-11-19-d1.json).
This accepts the optional SQLite adapter; it does not select SQLite as the durable
primary or satisfy the FoundationDB concurrent-service gate below.

Use the same engine over an embedded persistent adapter with explicit serialized
transactions, protected present/absent/range reads, exact detached values and
atomic publication. Select Node's built-in SQLite binding only after fresh adapter
conformance; WAL and FULL synchronization are configuration, not crash evidence.
Busy/conflicting writers reject definitively before callback execution. Preserve
the original operation ID, request digest and result in the same commit as data;
unknown acknowledgement recovers that receipt without inventing a new operation.

**Exit.** Reopen and independently check structural/path/reactive inventories and
generation state. Independent adapter witnesses cover rollback, no-op, Unicode/NUL
keys, exact values, absent/empty guards, multiple handles/processes, writer conflict,
process death before publication and after committed-but-unacknowledged receipt.
Mismatch/replay protection returns the original result once. No distributed or
optimistic-concurrency claim follows from serialized SQLite locking.

## D1F — FoundationDB and one concurrent writable service

**Accepted 2026-11-19.** 343 tests passed, zero failed/skipped, on Node26.12.0
after `npm ci`. FoundationDB binding2.0.1/API720 with native client/server7.3.81
passed30 adapter/engine tests and13 single-service tests. Disjoint source mutations
overlap and both commit; all ten shared conflict schedules reject stale publication;
lost committed HTTP response recovers its original receipt once across service
SIGKILL/reopen. See [D1F evidence](evidence/2026-11-19-d1f.json).

Reused cases: exact detached values/batches, Unicode/NUL ordering, paged overlays,
callback/hook rollback, no-op guards and original receipts; predecessor schedules
for present/absent batches, absence claims, empty-range insertions, range deletions,
limited pages, no-op dependencies, references and revocation. Added fresh Node
single-service concurrency/response-loss witnesses, zero-limit protection, native
physical limits, binary namespace/format isolation and close/callback lifetime.

FoundationDB is the first durable deployment target. Keep the same engine behind
the ordered transactional KV boundary. One Node service accepts writes, with
multiple simultaneous transactions rather than a service-wide writer queue.
Normal reads protect exact points/absence and the entire requested range, including
keys beyond a limited page. Conflict aborts and unknown outcomes are distinct;
callbacks do not silently retry. Original receipts publish with all state once.

**Exit.** Reuse the shared Node byte/value/overlay/rollback/receipt cases and semantic
schedules from the predecessor conformance harness. On a fresh FoundationDB target,
prove disjoint points both commit; present/absent batch reads, insertion/deletion
phantoms, limited ranges, no-op dependencies, references and revocation protect
stale publication. Verify engine reopen, structural paths, reactive state and
generation lifecycle through the same adapter. Through one writable Node service,
exercise disjoint commits and those conflict schedules; drop a committed response
and recover the original receipt without applying twice. Record actual Node/native
client/server versions, physical limits and target ownership. One local FoundationDB
server is not replicated-failure or power-loss acceptance. D2a follows this gate.

## D2 — Credential custody, identity and session fencing

**D2a accepted 2026-11-19.** 377 tests passed, zero failed/skipped, Node26.12.0,
including nineteen primary FoundationDB identity witnesses, fourteen optional SQLite
witnesses and one independent keyring witness. Concurrent policy revocation,
credential revision, owner transfer and original issuance collisions reject stale
publication. Private session-fenced cursors reject replacement sessions and restore
incarnations while unrelated source edits allow progress. See
[D2a evidence](evidence/2026-11-19-d2a.json). D2b delivered email/phone proofs follows D3.

D2a follows D1F: real credentials and opaque sessions use external encryption/HMAC
keys, one existing authority graph, bounded authentication attempts, expiry,
revocation and an external restore incarnation. Actor/tenant comes from validated
sessions; initial and final authority checks remain in the transaction. Original
session issuance recovery must not store plaintext bearer credentials in receipts.
D2b follows D3 delivery: email/phone proof admission uses privately delivered proof,
bounded attempts, expiry, one-use issuance and original recovery.

**Exit.** Credential rotation, lost acknowledgement, invalid/expired/revoked tokens,
owner transfers, policy revocation and restore reject or recover correctly. Restored
credentials can issue new sessions while pre-restore sessions/proofs stay invalid.
No public route accepts an actor, authority provider, encryption key or raw KV access.

## D3 — Durable effects and provider protocols

**Accepted 2026-11-19.** 418 tests passed, zero failed/skipped, Node26.12.0.
Thirty-seven new effect witnesses (21 primary FoundationDB,16 optional SQLite)
use a separately durable provider ledger, exact start reconstruction and source
checking. SIGKILL covers claim/start/provider success/fact/application and provider
ledger publication. Concurrent quota, revocation, stale completion and application
schedules pass. Four shared identity recovery regressions close bounded guessing
against original successful login receipts. See [D3 evidence](evidence/2026-11-19-d3.json).
Delivered proof D2b follows; older-backup effect fencing remains D5.

Commit effect intent with business state and receipt. Claim/start evidence, pinned
credential revision and admission charge precede external execution. Calls occur
outside engine transactions. Preserve fenced leases, immutable provider operation
identity, known/unknown facts, idempotent application and original-key reconciliation.
Two concrete fake protocols exercise the worker; live provider credentials remain
operator supplied. Unsupported work rejects before acceptance or is isolated
without blocking unrelated jobs.

**Exit.** Kill/reopen around claim, physical start, provider success, fact storage
and application. Stale workers cannot apply; unknown work never blindly resends.
Cancellation, retries, admission and credential changes preserve original evidence.
Fresh fake transports prove protocol semantics, not live-provider certification.

## D4 — Objects, authenticated service and full pipeline

Use ordinary Node imports and HTTP routes over the same prepared catalog. Private
upload grants bind owner, session/incarnation, expiry, size and digest; filenames
are server selected. Stream/hash/stage, durable finalize, activation, tombstone and
cleanup reconcile file/DB crash boundaries. Downloads require current record Read.
Expose only authorized mutations/queries/relations/publications and selected service
operations. Complete the delivered-proof, effect and object pipeline locally.

**Exit.** Adversarial HTTP and subprocess witnesses cover request limits, tenant and
owner boundaries, missing grants, transfers/revocation, stale upload/finalize tokens,
oversized/mismatched bytes, crash recovery and idempotent cleanup/application.
No secret, hidden source ID, undeclared summary count or raw diagnostic reaches clients.

## D5 — Recovery, release and selected operations

Build a reproducible self-hosted Linux release with external custody keys, explicit
bootstrap, health/shutdown, database/object backup and restore incarnation rotation.
Verify install/start/stop/reopen, complete restored source/file inventories and
session invalidation. Report seeded workload latency, visits, logical/physical bytes,
memory and adapter scan costs separately. Publish deployment instructions and actual
local release evidence; remote hosting requires a concrete operator target.

**Exit.** Restore a verified backup and independently check the full pipeline; old
sessions/proofs/cursors reject. Faulted/incomplete backups never replace the active
installation. Release smoke and measured capacity include dataset/runtime/limits.
Online catalog migration, multi-host writes, recurrence/frontend/full domain suites
retain separate explicit acceptance and are not prerequisites for this release.

### Checkpoint method

For each implementation checkpoint, run the relevant independent witnesses and
the Node test suite. Record the revision or tested source digest, runtime,
commands, counts and remaining limits before advancing the handoff. The expected
inventory and order must be derivable from source facts independently of the
writer's projection, routing and tree traversal.

Persistent transaction conflicts, crash/reopen, protected ranges and original
receipt recovery require a durable adapter. Session authentication, restore
fencing, credentials and key custody, durable effect intent, provider fencing and
unknown outcomes, object lifecycle, services and deployment retain separate gates.
Capacity, backup/restore, staged rollout and online migration require their own
evidence. See the [preservation inventory](capabilities.md) for retained application
and operational obligations.
