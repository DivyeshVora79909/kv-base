# Checkpoints and acceptance gates

**R4 is the first unmet gate. R1–R3 are accepted.** This roadmap starts from `8f337cd`, which includes
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

**Needs adaptation.** Keep the existing tenant, action and owner-DAG checks,
hydration, same-snapshot merge and both-owner/final-authority mutation checks. Bind
accessible owners to a structural partition role backed by the **contributing
record's trusted owner header**. A related record's owner has a different meaning.
Plan ordinary compatible partitions and merge cached stream heads by the R2 order.
Direct, indexed and reverse-reference results use the same record policy. The
traversal path grants no permission and requires no extra read grant on an
intermediate record. Keep internal integrity checks over complete business state.
Explicit publication of selected full-anchor summaries is still missing.

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

**Missing.** Raw windows and fixture reads already share a transaction snapshot.
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
