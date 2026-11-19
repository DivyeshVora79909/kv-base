# Capability preservation map

This map retains predecessor obligations; it does not claim those services, native
adapters, models or tools are present in kv-base. Pinned links identify predecessor
contracts and historical evidence. Current local implementation status is authoritative
in [checkpoints](checkpoints.md). No historical result is new acceptance.

Preserve capabilities and obligations, rather than exported names or compatibility.
Legacy semantics are design inputs, not an instruction to port SurrealQL, its
compiler, storage bindings or every business model. B+ tree replacement and
SurrealDB coupling are outside the selected design. The target is ordinary Node
functions over explicit state, one canonical authored model and model-declared augmented
AVL indexes with structural identities. The Node implementation is authorized.

## Engine and model obligations

| Capability | Baseline evidence / reuse point | Prototype obligation |
| --- | --- | --- |
| Canonical model | [Compiled models](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/compiled-models.md), [model source](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/model.rs), [prepared Node model](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/model.mjs) | One declaration supplies validation, references, roots, memberships, dependencies and inspection metadata; projections are generated views, not another schema authority. |
| Model-specific calculations | [Bindings](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/binding.rs), [models](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/models), [compiled model contracts](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/compiled-models.md) | Map authored inputs, parameters, published fields, managed outputs and invariants into ordinary JS functions; do not encode domain names into the core. |
| Reusable ordered summaries | [Index mathematics](index-mathematics.md), [AVL implementation](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/tree.rs), [algebras](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/algebra.rs) | Name each index's scope/root, order and ties, lift/combine/identity, structural count, queries and validators. Preserve order-sensitive associative summaries and exact arithmetic. |
| Explicit multiplicity | [Membership semantics](index-mathematics.md) | A record may contribute m independent memberships. Stable occurrence slots distinguish two contributions in one root; counts measure occurrences. Root anchors differ from rotating AVL heads. |
| Query/read semantics | [Rank reads](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/rank-reads.md), [ordered access](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/ordered-access.md), [reader](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/tree_read.rs) | One snapshot; bounded seeks, range folds, traversal, hydration and continuation. Raw membership rank differs from visible/filtered rank. No automatic cross-resource union. |
| Direct dependencies | [Binding declarations](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/compiled-models.md), [dependency engine](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/dependency.rs) | Declare authored-parent field reads and published-parent inputs; repair old/new routes on retargeting. A zero-valued result still has declared dependencies. |
| Ordered range dependencies | [Reactive windows](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/reactive-windows.md), [native ranges](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/native_range.rs), [causal settlement](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/causal.rs) | Exact inclusive/exclusive cuts and order identity; discover affected consumers on insert/delete/rekey/contribution change. Preserve protected empty reads and final-graph causality. |
| References and reverse links | [Relationships](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/relationships.md), [relationship engine](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/relationships.rs) | Typed scalar/array/polymorphic targets; reject dangling/cross-tenant links. Derive incoming routes from the same reference declarations, separately from calculation subscriptions. |
| Uniqueness and predicates | [Relationship rules](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/relationships.md), [claim engine](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/uniqueness.rs) | Protect present/absent claims; release changed old claims before final acquisitions for swaps. Recheck immediate-parent predicates after parent edits, including hidden consumers. |
| Atomic mutation | [Mutation lifecycle](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/mutation.rs), [staging](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/staging.rs), [receipts](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/receipts.rs) | Publish source, derived output, memberships, summaries, routes, claims, authority effects, receipts and selected effect intent together or publish none. |
| Adapter guarantees | [Storage backends](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/storage-backends.md), [storage interface](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/storage.rs) | Explicit snapshot, protected point/absence/range reads and atomic commit. Distinguish definite conflict/abort from unknown commit; recover the original operation before replanning. |
| Independent checking | [Integrity engine](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/integrity.rs), [Node source oracles](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/oracles.mjs) | Rebuild expected membership/order/count/summary/reference/claim inventories from source facts and declared semantics, never writer caches or generated expected trees. |

Current native models include calculation, equipment, invoicing, effect-demo and
services behavior; see the [compiled model guide](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/compiled-models.md) and
[model sources](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/models). They provide mappings and counterexamples, not
a requirement to implement every model in JS-P1–JS-P5. Use small generic witnesses first:
count-only order, exact additive summary and an order-sensitive prefix summary;
then direct and range-dependent calculations with reference repairs. Full accounting
and invoice semantics remain deferred, even when their algebra informs a witness.

No implicit tenant-wide population counter, capacity row or analytics root is
allowed. Structural subtree counts belong to declared indexes; source-derived
inventory statistics belong to inspection. An explicit business root may coordinate
writes when its actual invariant requires it. For m changed memberships, lawful
constant-size summaries permit O(m log n) index path work; dependency cascades,
authority discovery, arithmetic size and bytes add work. This is no universal
logarithmic transaction or unlimited capacity claim.

## Authority and external-service obligations

The predecessor policy and fixture authority are owner-only, superseding historical readers. Read/Create/
Update require tenant, action and accessible owner: self, strict descendants or
direct group parents. Delete permits self or strict descendants. Tenant-parent
data is excluded from ordinary scope; inherited tenant grants are separate.
Admin satisfies ordinary action grants except Use and does not widen data scope. Owner creation defaults to actor; transfer checks
both old and new owner before CAS diagnostics and against the final graph.
Internal validation may touch hidden records without granting client disclosure.
Public FULL root summaries do not confer access to contributor bodies or IDs.
See [current instructions](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/AGENTS.md), [owner-only checkpoint](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/roadmap.md)
and [access implementation](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/access.rs). Do not reinstate reader repair.
Operator graph replacement is not delegated principal CRUD; the latter needs explicit
scope/grant-subset checks and its own acceptance if exposed. Generic per-field ACLs
are not implied by record authorization or secret-envelope protection. Do not claim
either capability is already supplied by the baseline or the prototype.

| Service capability | Baseline status and boundary | Design preservation |
| --- | --- | --- |
| Credentials and grants | [E1 accepted](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/e1-credentials.md): encrypted ingress/custody, revision-pinned use and request-bound grants | Metadata access and secret Use are distinct; encrypted payloads stay private, while root/proof/signing keys remain outside stores. |
| Durable execution | [Effects](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/external-effects.md), [worker source](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/effects) | Commit intent first; external calls occur outside engine transactions. Preserve lease fencing, exact start evidence, durable facts, application and unknown-outcome reconciliation. |
| Email and admission | [E2/R0 accepted](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/e2-email.md), [rate admission](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/rate-admission.md) | Two fake provider protocols, retained credential revisions and charged physical starts; range summaries alone are not an admission protocol. |
| Identities and sessions | [A0 accepted](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/a0-identity.md), [A1/A2 accepted](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/a1-proof-sessions.md) | Protected claims, private delivered proof, bounded attempts, one-use issuance, original recovery, revocation and restore generation fencing. |
| Objects / full pipeline | [Service plan](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/server-execution.md) | E3 object grants/finalize/cleanup, E5 full fake-pipeline recovery and fresh release/bootstrap acceptance are unfinished. |

These accepted labels describe the Rust baseline's recorded local evidence only.
A1/A2 records 442 Rust tests and 253 Node tests plus one-host FoundationDB/redb
controls in [CONTEXT](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/CONTEXT.md). Fake transports do not certify live
providers; one local host does not certify multi-host operation. No result here
certifies a rewritten JS engine. Preserve service contracts in the redesign map;
their full implementation is a later production gate, outside the first engine proof.
Clocks, recurrence/schedules, frontend, full accounting and TiKV are deferred.
Time-window declarations may inform generic ordered cuts without adding a scheduler.
Service acceptance must distinguish unsupported per-job configuration from store
corruption: reject unsupported work before acceptance or durably isolate the job,
without provider calls/charges or stopping unrelated operations. This is a retained
regression obligation, not a fresh diagnosis of the updated A1/A2 worker.

## Developer workbench capabilities

The predecessor [public barrel](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/index.mjs) and
[function guide](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/developer-tools.md) already support explicit ESM composition.
Keep normal Node imports, REPL, callbacks, filesystem, fetch and subprocess APIs;
do not introduce a bespoke CLI or workflow/query language. Predecessor native handles
are integration examples, not a permanent Rust requirement for the new prototype.

| Capability | Existing source / semantics to retain |
| --- | --- |
| Model preparation and catalog inspection | [model.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/model.mjs): prepared identity, schemas, references and contract metadata; expose declarations and actual record participation separately. |
| Adaptive population | [population.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/population.mjs), [C4 contract](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/docs/c4-population.md): deterministic seeds, requested additional counts, existing and same-batch anchors, bounded attempts/work and unsatisfied diagnostics. |
| Reference/cardinality shaping | Same C4 sources: scalar/array occurrence usage, max fanout, uniform/balanced/skew policies, tuple uniqueness and final parent checks; do not weaken quotas or constraints to finish. |
| Optional solver | C4 callback proposals pass the same schema/reference/fanout/rule validation. Legacy [fixture README](https://github.com/DivyeshVora79909/ReBase/blob/270d875a6e97b276d9cf7876a9bb4666a21d0f53/dev-tools/README.md) additionally documents scoped native Z3 sessions, timeout/unknown distinction and cleanup; these are optional semantics, not a required dependency. |
| Receipt-bound progress | [client.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/client.mjs), population accept/reject: advance committed pools once after original receipt; retain uncertain proposals and exact operation context. |
| Inspection and explainability | [native inspector](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/src/inspector.rs), [legacy inspector](https://github.com/DivyeshVora79909/ReBase/tree/270d875a6e97b276d9cf7876a9bb4666a21d0f53/dev-tools/inspector): show inputs, formulas, direct/range dependencies, roots, slots and summary algebra. Browser layout/export features are inspiration; frontend implementation is deferred. |
| Workloads and exploration | [workload.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/workload.mjs), [experiments.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/experiments.mjs), [scenarios.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/scenarios.mjs): bounded runs, traces, replay, failure shrinking, churn, overload and fault scenarios. |
| Resource measurements | [observe.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/observe.mjs): explicit PIDs/roles and paths, Linux RSS/CPU/descriptors/threads, deduplicated physical bytes, observer cost and finite budgets; unavailable metrics remain incomplete. |
| Lifecycle and operations | [operator.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/operator.mjs), [native.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/native.mjs): explicit ownership, scoped check/inspect/backup/restore/archive/reset/service control and close; exact target identity and grants. |
| Disposable infrastructure / packaging | [infra.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/infra.mjs), [release.mjs](https://github.com/DivyeshVora79909/next-base/blob/76b6e84/tools/node/release.mjs): owned local environments and source-bound packages. Existing helpers do not close outstanding fresh release/bootstrap acceptance. |

Legacy [dev-tools](https://github.com/DivyeshVora79909/ReBase/blob/270d875a6e97b276d9cf7876a9bb4666a21d0f53/dev-tools/README.md) also supplies reproducible
offline recipes, incremental generation against current parent facts, typed binding,
transaction-owned insertion, rollback on stalls, schema preparation/clearing and
read-only access inspection. Reuse the underlying capabilities where selected;
replace SurrealDB schema parsing, native SDK types, SQL callbacks and installation
mechanics. Legacy separately authored public JSON Schemas do not satisfy the new
single canonical model requirement by themselves.

## Prototype versus production acceptance

Fresh local D1/D1F/D2a/D3 evidence now covers optional SQLite, primary concurrent
FoundationDB, encrypted opaque identity sessions and durable provider effects.
These results use the current Node sources, not the predecessor counts. D2b now
adds delivered email/phone proofs with bounded one-use issuance. D4 now accepts
the authenticated object/service pipeline with fresh subprocess and HTTP witnesses.
Selected Linux backup/restore/release remains D5. Live provider certification,
replicated failure, power loss and larger capacity remain unaccepted.

The original JS-P1–JS-P5 plan below records which obligations the prototype took
from the native baseline. The [active roadmap](checkpoints.md) carries the
R1–R5 redesign, now locally accepted, and separates later durable/service gates.

- JS-P1: canonical model specification/compiler, independent source oracle and
  in-memory transactional adapter with explicit atomic staging and read semantics.
- JS-P2: named lawful AVL contracts, explicit membership multiplicity, typed/reverse
  references, uniqueness and scoped validation; adversarial orders, rotations,
  deletion, final-graph repairs and rollback witnesses against the source oracle.
- JS-P3: direct and ordered-range dependency settlement; old/new route repairs,
  zero-output dependencies, causality rejection and command-order permutations.
- JS-P4: bounded authorized batches, owner-stream k-way reads and cursors using
  fixture authority; hidden-record nondisclosure, transfers and authority changes.
- JS-P5: index lifecycle, ordinary Node population/inspection/workload composition,
  demo, deterministic replay and measured work/resources and index benefit/cost.

Each gate must have fresh JS evidence tied to its source and selected adapter.
Later production gates add durable adapter concurrency and receipts, crash/unknown-
commit recovery, real authentication, service custody and worker protocols,
full identity/session acceptance, objects,
fake full-pipeline recovery, release/bootstrap and selected deployment topology.
The prototype must state unsupported guarantees rather than borrow native evidence.
