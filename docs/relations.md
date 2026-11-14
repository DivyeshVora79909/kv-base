# Relations: research intent and implemented contract

This application-linked engineering note retains the reference/reverse-reference research behind the Node redesign moved from `Next-Base/prototypes/node-engine`. The working question is how people, teams, profiles and project participation can share one transactional KV consistency boundary with the custom augmented AVL engine. The executable declaration is [examples/relations.mjs](../examples/relations.mjs); importing it performs no I/O. The example uses existing engine primitives and introduces no reverse-query API.

## Representation

| Application relation | Authoritative source fact | Derived engine metadata |
| --- | --- | --- |
| Team has many people (1:N) | Each person has one scalar `team` reference | One incoming entry per person targeting that team |
| Person has at most one profile (1:1) | Profile has scalar `person` reference, with a uniqueness claim on `person` | Incoming entry plus uniqueness claim |
| People participate in projects (N:M) | One participation junction record with two scalar references | Two incoming entries, one for each target; optional pair uniqueness claim |

The example enables pair uniqueness on `(person, project)`. Without it, separate junction IDs may represent repeated participation facts. This is one business edge record with two endpoints, not two duplicated business edge records. Junction records can acquire their own application fields.

Required schema fields guarantee that each stored child supplies its target; reference validation guarantees that target exists and has an allowed model. Scalar reference plus uniqueness means at most one profile per person. It does **not** require every person to have a profile (Bob deliberately has none). Exactly one in both directions requires an additional final existence/invariant obligation; that obligation is not implemented by this declaration.

References do not imply business tree membership. Memberships must be declared separately when an application needs ordered or aggregated views. Incoming entries are maintained engine metadata, not another user table or business model. IDs identify sources within a tenant; model classes are separate metadata and allowed-target declarations. There is no SurrealDB record-ID or relation syntax dependency.

## What the implementation currently does

`catalog.mjs` compiles explicit reference slots, allowed target models and uniqueness selectors. Only `onDelete: 'reject'` is supported; cascade and nullification are not implemented. `stage.mjs` derives incoming entries and claims from source facts, checks old metadata before releasing it, releases old claims before acquiring final claims, and checks final target existence and model. Target deletion succeeds only when no final incoming reference remains. Retargeting, deleting referencers, and deleting targets can therefore be joint repairs in one transaction, independent of command order. Errors abort the transaction, preserving sources, incoming metadata, claims and tree state together.

`keys.mjs` namespaces source keys by tenant and derived incoming/claim keys by tenant and catalog identity. Reference IDs resolve in the mutation's tenant; there is no implicit cross-tenant lookup. Claims are also scoped by model and slot. `check.mjs` independently reconstructs reference and claim inventories from source facts and compares them with stored metadata, alongside AVL integrity checks.

The three tests in [test/relations-example.test.mjs](../test/relations-example.test.mjs) exercise the reusable application declaration: both junction directions, 1:N inspection, pair/profile uniqueness rollback, reference retargeting, rejected deletion rollback, and joint repairs/deletions. They use public preparation, mutation and integrity checking. Their `incomingPrefix` import and `tx.scan` calls inspect internal metadata for verification only. Raw scans are not an authorized client relation API. JS-P4b.2 now supplies the separate fixture-authorized surface below.

`readIncomingAuthorized(store, catalog, { tenant, actor }, { targetId, model, slot,
limit }, { getAuthority, ...budgets })` reads authority and candidates in one snapshot.
The target must pass Read. Each incoming key and source reference slot/array position
must agree with the hydrated source, then that source must independently pass Read.
Readable target ownership does not authorize hidden referencers. Results contain
distinct readable source records sorted by UTF-8 ID, with no raw counts or summary.
Repeated array positions are deduplicated. Work metrics go only to a trusted host
callback; see [the API contract](api.md#js-p4b2-authorized-request-composition).

Authorized mutations require explicit Use for new/retargeted reference positions,
including original/proposed/final target owners and final authority. Existing
references and engine-managed repairs retain their separate integrity obligations.
The independent [authorized read](../test/authorized-read.test.mjs) and
[mutation](../test/authorized-mutate.test.mjs) witnesses cover hidden contributors,
missing Use, owner transfers, retargeting and atomic rollback. The existing full
source-derived checker remains unchanged.

## Performance and adapter boundary

The memory adapter enumerates the store for a prefix scan, then sorts matching keys. Even an incoming scan with `limit: 1` currently performs O(store size) enumeration; this is not an indexed O(log n) reverse lookup. Its declared scan cost includes overlay enumeration and sorting matching results. Future durable adapters can implement ordered prefix/range scans while preserving transactional point, absence and range guards. No future adapter complexity or durability guarantee is demonstrated by this fixture.

The KV adapter harness directly exercises the custom AVL nodes, sources, incoming entries and claims inside the same transaction. A SurrealDB prototype would require another consistency integration and duplicate this harness without being necessary to establish these semantics. Reconsider a backend only when its transaction and scan contract can be tested against the adapter obligations. Future primitives should remain pure compositions of existing operations; trivial filesystem loops do not need wrappers presented as new engine abstractions.
