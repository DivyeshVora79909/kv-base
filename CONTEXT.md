# Current context and local handoff

**Start at R5, continuations, generation lifecycle and developer tools. R1–R4 are accepted.** The implementation baseline is
`8f337cd` (JS-P4b.2). The [checkpoint roadmap](docs/checkpoints.md) gives the
R1–R5 sequence, the acceptance tests and the work each gate can reuse.

KV-Base is the standalone Node engine repository. The extracted implementation originates
from [Next-Base at 76b6e84](https://github.com/DivyeshVora79909/next-base/tree/76b6e84/prototypes/node-engine),
plus the small JS-P4b.1 access/fixture-authority checkpoint completed during migration.
Historical test results and native contracts remain pinned predecessor references;
only fresh verification against this repository can accept the migrated code.
Migration and JS-P4b.1 accepted on 2026-11-14: **171 Node tests passed**, zero
failed/skipped, on Node 24.22.1 after a fresh dependency installation here.
`npm audit --omit=dev` reports zero vulnerabilities. The suite includes seven access
declaration tests, nine fixture-authority tests and three relation-example tests.
See [fresh evidence](docs/evidence/2026-11-14-migration.json) for provenance and hashes.

**JS-P4b.2 accepted on 2026-11-14:** fixture-authorized owner-stream reads, reverse
reference disclosure and authored mutations, preserving atomic reactive settlement.
Fresh verification: **206 Node tests passed**, zero failed/skipped, on Node 26.12.0
after `npm ci`. This includes 16 independent authorized-reader witnesses and 19
authorized-mutation witnesses. See [checkpoint evidence](docs/evidence/2026-11-14-js-p4b2.json)
for source hashes, commands, example work counters and limits.

## What exists

- Prepared JSON Schema 2020-12/Ajv catalogs; reusable contracts, named views and
  explicit stable occurrence slots; exact BigInt monoids and independent source oracle.
- Serialized volatile transactional adapter; generic augmented AVL mutation,
  point/select/fold reads, typed forward/reverse references, final-state uniqueness,
  joint validation and independent active-generation checking (JS-P1/JS-P2).
- Direct/range reverse routing, potential-output inventories, final-graph causality,
  managed identities and incremental atomic settlement, checked against a separate
  full-snapshot reference (JS-P3).
- Same-snapshot raw rank/key windows with explicit node/batch/output/byte budgets
  (JS-P4a). Counts and member IDs remain privileged.
- Private-by-default optional model access orders, `prepareAuthority`,
  `resolveAuthority`, `permits` (JS-P4b.1). These are pure fixture policy functions;
  raw reads and mutations are still trusted APIs without enforcement.
- `readAuthorized` merges cached hydrated owner heads with exact ordering and finite
  discovery/setup/candidate/refill/node/byte budgets. `readIncomingAuthorized` composes
  existing incoming entries with the same source validation/disclosure. Only readable
  records are returned; diagnostic counters remain privileged (JS-P4b.2).
- `mutateAuthorized` checks both owners, new/retargeted reference Use, settled targets
  and final transaction-loaded authority. Hidden managed repairs retain the same
  atomic settlement boundary. Fixture enforcement is not authentication (JS-P4b.2).

## Latest acceptance and resume

R1 accepted on 2026-11-18: **213 tests passed**, zero failed/skipped, on Node
26.12.0 after `npm ci` (zero audit vulnerabilities). The seven independent
structural witnesses cover ordered signature arrays and boundaries, incompatible
bindings, unanchored country/year lookup, two contributors, two owner partitions,
repeated slots, explicit openings and atomic validator failure. Source hashes and
commands are in [R1 evidence](docs/evidence/2026-11-18-r1.json).

R2 accepted on 2026-11-18: **221 tests passed**, zero failed/skipped, on Node
26.12.0. Eight new independent witnesses compare tuple order, stable ties, ranks,
noncommutative folds, complete/group bounds, batched pages, interval matches,
potential inventories and incremental settlement against source recomputation.
Second-component-only moves reach both old/new subscribers; identical projections
and payload-only edits write no occurrences. See [R2 evidence](docs/evidence/2026-11-18-r2.json).
R1 was published at `506e3e4`; HTTPS Git credentials were unavailable, so verified
trees are published through the connected GitHub API with a checked branch head.

R3 accepted on 2026-11-19: **231 tests passed**, zero failed/skipped, on Node
26.12.0. Ten independent path witnesses cover untouched contributors, tied keys,
old/final graphs, terminal edits, absent/re-entry, repeated stages/new edges,
managed potential dependencies while actual outputs are absent, independent full
recomputation, and exact rollback of sources/edges/trees/routes/outputs. See
[R3 evidence](docs/evidence/2026-11-19-r3.json) and `examples/reference-path.mjs`.

R4 accepted on 2026-11-19: **245 tests passed**, zero failed/skipped, on Node
26.12.0. Fourteen independent authority witnesses check structural owner plans,
shared/tied/repeated streams, point/indexed/reverse record policy, publication20
from visible+100 and hidden−80, both-owner transfers, revocation, tenant/budget
limits, request detachment and joint ancestor/owner changes in one snapshot. See
[R4 evidence](docs/evidence/2026-11-19-r4.json). R3 was published at `857890a`.

Start at R5. Authorized structural reads derive compatible partitions from the
contributor's local trusted owner header. Publication is an explicit selected-
aggregate contract tied to a readable declaring-model anchor. Related/intermediate
records grant no permission and require no extra Read. Trusted hosts can compose
`readAuthorizedInTransaction` and `readAnchorSummaryAuthorizedInTransaction` in
one read-only snapshot. Existing both-owner/final-authority mutation checks remain.
R5 must fence cross-request positions by query/catalog/generation/authority/relevant
source state, build/check/activate candidate generations atomically, retire after
reads finish, and expose ordinary importable workload/inspection/population tools.
Fixture policy is not authentication. All storage remains volatile/serialized.

Later gates retain persistent KV transactions/concurrency/crash/original-receipt
recovery; identity/session and restore fencing; credentials/key custody; durable
effect intent, provider fencing and unknown outcomes; object lifecycle/full pipeline;
release/bootstrap, operational capacity and selected deployment. None is supplied by
native files or adapters in this repository. Full accounting, recurrence, temporal
wakeups, live feeds, frontend, online migration and reactive rank windows are deferred.
No persisted Rust-format compatibility or automatic database migration is promised.

## Working method

Read [AGENTS](AGENTS.md), this page, then the relevant canonical contract. Use normal
Node imports, REPL/scripts and the built-in test runner. Keep implemented, proposed,
measured and assumed claims distinct. Verify each small coherent checkpoint, update
its evidence and handoff, commit and push when authorized, and continue through the
agreed gates when authorized to continue.
Use the actual local environment's available models and tools. No fixed commit quota,
subscription assumption or artificial billing rule is part of the engineering plan.
Do not revive unrelated predecessor tasks or claim production parity from fixture tests.
