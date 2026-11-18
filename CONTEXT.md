# Current context and local handoff

**Start at R3, reference-path participation. R1 and R2 are accepted.** The implementation baseline is
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

Start at R3. Structural order bindings now remain typed arrays even for singleton
orders, and prepared catalog identity pins representation version 2. Named scalar
fixtures remain regression coverage. Mutations, authorized reads and checkStore
fence catalog identity; raw transaction tree primitives remain privileged. R3
must compile finite typed scalar paths, discover untouched contributors via old/final
edges, reconcile old/final occurrences and route their changes through settlement.
R4 owner planning/publication and R5 continuation/generation/tools remain unmet.

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
