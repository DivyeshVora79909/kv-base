# Current context and local handoff

KV-Base is the clean Node engine repository. The extracted implementation originates
from [Next-Base at 76b6e84](https://github.com/DivyeshVora79909/next-base/tree/76b6e84/prototypes/node-engine),
plus the small JS-P4b.1 access/fixture-authority checkpoint completed during migration.
Historical test results and native contracts remain pinned predecessor references;
only fresh verification against this repository can accept the migrated code.
Migration and JS-P4b.1 accepted on 2026-11-14: **171 Node tests passed**, zero
failed/skipped, on Node 24.22.1 after a fresh dependency installation here.
`npm audit --omit=dev` reports zero vulnerabilities. The suite includes seven access
declaration tests, nine fixture-authority tests and three relation-example tests.
See [fresh evidence](docs/evidence/2026-11-14-migration.json) for provenance and hashes.
The migration run stopped at this verified commit as requested; that stop applied
to that run, not to future agents authorized to continue.

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

## Resume here

The clean repository contains the migration plus JS-P4b.1. Inspect Git status and
the latest commits before resuming; do not assume this page supersedes later evidence.

Next implementation gate is **JS-P4b.2**: validate hydrated source/occurrence identity,
merge permitted owner streams with cached heads on one snapshot, budget discovery,
setup/candidates/refills, and enforce authorized mutations, before/after owner transfers
and retargeted-reference checks. Compare against an independent filtered oracle;
exercise private models, hidden contributors, missing grants and final authority.
Forward/reverse reference maintenance already exists; the new relation fixture proves
1:N, uniqueness-backed 1:1 and N:M junctions. Compose authorized reverse-reference
discovery with the same hydration/disclosure primitive rather than adding a second
relation engine. Raw incoming scans remain internal and physically linear in the
memory adapter. See [the relation contract](docs/relations.md).
**JS-P4b.3** covers explicitly published anchor summaries if needed: disclosure of a
FULL summary never grants contributor IDs/bodies. **JS-P4c** adds exact-query,
catalog/authority and relevant-state continuation fencing, including payload-only
updates and owner transfers. No global mutation/revision counter. Full P4 is open.

**JS-P5** follows P4: writer-paused generation build/check/activation/retirement,
ordinary importable model inspection/population/workload functions and reproducible
bounded measurements. Keep population tied to the same catalog, constraints and
transaction outcomes. See [checkpoints](docs/checkpoints.md) for witnesses and gates.

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
