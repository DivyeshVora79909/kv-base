# Current context and local handoff

**R1–R5, optional SQLite D1, primary FoundationDB D1F, D2a, D2b and D3 are accepted; D4 is next.** The implementation baseline is
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

R5 accepted on 2026-11-19: **272 tests passed**, zero failed/skipped, on Node
26.12.0, including 27 independent lifecycle/workload witnesses. Source hashes,
commands, dataset, budgets, work, bytes and timings are in
[R5 evidence](docs/evidence/2026-11-19-r5.json). R4 was published at `cda0b3f`.
The ordinary `examples/workload.mjs` seed20261119 run populated64 contributors
(hot52/warm6/cold6), executed80 steps (65 accepted/15 deliberate rejections or
rollbacks), independently checked67 sources/192 occurrences and rebuilt generation1.
These are small-fixture measurements, not a capacity acceptance.

## Next boundary and remaining limits

D1 accepted on 2026-11-19: **287 tests passed**, zero failed/skipped, Node 26.12.0.
Fifteen independent SQLite/engine witnesses cover exact persistence, rollback,
absence/empty range protection, multiple handles/processes, SIGKILL before/after
commit, original receipts, structural paths, reactive settlement and generations.
See [D1 evidence](docs/evidence/2026-11-19-d1.json). SQLite WAL/FULL uses immediate serialization and
definite busy rejection; power-loss and multi-host guarantees are not accepted.

D1F accepted on 2026-11-19: **343 tests passed**, zero failed/skipped, Node26.12.0
after fresh `npm ci` (zero vulnerabilities; packaged N-API prebuild used). FoundationDB
is the first durable target; SQLite remains optional. One Node service accepts writes
with concurrent native serializable transactions. Thirty FDB adapter/engine witnesses
and thirteen single-service tests pass, including disjoint actual source mutations,
ten shared point/absence/range/reference/revocation conflict schedules, native physical
limits, process kills and original receipt recovery after a dropped committed response.
See [D1F evidence](docs/evidence/2026-11-19-d1f.json). Binding2.0.1/API720 uses official
native client/server7.3.81. The owned test cluster is one local process/coordinator;
replication/server-failure, native commit-ack injection, disk/power loss and capacity
are unverified. Raw conformance HTTP routes are test fixtures, not production APIs.

D2a accepted on 2026-11-19: **377 tests passed**, zero failed/skipped, Node26.12.0.
The34 new registrations cover primary FDB19, optional SQLite14 and independent
keyring1. Passwords use salted scrypt (default N131072/r8/p1), bounded work/attempts
and encrypted external-key custody. Session digests and sealed original receipts
preserve expiry, revocation, password rotation, concurrent owner/policy conflicts and
restore fencing. Credential encryption remains stable across restore; session/result
incarnations change. Authorized cursors bind the private session identity, so another
token for the same actor cannot resume an old handle. See
[D2a evidence](docs/evidence/2026-11-19-d2a.json). D1F was published at `638e97d` and
its43 post-commit adapter/service checks passed. Real delivered email/phone proofs
remain D2b after D3; enrollment/graph replacement remain trusted operator functions.

The requested R1–R5 sequence is complete. `createEngine` owns fixed-tenant active
snapshot reads/writes, opaque raw/authorized continuations and rebuild/check/retire.
One source authority and prepared catalog remain shared; only derived namespaces
rotate. Candidate and staged state are independently checked before activation.
Failed build/publication preserves the old active state; retirement failures report
successful activation with pending cleanup. Context tenant mismatch rejects before
provider/I/O. Source, authority setup and physical scan costs are explicitly metered.
Importable `inspectEngine`, `populateAdaptive` and `runSeededWorkload` report
actual transaction outcomes, including nonzero staged byte work on validator rollback.

The memory adapter remains volatile; optional SQLite supplies durable serialization;
FoundationDB supplies concurrent serializable durable storage behind the same engine.
Cursor handles are process-local, consumed
on successful resume and invalid after restart/another engine/generation; unused
handles can be released and their inventory is bounded. Rebuild currently uses the
same catalog and rejects changed authoritative/generated source inventory. Catalog
migration, online catch-up and durable/session restore continuations are deferred.
All lifecycle writes should use the engine facade; raw adapter writes remain privileged
and can bypass active-generation maintenance. Logical byte caps do not bound eager
cloning or heap usage. Whole-source continuation witnesses are budgeted scans, not
logarithmic queries. Fixture policy alone is not authentication; the D2a identity
boundary derives actor/tenant from live opaque sessions.

D3 accepted on 2026-11-19: **418 tests passed**, zero failed/skipped, Node26.12.0.
Thirty-seven independent effect witnesses cover21 primary FDB/16 optional SQLite:
atomic intent/domain/receipt, retained encrypted revisions, exact quota admission,
immutable provider identity, unknown reconciliation, external fencing, cancellation,
worker/provider SIGKILL and concurrent schedules. Four additional shared identity
regressions bound changed-password recovery guesses and block original result release
while locked. See [D3 evidence](docs/evidence/2026-11-19-d3.json). D2a was published at
`66cf23e`; its34 focused post-commit checks passed. Fake Resend/Brevo email and Brevo
SMS protocols own a separate durable ledger; no real delivery was sent or certified.
Admission may retain a charge after death before dispatch. External custody keys
remain operator supplied; memory erasure is not claimed. Restore of an older pending
effect needs D5 preflight fencing before workers run.

D2b accepted on 2026-11-19: **434 tests passed**, zero failed/skipped, Node26.12.0.
Sixteen independent primary FDB witnesses verify private email/E164 SMS delivery,
independent HMACs, bounded proof and recovery guesses, one-use issuance, encrypted
original results, restore/contact/password fences, concurrent completion and SIGKILL
before/after commit. See [D2b evidence](docs/evidence/2026-11-19-d2b.json). D3 was
published at `919e63d`; its54 focused effects/identity/keyring checks passed.
Contacts, delivery principal and providers remain operator supplied. Proof admission
returns no code; live email/SMS acceptance is unverified.

Next gates retain object lifecycle/full pipeline;
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
