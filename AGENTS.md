# Agent instructions

Read [CONTEXT.md](CONTEXT.md) for current status and next acceptance gates.
Check Git status before editing; preserve unrelated work.

- `src/` owns the reusable Node ESM engine; `test/` owns independent witnesses;
  `examples/` owns small importable demonstrations. Use Node 24+, `npm ci`, `npm test`.
- `docs/api.md` owns executable API/calculation contracts; `docs/architecture.md` owns
  design invariants; `docs/checkpoints.md` owns gates; `docs/capabilities.md` owns
  deferred preservation obligations; `docs/relations.md` owns relation guidance.
  Update the owner document rather than creating parallel authorities.
- One prepared catalog is the authority for schemas, references, memberships,
  calculations and access metadata. No duplicate schema/DSL, custom CLI, Python,
  Rust host or SurrealDB port is required.
- Preserve tenant isolation, exact comparison/arithmetic, stable occurrences,
  ordered monoid laws, final-state atomicity, protected absent/empty reads and
  source-derived independent checking. Never infer logarithmic whole-transaction
  cost from an AVL path or borrow predecessor evidence as fresh acceptance.
- Models are private by default. Fixture authority is not authentication or enforced
  query/mutation authorization. Admin never widens owner scope or grants Use.
  Keep both-owner/final-authority checks and summary/member disclosure distinct.
- Verify changed behavior with appropriate independent tests. Record runtime,
  command, source identity and limits in fresh checkpoint evidence. Commit coherent
  verified checkpoints and follow the user's current push/continuation instruction.
- Native predecessor files, services, adapters and historical tests are linked
  references, not local implemented capabilities. No arbitrary fixed commit count
  or assumed model/billing constraint applies.
