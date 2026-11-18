# Agent instructions

Read the [current handoff](CONTEXT.md) and the [R1–R5 roadmap](docs/checkpoints.md)
before choosing work. Start with the first unmet acceptance gate. Check Git
status and the current commit before editing.

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
- Address new indexes by `[declaringModel, partitionFields[], orderFields[]]`.
  Keep both ordered arrays intact. Stable occurrence slots identify repeated
  participation. Verify derived state against independently reconstructed source facts.
- Models are private by default. The authorized entry points enforce fixture
  policy. Raw APIs are privileged. Check tenant, action and source owner, including
  both owners on transfer and final authority. Admin grants ordinary actions within
  owner scope. Use requires an explicit grant. An anchor summary discloses only
  the fields its publication contract permits.
- Verify changed behavior with appropriate independent tests. Record runtime,
  command, source identity and limits in fresh checkpoint evidence. Commit coherent
  verified checkpoints and follow the user's current push/continuation instruction.
- Native predecessor files, services, adapters and historical tests are linked
  references, not local implemented capabilities. No arbitrary fixed commit count
  or assumed model/billing constraint applies.
