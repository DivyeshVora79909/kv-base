# KV-Base

A small Node ESM engine built around partitioned augmented AVL indexes, exact ordered
monoids, declarative model bindings and transactional incremental computation.
This repository contains the Node redesign extracted from Next-Base. Rust is a
historical contract reference; no native build, Python runtime, SurrealDB dependency
or custom application CLI is required.

Use Node 24+:

```sh
npm ci
npm test
node
```

From the repository-root Node REPL:

```js
const engine = await import('./src/index.mjs');
const example = await import('./examples/structural.mjs');
const catalog = engine.prepareCatalog(example.structuralActivityDefinition);
const store = engine.createMemoryStore();
await engine.mutate(store, catalog, 'tenant',
  example.activityRecords().map(record => ({ type: 'put', record })));
await engine.checkStore(store, catalog, 'tenant');
const scope = { tenant: 'tenant', index: example.activityIndexes.resourceEffective, partition: ['resource'] };
await store.read(tx => engine.treeAggregate(tx, catalog, scope));
```

The serialized volatile adapter supports atomic source/index/reference/uniqueness
updates and reactive settlement. Fixture-authorized owner-stream reads, reverse
reference discovery and mutations now compose the same transaction boundary.
Structural declarations, composite ordering, authored reference paths, selected
anchor publication, owner queries and mutations are implemented. The importable
`createEngine` lifecycle supplies volatile continuations and checked generation
rebuilds; population, inspection and seeded workloads use the same prepared catalog.
Storage and fixture policy remain serialized and volatile.

Fresh R1–R5 verification: **272 tests pass** on Node 26.12.0, zero failed/skipped.
See [R5 evidence](docs/evidence/2026-11-19-r5.json) for source hashes, commands,
measured workload and limits. MIT licensed; see [LICENSE](LICENSE).
[.nvmrc](.nvmrc) selects the Node 24 baseline. Hosted CI results are separate from
this local verification. Durable storage, authentication, online migration and
operational capacity retain separate gates.

Read [current handoff](CONTEXT.md), the [API guide](docs/api.md),
[architecture](docs/architecture.md), [mathematics](docs/index-mathematics.md),
[relations](docs/relations.md), [capability preservation](docs/capabilities.md) and
[checkpoint gates](docs/checkpoints.md). The API guide owns function and calculation
contracts; the handoff owns current acceptance and next work.
