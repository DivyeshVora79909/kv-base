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
const example = await import('./examples/activity.mjs');
const catalog = engine.prepareCatalog(example.activityDefinition);
const store = engine.createMemoryStore();
await engine.mutate(store, catalog, 'tenant',
  example.activityRecords().map(record => ({ type: 'put', record })));
await engine.checkStore(store, catalog, 'tenant');
const scope = { tenant: 'tenant', view: 'resourceEffective', partition: ['resource'] };
await store.read(tx => engine.treeAggregate(tx, catalog, scope));
```

The serialized volatile adapter supports atomic source/index/reference/uniqueness
updates and reactive settlement. Fixture-authorized owner-stream reads, reverse
reference discovery and mutations now compose the same transaction boundary.
Cross-request cursors and explicit summary publication remain unfinished. This is
a local engine with fixture policy enforcement, not production authentication or
persistence.

Fresh JS-P4b.2 verification: **206 tests pass** on Node 26.12.0, including independent
authorized-read and atomic-mutation witnesses. See [evidence](docs/evidence/2026-11-14-js-p4b2.json).
Dependency audit reports zero vulnerabilities. MIT licensed; see [LICENSE](LICENSE).
[.nvmrc](.nvmrc) selects the Node 24 baseline; the standard test workflow is checked
in for CI. Hosted CI results are separate from this local verification.

Development starts at R1, model-declared structural indexes. The example above
uses the named-view API available at JS-P4b.2.

Read [current handoff](CONTEXT.md), the [API guide](docs/api.md),
[architecture](docs/architecture.md), [mathematics](docs/index-mathematics.md),
[relations](docs/relations.md), [capability preservation](docs/capabilities.md) and
[checkpoint gates](docs/checkpoints.md). The API guide owns function and calculation
contracts; the handoff owns current acceptance and next work.
