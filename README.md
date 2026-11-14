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
updates and reactive settlement. Raw batched windows and fixture owner authority
are available. Hydrated authorized owner-stream reads, authorized mutations and
cross-request cursors remain unfinished. This is a local engine, not a server or
production persistence implementation.

Fresh migration verification: **171 tests pass**, including reference/junction
examples and owner-policy fixtures. Dependency audit reports zero vulnerabilities.
MIT licensed; see [LICENSE](LICENSE). The Node version used for verification is in
[.nvmrc](.nvmrc), and the standard test workflow is checked in for CI.

Read [current handoff](CONTEXT.md), the [API guide](docs/api.md),
[architecture](docs/architecture.md), [mathematics](docs/index-mathematics.md),
[relations](docs/relations.md), [capability preservation](docs/capabilities.md) and
[checkpoint gates](docs/checkpoints.md). The API guide owns function and calculation
contracts; the handoff owns current acceptance and next work.
