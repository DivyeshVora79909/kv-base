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
FoundationDB is the durable primary behind the same ordered transactional adapter.
One Node service accepts writes through concurrent serializable transactions;
SQLite remains an optional serialized adapter. Encrypted identity sessions,
privately delivered email/phone proofs and durable provider effects compose the
same catalog and owner policy. The memory adapter remains volatile.

Private local object grants, streaming upload/finalize/download and fixed
authenticated HTTP routes are implemented. The [pipeline example](examples/pipeline.mjs)
shares one ordinary catalog for ledger calculations, typed object attachments and
private provider delivery. Signed complete offline backup/restore, restore fencing,
explicit bootstrap, draining shutdown and a checked self-hosted Linux source release
are implemented. Fresh D5 verification passed526 tests, zero failed/skipped, including
an extracted, locked-install subprocess start/stop/reopen.

Post-D5 review corrections passed606 tests with zero skips/failures on Node24.22.1
against a real FoundationDB target and an installed release. R1–R5, D1–D5 and
corrective verification are recorded in the
[handoff](CONTEXT.md) and [checkpoint evidence](docs/checkpoints.md). Primary-target
tests require an owned FoundationDB cluster and its native client library:

```sh
KV_BASE_FDB_CLUSTER_FILE=/path/to/fdb.cluster npm test
```

Set the native library search path if `libfdb_c.so` is not installed system-wide.
Absent a selected cluster, real-target tests skip and do not establish acceptance.
The installed release smoke also requires `KV_BASE_RELEASE_PACKAGE` pointing to
a checked extracted package after its locked `npm ci`; see
[Linux deployment](docs/deployment.md) for release and custody setup.
Hosted CI additionally runs `node test/helpers/verified-environment.mjs`, creating
its own native cluster and locked installed release and rejecting skipped tests.
Run that same helper locally with absolute `KV_BASE_FDB_SERVER` and
`KV_BASE_FDB_CLI` paths to the verified7.3.81 artifacts and its native library path.
MIT licensed; see [LICENSE](LICENSE).
[.nvmrc](.nvmrc) selects the Node 24 baseline. Hosted CI results are separate from
this local verification. Live provider delivery, replicated failure, power loss,
online migration and larger operational capacity retain separate evidence.

Read [current handoff](CONTEXT.md), the [API guide](docs/api.md),
[architecture](docs/architecture.md), [mathematics](docs/index-mathematics.md),
[relations](docs/relations.md), [capability preservation](docs/capabilities.md) and
[checkpoint gates](docs/checkpoints.md). The API guide owns function and calculation
contracts; the handoff owns current acceptance and next work.
