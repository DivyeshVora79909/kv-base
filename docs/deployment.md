# Self-hosted Linux operations

The selected topology is one Node service owning write ingress and its effect worker,
over FoundationDB's ordered serializable adapter. Disjoint transactions remain
concurrent. SQLite is an optional serialized engine adapter. The reusable APIs are
specified in [api.md](api.md); gates and actual limits are in
[checkpoints.md](checkpoints.md) and the [handoff](../CONTEXT.md).

Use Node24+ and the locked dependencies (`npm ci`). Fresh local evidence uses
Node26.12.0, FoundationDB npm binding2.0.1/API720 and official client/server7.3.81.
Install the compatible native client library on the Linux host and supply an
explicit cluster file. Set the native library search path when required. The
application namespace is a distinct operator-selected prefix in that cluster.
Database replication and power-loss evidence are separate from this local release.

Keep the service user's external keyring, deployment descriptor and configuration
outside the release and backups. Keyring/descriptor files are regular, privately
owned mode0600 files; the object root and its subdirectories are mode0700. Retain all
keys needed by encrypted credentials, proofs, effect requests and original receipts.
The keyring file is `{version:'1',activeKey,keys:{keyId:base64Of32Bytes}}`. Generate
keys once with Node `randomBytes(32)` and persist them privately. Recreating keys
does not recover existing encrypted state.

Prepare one catalog using the application's ordinary definition. For the executable
demonstration use `pipelineDefinition()` from [pipeline.mjs](../examples/pipeline.mjs).
The external deployment descriptor is:

```js
{
  version: '1', clusterFile: '/etc/foundationdb/fdb.cluster',
  prefix: 'operator-selected-namespace', objectRoot: '/var/lib/kv-base/objects',
  tenant: 'operator-tenant', catalogIdentity: catalog.identity,
  incarnation: randomUUID()
}
```

`writeDeployment` creates the initial descriptor; restore publishes a new one.
Open the installation with `openSelfHosted`, supplying that descriptor/file, one
catalog, the private keyring file, username mapping, operator contacts, delivery
channels and provider clients. The opener admits no HTTP or workers by default.
`bootstrap` is an explicit operator call with one original operation ID, authority
policy, enrolled credentials, initial ordinary sources and provider credentials.
It retains private request identity and resumes its original phases after restart;
a different request cannot take over the same initialized namespace. Provider
credentials require a delivery principal with explicit Credential Use. No public
bootstrap, enrollment, policy or secret endpoint exists.

After bootstrap, call `start({host:'127.0.0.1',port:8080})`. A service manager can run
an operator-owned ordinary ESM entry module that imports these functions and the
private configuration; no custom application CLI is needed. Handle SIGTERM/SIGINT
with `await runtime.close()`. The runtime stops admission and drains admitted HTTP,
uploads and registered provider work before closing storage. Health exposes only a
sanitized status. Keep privileged source/worker/inspection APIs inside that process;
use the runtime's registered worker admission when serving requests.

Expose the loopback service through the host's chosen TLS proxy and keep one active
write ingress. Public routes use Bearer sessions, fixed model allowlists and
published summaries. Operator error logging receives sanitized codes. Browser
Origin requests are outside the current service contract. The concrete fake
Resend/Brevo transports support local pipeline tests; live delivery requires real
operator credentials and its own protocol/retention acceptance.

`prepareRelease({repository,directory,sourceIdentity:revision,catalogIdentity})`
copies a checked source inventory, owner docs, importable examples and npm lockfile
to a separate directory. `checkRelease` verifies it against `RELEASE.json`; its
source identity contains revision, source hash, lock hash and catalog identity.
External configuration, custody, object bytes and installed dependencies are
excluded. Run ordinary `npm ci` and `npm pack` in the checked release directory.
The generated `RELEASE-LOCK.json` preserves the original lock bytes across npm's
archive exclusions; `checkRelease` requires it and compares it with the original
lock whenever both are present. For deployment, extract the archive into a separate
release directory, check it, copy `RELEASE-LOCK.json` to `package-lock.json`, run
`npm ci` there and check again. Start the entry module from that directory so it
resolves the locked dependencies. The local smoke verifies every resolved dependency
against that lock and starts/stops/reopens a subprocess from the extracted package.
Installing the package inside another application's dependency tree requires that
host's own lock and is outside this installation procedure. This is not a remote
deployment or a claim that arbitrary native binary builds are identical.

Back up with `writeBackup`, passing the runtime, object root, unused backup
directory, keyring, incarnation, checked release source identity and
`quiesce: callback => runtime.quiesce(callback)`. Quiescence stops ingress and
drains workers/uploads for the complete operation, then closes the runtime. It
does not silently reopen the old installation after restore. The backup contains
a signed manifest, exact selected-namespace data/receipt/format bytes, and hashed
ready/staged files. Checks are bounded full inventories; native transaction limits
also apply. This initial snapshot is a small offline backup, not FoundationDB's
large online backup system. Reopen the selected installation explicitly after a
completed backup when continuing service.

Restore into a new empty FoundationDB namespace and a new object root. Call
`prepareRestoration` with the signed backup, matching catalog, verified target
adapter identity and a candidate runtime opener. The candidate remains offline;
checks compare complete source/derived/file state and exact original receipts.
Never restore over the selected namespace. A partial or faulted candidate stays
unselected and must be discarded or investigated by the operator.

`activateRestoration` requires the verified process-local handle, deployment file,
original operation ID and `stopPrevious` callback. It drains/stops the old runtime,
quiesces the candidate through final checks and publication, then publishes one
fsynced external descriptor with a
fresh incarnation. A lost activation acknowledgement recovers the already
published descriptor using the original operation identity. Reopen that selected
descriptor and start service explicitly. Old sessions/proofs/upload grants/cursors
reject; retained credentials can authenticate again.

Restored pending effects cannot assume a send never happened: an external operation
may have succeeded after the backup. Before new dispatch, original-key provider
lookup must fence delayed obsolete starts and advance past the external floor.
Lease tokens also bind the current incarnation. Unsupported or unsafe restored
work is isolated. Live idempotency headers alone do not establish indefinite
deduplication across backup age. Do not replace the original provider operation key.

The measured workload reports dataset/seed/runtime, latency, source scans, tree
visits, logical staged/publication bytes and native client key/value bytes separately.
RSS/CPU and object/native storage observations are local measurements. Client byte
counters exclude disk amplification, network framing and replication. No larger
capacity, multi-host writer, server-failure or power-loss guarantee follows from
these small local fixtures. Online migration, recurrence, frontend and full domain
suites retain separate gates.
