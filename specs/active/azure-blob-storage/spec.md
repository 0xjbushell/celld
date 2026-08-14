# Native Azure Blob Storage

Phase: v1

## Context and delivery bar

- This project adds Azure Blob Storage as a first-class celld fleet bucket so
  self-hosted operators can run entirely within Azure without an S3 or GCS
  dependency.
- Intended users are celld operators deploying standalone fleets on Azure
  Linux virtual machines or equivalent environments with private peer
  networking and local block storage.
- The supported operating environment is a standard Azure Blob container in a
  general-purpose v2 storage account. Public Worker ingress may be exposed, but
  the fleet bucket, credentials, and internal peer/operator listener remain
  private.
- The v1 safety bar preserves celld's existing one-writer fencing and RPO=0
  acknowledged-write guarantees. Missing CAS tokens, ambiguous conditional
  writes, unsupported provider combinations, and failed replication must
  remain loud and fail closed.
- Delivery must remain suitable for upstream contribution: the implementation
  is based on the latest `denoland/celld` main branch and divided into focused
  patches that preserve existing S3/GCS behavior and avoid unrelated
  refactoring.
- Managed celld control-plane storage, automatic Azure provisioning, OneLake,
  Azure Files or BlobFuse for active SQLite, and formal qualification of Azure
  Stack, sovereign-cloud, and ADLS-specific endpoint behavior are deferred.

## Problem / desired outcome

Current celld fleets require S3-compatible storage or Google Cloud Storage.
Azure Blob has the consistency and ETag conditional-write semantics celld
needs, but the executable does not construct or configure the Azure backend.

Operators must be able to deploy, run, and diagnose a standalone celld fleet
against Azure Blob with the same ownership, replication, prefix isolation,
failure handling, and operational clarity provided by the existing S3 and GCS
backends.

## Scope and non-goals

In scope:

- A first-class Azure bucket scheme accepted by deploy, run, and diagnose.
- Standard Azure authentication through the official Azure SDK for Rust
  identity types, with Microsoft Entra managed identity as the recommended
  production path.
- Azure ETag conditional create and overwrite for ownership, leases, wake
  records, deployment pointers, and other fleet records.
- Azure-backed LTX upload, listing, restore, compaction, metadata, multipart
  upload, and deletion.
- Fleet key prefixes, provider-specific configuration validation, operator
  documentation, emulator coverage, real-Azure qualification, and Azure-hosted
  single-node and multi-node evidence.
- A focused upstream core-storage patch series followed by a separately
  reviewable official Azure SDK identity patch.

Non-goals:

- Replacing celld's single shared object-store coordination model.
- Introducing Cosmos DB, Table Storage, Blob leases, or an S3-to-Blob gateway.
- Adding managed control-plane enrollment or issued Azure credentials.
- Provisioning storage accounts, containers, identities, role assignments,
  private endpoints, virtual machines, or Kubernetes resources.
- Claiming production support from Azurite-only evidence.
- Upgrading the object-store dependency unless implementation evidence shows
  the pinned release cannot satisfy an acceptance criterion.
- Bundling fork-only specifications, architecture-study artifacts, Azure
  infrastructure provisioning, or a broad storage abstraction rewrite into
  the upstream patch.

## Settled behavior and design decisions

- `az://CONTAINER[/PREFIX]` selects Azure Blob Storage. Prefix normalization and
  isolation behave exactly as they do for current S3 and GCS fleets.
- Bucket strings from `CELLD_BUCKET`, `--bucket`, and `celld deploy --bucket`
  remain intact until the shared bucket parser. That parser is the sole owner
  of `s3://`, bare-S3, `gs://`, and `az://` scheme selection.
- The core backend resolves the storage account through the standard
  `AZURE_STORAGE_ACCOUNT_NAME` variable. It is required outside emulator mode;
  no new `--account` flag is added.
- `--endpoint` overrides the selected provider's endpoint. Without that flag,
  S3 uses `S3_ENDPOINT`, Azure uses `AZURE_STORAGE_ENDPOINT`, and GCS takes no
  endpoint. An `az://` bucket with `S3_ENDPOINT` set fails before I/O rather
  than consuming or silently ignoring the S3 setting.
- Azure ignores `--region`, `AWS_REGION`, and `AWS_DEFAULT_REGION`, as GCS does.
  AWS and Google credential variables are never consulted for an Azure bucket.
- Azurite is selected only by `AZURE_STORAGE_USE_EMULATOR=1`. In that mode the
  account defaults to `devstoreaccount1`, the blob endpoint comes from
  `AZURITE_BLOB_STORAGE_URL` with the object-store default
  `http://127.0.0.1:10000`, and `--endpoint` or `AZURE_STORAGE_ENDPOINT` is a
  configuration error because object-store 0.11.2 ignores them in emulator
  mode. `CELLD_AZURE_AUTH` must be unset; v1 emulator qualification uses the
  well-known Azurite account rather than a production credential mode.
- The existing storage-backend seam gains an Azure variant rather than adding
  a second bucket-reference abstraction or refactoring unrelated consumers.
- Implementation starts from the latest upstream `denoland/celld` main branch,
  not the v0.1 Azure fork and not fork-only documentation or specification
  commits.
- The upstream core is delivered as two focused patches: first, provider-safe
  LTX timestamp metadata configuration with no S3/GCS behavior change; second,
  the Azure backend through the current storage and replication seams.
- The core Azure patch uses the existing Apache Arrow `object_store` Azure
  builder, but configures only the account, endpoint/emulator, account-key, and
  SAS inputs selected by celld's contract. It must not inherit object-store's
  implicit managed-identity or Azure CLI fallback. The core does not add the
  official Azure SDK identity bridge.
- Outside emulator mode, `CELLD_AZURE_AUTH` is required and selects exactly one
  mode. Unknown modes, missing required values, or incomplete selected-mode
  configuration fail before any storage request. Variables belonging to an
  unselected mode are not consulted and never form a fallback chain.

| `CELLD_AZURE_AUTH` | Delivered by | Required configuration | Settled behavior |
| --- | --- | --- | --- |
| `account-key` | Core patch | `AZURE_STORAGE_ACCOUNT_KEY` | Shared-key authorization through `MicrosoftAzureBuilder`; no Entra credential is constructed. |
| `sas` | Core patch | `AZURE_STORAGE_SAS_KEY` | Percent-encoded SAS authorization through `MicrosoftAzureBuilder`; no Entra credential is constructed. |
| `managed-identity` | Identity follow-up | Optional `AZURE_CLIENT_ID` | No client ID selects the system-assigned identity; a client ID selects that user-assigned identity. Object-ID and resource-ID selectors are deferred. |
| `workload-identity` | Identity follow-up | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_FEDERATED_TOKEN_FILE` | Uses `WorkloadIdentityCredential`; the projected token file may rotate without process restart. |
| `client-secret` | Identity follow-up | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | Uses `ClientSecretCredential`; incomplete configuration does not fall through. |
| `developer-tools` | Identity follow-up | None beyond authenticated tooling | Explicitly constructs `DeveloperToolsCredential`, which tries Azure CLI and then Azure Developer CLI. It is never a production fallback. |

- Azure uses the blob ETag as its opaque CAS token. Conditional create maps to
  `If-None-Match: *`; conditional update maps to `If-Match`.
- A successful read, head, or conditional write without a nonempty ETag is an
  error. It must never produce an empty token.
- Conditional writes use a dedicated client with retries disabled. A clean
  precondition rejection remains distinct from an ambiguous failure that may
  have committed.
- Ordinary fleet operations retain the bounded control-plane timeout and retry
  policy. LTX replication uses a separately constructed Azure store and
  connection pool with retries enabled, following the current GCS construction
  seam; no additional retry or timeout values are invented without evidence.
- The provider-neutral LTX client remains the stable replication interface.
  Azure is injected as an object-store implementation rather than adding an
  Azure-specific replication algorithm.
- `ObjectStoreConfig` gains a `timestamp_metadata_key: String` field. Its
  `Default` value is `litestream-timestamp`; Azure sets
  `litestream_timestamp`. An explicitly empty value fails before an object
  write, and existing S3/GCS metadata compatibility is not degraded.
- `LtxRepl` constructs one provider-specific store and one cloneable node-level
  `ObjectStoreConfig`; each per-cell client changes only its path before
  calling `ObjectStoreClient::with_store`. Azure construction must not reuse
  the S3-only `node_config` environment/credential path.
- `MicrosoftAzure` in object-store 0.11.2 does not override the public
  `ObjectStore::list_with_offset`; the trait default performs a full listing
  followed by client-side offset filtering. This is the accepted Azure v1
  correctness path. Code must not call the internal Azure list client with an
  offset, and qualification records the retained-history scan cost.
- Standard object keys, epoch prefixes, seals, deployment publication order,
  ownership records, wake records, and peer-authentication records are
  unchanged.
- Microsoft Entra authentication uses the stable official Azure SDK for Rust
  `azure_identity` credential implementations through an adapter from
  `azure_core::credentials::TokenCredential` to
  `object_store::azure::AzureCredentialProvider`. The design must not claim a
  Rust `DefaultAzureCredential`, because the current SDK does not provide one.
- Official Azure SDK identity support remains required for v1, but is delivered
  as a separate follow-up patch after the core backend so credential caching,
  refresh, and precedence do not obscure review of storage correctness.
- The supported Entra modes are system-assigned and user-assigned managed
  identity, workload identity federation, service-principal client secret, and
  an explicitly selected developer-tools chain using Azure CLI followed by
  Azure Developer CLI. Developer credentials are never an implicit production
  fallback.
- The identity adapter requests exactly one scope,
  `https://storage.azure.com/.default`, and returns
  `object_store::azure::AzureCredential::BearerToken`.
- The adapter caches `AccessToken` values until five minutes before expiry,
  permits only one in-flight refresh, and rechecks the cache after acquiring
  the refresh lock. If refresh fails while the cached token is still valid, it
  may continue using that token; once expired, refresh failure propagates.
- The ordinary and CAS builders share one adapter instance and token cache.
  The separately constructed replica store owns a second instance, preserving
  the current separation between control-plane and replication traffic.
- Account key and SAS remain explicitly selected non-Entra authorization modes
  and never construct or consult an Azure SDK credential.
- Documentation recommends managed identity with container-scoped Blob data
  permissions and identifies workload identity as the Kubernetes production
  path.
- Azurite is a fast local compatibility target, not the behavior oracle for
  cloud consistency, metadata, authentication, or failure semantics.
- Standalone fleets are supported in this phase. Managed/control-plane mode
  rejects Azure explicitly until its external storage schema and credential
  lifecycle support Azure.

## Context and evidence

- [`docs/fencing.md`](../../../docs/fencing.md) defines the conditional-write,
  acknowledgement, epoch, and restore-seal invariants.
- [`docs/testing.md`](../../../docs/testing.md) defines the compatibility,
  simulation, and live-fleet evidence model.
- [`denoland/celld#139`](https://github.com/denoland/celld/issues/139) is the
  existing upstream feature request for native Azure Blob support and records
  the same metadata, CAS, credential, and block-upload concerns.
- [`denoland/celld#145`](https://github.com/denoland/celld/issues/145) is a
  SeaweedFS-specific proposal to run the existing S3-shaped CAS contract in CI.
  It reinforces the value of live storage-contract evidence but is not Azure
  qualification or a general maintainer endorsement.
- Upstream exposes only `main`, disables pull requests and Discussions, and
  documents focused `git format-patch` email submissions to Ryan Dahl. No
  public upstream Azure branch or implementation was found.
- v0.2.0 established the current GCS-era backend seam, while `celld-ltx` has
  declared an Azure object-store feature since v0.0.2.
- Current S3 and GCS behavior provides the provider-parity oracle.
- Azure Blob concurrency documentation establishes strong read-after-write and
  list consistency, ETag updates, `If-Match`, `If-None-Match`, and HTTP 412
  precondition failures.
- Apache Arrow Rust `object_store` 0.11.2 maps Azure create/update modes to
  those conditions, requires ETags on GET/HEAD, preserves metadata on
  multipart completion, accepts a custom Azure credential provider, and uses
  client-side filtering for public `list_with_offset`.
- The current stable official Azure SDK for Rust release is
  `azure_identity` 1.0.0, released 2026-05-11. It exposes explicit managed
  identity, workload identity, client-secret, Azure CLI, Azure Developer CLI,
  and developer-tools credential types. It does not expose
  `DefaultAzureCredential`; the 1.1.0 beta is not selected for v1 without
  implementation evidence requiring it.
- `crloz/celld@c594c78db327ff86ba5eb258beac4ef1c570c41a` demonstrates a v0.1
  prototype and Azurite test shapes, but predates current GCS support and is not
  the implementation baseline.

## Traceability

Key: `azure-blob-storage`

Spec path: `specs/active/azure-blob-storage/spec.md`

Requirement refs: `Problem / desired outcome`; `Scope and non-goals`;
`Settled behavior and design decisions`; `Acceptance criteria`

Context refs: `docs/fencing.md`; `docs/testing.md`; `docs/limitations.md`

Exploration refs:
`/home/jb/.copilot/session-state/d867572c-0a94-4611-98c1-12ae0463f63f/files/celld-azure-storage-exploration.md`

Decision / ADR refs: None

Evidence refs: Microsoft Azure Blob concurrency, conditional-header, Entra,
RBAC, and SAS documentation; official Azure SDK for Rust `azure_identity`
1.0.0 release and credential sources as verified 2026-08-13; Apache Arrow Rust
`object_store` 0.11.2 Azure adapter;
`denoland/celld#139`; `denoland/celld#145`; upstream contribution policy and
public branch/fork search; `crloz/celld@c594c78db327ff86ba5eb258beac4ef1c570c41a`

Tracker parent/ref:
[`0xjbushell/celld#1`](https://github.com/0xjbushell/celld/issues/1)

## Stable seam and testing

The stable seam is the current fleet storage backend plus the provider-neutral
object-store replication client. Azure must satisfy the same observable bucket
and LTX contracts as S3 and GCS without changing coordination decisions.

Configuration ownership remains local to the storage seam: the shared bucket
parser chooses `StorageBackend`; provider construction resolves endpoint,
authentication, retry, and token dialect; `ObjectStoreClient::with_store`
receives the already-built replica adapter.

The delivery seam is a two-patch core series followed by an independent
identity patch. Each patch must apply to a clean upstream-main baseline and
remain reviewable without fork-only files or unrelated architecture changes.

The behavior oracle is:

- Existing S3/GCS unit and integration behavior remains unchanged.
- Generic replica-client conformance passes against Azure.
- Real Azure conditional-write tests distinguish applied, rejected, and
  ambiguous outcomes.
- Live-fleet verification proves that acknowledged state survives owner loss
  and local database deletion and that one epoch never has two authoritative
  writers.

Azurite covers parsing, construction, basic operations, CAS shape, metadata,
listing, multipart, and replica conformance where supported. A real Azure
container covers service consistency, pagination, authentication, error
mapping, and fault behavior.

## Acceptance criteria

- [ ] Deploy, run, and diagnose accept `az://CONTAINER[/PREFIX]` and report the
  Azure scheme consistently.
- [ ] Azure account and endpoint configuration are documented, provider-aware,
  and reject conflicting S3/GCS settings.
- [ ] Outside Azurite, `CELLD_AZURE_AUTH` accepts only the six settled values
  and enforces the exact required variables and no-fallback behavior in the
  authentication table.
- [ ] Azurite uses `AZURE_STORAGE_USE_EMULATOR=1` and
  `AZURITE_BLOB_STORAGE_URL`; incompatible endpoint settings fail before I/O.
- [ ] The core implementation is based on latest upstream main and can be
  emitted as a focused two-patch `git format-patch` series without fork-only
  specifications, study artifacts, or a broad storage abstraction rewrite.
- [ ] Existing S3 and GCS fleets, prefixes, credentials, metadata, and tests
  retain their behavior.
- [ ] Azure reads, heads, and successful conditional writes return nonempty
  ETag tokens; missing tokens fail closed.
- [ ] Real Azure conditional create, duplicate-create rejection, update, and
  stale-update rejection satisfy the existing CAS contract.
- [ ] Generic LTX conformance passes against Azure, including paginated
  listing, bounded seek behavior, exact reads, metadata, subset deletion, and
  cleanup.
- [ ] Azure bounded seek uses object-store's public client-side offset fallback
  without panic or incorrect results, and qualification records the scan cost
  over a retained-history fixture.
- [ ] Azure single-put and multipart LTX objects round-trip exact bytes and
  provider-compatible timestamp metadata.
- [ ] Microsoft Entra tokens are obtained through official
  `azure_identity` credential implementations of
  `azure_core::credentials::TokenCredential` and adapted to the object-store
  credential provider with the settled single scope and refresh policy.
- [ ] System-assigned managed identity, user-assigned managed identity,
  workload identity federation, service-principal client secret, and the
  explicit Azure CLI-to-Azure Developer CLI developer chain satisfy their
  documented selection behavior.
- [ ] Account key and SAS modes remain separate from Entra selection, and
  malformed explicit modes do not fall back to another credential.
- [ ] Official Azure SDK identity integration remains a separately reviewable
  follow-up that does not change the accepted Azure CAS, prefix, or replication
  contracts.
- [ ] A standalone node authenticates with Microsoft Entra managed identity
  and container-scoped Blob data permissions without account keys.
- [ ] A one-node Azure-hosted deployment can publish and serve the counter
  example with its active SQLite directory on local Azure block storage.
- [ ] A multi-node Azure-hosted fleet survives forced owner loss and deletion
  of that owner's local database with no lost acknowledged state and no
  duplicate authoritative writer.
- [ ] Azure throttling, temporary unavailability, and conditional-write
  transport ambiguity preserve self-fencing and error semantics.
- [ ] Documentation describes supported account/container types, credentials,
  prefix behavior, private networking, local-disk requirements, emulator
  limitations, and deferred managed-control-plane support.
- [ ] The upstream submission cover letter references `denoland/celld#139`,
  explains why the v0.1 fork was not cherry-picked, and records real Azure CAS,
  LTX, restart, and regression evidence.
