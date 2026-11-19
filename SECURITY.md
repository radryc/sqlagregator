# Security model

This document describes who is trusted to call what, and which flags are
required before exposing the federated SQL surface.

## Trust boundaries

```text
operator ──► sqlag CLI / library ──► Omni / BigQuery / ClickHouse
                    (backend credentials only)

user ──► Doctor ──► MonoFS router ──► MonoFS nodes ──► Parquet/ClickHouse
          (auth +    (policy gate,     (internal token,
           policy)    fail-closed SQL)  view-enforced scope)
```

| Surface | Authentication | Authorization | Notes |
|---|---|---|---|
| `sqlag` library | none | caller-supplied `Scope` | **Not user-facing.** The backend credentials are the boundary. |
| `sqlag` CLI | none | none | Operator tool. Never run it with production credentials on behalf of users. |
| Router `QuerySQL` / `QueryDuckDBSQL` | gRPC bearer token (`MONOFS_GRPC_REQUIRE_TOKEN`, default true) | fail-closed SQL policy (see below) | The router is the policy enforcement point. |
| Router other telemetry reads | gRPC bearer token | follows `--access-enforce` (default observe) | Set `--access-enforce=true` to filter logs/traces on these paths. |
| Node `QueryDuckDBSQL` / `QueryPartial` | cluster-internal token | request scope, enforced inside the dataset view | Internal-only. A direct caller cannot assert an unrestricted scope. |
| Doctor `/v1/sql/query` | Doctor session/bearer | `PolicyGate.VisiblePartitions` (fails closed when a gate is configured) | Without a policy gate Doctor forwards an unrestricted scope. |

## Fail-closed SQL on the router

`QuerySQL` and `QueryDuckDBSQL` do **not** follow the observe-mode behavior of
the other read paths. They call `querySQLScope`, which:

1. Allows break-glass admins and policy admins.
2. Allows the query when a loaded access policy yields the caller's visible
   partitions (even when `--access-enforce=false`).
3. Denies with `PermissionDenied` when no policy source is configured or the
   policy has not loaded.
4. Intersects the router's partitions with any restriction forwarded by Doctor
   (a Doctor restriction is preserved, never widened).
5. Keeps metrics unrestricted, matching the existing Strata policy.

The only opt-out is the router flag:

```text
--sql-require-policy=false     # development clusters only
```

## Node SQL RPCs are internal-only

`QueryDuckDBSQL` and `QueryPartial` accept generated SQL and a partition scope.
Both are gated by `requireInternalSQL`:

- The node must have `--internal-token` (or `MONOFS_TOKEN`) configured.
  Otherwise the RPCs return `FailedPrecondition` and SQL is disabled.
- The router attaches `x-monofs-internal: <token>` on every router → node dial
  (including telemetry reconnects).
- A caller without the token gets `PermissionDenied`, even with a valid user
  token. The scope in the request is never trusted on its own.

The token crosses the network: use TLS/mTLS between the router and nodes
(`STRATA_TLS_*` / the transport configuration).

## Required deployment settings

| Component | Setting | Why |
|---|---|---|
| router | `--internal-token` / `MONOFS_TOKEN` | Lets nodes accept router SQL calls; also authenticates control-plane traffic. |
| router | access policy file/URL | SQL fails closed without it. |
| router | `--access-enforce=true` | Applies partition filtering to non-SQL telemetry reads. |
| node | `--internal-token` / `MONOFS_TOKEN` | Enables SQL RPCs and validates the router. |
| node | ClusterIP-only, no public gRPC | Nodes trust the router for scope; never expose them. |
| Doctor | policy gate configured | Without it, `/v1/sql/query` forwards an unrestricted scope. |

## Ingestion identity (implemented)

- Guardian pushers overwrite `guardian.partition`, `guardian.intent`, and
  `guardian.asset` in `OTEL_RESOURCE_ATTRIBUTES`; a workload cannot claim
  another partition by setting its own resource attributes.
- The generated AWS collector config upserts `guardian.partition` in every
  pipeline (`resource/guardian`), so telemetry passing through a shared
  collector is re-stamped with the collector's partition.
- Backends should still validate the partition at ingest; the producer and
  collector controls are the first two layers.
- Omni and Logs Insights placements support `assume_role`: the router assumes
  the placement's role ARN before querying, so one router can read several
  member accounts. The base credential still needs `sts:AssumeRole` on that
  role. Without `assume_role` the router's ambient AWS credential is used.
- BigQuery uses a mounted service-account/ADC file; the router only needs job
  creation and dataset read access in the configured project.

## Remote engines

- **ClickHouse:** the coordinator injects the partition predicate when the
  logical table has a `partition` column; otherwise a restricted query is
  refused unless the endpoint declares `enforces_partition_scope`.
- **BigQuery:** same rule; the partition filter is injected into the query.
- **Omni:** schemaless. If the catalog cannot map a trustworthy partition
  field, restricted queries fail with `scope_not_enforceable`. Use one Omni
  space per tenant, or map a real partition field. Unrestricted calls still use
  whatever the AWS credentials can see.
- Direct drivers (CLI) never add identity-based restrictions; the credentials
  define the blast radius.

## Known limitations

- **Metrics are unrestricted** by the Strata policy (`VisiblePartitions`
  returns all for metrics). If metric data is tenant-sensitive, add a policy
  rule or keep metrics out of the shared surface.
- **Other read paths are observe-by-default.** `--access-enforce=true` is
  required for logs/traces queried through `QueryLogs`/`QueryTraces` and the
  streaming RPCs. SQL is fail-closed regardless.
- **Doctor without a policy gate** is unrestricted. This is pre-existing and
  is the reason the gate should be mandatory in production.
- **The CLI/library has no user model.** Do not expose it as a service.

## Verification checklist

```sh
# Router: SQL denied without a policy
grpcurl -plaintext -H "authorization: Bearer $USER_TOKEN" \
  -d '{"dataset":"logs","query":"SELECT count(*) FROM logs"}' \
  router:9090 monofs.MonoFSRouter/QuerySQL
# expect PermissionDenied

# Node: SQL denied without the internal token
grpcurl -plaintext -H "authorization: Bearer $USER_TOKEN" \
  -d '{"dataset":"logs","sql":"SELECT 1"}' \
  node:9091 monofs.MonoFS/QueryPartial
# expect PermissionDenied
```
