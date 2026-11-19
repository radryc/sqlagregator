# Partition definition, shard routing, and limits — proposal

Status: **Phases 1–5 implemented** (policy/compiler, bindings/predicates,
shard routing, Omni placement + ingestion hardening, CloudWatch Logs Insights
SQL). All four federated log backends (Omni, Logs Insights, BigQuery,
`monofs_local`) have resolvers, and the divisions demo exercises them per user.
Related: `PARTITIONING.md` for how the partition attribute travels through
ingestion; `PARTITION-POLICY.md` supersedes its routing/limit sections.

---

## 1. The correction: partitions have shards

A partition is **not** one backend target. It is the unit of **access, quota,
and attributes**. Its data is split into **shards** — disjoint slices by
platform/account/region/time. Each shard has one or more **placements** (backend
copies) and exactly one is used per query unless it fails.

```text
Partition: observability            # one access/quota identity
  ├── Shard: k8s-eu        (logs)  → MonoFS/ClickHouse
  ├── Shard: aws-account-A (logs)  → Omni (primary) + Logs Insights (replica)
  ├── Shard: aws-account-B (logs)  → Logs Insights
  ├── Shard: gcp-prod      (logs)  → BigQuery
  ├── Shard: aws-A-traces  (traces)→ Omni
  └── Shard: gcp-metrics   (metrics)→ CloudWatch Metrics (PromQL)
```

A query for the partition fans out to **all shards** for the signal, emits the
right dialect for each shard's placement, and merges the partials. This is how
one partition can be k8s + GCP + AWS, and how two AWS accounts under the same
partition can use different backends.

### The three levels

| Level | Answers | Used for |
|---|---|---|
| **Partition** | Who may read it? What are its attributes and quota? | access, selectors, limits |
| **Shard** | Which disjoint slice of the data is this? | coverage, fan-out, merge |
| **Placement** | Where does this shard live, and how is it queried? | backend, source, dialect, partition binding |

Rules:

- Shards for one `(partition, signal)` must be **disjoint**. Put copies of the
  same data in the same shard as multiple placements, not in two shards.
- Within a shard, exactly one placement is primary; the others must declare
  `replica_of` the primary. Fallback is **per shard**.
- Never substitute another shard's data for a failed shard. That is a different
  slice, not a replica.
- Coverage is complete only when every shard answered (or a replica answered
  for it).
- The earlier `routes` map is removed. Backend choice lives on the shard's
  placements. A `route` is effectively “the primary placement of shard X”.

## 2. Partition definition (per partition + global overrides)

Definitions live with each partition, with global defaults. That matches how
Guardian already owns partition manifests.

```text
guardian/partitions/defaults.yaml            # global overrides
guardian/partitions/<name>/partition.yaml    # per-partition definition
```

Per partition:

```yaml
# guardian/partitions/observability/partition.yaml
name: observability
attributes:
  role: observability
  environment: prod
  data_classification: internal
quota:
  profile: medium

shards:
  - name: k8s-eu
    signal: logs
    platform: kubernetes
    attributes: {region: eu-west-1, runtime: k8s}
    placements:
      - id: monofs
        backend: monofs_local
        source: logs
        partition_binding: {kind: column, column: partition}

  # One AWS account, two copies: Omni primary, Insights fallback.
  - name: aws-a
    signal: logs
    platform: aws
    account: "111111111111"
    region: us-east-1
    attributes: {region: us-east-1, account: "111111111111"}
    placements:
      - id: omni-primary
        backend: omni
        source: logs.default
        log_groups: ["/strata/observability"]
        partition_binding: {kind: log_group, log_group_field: "@logGroupName"}
      - id: insights-replica
        backend: cloudwatch_logs_insights
        log_groups: ["/strata/observability"]
        replica_of: omni-primary
        partition_binding: {kind: log_group}

  # A second AWS account, different backend, different shard.
  - name: aws-b
    signal: logs
    platform: aws
    account: "222222222222"
    region: us-west-2
    attributes: {region: us-west-2, account: "222222222222"}
    placements:
      - id: insights
        backend: cloudwatch_logs_insights
        log_groups: ["/strata/observability"]
        indexed_fields: {region: us-west-2}
        partition_binding: {kind: log_group}

  - name: gcp-prod
    signal: logs
    platform: gcp
    project: strata-prod
    attributes: {region: europe-west1, platform: gcp}
    placements:
      - id: bq
        backend: bigquery
        source: strata_prod.telemetry.logs
        partition_binding: {kind: column, column: partition}

  - name: aws-a-traces
    signal: traces
    platform: aws
    account: "111111111111"
    region: us-east-1
    placements:
      - id: omni
        backend: omni
        source: traces.default
        partition_binding:
          kind: attribute
          path: ["resource", "attributes", "guardian.partition"]

  - name: gcp-metrics
    signal: metrics
    platform: gcp
    placements:
      - id: cloudwatch-metrics
        backend: cloudwatch_metrics
        account: "111111111111"
        region: us-east-1
```

Global overrides:

```yaml
# guardian/partitions/defaults.yaml
defaults:
  quota: {profile: small}
  attributes:
    environment: prod
  placement_defaults:
    logs:
      - backend: monofs_local
        source: logs
        partition_binding: {kind: column, column: partition}
  backend_defaults:
    omni:
      region: us-east-1
    cloudwatch_logs_insights:
      indexed_fields: {region: us-east-1}

  # Optional templates for the common platform/account shapes.
  shard_templates:
    aws-logs:
      platform: aws
      signal: logs
      placements:
        - id: insights
          backend: cloudwatch_logs_insights
          log_groups: ["/strata/{partition}"]
          partition_binding: {kind: log_group}
    k8s-logs:
      platform: kubernetes
      signal: logs
      placements:
        - id: monofs
          backend: monofs_local
          source: logs
          partition_binding: {kind: column, column: partition}
```

A per-partition shard can use a template:

```yaml
shards:
  - name: aws-a
    template: aws-logs
    account: "111111111111"
    region: us-east-1
    attributes: {region: us-east-1, account: "111111111111"}
    # explicit placements replace the template's placements
```

Merge order: built-in defaults → global overrides → template → per-partition
shard. Explicit fields win; explicitly declared `placements` replace the
template list (there is no implicit append). Shards are always listed
explicitly — templates only remove repetition.

## 3. Access policy (team/repo-owned)

Grants use **selectors matching attributes**, so a new partition with
`role: observability` is automatically covered. `attributes` is the canonical
spelling; the existing compiler's `labels` key remains accepted.

```yaml
version: 1

defaults:
  partitions: deny
  logs: deny
  traces: deny
  sql: deny
  monofs: deny

selectors:
  observability: { attributes: { role: observability } }
  aws-native:    { attributes: { stack: aws, role: platform } }

groups:
  observability:
    partitions: [observability, aws-native]
    logs:       [observability, aws-native]
    traces:     [observability]
    sql:        [observability, aws-native]
    backends:   [omni, cloudwatch_logs_insights]
    shards:
      include:
        - attributes: {region: us-east-1}
      exclude:
        - attributes: {account: "222222222222"}
    limits:
      query:
        requests_per_second: 5
        max_concurrent: 2
        max_partitions: 3
        max_shards: 8
        max_backends: 3
        max_range: 12h
        max_rows: 5000
        max_bytes_scanned: 2GiB

users:
  analyst@example.com:
    partitions: [observability]
    logs:       [observability]
    sql:        [observability]
    backends:   [omni]                  # cannot use Insights shards
    limits:
      query: {max_concurrent: 1, max_range: 6h, max_rows: 2000}
```

Attributes:

- **Fixed core**: `role`, `stack`, `environment`, `region`, `account`,
  `data_classification`.
- **Free-form extras** are allowed for selectors; they do not affect reserved
  behavior.
- Shard-level attributes (`platform`, `account`, `region`) can be used to
  narrow a grant to specific shards. If shard-level access is enabled, a query
  only fans out to the matching shards; the rest are not read and are reported
  as out-of-scope (not as a coverage gap).

Backend semantics:

- `backends` is an **intersection** with the shard's allowed placements.
- If a shard has no allowed placement, that shard cannot be read. The query
  fails (or returns a labelled partial result if `allow_partial`), never a
  silently smaller aggregate.
- `max_shards` counts the **in-scope** shards after selectors; `max_backends`
  bounds distinct backend kinds actually queried.

## 4. Routing a query

1. Resolve visible partitions for the signal (`VisibleSQLPartitions` for SQL).
2. For each partition, list **all shards** for the signal.
3. Apply the grant's shard selectors (include/exclude) and the user's
   `backends` intersection. Out-of-scope shards are removed from the user's
   view before coverage is computed; they are not gaps.
4. For each shard, choose the primary placement; keep replicas as fallbacks.
5. Build one endpoint per selected placement:
   `id = <partition>/<signal>/<shard>/<placement>`.
6. Emit the pushdown with that placement's dialect and partition binding.
7. Scatter/gather; merge partials; apply `HAVING`, final `ORDER BY`,
   `LIMIT`/`OFFSET` locally.
8. Coverage = every in-scope shard answered. A missing shard is a coverage gap;
   a missing partition is a denied/absent scope.

Fallback:

- Primary placement fails → retry a placement in the **same shard** with
  `replica_of` pointing at the primary.
- Never retry in another shard; that would mix different data.
- If no replica exists, the shard is a gap.

Example for the YAML above:

```text
observability (logs)
  k8s-eu   → MonoFS  ─┐
  aws-a    → Omni    ─┼─► partials ─► merge ─► result
  aws-b    → Insights─┤
  gcp-prod → BigQuery┘

aws-a fails → insights-replica (same shard) retried
gcp-prod fails → no replica → coverage gap
```

## 5. Backend capabilities

| Backend | logs | traces | metrics | Language | Partial aggregates | Notes |
|---|---|---|---|---|---|---|
| Omni | yes | yes | PromQL only | SQL / PromQL | yes | one space may span accounts/regions; scope with account_region/log_group/attribute |
| CloudWatch Logs Insights | yes | no | no | OpenSearch SQL | SUM/COUNT/MIN/MAX/AVG | 50 log groups, Standard class |
| CloudWatch Metrics | no | no | yes | PromQL | n/a | not part of SQL |
| BigQuery | yes | yes | yes | GoogleSQL | yes | bytes billed |
| ClickHouse | yes | yes | yes | ClickHouse SQL | yes | shared/local |
| MonoFS local | yes | yes | yes | DuckDB SQL | yes | Parquet chunks |

### CloudWatch Logs Insights is OpenSearch SQL

`StartQuery` with `queryLanguage=SQL`, so this is a SQL dialect, not a custom
Insights QL translator:

| Generic | Logs Insights SQL |
|---|---|
| `FROM logs` | `` FROM `logGroups(logGroupIdentifier: [...])` `` |
| `WHERE` | `WHERE` |
| aggregate + `GROUP BY` | `SUM/COUNT/MIN/MAX/AVG … GROUP BY …` |
| `FILTER (WHERE …)` | `SUM(CASE WHEN … THEN x END)` / `COUNT(CASE WHEN … THEN 1 END)` |

`HAVING`, final `ORDER BY`, and `LIMIT`/`OFFSET` stay local. The log group set
is the shard's boundary, so no extra partition predicate is needed
(`enforces_partition_scope=true` by construction).

Restrictions: Standard Log Class only; up to 50 log groups; one JOIN per
SELECT; one level of nested subqueries; no multi-statement; case-only field
collisions unsupported; functions must reference fields; backtick special
fields (`` `@timestamp` ``, `` `@message` ``). `filterIndex`/`dataSource` are
optional placement hints.

Verify: `date_trunc` vs `date_format` for buckets, `DECIMAL` support,
`NULLS FIRST/LAST`, timestamp literal syntax, and row/timeout quotas.

## 6. Limits

Reuse `policy.Quota` and extend `QueryQuota`:

```go
type QueryQuota struct {
    RequestsPerSecond float64
    MaxConcurrent     int
    BudgetPerSecond   int64
    MaxRange          string
    MaxSamples        int64
    MaxSeries         int64
    MaxPartitions     int

    MaxShards          int      `json:"max_shards,omitempty"`
    MaxBackends        int      `json:"max_backends,omitempty"`
    MaxRows            int64    `json:"max_rows,omitempty"`
    MaxBytesScanned    int64    `json:"max_bytes_scanned,omitempty"`
    AllowedBackends    []string `json:"allowed_backends,omitempty"`
    AllowedSignals     []string `json:"allowed_signals,omitempty"`
    MaxQueriesPerDay   int64    `json:"max_queries_per_day,omitempty"`
}
```

Resolution: global → group → user/client → partition → backend ceiling.
Numeric caps take the minimum; ranges take the shortest; allowlists intersect.
Enforce at the router (partitions, shards, backends, range, RPS/concurrency),
the coordinator (rows/groups/bytes in flight), and drivers (native caps).
Report bytes scanned and duration per shard/backend in the result and audit.

## 7. Ingestion and partition identity

- Guardian pushers write `guardian.partition` in
  `OTEL_RESOURCE_ATTRIBUTES` and **overwrite** the identity keys, so a workload
  cannot claim another partition.
- The collector overwrites again; the backend validates.
- CloudWatch OTLP logs: set `x-aws-log-group: /strata/<partition>` per
  collector/shard; use the `log_group` binding.
- CloudWatch OTLP metrics: low-cardinality `guardian_partition` label.
- CloudWatch OTLP traces: `guardian.partition` resource attribute; enable
  Transaction Search.
- Omni: `log_group` for logs, `attribute` for traces; `account_region` fallback.
- Native AWS/GCP: log groups/labels/project fallback; tags do not propagate to
  native metrics automatically.

See `PARTITIONING.md` §2.1–2.2 and §7.1.

## 8. Security

- SQL is fail-closed: router requires a loaded policy and the `sql` scope;
  nodes require the cluster-internal token.
- A user cannot add a shard, partition, backend, or log group outside policy.
  The coordinator emits sources/log groups from the placement, never from the
  request.
- Per-shard IAM:
  - `logs:PutLogEvents` on `/strata/<partition>/*`;
  - `logs:StartQuery`/`GetQueryResults` on the shard's log groups;
  - Omni/BigQuery/ClickHouse credentials per backend.
- Audit: subject, partitions, shards, attributes matched, backends, time range,
  rows, bytes scanned, partial/coverage.
- Never trust a client-supplied `guardian.partition`; overwrite and reject.

## 9. Code impact

| Area | Change |
|---|---|
| `pkg/authz/policy` | `PartitionPolicy`, `Shard`, `Placement`, `PartitionBinding`; `ScopeSQL`; `Grant.SQL`/`Grant.Backends`; quota fields; compile/lint |
| partition manifests | per-partition `partition.yaml` + `defaults.yaml`; compiler merges |
| `internal/access` (Doctor) | `VisibleSQLPartitions`; shard selector evaluation; quota merge |
| `internal/router` | visible partitions → in-scope shards → placements → `sqlag` endpoints; fan-out/coverage |
| `pkg/sqlag` | endpoint `Partition`/`Shard`; attributes as tags; `Scope.Bindings`; binding predicate emitter |
| `drivers/omni` | per-account/region space; `log_group`/`attribute` bindings |
| `drivers/cloudwatchlogsinsights` (new) | OpenSearch SQL dialect; `StartQuery(queryLanguage=SQL)`; log-group source; bytes scanned |
| `monofs/internal/sqlfederation` | local shard = node; `column` binding |
| ingestion | overwrite `guardian.partition`; per-shard log group; CloudWatch OTLP exporters |

## 10. Types

```go
type PartitionPolicy struct {
    Name       string
    Attributes map[string]string
    Shards     []Shard
    Quota      *policy.Quota
}

type Shard struct {
    Name       string
    Signal     Signal
    Platform   string            // kubernetes | aws | gcp | local
    Account    string
    Region     string
    Project    string
    Attributes map[string]string
    Range      *TimeRange
    Placements []Placement
}

type Placement struct {
    ID        string
    Backend   string            // omni | cloudwatch_logs_insights | ...
    Source    string
    LogGroups []string
    ReplicaOf string            // placement ID in the same shard
    Columns   map[string]string
    Expressions map[string]string
    Partition *PartitionBinding
    IndexedFields map[string]string
    DataSources   []string
}
```

`replica_of` is scoped to the shard. A placement with no `replica_of` in a shard
with another non-replica is a configuration error.

## 11. Rollout

1. **Types + compiler**: partition/shard/placement; defaults merge; no
   behavior change.
2. **SQL scope + limits**: `sql`, `backends`, `max_shards`/`max_backends`,
   `max_rows`/`max_bytes_scanned`.
3. **Binding predicates**: six kinds, fail-closed; golden tests.
4. **Shard routing**: fan-out/coverage/fallback; mixed-backend tests.
5. **Omni placements**: per-account/region; log_group/attribute.
6. **Logs Insights SQL** — ✅ OpenSearch SQL dialect,
   `StartQuery(queryLanguage=SQL)` driver, resolver.
7. **Ingestion hardening**: overwrite partition; per-shard log groups; OTLP
   exporters.
8. **Cutover**: shadow, per-partition rollout, remove the flat endpoint path.

## 12. Tests

- Partition with 4 shards (k8s, GCP, AWS-A Omni, AWS-B Insights) → one query
  merges 4 partials.
- AWS-A primary fails → Insights replica used; GCP fails → coverage gap.
- Cross-shard `AVG` = sum/count; no average-of-averages.
- Backend allowlist removes a shard → denied/partial, never a silent subset.
- Shard/pool limits: `max_shards`, `max_backends`, `max_rows`,
  `max_bytes_scanned`, `max_range`.
- Binding kinds: column, attribute, account_region, data_source, log_group,
  fixed; missing mapping → `scope_not_enforceable`.
- Logs Insights SQL: `logGroups(...)`, filter, aggregates, FILTER rewrite,
  types/buckets; capability rejection.
- Shard-level access: include/exclude by region/account; out-of-scope shard is
  not read, not reported, and not a coverage gap; empty include denies.
- Shard templates: explicit shard plus template merges fields; explicit
  placements replace the template list.
- Security: spoofed partition overwritten/rejected; log group outside shard
  denied; SQL without policy denied.

## 13. Open questions

1. ~~Cross-account Omni~~ **Resolved:** one Omni space can hold data from
   multiple accounts and/or regions. Each account/region still arrives through
   its own Dataset integration; scope SQL inside the shared space with
   `account_region`, `log_group`, or `attribute` bindings.
2. **Logs Insights SQL**: confirm quotas, `date_trunc`/`DECIMAL`, and
   `StartQuery` source semantics.
3. **Backend naming**: keep `cloudwatch_logs_insights` or use a shorter
   `logs_insights` ID in policy?
4. **Shard attribute matching time**: shard attributes are static declarations,
   so selectors can be expanded at policy-compile time. Confirm you do not need
   runtime/dynamic shard membership.

---

## Implementation status (Phase 1)

Implemented in `monofs/pkg/authz/policy`:

- `PartitionPolicy`, `Shard`, `Placement`, `PartitionBinding`,
  `PartitionTimeRange`, `ShardTemplate`, `PartitionDefaults`.
- `ParsePartitionPolicy`, `ParsePartitionDefaults`, `Apply`, `Validate`,
  `ValidatePartitions`.
- `CompileWithPartitions(file, catalog, partitions, now)`; `Compile` remains
  backward compatible.
- `Bundle.Partitions`; `Policy.Partitions`, `Policy.Partition`,
  `Policy.SortedPartitionNames`.
- Access policy: `sql` scope; `Subject`/`Grant` `SQL`, `Backends`, `Shards`
  (`include`/`exclude` attribute matchers).
- `Policy.CanReadSQL`, `ResolveVisibleSQLPartitions`, `AllowedBackends`,
  `ShardAllowed`.
- Quota: `MaxShards`, `MaxBackends`, `MaxRows`, `MaxBytesScanned`,
  `AllowedBackends`, `AllowedSignals`, `MaxQueriesPerDay`, with clone/merge/
  validation.
- Tests: defaults/template merge, partition validation, shard selector
  semantics, SQL grant compilation, quota fields.

Not yet wired:

- The router still uses the legacy endpoint selection; `Bundle.Partitions` is
  compiled but not consumed.
- `pkg/sqlag` does not yet receive `PartitionBinding` or `Scope.Bindings`.
- No Logs Insights driver yet.

### Phase 2 (bindings/predicates) — implemented

- `catalog.PartitionBinding` on `Endpoint`; `EnforcesPartitionScope` is derived
  when a binding exists.
- `PartitionBindingValue` and `Scope.Bindings` in `execute`, `rewrite`, and
  `pkg/sqlag`.
- `Dialect.NestedField` (Omni paths such as
  `attributes['guardian.partition']` and
  `resource['attributes']['guardian.partition']`).
- `rewrite.PartitionFilter` + emitter for `column`, `attribute`,
  `account_region`, `data_source`, `log_group`, and `fixed`.
- `rewrite.ErrScopeNotEnforceable`; missing bindings and fixed-subset
  violations fail closed.
- legacy fallback: an endpoint without a binding still uses the logical
  `partition` column when present.
- Tests: logs/traces attributes, account/region pairs, data source, log group,
  fixed subset, column fallback, and error paths.

Corrected constraint: **one Omni space may span accounts/regions.** The
single-space validation was removed; Omni placements still warn when a shard
has no account/region because `account_region` bindings need them.

### Phase 3 (shard routing) — implemented

- `sqlfederation.Route` resolves visible partitions → in-scope shards →
  one primary placement per shard; out-of-scope shards are not read and not
  counted as coverage gaps.
- Per-shard `replica_of` preflight fallback: if the primary backend is not
  allowed for the caller, an allowed replica is used (with a warning).
- `RoutePlan.Catalog` builds a `sqlag` catalog with one coverage group per
  `partition/signal`; `Members` is the in-scope shard count, so a shard without
  an allowed placement becomes a coverage gap.
- **Per-endpoint bindings:** `Endpoint.Binding` carries the shard's native
  identifiers. This prevents a shared Omni space from being scanned twice when
  one partition has multiple accounts/regions.
- Router `QuerySQL` uses the policy path whenever the bundle defines
  partitions; otherwise it keeps the legacy local-node path.
- A `sqlfederation.BackendResolver` supplies the executor per selected
  placement. Without one, policy-routed SQL fails closed with `Unimplemented`.
- Tests: 4-shard mixed-backend merge, shard include selectors, backend
  allowlists, replica fallback, and the full router `QuerySQL` path.

Not wired yet: production resolvers for Omni/Logs Insights/BigQuery/ClickHouse
and the expansion of a `monofs_local` shard into node endpoints. Those are the
next backend-integration steps.

Corrected constraint: one Omni space may hold data from multiple accounts and/or
regions; the single-space validation was removed. Scope it with
`account_region`, `log_group`, or `attribute` bindings.

### Phase 4 (Omni placements + ingestion) — implemented

- `sqlfederation.OmniResolver` builds/caches one Omni driver per endpoint; the
  shard region wins over the configured default; sessions are reused.
- `sqlfederation.MultiResolver` dispatches by backend. The router installs an
  Omni resolver at startup (`--omni-region`, `--omni-profile`,
  `--omni-endpoint`, `--omni-session-prefix`, `--omni-timeout`).
- The pusher now **overwrites** `guardian.partition`, `guardian.intent`,
  `guardian.asset` and related identity attributes; explicit workload values
  can no longer claim another partition.
- The generated AWS collector config upserts `guardian.partition` with a
  `resource/guardian` processor in every pipeline. Together with
  `resource_to_telemetry_conversion`, the partition becomes an EMF/CloudWatch
  metric dimension.
- Tests: resolver caching/region/session, multi-resolver dispatch, spoofed
  partition overwritten in both OTel env builders, AWS collector config
  contains the partition upsert.

Not wired yet: resolvers for ClickHouse/BigQuery/Logs Insights, expansion of a
`monofs_local` shard into node endpoints, per-shard log-group headers and
CloudWatch OTLP exporters, and runtime (post-failure) replica retry.

### Phase 5 (CloudWatch Logs Insights SQL) — implemented

- `dialect.CloudWatchLogsInsights` emits OpenSearch SQL for the Logs Insights
  SQL endpoint:
  - log group source: ``FROM `logGroups(logGroupIdentifier: [...])` `` (a bare
    placement source is expanded to that form);
  - `FILTER (WHERE ...)` aggregates are rewritten to
    `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` / `COUNT(CASE ...)` /
    `MIN|MAX(CASE ...)` because the endpoint has no aggregate `FILTER`;
  - time buckets use `date_format(...)` + `CAST(... AS TIMESTAMP)`;
  - `LONG`/`DOUBLE` casts stand in for exact `DECIMAL`.
- `drivers/cloudwatchlogsinsights` starts a SQL query, polls
  `GetQueryResults`, decodes typed rows, and fails closed on missing columns,
  failed/cancelled/timeout status, or exceeding `MaxRows`.
  `assume_role` supports cross-account reads.
- `sqlfederation.InsightsResolver` caches one driver per endpoint; shard
  region wins over the configured default.
- `sqlfederation.BigQueryResolver` (REST driver) and
  `sqlfederation.LocalResolver` (fan-out `monofs_local` over healthy nodes)
  complete the placement resolver set; `MultiResolver` dispatches by backend.
- Router flags: `--insights-region|profile|endpoint|timeout|max-rows`,
  `--bigquery-project|location|credentials|endpoint|timeout|max-rows`,
  and `--omni-*`; OIDC flags now read `MONOFS_OIDC_*`.
- `Placement.assume_role` lets one router read another account's CloudWatch.
- Demo integration: `demo-divisions/policy/routing/{diva,divb}-shared/
  partition.yaml` defines per-provider shards, is published by
  `guardianctl access publish` and embedded in the compiled bundle.
  `./demo.sh deploy-shared` owns the whole flow: it deploys the stacks,
  creates the BigQuery sinks, publishes the routing ACL, wires router
  AWS/GCP credentials + OIDC, and then runs `sql-verify` (usera: Omni +
  BigQuery + local; userb: Insights + BigQuery + local). Wiring and
  verification failures abort `deploy-shared`; `./demo.sh sql-verify` re-runs
  just the isolation check.

Remaining gaps:
- Cross-dialect **runtime** replica retry (preflight fallback is implemented).
- Live quotas (`max_rows`/`max_bytes_scanned`) are still only encoded, not
  enforced per query.
- `expressions`/`indexed_fields` are accepted by the policy schema but not
  applied by the emitters.
