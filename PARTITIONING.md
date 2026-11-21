# Partitioning and policy for federated SQL

Status: background (implementation status in `PARTITION-POLICY.md`)
Scope: how a user's visible partitions become a predicate on every backend,
including CloudWatch Omni when it is a centralized, schemaless store.

> Per-partition backend routing, partition definitions with attributes, and
> user/partition limits are consolidated in **`PARTITION-POLICY.md`**. This
> document remains the reference for partition identity, ingestion, and
> predicates.

---

## 1. Current model (facts from the code)

The policy source (`access/base.yaml`) is compiled into a versioned bundle
(`monofs/pkg/authz/policy`) and consumed by Guardian, MonoFS, and Doctor.

```yaml
version: 1

defaults:
  partitions: deny
  logs: deny
  traces: deny
  monofs: deny

selectors:
  observability: { labels: { role: observability } }
  aws:           { labels: { stack: aws } }

groups:
  observability:
    partitions: [observability, aws]   # the universe of visible partitions
    logs:       [observability, aws]   # which of them may be read as logs
    traces:     [observability, aws]   # which may be read as traces
    monofs:     [...]
    role: viewer

users:
  analyst@example.com:
    partitions: [observability]
    logs:       [observability]
    traces:     [observability]
    role: viewer

aws_bindings:
  - partition: aws-demo
    account: "123456789012"
    region: us-east-1
```

Evaluation today:

- `Policy.VisiblePartitions(id)` returns the names from the **`partitions`**
  scope (or all for admins).
- `Policy.CanReadSignal(id, name, signal)` then filters those names through the
  **`logs`** or **`traces`** scope. Metrics always return true.
- `aws_bindings` maps an AWS record's account/region to a partition
  (`Policy.PartitionForAWS`); it is used for federated record attribution, not
  yet for SQL predicates.
- The router's `querySQLScope` uses `VisiblePartitions` + `CanReadSQL`,
  fails closed, and intersects any restriction forwarded by Doctor.

## 2. Where partition identity lives in telemetry

| Backend | Logs | Traces | Notes |
|---|---|---|---|
| MonoFS Parquet | `labels["guardian.partition"]` (`guardian_partition` accepted) | `attributes["guardian.partition"]` | exposed as the `partition` column in the DuckDB view |
| ClickHouse | `labels['guardian.partition']` | `attributes['guardian.partition']` | `partition` expression in the dataset view |
| AWS federated | `@aws.account` / `@aws.region` → `aws_bindings` | same | legacy records may carry `guardian_partition = region` |
| BigQuery | a `partition` column (must be added) | same | or one dataset/table per partition |
| **Omni** | **no built-in partition field** | **no built-in partition field** | see §3 |

The canonical logical column is `partition` (string). The SQL emitter already
injects `partition IN (...)` when the table declares that column.

### 2.1 What the pusher already provides (verified)

`AssetInput.PartitionName` is available to every Guardian driver. The OTel
environment injection already writes the partition into the workload:

```go
// guardian/internal/pusher/driverutil/otelenv.go
force("service.namespace", in.PartitionName)
force("guardian.partition", in.PartitionName)
force("guardian.intent", in.IntentName)
force("guardian.asset", in.Asset.Name)
...
env["OTEL_RESOURCE_ATTRIBUTES"] = formatOtelResourceAttrs(attrs)
```

So Guardian-managed workloads already emit:

```text
OTEL_RESOURCE_ATTRIBUTES=service.namespace=<partition>,guardian.partition=<partition>,...
OTEL_EXPORTER_OTLP_ENDPOINT=<collector or gateway>
```

Additional facts:

- AWS resources are tagged `guardian-partition=<partition>` (`awsTags`), and
  Guardian creates CloudWatch log groups with those tags.
- The AWS collector config uses `awsemf` with
  `resource_to_telemetry_conversion: enabled`, so `guardian.partition` becomes a
  CloudWatch metric **dimension** for metrics that go through the collector.
- GCP federation resolves `guardian_partition` from application labels before
  falling back to project/region (`gcpIdentityPartition`).

**Spoofing is closed at the producer and collector:**

1. The pusher **overwrites** `guardian.partition`, `guardian.intent`,
   `guardian.asset` and the related identity attributes; explicit workload
   values are replaced.
2. The generated AWS collector config upserts `guardian.partition` with a
   `resource/guardian` processor in every pipeline. With
   `resource_to_telemetry_conversion`, it becomes an EMF/CloudWatch metric
   dimension.
3. The backend should still reject a record whose partition does not match the
   authenticated collector/tenant; that validation remains a deployment
   backstop.

**Export completeness:** the generated AWS collector config forwards **metrics**
to OTLP when an endpoint is set, but logs go only to `awscloudwatchlogs` and
traces only to `awsxray`. If Omni is the SQL target and should ingest OTLP
logs/traces, add `otlp`/`otlphttp` exporters to those pipelines, or rely on
CloudWatch/X-Ray ingestion plus tag/annotation mapping.

### 2.2 CloudWatch OTLP endpoints (Omni's canonical ingestion path)

CloudWatch Omni reads what CloudWatch ingested. The CloudWatch OTLP endpoints
feed CloudWatch Logs, CloudWatch Metrics, and X-Ray; the CloudWatch **Dataset
integration** then forwards logs and traces into the Omni Dataset. Metrics are
not forwarded: Omni queries CloudWatch Metrics directly with PromQL.

Endpoints (HTTP only; SigV4 or bearer token):

| Signal | Endpoint | Notes |
|---|---|---|
| Logs | `https://logs.<region>.amazonaws.com/v1/logs` | requires `x-aws-log-group` and `x-aws-log-stream` headers |
| Metrics | `https://monitoring.<region>.amazonaws.com/v1/metrics` | resource/scope/datapoint attributes become CloudWatch labels |
| Traces | `https://xray.<region>.amazonaws.com/v1/traces` | enable Transaction Search first |

Use them as the **export leg** from our collectors, not as a replacement for
them:

```text
workload ──► our collector (stamp/overwrite guardian.partition)
          ──► CloudWatch OTLP endpoint (SigV4)
          ──► CloudWatch Logs / Metrics / X-Ray
          ──► Omni Dataset (logs + traces) / CloudWatch Metrics (metrics, PromQL)
```

Partition implications:

- **Logs:** set `x-aws-log-group: /strata/<partition>` from the collector's
  identity. The log group is the strongest partition key and does not depend on
  attribute preservation. Bind Omni with `kind: log_group` (`@logGroupName`);
  the `attribute` binding is the fallback.
- **Metrics:** keep a low-cardinality `guardian_partition` label; attributes
  become CloudWatch labels (150-label limit).
- **Traces:** keep `guardian.partition` as a resource attribute; use a
  per-partition collector. Fall back to `@aws.account`/`@aws.region` if Omni
  does not preserve the attribute.
- **Account/region scope:** the Dataset integration is one per account and
  Region, and forwarding is same-account/same-region. A single Omni space
  **can** hold data from multiple accounts/regions, but each account/region
  arrives through its own Dataset integration. Scope SQL within the shared
  space with `account_region`, `log_group`, or `attribute` bindings; do not
  assume the space itself is account-scoped.
- **Limits:** metrics 500 TPS/account, 1,000 datapoints/request, 150 labels,
  1 KB values; logs 1 MB event, 20 MB request, 16 KB resource+scope, 5k TPS;
  traces 10k spans/request, 16 KB resource+scope, 200 KB span.

Security: do not let workloads send directly to the OTLP endpoint with broad
credentials. They could write to another partition's log group or stamp a
different `guardian.partition`. Give the collector a per-partition IAM role
(scoped `logs:PutLogEvents` on `/strata/<partition>/*`), overwrite the identity
attributes in the collector, and prefer SigV4 over long-lived bearer tokens.

## 3. Omni: the centralized, schemaless case

A single Omni space can receive every tenant's telemetry. Omni has **no schema
and no partition column**. It does expose these fields:

- `@timestamp`, `@ingest_time`, `@telemetry_type`
- `@aws.account`, `@aws.region`
- `@data_source_name`, `@data_source_type`, `@data_format`
- `@logGroupId`, `@logGroupName`, `@logStream`, `@logStreamId`
- `@entity`, `@message`
- map access: `attributes['guardian.partition']` on logs,
  `resource['attributes']['guardian.partition']` on traces

So a restricted Omni query is only safe if one of these is true:

| Omni deployment | Binding | Isolation |
|---|---|---|
| One space per tenant (`logs.tenant-a`) | `fixed` values | strongest; no predicate needed |
| Shared space, ingestion preserves `resource.attributes['guardian.partition']` | `attribute` | strong |
| Shared space, each partition is a distinct AWS account/region | `account_region` + policy bindings | medium |
| Shared space, datasource or log-group convention maps to partitions | `data_source` / `log_group` | medium |
| Shared space with no trustworthy field | none → `enforces_partition_scope=false` | restricted SQL **refused** |

> A space **may** hold data from multiple accounts and/or regions. The rows
> above describe how a partition is expressed, not a single-account limit. For
> a shared multi-account space, use `account_region`, `log_group`, or
> `attribute` bindings to scope each query.

**Current bug to fix:** `examples/catalog-multi.yaml` maps the logical
`partition` column to a physical `partition` field and declares
`enforces_partition_scope: false`. Because the logical table declares
`partition`, the emitter currently injects `"partition" IN (...)`; Omni has no
such field, so the query errors (or silently matches nothing). The binding
below makes this explicit and validates it at startup.

## 4. What the endpoint binding needs

An endpoint binding must answer four questions. Today it answers only the first
two.

1. **Where is the data?** `Source` — `logs.default`, `strata.logs`,
   `proj.dataset.logs`.
2. **How do logical columns map to physical fields?** `Columns` — including
   nested/schemaless fields (`attributes['x']`, `resource['attributes']['x']`).
   A plain identifier map is not enough for Omni; the binding needs a trusted
   expression or a structured path.
3. **How is a partition expressed?** *(new)* — a `PartitionBinding`:
   `column`, `attribute` (with a path), `account_region`, `data_source`,
   `log_group`, or `fixed`.
4. **Can the endpoint enforce the scope?** *(new)* — derived, not hand-written:
   `enforces_partition_scope = binding != nil`. If false, the endpoint is
   admin/unrestricted only; the planner refuses it for restricted callers.

Proposed catalog types:

```go
type PartitionBinding struct {
    // Kind: column | attribute | account_region | data_source | log_group | fixed
    Kind string `yaml:"kind" json:"kind"`

    Column string   `yaml:"column,omitempty"`   // column: "partition"
    Path   []string `yaml:"path,omitempty"`     // attribute: ["resource","attributes","guardian.partition"]

    AccountField    string `yaml:"account_field,omitempty"`     // "@aws.account"
    RegionField     string `yaml:"region_field,omitempty"`      // "@aws.region"
    DataSourceField string `yaml:"data_source_field,omitempty"` // "@data_source_name"
    LogGroupField   string `yaml:"log_group_field,omitempty"`   // "@logGroupName"

    Values []string `yaml:"values,omitempty"` // fixed: ["tenant-a"]
}

type Endpoint struct {
    // ... existing fields ...
    Partition *PartitionBinding `yaml:"partition,omitempty" json:"partition,omitempty"`

    // Trusted, operator-configured expressions for schemaless fields, e.g.
    // "timestamp": "@timestamp", "service": "resource['attributes']['service.name']".
    // Never request-derived; validated at load time.
    Expressions map[string]string `yaml:"expressions,omitempty" json:"expressions,omitempty"`
}
```

Examples:

```yaml
# MonoFS local / ClickHouse / BigQuery
- id: ch-prod
  backend: clickhouse
  source: strata.logs
  partition: {kind: column, column: partition}

# Omni, shared space with a canonical resource attribute
- id: omni-shared
  backend: omni
  source: logs.default
  columns: {timestamp: "@timestamp"}
  partition:
    kind: attribute
    path: ["attributes", "guardian.partition"]   # traces: ["resource","attributes","guardian.partition"]

# Omni, one space per tenant
- id: omni-tenant-a
  backend: omni
  source: logs.tenant_a
  partition: {kind: fixed, values: [tenant-a]}

# Omni, AWS-only space
- id: omni-aws
  backend: omni
  source: logs.default
  partition:
    kind: account_region
    account_field: "@aws.account"
    region_field: "@aws.region"
```

## 5. Policy changes

### 5.1 A separate `sql` scope

SQL is an aggregation/exfiltration surface; it should be grantable separately
from logs/traces.

```yaml
defaults:
  partitions: deny
  logs: deny
  traces: deny
  sql: deny
  monofs: deny

groups:
  observability:
    partitions: [observability, aws]
    logs:       [observability, aws]
    traces:     [observability, aws]
    sql:        [observability, aws]
    monofs:     [...]
    role: viewer

users:
  analyst@example.com:
    partitions: [observability]
    logs:       [observability]
    traces:     [observability]
    sql:        [observability]
    role: viewer
```

Evaluation: `VisibleSQLPartitions = VisiblePartitions ∩ SQL`, and `deny: [sql]`
blocks the surface wholesale. The router's `querySQLScope` uses `CanReadSQL`;
`QueryLogs`/`QueryTraces` keep their current scopes.

### 5.2 Generic partition bindings

Generalize `aws_bindings` (keep the old key for compatibility):

```yaml
partition_bindings:
  - partition: aws-demo
    account: "123456789012"
    region: us-east-1
  - partition: tenant-a
    data_source: strata-tenant-a
  - partition: tenant-b
    log_group: /strata/tenant-b
  - partition: tenant-c
    attributes: {guardian.partition: tenant-c}
```

These are the inverse of the endpoint binding: they map a **visible partition**
to the native identifiers a backend stores. A partition may have several
bindings (two accounts, several log groups).

## 6. Predicate emission

The host resolves the policy and passes a structured scope to the coordinator;
the coordinator never accepts raw SQL predicates.

```go
type Scope struct {
    Restricted bool
    Partitions []string
    Bindings   []PartitionBindingValue // partition -> account/region/datasource/attributes
}
```

For each selected endpoint:

| Binding | Emitted predicate |
|---|---|
| `column` | `"partition" IN ('a','b')` |
| `attribute` | `attributes['guardian.partition'] IN ('a','b')` (or `resource['attributes']...`) |
| `account_region` | `((@aws.account = '123' AND @aws.region = 'us-east-1') OR ...)` |
| `data_source` | `@data_source_name IN ('strata-tenant-a', ...)` |
| `log_group` | `@logGroupName IN ('/strata/tenant-b', ...)` |
| `fixed` | no predicate; validate `visible ⊆ values`, otherwise refuse |

Fail-closed rules:

- A visible partition with no binding value for the endpoint's kind → error
  `scope_not_enforceable`.
- A `fixed` endpoint whose values do not contain every visible partition →
  error.
- An endpoint with no binding and `enforces_partition_scope=false` → refused
  for restricted callers.
- Never "skip the filter because it is hard"; an empty predicate is a bug.

## 7. Ingest contract (the durable fix)

The predicate approach only works if ingestion stamps a stable identity.

- Canonical key: `guardian.partition` (dotted OTel resource attribute). Keep
  `guardian_partition` (underscore) as the label alias already used by
  `telemetrylabels` and the DuckDB view.
- OTLP collectors/resource processors upsert
  `resource.attributes["guardian.partition"] = <partition>`; the pusher sets it
  at the workload and the collector is the trusted enforcement point
  (implemented for the generated AWS collector config).
- Omni: verify the ingestion path preserves resource attributes. If the
  CloudWatch/Omni source drops unknown attributes, fall back to
  `@data_source_name`/`@logGroupName` or a space per tenant.
- BigQuery: add a `partition` column (or one dataset per partition).
- ClickHouse: promote the label to a materialized `partition` column for
  index-friendly filtering.

### 7.1 Native AWS and GCP resources

Tagging is not the same as propagation. Resource tags do not automatically
become telemetry dimensions or log fields.

| Source | Identity available | Reaches telemetry? | Recommended attribution |
|---|---|---|---|
| AWS CloudWatch Logs | log group tags (`guardian-partition`) | yes, via subscription filter/collector | read the log group tag, or map log group → partition |
| AWS custom metrics via collector (EMF) | resource attributes → dimensions | yes | `guardian.partition` dimension |
| AWS native metrics (ALB, RDS, EC2) | resource tags | **not automatically** | account/region fallback, or a cached Resource Groups Tagging inventory (ARN → partition) |
| AWS X-Ray | no resource tags | no | `guardian.partition` annotation from the app/collector; account/region fallback |
| GCP Cloud Logging | resource labels + log entry labels | yes, via log sink | `guardian_partition` label; project fallback |
| GCP Cloud Monitoring | resource/metric labels | partial; custom labels are not universal | project/location fallback, application labels, Cloud Asset Inventory |
| GCP Cloud Trace | span attributes/labels | yes when instrumented | `guardian.partition` attribute; project fallback |

Practical rules:

- **Workloads we deploy** should use the collector path; the pusher already
  knows the partition and can stamp it.
- **Managed services** need native mapping. Account/region is the coarse
  fallback; if multiple partitions share an account/region, tags/labels or an
  inventory mapping are mandatory.
- Tagging permissions matter: `tag:GetResources` (Resource Groups Tagging API)
  for AWS, Cloud Asset Inventory for GCP; cache the mapping.
- GCP resources that do not accept custom labels cannot be attributed finer
  than project/location.

Migration:

- Existing Omni data without the attribute: either re-ingest, or bind the whole
  space as `fixed` to one partition, or accept that restricted queries are
  refused.
- Existing BQ/CH data: backfill from account/region/datasource mappings.
- New tenants: require the attribute in the ingest contract before enabling SQL.

## 8. Code changes and tests

1. `pkg/authz/policy`: add `ScopeSQL`, `Defaults.SQL`, `Subject.SQL`,
   `Grant.SQL`, `CanReadSQL`, `VisibleSQLPartitions`, `Deny: [sql]`, compile
   validation and lint. Keep `format_version = 1`; deploy the router before a
   policy that uses `sql`.
2. `internal/access` (Doctor) and `internal/router`: use
   `VisibleSQLPartitions`; pass the binding values with the scope.
3. `pkg/sqlag`: add `PartitionBinding` to the endpoint, `Scope.Bindings`,
   validation, and per-kind predicate emission. Extend `Emitter`.
4. `monofs/internal/sqlfederation`: set `column` bindings for local DuckDB and
   ClickHouse; keep the existing `timestamp` → `time` column mapping.
5. Driver catalogs for Omni/BigQuery: add the binding and `expressions`.
6. Ingest: stamp `guardian.partition`; add BQ/CH columns.
7. Tests:
   - golden predicates for every binding kind;
   - policy test: `sql` deny overrides logs access;
   - Omni shared + attribute: emits the nested path;
   - Omni shared + no field: `scope_not_enforceable`;
   - Omni fixed: subset validation;
   - account/region mapping from `partition_bindings`;
   - end-to-end: user with partition `tenant-a` sees only `tenant-a` rows.

## 9. Decision matrix for Omni

| Situation | Recommendation |
|---|---|
| Shared space, one log group per partition (CloudWatch OTLP logs) | **primary for logs:** `log_group` (`@logGroupName`) |
| Shared space, OTLP resource attributes preserved (traces/metrics) | **primary for traces:** `attribute` with `guardian.partition` |
| Shared space, partitions are AWS accounts/regions | `account_region` + `partition_bindings` |
| Shared space, one datasource per tenant | `data_source` |
| One space per tenant | optional `fixed`; not the default plan |
| Shared space, no stable field | do **not** enable restricted SQL on it |

The last row is important: a centralized store without a trustworthy partition
field cannot be safely federated for non-admin users. Refusing the query is the
correct behavior.

**Recommended sequence (shared space, not one space per tenant):**

1. Stamp `guardian.partition` at the pusher and overwrite it at the collector.
2. For Omni logs, bind `log_group` and set
   `x-aws-log-group=/strata/<partition>` in the collector's OTLP/HTTP exporter.
   For traces, bind `attribute` with
   `["resource","attributes","guardian.partition"]`.
3. For AWS-native data, prefer log-group tags and EMF dimensions; use
   account/region only as a fallback.
4. If Omni drops the attribute, fall back to `@data_source_name`/`@logGroupName`
   or `account_region`; otherwise keep restricted SQL disabled on that space.
