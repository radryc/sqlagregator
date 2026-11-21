# sqlagregator

Federated SQL coordinator for CloudWatch Omni, CloudWatch Logs Insights,
BigQuery, ClickHouse, and MonoFS node-local DuckDB. See
[DESIGN.md](./DESIGN.md) for the architecture.

This repository contains the coordinator core and live backend drivers:

- canonical values and types (`internal/value`)
- live drivers for ClickHouse (HTTP/JSON), BigQuery (REST), CloudWatch Omni
  (the `cloudwatchomni` SDK handles CBOR/SigV4), and CloudWatch Logs Insights
  (OpenSearch SQL via `StartQuery`), each in its own module
- the restricted SQL parser (`internal/sql`, `internal/ast`)
- the catalog and endpoint registry (`internal/catalog`)
- endpoint include/exclude resolution with coverage validation (`internal/selector`)
- binder and two-phase planner (`internal/plan`, `internal/rewrite`)
- pushdown emitters for DuckDB, ClickHouse, BigQuery, Omni, and Logs Insights (`internal/dialect`)
- pure-Go hash aggregation (`internal/merge`)
- the coordinator (`internal/execute`)
- the `sqlag` CLI (`cmd/sqlag`)
- the public library surface (`pkg/sqlag`) consumed by MonoFS

## Modules

| Module | Contents |
|---|---|
| `.` | core: parser, planner, dialects, merge, execute, `pkg/sqlag` |
| `drivers/clickhouse` | ClickHouse HTTP driver (stdlib only) |
| `drivers/bigquery` | BigQuery REST driver |
| `drivers/cloudwatchlogsinsights` | CloudWatch Logs Insights SQL driver |
| `drivers/omni` | CloudWatch Omni driver (AWS SDK) |
| `cmd/sqlag` | CLI, depends on the core and the ClickHouse, BigQuery, and Omni drivers |

The core module does **not** depend on any driver. A host that imports
`pkg/sqlag` only pulls `gopkg.in/yaml.v3`.

The nested modules use local `replace` directives (`../..`) and `go.work`
during development. For a release, remove them and require a tagged core
version (for example `github.com/radryc/sqlagregator v0.1.0`).

```sh
make build      # builds ./sqlag
make test       # tests every module
make tidy       # go mod tidy in every module
```

## Quick start

```sh
make build

./sqlag backends -config examples/catalog-25.yaml

./sqlag explain \
  -config examples/catalog-25.yaml \
  -q "SELECT service, count(*) AS n FROM logs WHERE level = 'ERROR' GROUP BY service ORDER BY n DESC LIMIT 5" \
  -from 2026-11-01 -to 2026-11-02
```

Excluding endpoints:

```sh
# exclude by id
./sqlag explain -config examples/catalog-25.yaml \
  -q "SELECT count(*) AS n FROM logs" \
  -exclude node-05,node-06 -from 2026-11-01 -to 2026-11-02

# exclude by tag; the 21 legacy replicas are already excluded by default
./sqlag explain -config examples/catalog-25.yaml \
  -q "SELECT count(*) AS n FROM logs" \
  -exclude-tag pool=legacy -from 2026-11-01 -to 2026-11-02
```

Excluding a real shard produces `coverage_gap`; add `-allow-partial` to get a
labelled partial result instead.

## Execute against a backend

`query` builds a driver for every enabled endpoint in the catalog before it
selects shards, so the command needs connection flags for every backend the
catalog declares. `examples/catalog-25.yaml` contains only `duckdb-local`
endpoints, which the CLI can plan/explain but cannot execute;
`examples/catalog-multi.yaml` declares Omni, BigQuery, and ClickHouse across
disjoint time shards.

```sh
# All three external engines over their time shards
./sqlag query -config examples/catalog-multi.yaml \
  -clickhouse-url http://localhost:8123 -clickhouse-database strata \
  -bigquery-project my-project -bigquery-location US \
  -omni-region us-east-1 \
  -q "SELECT service, count(*) AS n FROM logs GROUP BY service" \
  -from 2026-11-01 -to 2026-11-22
```

For one engine in isolation, point `-config` at a catalog that declares only
that endpoint (AWS credentials for Omni come from the default chain).

## Driver behavior

- **CloudWatch Omni**: starts one telemetry query session per driver. If the
  session name is already in use (for example two routers share an endpoint),
  the driver appends a process-unique suffix and retries once instead of failing
  the query.
- **CloudWatch Logs Insights**: emits the time window with `FROM_UNIXTIME(...)`
  because `@timestamp` is a string; the generic `CAST(<RFC3339> AS TIMESTAMP)`
  literal matches zero rows.
- **ClickHouse**: attribute bindings render as map subscripts
  (``attributes['guardian.partition']``), matching Omni's attribute predicate.

## MonoFS integration

The core library is `pkg/sqlag`. MonoFS (a separate module) imports it and
provides the host adapter:

- `QuerySQL` on the router federates across healthy local shards.
- `QueryPartial` on each node executes one generated pushdown and returns
  typed partial rows.
- The adapter lives in `monofs/internal/sqlfederation` (see DESIGN.md §16).
- SQL is **fail-closed**: the router requires a loaded access policy
  (`--sql-require-policy`, default true) and nodes require the cluster-internal
  token for SQL RPCs. See [SECURITY.md](./SECURITY.md) for the trust model and
  deployment settings.

Until the library is tagged, MonoFS uses a local `replace`:

```go
require github.com/radryc/sqlagregator v0.0.0
replace github.com/radryc/sqlagregator => ../sqlagregator
```

Remove the `replace` and require a released version for production.
