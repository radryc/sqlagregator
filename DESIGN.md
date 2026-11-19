# Federated SQL Coordinator — Design

**Status:** Draft for review
**Module:** `github.com/radryc/sqlagregator`
**Scope:** One generic SQL surface over three independent engines (CloudWatch Omni, BigQuery, ClickHouse), with correct two-phase aggregation and an in-memory merge engine.

---

## 0. Summary of decisions

| # | Decision | Why |
|---|---|---|
| D1 | Model backend **coverage** (disjoint shard vs replica) per endpoint, and make the execution set selectable/excludable per query. Merge only across disjoint shards; never sum replicas. | Confirmed disjoint shards, but a 25-endpoint deployment still contains replicas. Switching off the wrong 21 must produce a coverage error, not a low number. |
| D2 | Compile SQL → AST → **logical plan** → per-engine pushdown + merge plan. Never string-rewrite SQL. | Rewriting strings cannot distinguish "safe to decompose" from "silently wrong". |
| D3 | The generic dialect is a **documented subset**, and everything outside it is rejected with a typed error. | Matches the existing MonoFS/Doctor philosophy (`reject rather than silently mis-evaluate`). |
| D4 | **Do not use vitess** for the front end. Start with a hand-written subset parser; keep a seam to swap in `wasilibs/go-pgquery`. | Vitess is a MySQL grammar: no `ILIKE`, no `FILTER (WHERE …)`, no `INTERVAL '1' HOUR`, no `::` casts, no `@timestamp` identifiers. Omni-style SQL does not parse. |
| D5 | Push down only **group keys + partial aggregates + filters + top-N**. Apply `HAVING`, final `ORDER BY`, `LIMIT`, `OFFSET` locally. | All four are unsafe to finalize per shard. In particular top-N pushdown is invalid when `HAVING` is present (see §9.4). |
| D6 | Integer `SUM` is pushed as `SUM(CAST(x AS DECIMAL(38,0)))`, merged in `int128`. Float sums use compensated (Neumaier) summation. | `SUM(int64)` can overflow per engine; `UInt64` vs `Int64` breaks cross-engine accumulation; float addition is not associative. |
| D7 | `COUNT(DISTINCT)` is **exact with a cardinality budget**. `APPROX_COUNT_DISTINCT` is passthrough to one engine, or a portable-hash HLL in the coordinator. Engine-native sketches are never merged across engines. | A BigQuery `HLL_COUNT` sketch cannot be merged by ClickHouse, and vice versa. Cross-engine sketch merging is not a thing. |
| D8 | Non-decomposable shapes (window functions, cross-engine joins, quantiles, `STRING_AGG`) run in **passthrough** mode on one engine, or in budgeted raw-row mode. Otherwise reject. | Correctness over feature breadth for v1. |
| D9 | Local merge engine: pure-Go `goagg` by default; **DuckDB behind `-tags duckdb`** via a `LocalEngine` interface; Arrow as the columnar batch format. | Reuses what MonoFS already ships, keeps the default binary CGo-free, and keeps the escape hatch for spills/joins. |
| D10 | Fail closed by default. Partial results only with `allow_partial=true`, and they carry explicit coverage metadata. | These numbers feed dashboards and alerts. A partial `COUNT` must never look complete. |
| D11 | Omni is treated as a **schemaless, time-bounded, logs/traces-only** backend. Always project explicit columns with explicit `CAST`s; never emit `SELECT *`. | Verified in §1. |

---

## 1. Verified backend facts (these drive the design)

Fetched from vendor docs, not assumed:

| Property | CloudWatch Omni | BigQuery (GoogleSQL) | ClickHouse |
|---|---|---|---|
| SQL dialect | DataFusion/Postgres-like | GoogleSQL | ClickHouse SQL |
| Metrics | **PromQL only, not SQL** | SQL | SQL |
| Tables | `logs.default`, `traces.default`, `default` (logs+traces) | `project.dataset.table` | `db.table` |
| Time bound | **Required** (`StartTelemetryQuery` takes start+end); `@timestamp` quoted with backticks | Partition filter required on partitioned tables | None (but always add one) |
| `SELECT *` | Returns `@timestamp` + `@record` **JSON**, not flattened fields | Flattened columns | Flattened columns |
| Schema | None; inferred per query | Strong | Strong |
| `ILIKE` | Yes | **No** (use `LOWER(x) LIKE LOWER(y)` or `REGEXP_CONTAINS`) | Yes |
| Regex match | `~` | `REGEXP_CONTAINS` | `match()` |
| `INTERVAL` | `INTERVAL '1' HOUR` | `INTERVAL 1 HOUR` | `INTERVAL 1 HOUR` |
| `NULLS FIRST/LAST` | Supported | Supported (default `NULLS FIRST` ASC) | Supported, defaults differ |
| `FILTER (WHERE …)` | Yes | Yes | Yes (`countIf` also available) |
| Row cap | `LIMIT` max 100,000; default 10,000; pages of 1,000 via `nextToken` | No row cap; response/billing caps | `max_result_rows` setting |
| Query text cap | 64,000 chars | ~1 MB | `max_query_size` |
| Timeout | 60 min | job timeout | `max_execution_time` |
| Map access | `attributes['key']` | `JSON_VALUE` / struct path | `map['key']` |

Consequences:

1. **Omni cannot answer metric queries.** A logical `metrics` table must declare Omni as an incapable backend, and the planner must exclude it — with a clear error if the caller pinned Omni.
2. **Omni must always receive explicit columns and a bounded `@timestamp`.** A generic `SELECT *` cannot be translated without changing semantics.
3. **Omni's dynamic typing needs explicit casts.** The same field can be string in one record and numeric in another. Every projected/aggregated Omni field is emitted as `CAST(field AS <canonical type>)` so the canonical type contract holds.
4. **Pagination is part of the driver contract**, not an afterthought: the Omni driver loops `nextToken` until the row budget is reached.

---

## 2. The question that dominates everything: shard or replica?

**Confirmed with the operator: the engines are disjoint shards, so summing is correct.** That is the assumption the coordinator encodes — but the deployment can still contain replicas (the stated example is 25 registered endpoints of which only ~4 are real shards). So disjointness is a *declared per-endpoint property*, not a global truth, and §15 makes the execution set explicit and auditable.

```text
Shard (summing is correct)            Replica (summing is wrong)
─────────────────────────             ─────────────────────────
Omni      : 2026-11-01..11-08         Omni      : all data
BigQuery  : 2026-11-08..11-15         BigQuery  : all data
ClickHouse: 2026-11-15..11-22         ClickHouse: all data
COUNT 1+2+3 = 6  ✓                    COUNT 1+2+3 = 3 × truth  ✗
```

Therefore every logical table carries a coverage declaration per backend:

```go
type Coverage interface{ isCoverage() }

type Shard struct {
    // Disjoint partition. Two backends in the same group must not overlap.
    Group string        // e.g. "by-time" — all shards in a group must be disjoint
    Range *TimeRange    // optional declared interval
}
type Replica struct {
    Group string        // replicas of each other
}
```

Planner rules:

- **R1.** A query may be fanned out (and merged) only across backends in the same `Shard.Group`, and only if the group is declared complete for the query's time window.
- **R2.** Backends in a `Replica.Group` are never summed. Query one (lowest latency / healthiest); fail over on error. If two replicas disagree, that is a consistency alarm, not a merge input.
- **R3.** A mixed deployment (some shards, some replicas) is legal; the planner produces one pushdown per shard group and dedupes replicas.
- **R4.** Coverage is validated at startup against declared shard ranges; overlapping or missing ranges are a startup error, not a runtime surprise.

If the real topology turns out to be "three replicas", this design still works — the planner simply emits one query and picks the fastest healthy engine. That is the intended default until confirmed otherwise.

---

## 3. Corrections to the proposal

Keep: the four-stage shape, the pushdown/final split, the pure-Go-first stance, `errgroup` scatter/gather, canonical typing.

Fix or drop:

1. **Omni is not a restricted subset.** It supports CTEs, joins, window functions, `QUALIFY`, `TABLESAMPLE`, subqueries and `FILTER`. The *generic* dialect is the restricted thing, for portability — not because Omni is limited.
2. **Vitess is the wrong parser.** It is MySQL-shaped and will reject the very syntax Omni users write. See D4.
3. **`SELECT *` must never be pushed to Omni** (it yields `@record` JSON).
4. **Top-N pushdown is invalid under `HAVING`.** The proposal pushes `ORDER BY … LIMIT 10` then re-applies `LIMIT`; if a shard's top 10 groups are all removed by `HAVING`, the true top group is never fetched. Rule: top-N pushdown only when there is no `HAVING` (and tie-breaks are deterministic).
5. **`AVG` denominator is `COUNT(x)`, not `COUNT(*)`** — otherwise NULLs bias the mean.
6. **`SUM(int)` can overflow per engine and `UInt64` breaks `int64` accumulation.** Cast to decimal (D6).
7. **`COUNT(DISTINCT)` across engines cannot use native sketches.** They are not portable (D7).
8. **`LIMIT` without `ORDER BY` is not mergeable.** Each shard returns an arbitrary subset; the union is not "the top N". Require `ORDER BY` for cross-shard limits, or apply the limit only after a full merge and return a reproducibility warning.
9. **`NULL` ordering defaults differ** (BigQuery `NULLS FIRST` for ASC, ClickHouse different again). Emitters must always spell out `NULLS FIRST`/`NULLS LAST`.
10. **Group-key types differ.** `1` (BigQuery `INT64`), `1` (ClickHouse `UInt64`), `"1"` (Omni string) must not become three groups. The catalog assigns a canonical type and the emitter casts.
11. **`SUM` of floats is order-dependent across engines.** Compensated summation plus a documented non-determinism note.
12. **The three-engine assumption itself** (§2).

---

## 4. Architecture

```text
                    ┌──────────────────────────────────────────────┐
  SQL text  ───────▶│ Frontend: lexer → parser → AST                │
  QuerySpec ───────▶│ (structured JSON front door, same AST)       │
                    └──────────────────┬───────────────────────────┘
                                       ▼
                    ┌──────────────────────────────────────────────┐
                    │ Binder + Logical Planner                     │
                    │ catalog · types · capabilities · coverage    │
                    │ → LogicalPlan, or a typed rejection          │
                    └──────────────────┬───────────────────────────┘
                                       ▼
              ┌────────────────────────┴─────────────────────────┐
              │ Mode selection                                    │
              │ passthrough (one engine) │ federated (split)      │
              └───────┬──────────────────────────┬───────────────┘
                      │                          ▼
                      │            ┌──────────────────────────────┐
                      │            │ Rewriter                      │
                      │            │ pushdown plan(s) + MergePlan  │
                      │            └──────────────┬───────────────┘
                      │                           ▼
                      │            ┌──────────────────────────────┐
                      │            │ Scatter/Gather (errgroup)    │
                      │            └───┬──────────┬───────────┬───┘
                      │                ▼          ▼           ▼
                      │             Omni      BigQuery   ClickHouse
                      │                └──────────┬───────────┘
                      ▼                           ▼
              ┌──────────────────────────────────────────────────┐
              │ Local merge engine (goagg | duckdb | arrow)      │
              │ group → merge partials → HAVING → sort → slice   │
              └──────────────────────────┬───────────────────────┘
                                         ▼
                                ResultSet + coverage + warnings
```

Two execution modes, chosen by the planner:

- **passthrough**: the whole logical plan maps to exactly one backend. Fast path for single-source queries, window functions, joins, quantiles. No merge.
- **federated**: only used when the plan is decomposable and coverage is sound. Split into pushdown + merge.

`EXPLAIN` returns the mode, the chosen backends, the emitted SQL per backend, and the merge plan. This is the primary debugging tool and should exist from M0.

---

## 5. The generic SQL contract

Target users write telemetry queries, not arbitrary OLAP. The supported grammar:

```text
SELECT [DISTINCT] select_list
FROM   table [AS alias]
[WHERE boolean_expr]
[GROUP BY expr_list]
[HAVING  boolean_expr]
[ORDER BY sort_item_list]
[LIMIT n [OFFSET m]]

select_list := select_item (',' select_item)*
select_item := expr [AS alias] | aggregate_call [AS alias]
aggregate   := COUNT(*) | COUNT([DISTINCT] expr)
             | SUM(expr) | MIN(expr) | MAX(expr) | AVG(expr)
             | STDDEV(expr) | STDDEV_POP(expr) | VAR_SAMP(expr) | VAR_POP(expr)
             | APPROX_COUNT_DISTINCT(expr)
expr        := column | literal | CAST(expr AS type) | func(expr, ...)
             | expr (('+'|'-'|'*'|'/') expr)
             | time_bucket(unit, ts) | date_trunc(unit, ts)
             | expr ('='|'!='|'<'|'<='|'>'|'>=') expr
             | expr [NOT] IN (value_list)
             | expr [NOT] BETWEEN expr AND expr
             | expr [NOT] LIKE expr | expr [NOT] ILIKE expr
             | expr IS [NOT] NULL
             | boolean_expr (AND|OR) boolean_expr
             | NOT boolean_expr
```

Deliberately **out of v1**: joins, subqueries, CTEs, set operations, window functions, `QUALIFY`, `TABLESAMPLE`, arrays/structs, `GROUP BY` positions, `ORDER BY` ordinals, `HAVING` on non-aggregates.

Rejections are structured and actionable:

```go
type UnsupportedError struct {
    Clause     string // "JOIN"
    Reason     string // "cross-engine joins are not decomposable"
    Suggestion string // "run with mode=passthrough and target=clickhouse"
    Pos        int
}
```

Two front doors, one IR:

1. **SQL text** for humans and Grafana-style callers.
2. **`QuerySpec` JSON** (table, filters, group-by, aggregates, order, limit) for generated/LLM-produced queries and for callers that should not need a parser.

Both produce the same `ast.Select`, so there is exactly one binder and one planner.

---

## 6. Front end: parser choice

| Option | Verdict |
|---|---|
| `vitess/go/vt/sqlparser` | **No.** MySQL grammar. No `ILIKE`, `FILTER`, `INTERVAL '1' HOUR`, `::`, `@timestamp`. Pulls the Vitess tree. |
| `blastrain/vitess-sqlparser` | **No.** Stale 2019 fork, same grammar limits. |
| `pingcap/tidb/pkg/parser` | **No.** MySQL grammar, very large. |
| `auxten/postgresql-parser` | **No as-is.** Correct grammar family, but last release 2022 and unmaintained. |
| `pganalyze/pg_query_go/v6` | Viable but **CGo**. Contradicts the static-binary goal. |
| `wasilibs/go-pgquery` | **Best fallback.** Exact `libpg_query` grammar, pure Go via wazero, actively maintained (Nov 2026). Costs an embedded WASM module and a runtime. |
| Hand-written subset parser | **Chosen for v1.** ~1.2k lines, zero dependencies, exact grammar, precise errors, trivially fuzzable. |

Rule: keep the parser behind

```go
type Frontend interface {
    Parse(sql string) (ast.Statement, error)
}
```

If the grammar grows toward real Postgres (CTEs, joins, windows), swap in `go-pgquery` behind the same seam and keep the AST adapter. Do not let the parser choice leak into the planner.

Pre-parse normalisation is allowed and must be explicit: backticks → double quotes for identifiers (Omni users write both), `now()` → `CURRENT_TIMESTAMP`, `INTERVAL '1' HOUR` → an `Interval{1, hour}` AST node.

---

## 7. Logical plan and catalog

### 7.1 Plan nodes

```go
type Plan interface{ isPlan() }

type Scan struct {
    Table   string
    Alias   string
    Filters []Expr           // pushed down
    From, To time.Time       // always injected from the request
}
type Aggregate struct {
    GroupBy []Expr
    Aggs    []AggCall        // decomposed at rewrite time
}
type Filter  struct{ Pred Expr }   // HAVING
type Project struct{ Items []Expr } // final projection
type Sort    struct{ Keys []SortKey }
type Limit   struct{ N int; Offset int }
```

Single-select plans are a linear chain, which makes the pushdown/merge split a simple walk.

### 7.2 Catalog

```go
type Catalog struct {
    Tables map[string]*Table
}

type Table struct {
    Name    string            // "logs"
    Signal  Signal            // logs | traces | metrics | events
    Columns map[string]Column // canonical schema
}

type Column struct {
    Name     string
    Type     Type        // canonical: Int64, UInt64, Float64, Decimal, String, Bool, Timestamp, JSON
    Nullable bool
}

type Binding struct {
    Engine   string     // "omni" | "bigquery" | "clickhouse"
    Source   string     // `"logs.default"` | `proj.ds.logs` | `strata.logs`
    Coverage Coverage
    Columns  map[string]string // logical → physical ("timestamp" → "`@timestamp`")
    Types    map[string]string // canonical → per-engine spelling
}
```

The catalog is the **security boundary** as well as the type boundary: every identifier that reaches an emitted query is looked up here. A token that is not a catalog column/alias is not emitted. This is what makes "SQL → SQL" re-emission safe (no raw client strings, no `;`, no table functions, no `FORMAT` injection).

ClickHouse-specific hardening: reject table functions (`url`, `file`, `remote`, `s3`, `mysql`, …) at bind time; they turn a read-only query into an SSRF/file-read primitive.

---

## 8. Dialect layer

```go
type Dialect interface {
    Name() string
    Capabilities() Capabilities

    QuoteIdent(string) string
    QuoteString(string) string
    BindArg(index int) string          // or literal encoding
    Interval(n int64, unit string) string
    TimeBucket(expr string, unit string, n int64) string
    Cast(expr string, t Type) string
    Like(expr, pattern string, caseInsensitive bool) string
    Regex(expr, pattern string) string
    LimitOffset(n, offset int) string
    MapAccess(expr, key string) string
}
```

`Capabilities` is what the planner consults:

```go
type Capabilities struct {
    ILIKE, FilterAgg, Qualify, Window bool
    TimeBucket   map[string]bool // "hour", "minute", "5m", ...
    MaxLimit     int
    Pagination   Pagination       // none | nextToken
    MaxQueryText int
    NullsOrdering bool
}
```

Per-dialect emission notes:

**Identifiers**

- Omni: backticks required for `@timestamp`, `@message`, dotted/hyphenated names. `"logs.default"` for tables.
- BigQuery: backticks. Always fully qualify `project.dataset.table`.
- ClickHouse: backticks (avoid double quotes so `"x"` can never be read as a string under a non-standard setting).

**Time**

```text
date_trunc('hour', ts)
  Omni       date_trunc('hour', ts)
  BigQuery   TIMESTAMP_TRUNC(ts, HOUR)
  ClickHouse toStartOfHour(ts)

time_bucket(5, 'minute', ts)
  Omni       date_bin(INTERVAL '5' MINUTE, ts, TIMESTAMP '1970-01-01')
  BigQuery   TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(ts), 300) * 300)
  ClickHouse toStartOfInterval(ts, INTERVAL 5 MINUTE)
```

Verify the Omni spelling against its function list at implementation time; the portable fallback is integer arithmetic on the epoch (`to_timestamp(floor(epoch(ts)/300)*300)`). BigQuery needs `DIV` because `%` on `INT64` is fine but the multiply must stay integral.

Emit the request's `from`/`to` as literals in every backend's native form, and **always** add the bound predicate — Omni requires it, and the other two need it for partition pruning and cost control.

**Strings and regex**

- `ILIKE`: native on Omni/ClickHouse; BigQuery → `LOWER(x) LIKE LOWER(p)`.
- `~ 'p'`: Omni `x ~ p`; BigQuery `REGEXP_CONTAINS(x, p)`; ClickHouse `match(x, p)`.

**Sorting**

- Always emit explicit `NULLS FIRST`/`NULLS LAST` on every sort key.
- Always append the group keys as tie-breakers when a limit is pushed down, so pagination is stable.
- Strings compare as bytes (all three default to binary/UTF-8 comparison); document that this is the contract rather than inheriting per-engine collation.

**Limits**

- Omni: clamp to 100,000; page internally; stop at the coordinator's row budget.
- BigQuery: no clamp, but cap the job's bytes billed and use `maxResults` paging.
- ClickHouse: set `max_result_rows` / `max_execution_time` via settings per query.

---

## 9. Two-phase rewrite and merge algebra

### 9.1 Aggregate decomposition registry

Every supported aggregate declares its partial form and merge rule. The rewriter consults this table; anything absent forces passthrough.

| User expression | Pushdown projection | Merge operation | Notes |
|---|---|---|---|
| `COUNT(*)` | `COUNT(*) AS c0` | `sum(c0)` | |
| `COUNT(x)` | `COUNT(x) AS c0` | `sum(c0)` | NULLs excluded per SQL semantics |
| `SUM(x)` int/decimal | `SUM(CAST(x AS DECIMAL(38,0))) AS s0` | `int128` sum | Avoids overflow and `UInt64`/`Int64` mismatch |
| `SUM(x)` float | `SUM(x) AS s0` | Neumaier sum | Document non-associativity |
| `MIN(x)` / `MAX(x)` | `MIN(x) AS m0` / `MAX(x) AS m0` | min / max over canonical order | NULLs ignored; all-NULL → NULL |
| `AVG(x)` int/decimal | `SUM(CAST(x AS DECIMAL(38,0))) AS s0, COUNT(x) AS c0` | decimal `s0_total / c0_total` | `c0` is `COUNT(x)`, **not** `COUNT(*)`; zero → NULL |
| `AVG(x)` float | `SUM(x) AS s0, COUNT(x) AS c0` | Neumaier sum ÷ count | Never cast a float to `DECIMAL(…,0)`; that truncates |
| `COUNT(DISTINCT x)` | `GROUP BY x` (bounded) | set union, dropping the NULL key | Exact; ignores NULL like `COUNT(x)`; budgeted |
| `APPROX_COUNT_DISTINCT(x)` | passthrough by default; federated only via a **portable hash** (`SHA256(x)` → 64-bit) plus a coordinator HLL | HLL merge | Native sketches (`HLL_COUNT`, `uniqState`) are **not portable** across engines; see the note below |
| `STDDEV(x)` / `VAR(x)` | `COUNT(x), SUM(x), SUM(x*x)` | merge via `n`, `Σx`, `Σx²`; sample vs population at finalise | Numerically fragile; `x*x` can overflow, so cast to decimal; prefer passthrough for exactness |
| `STRING_AGG` / `ARRAY_AGG` | — | — | Passthrough only |
| `percentile` / `quantile` | — | — | Passthrough only (engine-specific) |
| `ANY_VALUE` / `FIRST` | — | — | Not mergeable; reject |

> **Native distinct sketches are not mergeable across engines.** BigQuery `HLL_COUNT.INIT` emits an opaque `BYTES` sketch, ClickHouse `uniqState` emits an aggregate-state blob, and DataFusion has no portable equivalent. There is no cross-engine `MERGE`. A federated `APPROX_COUNT_DISTINCT` therefore has exactly two honest shapes: (a) run it **passthrough** on one engine and accept that engine's error bound, or (b) ship one 64-bit `SHA256`-derived hash per distinct value from every shard and run the HLL in the coordinator. Shape (b) is still bounded by the cardinality budget, so it buys mergeability and narrower rows, not sublinear traffic. Anything else silently produces a wrong number.

### 9.2 What the pushdown query looks like

```sql
-- user query
SELECT service, AVG(latency_ms) AS avg_ms, COUNT(*) AS n
FROM traces
WHERE timestamp >= :from AND timestamp < :to AND status = 'ERROR'
GROUP BY service
HAVING COUNT(*) > 100
ORDER BY avg_ms DESC
LIMIT 10

-- pushdown (per shard)
SELECT
    CAST(service AS VARCHAR)              AS k0,
    SUM(CAST(latency_ms AS DECIMAL(38,0))) AS a0_sum,
    COUNT(latency_ms)                      AS a0_cnt,
    COUNT(*)                               AS a1
FROM <physical source>
WHERE <ts bounded> AND status = 'ERROR'
GROUP BY CAST(service AS VARCHAR)

-- merge plan
group by k0
  a0 = sum(a0_sum) / sum(a0_cnt)
  a1 = sum(a1)
having a1 > 100
order by a0 desc nulls last, k0 asc
limit 10 offset 0
```

Note: no `HAVING`, no `ORDER BY`, no `LIMIT` in the pushdown. That is deliberate.

### 9.3 Safety rules (the rewriter must enforce these)

- **R5.** The pushdown projects *only* group keys and partials. No user-visible aliases leak into the merge.
- **R6.** `WHERE` may be pushed whole; it is row-local and idempotent.
- **R7.** `HAVING` is never pushed (a shard's partial count is a lower bound; filtering on it can drop contributing groups).
- **R8.** `ORDER BY`/`LIMIT` are applied after merge unless top-N pushdown is provably safe (§9.4).
- **R9.** Aggregates over the same expression are computed once and referenced by multiple output columns.
- **R10.** A query with no `GROUP BY` still merges: every shard returns exactly one row; `SUM(NULL, x) = x`, `COUNT` sums, `AVG` divides totals.
- **R11.** Group-key canonicalisation happens in the pushdown (`CAST`), so the coordinator hashes uniform types.
- **R12.** If any required partial column is missing from a shard's result, the whole query fails; missing data is never treated as zero.

### 9.4 Top-N pushdown (optimization, opt-in)

Safe when **all** hold:

1. There is no `HAVING`.
2. The sort keys are a subset of the projected group keys and partial results.
3. The sort is a total order after appending group keys as tie-breakers.

Then push `ORDER BY <keys> LIMIT (offset+limit)` and re-sort/slice locally. This is semantically identical to a full scan because the global top-K is always a subset of the union of per-shard top-K.

If `HAVING` exists, do not push. If the order has ties and no tie-breaker exists, either add one or mark the result "page boundaries not stable".

### 9.5 Row-shaped queries (no aggregation)

Not every query aggregates. `SELECT ts, message FROM logs WHERE … ORDER BY ts DESC LIMIT 100` is a valid federated query and needs an explicit contract:

- Push projection + `WHERE` + time bounds. Do **not** push `LIMIT` unless `ORDER BY` is pushed and total; otherwise each shard's arbitrary 100 rows are not the global top 100.
- Concatenate the streams in the local engine, which applies the final `ORDER BY`, `OFFSET`, and `LIMIT`.
- Duplicate rows are **legitimate**: shards are disjoint, so two identical rows may be two real events. Never dedupe by row hash except for an explicit `SELECT DISTINCT`.
- The global row budget still applies. A query that would return 1M rows fails with `RESOURCE_EXHAUSTED`, not a silent truncation.
- Omni caps results at 100k rows and pages 1,000 at a time; the driver loops pages until the budget, and a row query that legitimately needs more is split into parallel time slices upstream.

### 9.6 Non-decomposable → mode selection

| Shape | v1 behaviour |
|---|---|
| Window function | passthrough to one backend that supports it |
| Join, both sides on one engine | passthrough |
| Join across engines | reject (v2: broadcast a small dimension, or pull rows into the local engine) |
| `SELECT DISTINCT` (no aggregates) | federated: push `GROUP BY` keys, union-dedupe locally |
| CTE / subquery / set operation | reject in v1 |
| `ORDER BY` on an expression not in the projection | reject in v1 (or compute locally if the expression is deterministic) |

---

## 10. Type system and value canon

```go
type Type uint8
const (
    TNull Type = iota
    TInt64      // signed 64
    TUInt64     // ClickHouse UInt64 that does not fit int64
    TFloat64
    TDecimal    // int128 + scale, exact
    TString     // UTF-8, byte comparison
    TBool
    TTimestamp  // UTC, microsecond precision
    TJSON
)
```

Rules:

1. **Timestamps** are canonicalised to UTC microseconds at the driver edge. Omni is ns, ClickHouse is ns, BigQuery is µs; truncating to µs keeps buckets aligned. Sub-µs precision is not exposed in v1.
2. **Integers**: use `int64`. A ClickHouse `UInt64` value above `MaxInt64` sets a `precision_loss` warning and is carried as `TUInt64`; sums promote to decimal, never wrap.
3. **Decimals**: `DECIMAL(38, 9)` is the common denominator. BigQuery `NUMERIC` is 38 digits / scale 9, ClickHouse `Decimal128(38, 9)`, DataFusion `Decimal128(38, 9)`. Integer sums use scale 0 internally and render at scale 9 when mixed with decimals.
4. **Floats**: canonical `float64`. Sums use Neumaier compensation. Comparisons for grouping never use floats as keys unless the user asked for it (and then we hash the bit pattern).
5. **Strings**: exact byte comparison, no collation. `ILIKE` is lowered explicitly. This makes grouping deterministic across engines.
6. **Booleans**: BigQuery `BOOL`, ClickHouse `UInt8`, Omni bool; all normalised at the driver.
7. **NULL** is a value, not an absence: it participates in grouping as its own key and is excluded from `COUNT(x)`/`SUM(x)`/`AVG(x)`.

The driver boundary is where per-engine oddities die:

```go
type RowStream interface {
    Schema() Schema
    Next(ctx context.Context) (Batch, error)   // io.EOF at end
    Close() error
}
```

Each driver (`omni`, `bigquery`, `clickhouse`) implements exactly this and is responsible for `CAST`-conformant output. The merge engine never sees a driver-specific type.

---

## 11. Execution model and resilience

```go
type Options struct {
    Deadline      time.Duration // coordinator budget, default 20s (matches Doctor)
    PerEngine     time.Duration // default 15s
    MaxRows       int           // default 50k
    MaxGroups     int           // default 1M
    MaxBytes      int64         // default 256 MiB in flight
    AllowPartial  bool          // default false
    MaxConcurrent int           // default 3 (one per backend)
}
```

- **Scatter/gather**: `errgroup.WithContext`, one goroutine per selected backend, bounded by a semaphore. First error cancels the rest unless `AllowPartial`.
- **Backpressure**: streams are consumed as they arrive; batches are size-capped (e.g. 8k rows) so one engine cannot pin memory.
- **Retries**: only idempotent reads, only on 429/5xx/connection reset, at most twice, jittered, honouring `Retry-After`. Never retry a query that already streamed rows into the aggregator (dual delivery would double-count) — restart the whole gather into a fresh aggregator instead.
- **Circuit breaker** per backend, keyed by target: Omni throttling must not turn every query into a 15s timeout.
- **Health-aware replica selection** (§2), not round-robin.
- **Result cache**: off by default. A plan cache keyed by `(normalized SQL, catalog version, from/to)` is fine; caching data rows is not, because telemetry is time-sensitive.

Failure policy:

| Scenario | Default | `allow_partial=true` |
|---|---|---|
| One shard errors/timeouts | error | result + `coverage.complete=false`, `missing=[engine]` |
| One replica errors | fail over to a replica | same |
| All shards for a group fail | error | error (nothing to merge) |
| Type/precision loss | result + warning | same |
| Group/row budget exceeded | `RESOURCE_EXHAUSTED` | same (never silently truncate groups) |

Response metadata is part of the contract:

```go
type Result struct {
    Columns  []ColumnMeta
    Batches  []Batch
    Backends []BackendStats   // rows, bytes, latency, retries, pages
    Coverage CoverageInfo     // expected, answered, missing, complete
    Warnings []Warning        // precision_loss, unstable_paging, float_sum, ...
}
```

---

## 12. Local compute: pure Go, Arrow, and DuckDB

### 12.1 Interfaces

```go
type LocalEngine interface {
    // Merge consumes one stream per backend and produces the final rows.
    Merge(ctx context.Context, plan MergePlan, inputs []RowStream) (ResultSet, error)
    Close() error
}
```

Implementations:

- **`goagg`** (default, pure Go). Hash aggregation over Arrow record batches. ~400–600 lines: group table keyed by canonical bytes + xxhash, partial accumulators (`int128`, Neumaier sums, sets), finalise, `HAVING`, sort, slice. No CGo, no spill, hard group/byte caps.
- **`duckdbsql`** (build tag `cgo && duckdb`). Registers each partial result as a temp table (or an Arrow scan), runs the merge plan as SQL, streams rows back. Enables spill-to-disk, joins, window functions, and large `COUNT(DISTINCT)` later.
- **`arrow`** is not a third engine; it is the batch format shared by drivers, `goagg`, and the DuckDB adapter.

### 12.2 Why Arrow

- It is already a MonoFS dependency (`github.com/apache/arrow-go/v18`), so the marginal cost inside the MonoFS build is zero.
- Columnar batches avoid `[]any` boxing in the hot path and let `goagg` touch `int64`/`float64` buffers directly.
- DuckDB ingests Arrow without row conversion: `duckdb-go/v2` exposes `NewArrowFromConn` **behind `-tags=duckdb_arrow`** (verified in the v2 README), and `Appender` for row-wise loads.
- Parquet fixtures for the differential test suite come for free.

The cost is binary size and one more dependency for a standalone build. Measure it in M0 (three builds: core-only, core+arrow, core+arrow+duckdb) and publish the numbers. If Arrow is too heavy, `goagg` can define a minimal `Batch` interface and Arrow becomes the optional adapter; the planner and drivers do not change.

Use `array.Record` with explicit `Retain`/`Release` ownership: the driver hands ownership to the merge engine, which releases after consumption.

### 12.3 Why DuckDB is optional, not the default

- It is CGo. `duckdb-go/v2` statically links prebuilt libs; MonoFS already builds with `-tags duckdb`, but the coordinator should still build CGo-free by default.
- It is the right tool for spill, cross-engine joins, and window functions — i.e. exactly the v2 features that are rejected in v1.
- Version alignment matters: MonoFS pins `duckdb-go/v2 v2.5.5` (DuckDB 1.4.4); current is `v2.20000.0` (DuckDB 2.0.0). A library consumed by MonoFS must match MonoFS's pin; a standalone binary can track latest. Keep the adapter behind the interface so the choice is a build decision.

### 12.4 Integration with MonoFS

Because `LocalEngine` and `RowStream` are interfaces, and MonoFS already threads a `QueryScope` through its query calls, MonoFS can:

1. use `sqlagregator` as a library and implement `LocalEngine` over its existing `duckQueryBackend.QueryDuckDB(ctx, sql, args...) (*sql.Rows, error)` seam (this is the "reuse the DuckDB we already have" path — no new DuckDB dependency, no version conflict), or
2. run it as a sidecar and speak its HTTP API, or
3. reuse only the dialect/rewrite packages over its own DuckDB views.

Recommendation: **library first**, sidecar second. The Doctor `/v1/sql/query` envelope (`dataset`, `nodes`, `restricted`, `visible_partitions`) should be reused rather than inventing a third response shape.

---

## 13. Public API

```go
package sqlag

type Coordinator struct{ /* ... */ }

func New(cfg Config) (*Coordinator, error)

func (c *Coordinator) Query(ctx context.Context, req Request) (*Result, error)
func (c *Coordinator) Explain(ctx context.Context, req Request) (*Explanation, error)
func (c *Coordinator) QuerySpec(ctx context.Context, spec QuerySpec) (*Result, error)

type Request struct {
    SQL          string
    From, To     time.Time       // required; default 1h if zero
    Limit        int
    Mode         Mode            // auto | federated | passthrough
    Target       string          // for passthrough
    Backends     Selector        // include / exclude / treat_as_replica (§15)
    AllowPartial bool
}
```

HTTP (mirrors Doctor's console):

```text
POST /v1/sql            { query, from, to, limit, mode, target, backends, allow_partial }
POST /v1/sql/explain    → { mode, selected[], excluded[{id,reason}], coverage, pushdown_sql[], merge_plan }
POST /v1/query          { spec: QuerySpec, from, to, backends, ... }
GET  /v1/sql/backends   → endpoint registry with role, tags, coverage group, selected_by_default
GET  /healthz           → per-backend health, circuit state, queue depth
GET  /metrics           → Prometheus: queries, partials, per-backend latency, rows, pages
```

`/v1/sql` returns `200` with `coverage.complete=false` only when `allow_partial=true`; otherwise a failed shard is `502`/`503` with the failing backend named.

---

## 14. Package layout

```text
sqlagregator/
  go.mod                         # core module (yaml only)
  pkg/sqlag/                     # public API consumed by hosts
  drivers/
    clickhouse/go.mod            # stdlib-only driver module
    bigquery/go.mod              # BigQuery REST driver module
    omni/go.mod                  # CloudWatch Omni driver module
  cmd/sqlag/go.mod               # CLI module (core + all drivers)
  go.work                        # local workspace across the modules
  internal/
    sql/                         # lexer, parser, ast, errors
    plan/                        # binder, logical plan, validate, explain
    catalog/                     # tables, columns, bindings, coverage, endpoint registry
    selector/                    # endpoint include/exclude resolution + coverage validation
    dialect/                     # interface + generic/bigquery/clickhouse/omni/duckdb
    rewrite/                     # pushdown split, aggregate registry, top-N
    execute/                     # coordinator, scatter/gather, retry, circuit, budgets
    # drivers live in ../../drivers/* as separate modules
    value/                       # canonical types, decimals, coercion, compare, hash
    merge/                       # goagg, having, sort, limit, hll
    local/duckdbsql/             # //go:build cgo && duckdb
    local/arrowbatch/            # shared batch helpers
    api/                         # HTTP/gRPC, auth, scope
  pkg/sqlag/                     # public Go API
  testdata/
    corpus/                      # golden SQL per dialect
    fixtures/                    # Parquet/CSV for differential tests
  DESIGN.md
```

Dependencies (core): `gopkg.in/yaml.v3` only.

**Drivers are separate modules:** `drivers/clickhouse` (stdlib), `drivers/bigquery` (`google.golang.org/api`), `drivers/omni` (AWS SDK), and `cmd/sqlag`. A host importing `pkg/sqlag` does not inherit any driver requirement; this is why MonoFS's AWS SDK stays at its original v1.41.5. Arrow/DuckDB remain optional and unimported.

Keep drivers in their own optional modules if the SDK weight becomes a problem: the core must not depend on the AWS or GCP SDKs.

### 14.1 Scaffold status (this repository)

Implemented and tested:

- `internal/value` canonical values, exact decimals, overflow-safe addition, grouping keys.
- `internal/sql` + `internal/ast` hand-written lexer/parser for the documented subset, with typed rejections (joins, `INTERVAL`, multi-statement, subqueries).
- `internal/catalog` YAML registry: tables, columns, endpoint roles, coverage groups, capability inheritance.
- `internal/selector` include/exclude/treat-as-replica resolution, hash and time coverage, overlap detection, `min_coverage`, hard `max`, exclusion reasons.
- `internal/plan` binder: types, aggregate decomposition, implicit/rejected group keys, HAVING/ORDER BY alias rewriting.
- `internal/rewrite` + `internal/dialect` pushdown builders and emitters for DuckDB, ClickHouse, BigQuery, Omni.
- `internal/merge` pure-Go hash aggregation (count/sum/avg/min/max), HAVING, sorting, offset/limit, row mode, global aggregates with no rows, compensated float sums.
- `internal/execute` coordinator: parse → bind → resolve → emit → scatter → merge, with coverage-aware partial results.
- live drivers: ClickHouse (HTTP/JSON, typed decoding), BigQuery (REST `jobs.query` + paging), CloudWatch Omni (`cloudwatchomni` SDK, session + polling + paging).
- `pkg/sqlag`: the stable public surface consumed by MonoFS.
- `cmd/sqlag` `backends`, `explain`, and `query` against a real catalog (`examples/`).
- multi-module layout: core (`pkg/sqlag`) + `drivers/{clickhouse,bigquery,omni}` + `cmd/sqlag`, with `go.work` and `make test`.
- MonoFS RPC integration: `QuerySQL` (router) + `QueryPartial` (node), typed partial rows, truncation as an error.
- SQL authorization hardening: fail-closed router scope, internal-token-only node SQL RPCs, 403 mapping in Doctor (`SECURITY.md`).

Explicitly not wired yet (next milestones):

- federation across **shared ClickHouse** mode (`QuerySQL` returns `Unimplemented`
  until the replica/coverage mapping is finished);
- `COUNT(DISTINCT)` and `APPROX_COUNT_DISTINCT`;
- top-N pushdown, spill, and the Arrow/DuckDB local engine;
- the MonoFS `QuerySQL`/`QueryPartial` RPCs and the Doctor `shadow` flag (§16).

---

## 15. Endpoint selection and exclusion API

A deployment can register many endpoints (25, 100, …) while only a subset are true disjoint shards; the rest may be replicas, duplicates, or migration leftovers. Selecting the execution set is therefore a first-class, auditable operation — not an afterthought and never a silent default.

### 15.1 Registry

```go
type Role string
const (
    RoleShard   Role = "shard"
    RoleReplica Role = "replica"
)

type Endpoint struct {
    ID            string            // stable; never positional
    Backend       string            // omni | bigquery | clickhouse | duckdb-local
    Tables        []string          // logical tables it can answer
    Tags          map[string]string // role, pool, region, tier, ...
    Role          Role              // shard | replica
    CoverageGroup string            // shards in one group must be disjoint
    ReplicaOf     string            // for Role=replica: the shard group it mirrors
    Range         *TimeRange        // nil = full retention
    Source        string            // physical table/namespace
    Columns       map[string]string // logical -> physical
    Caps          Capabilities
    Disabled      bool
}
```

Invariants:

- IDs and tags come from configuration/discovery, never from request input.
- `Role` defaults to `shard`. An endpoint not declared `replica` is load-bearing: excluding it can create a coverage gap.
- `CoverageGroup` scopes the disjointness check. Two selected `shard` endpoints in one group may not overlap in time.
- `ReplicaOf` names the group a replica mirrors; replicas are failover candidates, never summands.

### 15.2 Selector

Request JSON:

```json
{
  "query": "SELECT service, count(*) AS n FROM logs WHERE timestamp >= … GROUP BY service",
  "from": "2026-11-01T00:00:00Z",
  "to":   "2026-11-02T00:00:00Z",
  "backends": {
    "include": [{"tags": {"tier": "hot"}}],
    "exclude": [{"ids": ["node-21", "node-22"]}],
    "treat_as_replica": [{"tags": {"pool": "legacy"}}],
    "include_replicas": false,
    "max": 8,
    "min_coverage": 1.0
  },
  "allow_partial": false
}
```

Go:

```go
type Matcher struct {
    IDs      []string          // exact, or glob ("node-2*")
    Backends []string          // "clickhouse", "omni", ...
    Tags     map[string]string // all must match; values may be globs
    Roles    []string          // "shard" | "replica"
    Groups   []string          // coverage groups
}

type Selector struct {
    Include         []Matcher // filter: endpoint must match >= 1 matcher
    Exclude         []Matcher // subtract
    TreatAsReplica  []Matcher // reclassify as redundant *before* coverage
    IncludeReplicas bool      // add replicas to the failover set, never the sum set
    Max             int       // hard cap; exceeding is an error
    MinCoverage     float64   // 0..1; 1.0 = every shard required
}
```

Matching semantics: within a matcher all specified fields must match (AND); across matchers any may match (OR). Glob matching is `path.Match`-style and is applied to IDs and tag values. An empty matcher matches everything (useful for `exclude: [{}]`, which is rejected as "nothing left" later).

### 15.3 Resolution algorithm

1. Start from enabled endpoints that serve the logical table.
2. Apply `TreatAsReplica` (recorded as an operator assertion).
3. Apply `Include` (intersection), then `Exclude` (subtraction).
4. Apply the deployment default: replicas are excluded unless `IncludeReplicas`. (Reasons are reported in this order, so an explicit exclusion of a replica reads `request_exclude`, not `replica`.)
5. Drop endpoints missing a required capability (e.g. Omni for metrics) with reason `missing_capability`.
6. Drop unhealthy or circuit-open endpoints with reason `unhealthy` — **only if coverage survives**; otherwise fail instead of undercounting.
7. Split survivors into the **sum set** (`Role=shard`) and the **failover set** (`Role=replica`).
8. Validate coverage per `CoverageGroup`:
   - overlap between selected shards → `coverage_overlap` (hard error, always)
   - union of shard ranges must cover `[from, to]`; gaps → `coverage_gap`
   - covered fraction < `MinCoverage` → `coverage_below_min`
9. Enforce `Max`; sort deterministically by ID so plans and audits are reproducible.
10. Return the resolution, including **every excluded endpoint with a reason**.

```go
type Exclusion struct {
    ID     string
    Reason string // replica | request_exclude | not_included | missing_capability |
                  // unhealthy | disabled | exceeds_max | wrong_table
}

type Resolution struct {
    Shards   []*Endpoint
    Replicas []*Endpoint
    Excluded []Exclusion
    Coverage CoverageInfo // complete, covered [from,to], gaps, groups
    Warnings []string
}
```

### 15.4 The 25-endpoint case

Twenty-five endpoints are registered; four are real disjoint shards, twenty-one are suspected replicas. The operator either tags them or lists them:

```json
{"backends": {
  "treat_as_replica": [{"tags": {"pool": "legacy"}}],
  "exclude": [{"ids": ["node-21","node-22","node-23","node-24","node-25"]}],
  "min_coverage": 1.0
}}
```

Outcomes:

- If the twenty-one are replicas (declared or asserted), coverage stays complete; `explain` lists them as `excluded: replica` / `excluded: request_exclude`.
- If one excluded endpoint was actually a shard, the plan fails with `coverage_gap` naming that endpoint. That is the desired behaviour: the operator learns the assumption was wrong instead of receiving a low count.
- `allow_partial=true` turns the gap into a labelled partial result (`coverage.complete=false`, `missing_ranges=[…]`), never a silent one.

### 15.5 Discovery and explain

```http
GET /v1/sql/backends?table=logs
```

```json
{
  "endpoints": [
    {"id":"node-01","backend":"duckdb-local","role":"shard","tags":{"pool":"primary"},
     "groups":["monofs-local"],"selected_by_default":true,"healthy":true},
    {"id":"node-21","backend":"duckdb-local","role":"replica","tags":{"pool":"legacy"},
     "groups":["monofs-local"],"selected_by_default":false,"reason":"replica"}
  ]
}
```

`POST /v1/sql/explain` returns the same selection plus `excluded[].reason`, the pushdown SQL for each selected endpoint, and the merge plan. This is the API the operator uses to confirm that 21 endpoints really are excluded for the right reason.

### 15.6 Precedence and audit

Precedence is `request > tenant policy > deployment default`. `Include` narrows; `Exclude` subtracts; no request can add an endpoint that tenant policy removed. Every query records `selected[]`, `excluded[{id,reason}]`, `coverage`, `allow_partial`, and `mode` in the audit record. Operationally, the exclusion list is data — not code — so changing which replicas are ignored does not require a deploy.

---

## 16. Integration and replacement plan

### 16.1 What exists today (verified)

- Doctor `POST /v1/sql/query` → `LogEngineBackend.QueryDuckDBSQL(ctx, tenant, dataset, query, from, to, limit, visible, restricted)`.
- The MonoFS router `QueryDuckDBSQL` fans out to **every healthy node** and returns one result envelope per node. Its own comment says results are returned per node "rather than pretending a cross-node aggregate is exact" — the client/UI is left to merge.
- In ClickHouse mode (`sharedTelemetryReads()`), the router queries **one** node and returns a single envelope, because the backend already holds all data.
- Partition scope is applied **inside** the DuckDB/ClickHouse view, so the SQL text cannot widen access.
- Nodes validate SQL via DuckDB `json_serialize_sql` plus an allow-list: one statement, `SELECT`/`WITH`, dataset only, no table functions.

The current per-node fan-out is exactly the bug this project fixes: `COUNT(*)` across nodes is only correct because the client happens to sum, and `AVG` across nodes is wrong today because averages of averages are not averages.

### 16.2 Target topology

```text
Doctor  POST /v1/sql/query
   │        (public API, policy, audit, quotas)
   ▼
MonoFS router ── QuerySQL RPC ──▶ sqlagregator coordinator (in-process library)
   │                                  │
   ├── node-01 … node-N ──────────────┤  duckdb-local shards (disjoint)
   │      QueryPartial(pushdown SQL)   │
   ├── clickhouse (via one node) ─────┤  shared or replica group
   └── omni / bigquery (direct) ──────┘  external shards
                                      ▼
                              goagg merge → merged rows
```

The coordinator lives in the **MonoFS router**, not in Doctor:

- it already owns node fan-out, health, and the node client pool;
- it reaches ClickHouse through nodes, preserving the "Doctor does not hold a ClickHouse DSN" invariant;
- it can hold Omni/BigQuery credentials in the data plane, where telemetry credentials already live;
- Doctor remains the public API and policy layer and never fans out itself.

If you prefer Doctor-resident, the library works there too — but it then needs ClickHouse credentials (or a streaming proxy through MonoFS), which is a deliberate architectural change.

### 16.3 New RPC surface

**Implemented in v1:** `QuerySQL` on the router (local Parquet shards only),
`QueryPartial` on nodes, typed partial results (`types[]`), and truncation as
an error instead of a silent cut. Shared ClickHouse mode is rejected with
`Unimplemented` so replica data is never summed.


```proto
message BackendMatcher {
    repeated string ids = 1;
    repeated string backends = 2;
    map<string, string> tags = 3;
    repeated string roles = 4;
    repeated string groups = 5;
}

message BackendSelector {
    repeated BackendMatcher include = 1;
    repeated BackendMatcher exclude = 2;
    repeated BackendMatcher treat_as_replica = 3;
    bool include_replicas = 4;
    int32 max = 5;
    double min_coverage = 6;
}

message QuerySQLRequest {
    string dataset = 1;             // logs | metrics | traces
    string query = 2;               // generic SQL
    int64 from_unix_nano = 3;
    int64 to_unix_nano = 4;
    int32 limit = 5;
    repeated string visible_partitions = 6;
    bool visible_partitions_set = 7;
    BackendSelector backends = 8;
    bool allow_partial = 9;
    string mode = 10;               // auto | federated | passthrough
}

message QuerySQLResponse {
    string result_json = 1;         // columns, rows, coverage, backends, warnings
}

// Per-node pushdown. Returns typed partial rows, not a user-facing result.
message QueryPartialRequest {
    string dataset = 1;
    string sql = 2;                 // generated DuckDB/ClickHouse SQL
    int64 from_unix_nano = 3;
    int64 to_unix_nano = 4;
    int32 limit = 5;
    repeated string visible_partitions = 6;
    bool visible_partitions_set = 7;
    repeated string expected_columns = 8;
    repeated string expected_types = 9;
}

message QueryPartialResponse {
    string columns_json = 1;
    string types_json = 2;
    string rows_json = 3;           // Arrow IPC later
    bool truncated = 4;
}
```

`QueryDuckDBSQL` stays untouched for compatibility. `QuerySQL` is additive.

### 16.4 Dialect and scope split

| Endpoint | SQL emitted | Scope enforcement |
|---|---|---|
| `duckdb-local` (node) | DuckDB SQL over `logs`/`metrics`/`traces` views | view-enforced, unchanged |
| `clickhouse` | ClickHouse SQL over the same views, `FORMAT JSON`, read-only | view-enforced, unchanged |
| `omni` | explicit columns, explicit `CAST`s, mandatory `@timestamp` bound | coordinator-injected; endpoint must declare `enforces_partition_scope=true` or the query is refused |
| `bigquery` | fully-qualified table, partition filter | coordinator-injected; same capability requirement |

**SQL authorization is fail-closed.** `QuerySQL` and `QueryDuckDBSQL` call
`querySQLScope`, which computes visible partitions even when read enforcement is
in observe mode and denies when no policy is loaded (`--sql-require-policy`,
default true). A restriction forwarded by Doctor is intersected, never widened.
Nodes additionally require the cluster-internal token on `QueryDuckDBSQL` and
`QueryPartial`; the router attaches it on every node dial, so a direct node
caller cannot assert an unrestricted scope. See `SECURITY.md`.

Restricted callers (`QueryScope.Restricted`) may only use endpoints that can enforce the partition predicate. If a restricted query selects Omni/BigQuery without that capability, resolution fails with `scope_not_enforceable` — it does not silently return unfiltered rows.

Generated SQL must pass the node's existing allow-list. That means: single statement, no table functions, dataset view only, no CTE redefinition of the dataset. The emitter already obeys this; the allow-list is the backstop.

### 16.5 Compatibility and rollout

1. **Add**, don't replace: `QuerySQL` RPC + coordinator in the router; `QueryDuckDBSQL` unchanged.
2. Doctor flag `--sql-engine=duckdb|federated|shadow`, default `duckdb`.
3. **Shadow mode**: run both paths, compare exact plans (row counts, group-key sets, sums within a documented tolerance), return the legacy result, emit `sql_shadow_mismatch_total{dataset}`. This is how you earn confidence without risking dashboards.
4. Flip per tenant/dataset to `federated`. Raw-row queries use row mode; DuckDB-specific SQL falls back to passthrough on `duckdb-local`.
5. Migrate clients/UI to consume `coverage` + merged rows, then stop returning per-node envelopes.
6. Delete `sharedTelemetryReads()`; it becomes `role=replica` + `include_replicas=false` in the registry.

Response compatibility: keep `dataset`, `nodes[]`, `restricted`, `visible_partitions`. In federated mode return one synthetic `nodes[]` entry containing the merged result, plus `coverage`, `backends[]`, `warnings[]`. Old clients keep working; new clients read the metadata.

### 16.6 Replacing the log engine's query path

- The `duckdb-local` backend is implemented over the existing `duckQueryBackend.QueryDuckDB(ctx, sql, args)` seam — no new DuckDB dependency, no version conflict.
- `LogEngine.QueryDuckDBSQL` remains for raw/passthrough queries.
- A new `QueryPartial` path accepts generated pushdown SQL and returns typed columns (`DuckDBSQLResult` gains `Types []string`; Arrow later).
- `goagg` in the router merges partials. DuckDB-at-router is optional and only needed for spill/joins (M4).

### 16.7 Rollback

Flip `--sql-engine=duckdb` on the Doctor side. The legacy RPC path is untouched, so rollback is a flag change, not a redeploy of MonoFS.

---

## 17. Testing strategy

Correctness here is not "does it compile"; it is "does the merged number equal the single-machine number".

1. **Golden SQL per dialect.** Every logical query in `testdata/corpus` emits a stable SQL string per backend. Reviews catch dialect regressions.
2. **Differential (property) testing — the most valuable test.** Load a fixed dataset into a local DuckDB (ground truth). Split it into random disjoint shards, run the federated path through fake drivers (in-process SQLite/DuckDB shards), and compare the merged result to the ground-truth single query. Generate hundreds of random plans: filters × group keys × aggregates × having × order × limit/offset.
3. **Merge-algebra laws.** For each aggregate: `merge(part(A), part(B)) == part(A ∪ B)`, including NULLs, empty groups, all-NULL columns, duplicates, and boundary integers.
4. **Selector and coverage matrices.** 25 endpoints, every combination of `include` / `exclude` / `treat_as_replica` / `include_replicas`; assert selected IDs, excluded reasons, overlap errors, gap errors, `min_coverage`, and the "never zero endpoints" rule. This is a table test, not a live test.
5. **Type torture fixtures.** `UInt64` above `MaxInt64`, decimal scale edges, timestamps at µs/ns boundaries, mixed string/number fields (Omni), NaN/Inf floats.
6. **Failure injection.** One shard slow, one 429, one returns a truncated page, one returns an unexpected column type. Assert the failure policy and the coverage metadata.
7. **Emission safety.** Fuzz the parser; assert that no emitted SQL contains a raw input token, a statement separator, a table function, or an unquoted identifier that is not in the catalog.
8. **Pagination.** Omni `nextToken` loops, dedupe on page boundary, budget stop, and cancellation mid-page.
9. **Load/shape tests.** 1M groups, 50k rows, 3 backends with a 20s budget — assert bounded memory and clean `RESOURCE_EXHAUSTED`.
10. **Integration contract tests.** bufconn tests against a fake MonoFS router: `QuerySQL` → `QueryPartial` per selected node, scope forwarded, coverage metadata preserved, legacy `QueryDuckDBSQL` unchanged. Shadow mode must never alter the returned result.

CI runs (1)–(7) and (10) with no cloud credentials. Live backends are an opt-in integration suite.

---

## 18. Milestones

| Milestone | Deliverable | Exit criteria |
|---|---|---|
| **M0** | Skeleton, IR, catalog, **registry + selector + `EXPLAIN`**, parser for the SELECT subset, ClickHouse/DuckDB emitters, `goagg` | `SELECT service, count(*) FROM logs WHERE ts … GROUP BY service` explains into pushdown SQL for a chosen subset of endpoints; excluding 21 of 25 endpoints reports coverage correctly; differential merge tests run locally |
| **M1** | Aggregate registry, two-phase rewrite, full merge algebra, BigQuery + Omni emitters | Differential suite green for count/sum/min/max/avg, filters, group-by, having, order, limit/offset |
| **M2** | Omni driver (time bounds, paging, casts), BigQuery driver, failure policy, circuit breaker, metrics | Partial-failure and paging tests green; `coverage` metadata correct |
| **M2.5** | MonoFS `QuerySQL`/`QueryPartial` RPCs, Doctor `shadow` mode | Shadow runs both paths and reports mismatches without changing results |
| **M3** | `distinct` budget + portable HLL, `stddev/var`, top-N pushdown | Cardinality-budget behaviour documented and tested; no silent approximation |
| **M4** | DuckDB local engine + Arrow adapter, spill mode, optional raw-row mode | Same differential suite green with `-tags duckdb`; spill verified at 1M groups |
| **M5** | Doctor flip to `federated`, API polish, auth/scope, docs | End-to-end through Doctor with partition scoping; rollback is a flag |

Cut from v1 without regret: joins, subqueries, CTEs, window functions, set operations, `QUALIFY`, `TABLESAMPLE`, arrays/structs.

---

## 19. Risks and open questions

**Risks**

- **Endpoint classification drift.** A new node comes up and is neither tagged nor declared, or a shard is mislabelled `replica`. Mitigated by startup validation (every endpoint must declare `role`), `GET /v1/sql/backends` review, and coverage-gap failures.
- **Excluding the wrong endpoint.** `Exclude` can create a gap; the coverage model makes that loud. `TreatAsReplica` suppresses the gap, so it is recorded in the audit log and should be reviewed like a schema change.
- **Omni cost/throttling.** Omni bills bytes scanned; an unbounded fan-out is expensive. Always inject time bounds, cap rows, and add a per-tenant rate limit.
- **Semantic drift from engine "helpful" coercion.** Mitigated by casting on projection and by the differential suite.
- **Float and decimal expectations.** `AVG` over integers returning a decimal will surprise callers; document the exact output types.
- **Arrow/DuckDB binary size.** Measure before committing; keep the interface so the choice is reversible.
- **Omni API stability.** `StartTelemetryQuery` is new; isolate it behind the driver and keep a SigV4-signed HTTP fallback.

**Decisions still open**

1. Which logical signals does each engine hold? Is Omni logs+traces only, with metrics excluded from the federated surface? (The design assumes yes.)
2. Do you have a stable schema snapshot for BigQuery and ClickHouse, or should the catalog discover it at startup (`INFORMATION_SCHEMA`, `DESCRIBE`)?
3. Is a **partial result** ever acceptable for a dashboard, or should every multi-shard query fail closed? (Default: fail closed.)
4. Latency budget: is the existing Doctor 20s / 5k-row envelope the target, or should federated queries get more?
5. Do the 25 endpoints map 1:1 to MonoFS nodes (so the router's node registry is the endpoint registry), or is there a separate endpoint inventory?

---

## 20. One-paragraph verdict

Build the coordinator, not a SQL-string translator. The value is in the logical plan, the coverage model, the aggregate registry, and the differential test suite; the dialect emitters are the easy part. Use a hand-written parser for the documented subset (with `go-pgquery` as the escape hatch), push only decomposable aggregates and let the coordinator own `HAVING`/`ORDER BY`/`LIMIT`, canonicalise every type at the driver edge, and keep DuckDB as an optional local engine behind an interface so the default binary stays pure Go. Then measure everything against ground truth, because a federated `AVG` that is off by 2% is worse than an error.
