package dialect_test

import (
	"strings"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/dialect"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/rewrite"
	"github.com/radryc/sqlagregator/internal/sql"
)

func testCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	yaml := `tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
      - {name: level, type: string}
      - {name: latency, type: int64}
      - {name: partition, type: string}
coverage_groups:
  archive: {mode: time}
endpoints:
  - id: omni
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: archive
    source: logs.default
    columns: {timestamp: "@timestamp"}
  - id: bq
    backend: bigquery
    tables: [logs]
    role: shard
    coverage_group: archive
    source: strata_prod.telemetry.logs
    columns: {timestamp: timestamp}
  - id: ch
    backend: clickhouse
    tables: [logs]
    role: shard
    coverage_group: archive
    source: strata.logs
    columns: {timestamp: timestamp}
  - id: duck
    backend: duckdb-local
    tables: [logs]
    role: shard
    coverage_group: archive
    source: logs
    columns: {timestamp: timestamp}
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func TestPushdownPerDialect(t *testing.T) {
	cat := testCatalog(t)
	sel, err := sql.Parse(`
		SELECT service,
		       date_trunc('hour', timestamp) AS hour,
		       count(*) AS n,
		       avg(latency) AS mean
		FROM logs
		WHERE level = 'ERROR'
		GROUP BY service, date_trunc('hour', timestamp)
		ORDER BY hour`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(24 * time.Hour)

	cases := []struct {
		endpoint string
		want     []string
	}{
		{"omni", []string{
			`"logs.default"`,
			`CAST("service" AS VARCHAR) AS "k0"`,
			`date_trunc('hour', "@timestamp")`,
			`COUNT(*) AS "p0"`,
			`SUM(CAST("latency" AS DECIMAL(38,0)))`,
			`"@timestamp" >= TIMESTAMP '2026-11-01 00:00:00.000000'`,
			`"level" = 'ERROR'`,
		}},
		{"bq", []string{
			"`strata_prod.telemetry.logs`",
			"CAST(`service` AS STRING) AS `k0`",
			"TIMESTAMP_TRUNC(`timestamp`, HOUR)",
			"SUM(CAST(`latency` AS NUMERIC))",
			"TIMESTAMP '2026-11-01T00:00:00Z'",
		}},
		{"ch", []string{
			"`strata`.`logs`",
			"CAST(`service` AS String) AS `k0`",
			"toStartOfHour(`timestamp`)",
			"toDecimal128(`latency`, 0)",
			"toDateTime64('2026-11-01 00:00:00.000000', 6, 'UTC')",
		}},
		{"duck", []string{
			`"logs"`,
			`CAST("service" AS VARCHAR) AS "k0"`,
			`date_trunc('hour', "timestamp")`,
			`CAST("latency" AS DECIMAL(38,0))`,
			`TIMESTAMP '2026-11-01 00:00:00.000000'`,
		}},
	}

	for _, tc := range cases {
		t.Run(tc.endpoint, func(t *testing.T) {
			ep := cat.Endpoints[tc.endpoint]
			d, ok := dialect.ForBackend(ep.Backend)
			if !ok {
				t.Fatalf("no dialect for %s", ep.Backend)
			}
			p, err := rewrite.Build(q, ep, from, to, rewrite.Scope{})
			if err != nil {
				t.Fatalf("rewrite: %v", err)
			}
			sqlText, err := (&dialect.Emitter{D: d, EP: ep}).Emit(p)
			if err != nil {
				t.Fatalf("emit: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(sqlText, want) {
					t.Errorf("SQL missing %q\nSQL: %s", want, sqlText)
				}
			}
			if !strings.Contains(sqlText, "GROUP BY") {
				t.Errorf("SQL missing GROUP BY: %s", sqlText)
			}
			if strings.Contains(sqlText, "HAVING") || strings.Contains(sqlText, "LIMIT") {
				t.Errorf("pushdown must not finalize HAVING/LIMIT: %s", sqlText)
			}
		})
	}
}

func TestPushdownNeverSelectsStar(t *testing.T) {
	cat := testCatalog(t)
	sel, err := sql.Parse(`SELECT count(*) AS n FROM logs`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := cat.Endpoints["omni"]
	d, _ := dialect.ForBackend("omni")
	p, err := rewrite.Build(q, ep, time.Now().Add(-time.Hour), time.Now(), rewrite.Scope{})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	sqlText, err := (&dialect.Emitter{D: d, EP: ep}).Emit(p)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if strings.Contains(sqlText, "SELECT *") {
		t.Fatalf("Omni must never receive SELECT *: %s", sqlText)
	}
}

func TestScopeInjection(t *testing.T) {
	cat := testCatalog(t)
	sel, err := sql.Parse(`SELECT service, count(*) AS n FROM logs GROUP BY service`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := cat.Endpoints["duck"]
	d, _ := dialect.ForBackend("duckdb-local")
	p, err := rewrite.Build(q, ep, time.Now().Add(-time.Hour), time.Now(), rewrite.Scope{
		Restricted: true,
		Partitions: []string{"prod-us", "prod-eu"},
	})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	sqlText, err := (&dialect.Emitter{D: d, EP: ep}).Emit(p)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	want := `"partition" IN ('prod-us', 'prod-eu')`
	if !strings.Contains(sqlText, want) {
		t.Fatalf("scope predicate missing\nwant: %s\nSQL: %s", want, sqlText)
	}
}

func TestCountStringIsNotCast(t *testing.T) {
	cat := testCatalog(t)
	sel, err := sql.Parse(`SELECT count(service) AS n FROM logs`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := cat.Endpoints["duck"]
	d, _ := dialect.ForBackend("duckdb-local")
	p, err := rewrite.Build(q, ep, time.Now().Add(-time.Hour), time.Now(), rewrite.Scope{})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	sqlText, err := (&dialect.Emitter{D: d, EP: ep}).Emit(p)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(sqlText, `COUNT("service")`) {
		t.Fatalf("COUNT(string) was rewritten unexpectedly: %s", sqlText)
	}
	if strings.Contains(sqlText, `COUNT(CAST("service"`) {
		t.Fatalf("COUNT(string) must not cast: %s", sqlText)
	}
}

func TestGroupOnlyPushdown(t *testing.T) {
	cat := testCatalog(t)
	sel, err := sql.Parse(`SELECT service FROM logs GROUP BY service`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := cat.Endpoints["duck"]
	d, _ := dialect.ForBackend("duckdb-local")
	p, err := rewrite.Build(q, ep, time.Now().Add(-time.Hour), time.Now(), rewrite.Scope{})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	sqlText, err := (&dialect.Emitter{D: d, EP: ep}).Emit(p)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(sqlText, `AS "k0"`) || !strings.Contains(sqlText, "GROUP BY") {
		t.Fatalf("group-only pushdown is wrong: %s", sqlText)
	}
}

func TestScopeNotEnforceable(t *testing.T) {
	yaml := `tables:
  traces:
    signal: traces
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
endpoints:
  - id: omni
    backend: omni
    tables: [traces]
    role: shard
    coverage_group: omni
    source: traces.default
    columns: {timestamp: "@timestamp"}
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	sel, err := sql.Parse(`SELECT count(*) AS n FROM traces`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	_, err = rewrite.Build(q, cat.Endpoints["omni"], time.Now().Add(-time.Hour), time.Now(), rewrite.Scope{
		Restricted: true,
		Partitions: []string{"p1"},
	})
	if err == nil {
		t.Fatal("expected scope_not_enforceable")
	}
	if !strings.Contains(err.Error(), "cannot enforce partition scope") {
		t.Fatalf("error = %v", err)
	}
}
