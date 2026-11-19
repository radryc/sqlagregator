package merge_test

import (
	"fmt"
	"testing"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/merge"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/sql"
	"github.com/radryc/sqlagregator/internal/value"
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
  metrics:
    signal: metrics
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
      - {name: value, type: float64}
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func bind(t *testing.T, cat *catalog.Catalog, query string) *plan.Query {
	t.Helper()
	sel, err := sql.Parse(query)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	return q
}

func schemaFor(q *plan.Query) merge.Schema {
	var cols []string
	for i := range q.Groups {
		cols = append(cols, fmt.Sprintf("k%d", i))
	}
	for _, agg := range q.Aggs {
		for _, p := range agg.Partials {
			cols = append(cols, p.Alias)
		}
	}
	return merge.Schema{Columns: cols}
}

func TestMergeAggregateAlgebra(t *testing.T) {
	cat := testCatalog(t)
	q := bind(t, cat, `
		SELECT service,
		       count(*) AS n,
		       sum(latency) AS total,
		       avg(latency) AS mean,
		       min(latency) AS lo,
		       max(latency) AS hi
		FROM logs
		GROUP BY service
		HAVING count(*) > 1
		ORDER BY total DESC
		LIMIT 2`)

	// Aggregation order: count, sum, avg(sum,count), min, max.
	streams := []merge.Stream{
		{
			EndpointID: "node-01",
			Schema:     schemaFor(q),
			Rows: []merge.Row{
				{value.Str("a"), value.Int64(2), value.Int64(10), value.Int64(10), value.Int64(2), value.Int64(3), value.Int64(7)},
				{value.Str("b"), value.Int64(1), value.Int64(1), value.Int64(1), value.Int64(1), value.Int64(2), value.Int64(2)},
			},
		},
		{
			EndpointID: "node-02",
			Schema:     schemaFor(q),
			Rows: []merge.Row{
				{value.Str("a"), value.Int64(1), value.Int64(5), value.Int64(5), value.Int64(1), value.Int64(4), value.Int64(9)},
			},
		},
	}

	res, err := merge.Run(q, streams)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (b is filtered by HAVING)", len(res.Rows))
	}
	row := res.Rows[0]
	if got := row[0].AsString(); got != "a" {
		t.Fatalf("service = %q", got)
	}
	if got := row[1].Int(); got != 3 {
		t.Fatalf("n = %d, want 3", got)
	}
	if got := row[2].Int(); got != 15 {
		t.Fatalf("total = %d, want 15", got)
	}
	if got := row[3].Float64(); got != 5 {
		t.Fatalf("mean = %v, want 5", got)
	}
	if got := row[4].Int(); got != 3 {
		t.Fatalf("min = %d, want 3", got)
	}
	if got := row[5].Int(); got != 9 {
		t.Fatalf("max = %d, want 9", got)
	}
}

func TestGlobalAggregateWithNoRows(t *testing.T) {
	cat := testCatalog(t)
	q := bind(t, cat, `SELECT count(*) AS n, avg(latency) AS mean FROM logs`)
	res, err := merge.Run(q, []merge.Stream{{
		EndpointID: "node-01",
		Schema:     schemaFor(q),
	}})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}
	if got := res.Rows[0][0].Int(); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
	if !res.Rows[0][1].IsNull() {
		t.Fatalf("avg over no rows must be NULL, got %v", res.Rows[0][1].AsString())
	}
}

func TestRowModeConcatenatesAndSorts(t *testing.T) {
	cat := testCatalog(t)
	q := bind(t, cat, `SELECT service, level FROM logs ORDER BY service ASC LIMIT 2`)
	streams := []merge.Stream{
		{EndpointID: "node-01", Schema: merge.Schema{Columns: []string{"service", "level"}}, Rows: []merge.Row{
			{value.Str("c"), value.Str("error")},
			{value.Str("a"), value.Str("info")},
		}},
		{EndpointID: "node-02", Schema: merge.Schema{Columns: []string{"service", "level"}}, Rows: []merge.Row{
			{value.Str("b"), value.Str("warn")},
		}},
	}
	res, err := merge.Run(q, streams)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.Rows))
	}
	if res.Rows[0][0].AsString() != "a" || res.Rows[1][0].AsString() != "b" {
		t.Fatalf("unexpected order: %s, %s", res.Rows[0][0].AsString(), res.Rows[1][0].AsString())
	}
}

func TestFloatSumIsCompensated(t *testing.T) {
	cat := testCatalog(t)
	q := bind(t, cat, `SELECT sum(value) AS s FROM metrics`)
	if q.Aggs[0].Type != catalog.TypeFloat64 {
		t.Fatalf("sum type = %s, want float64", q.Aggs[0].Type)
	}
	// 0.1 + 0.2 + 0.3 should be 0.6 within float tolerance.
	res, err := merge.Run(q, []merge.Stream{
		{EndpointID: "a", Schema: schemaFor(q), Rows: []merge.Row{{value.Float64(0.1)}}},
		{EndpointID: "b", Schema: schemaFor(q), Rows: []merge.Row{{value.Float64(0.2)}, {value.Float64(0.3)}}},
	})
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	got := res.Rows[0][0].Float64()
	if got < 0.599999999999 || got > 0.600000000001 {
		t.Fatalf("sum = %v, want ~0.6", got)
	}
}

func TestDistinctRowMode(t *testing.T) {
	cat := testCatalog(t)
	q := bind(t, cat, `SELECT DISTINCT service FROM logs`)
	streams := []merge.Stream{
		{EndpointID: "a", Schema: merge.Schema{Columns: []string{"service"}}, Rows: []merge.Row{
			{value.Str("x")}, {value.Str("y")},
		}},
		{EndpointID: "b", Schema: merge.Schema{Columns: []string{"service"}}, Rows: []merge.Row{
			{value.Str("x")}, {value.Str("z")},
		}},
	}
	res, err := merge.Run(q, streams)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("rows = %d, want 3 distinct values", len(res.Rows))
	}
}
