package execute_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/execute"
	"github.com/radryc/sqlagregator/internal/merge"
	"github.com/radryc/sqlagregator/internal/selector"
	"github.com/radryc/sqlagregator/internal/value"
)

type fakeBackend struct {
	id     string
	schema merge.Schema
	rows   []merge.Row
	err    error
}

func (f *fakeBackend) ID() string { return f.id }

func (f *fakeBackend) Query(ctx context.Context, req execute.PartialRequest) (*merge.Stream, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &merge.Stream{EndpointID: f.id, Schema: f.schema, Rows: f.rows}, nil
}

func catalog2(t *testing.T) *catalog.Catalog {
	t.Helper()
	yaml := `tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
      - {name: partition, type: string}
coverage_groups:
  shards: {mode: hash, members: 2}
endpoints:
  - id: node-01
    backend: duckdb-local
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs
  - id: node-02
    backend: duckdb-local
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func newCoordinator(t *testing.T) *execute.Coordinator {
	t.Helper()
	cat := catalog2(t)
	coord := execute.New(cat)
	schema := merge.Schema{Columns: []string{"k0", "p0"}}
	coord.Register(&fakeBackend{id: "node-01", schema: schema, rows: []merge.Row{
		{value.Str("a"), value.Int64(2)},
		{value.Str("b"), value.Int64(1)},
	}})
	coord.Register(&fakeBackend{id: "node-02", schema: schema, rows: []merge.Row{
		{value.Str("a"), value.Int64(3)},
	}})
	return coord
}

func request() execute.Request {
	return execute.Request{
		SQL:  `SELECT service, count(*) AS n FROM logs GROUP BY service`,
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
	}
}

func TestQueryMergesShards(t *testing.T) {
	coord := newCoordinator(t)
	res, err := coord.Query(context.Background(), request())
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("rows = %d, want 2", res.RowCount)
	}
	if res.Rows[0][0] != "a" || res.Rows[0][1].(int64) != 5 {
		t.Fatalf("row a = %#v, want a/5", res.Rows[0])
	}
	if res.Rows[1][0] != "b" || res.Rows[1][1].(int64) != 1 {
		t.Fatalf("row b = %#v, want b/1", res.Rows[1])
	}
	if !res.Coverage.Complete || res.Partial {
		t.Fatalf("unexpected coverage: %+v partial=%v", res.Coverage, res.Partial)
	}
	if len(res.Backends) != 2 {
		t.Fatalf("backends = %d, want 2", len(res.Backends))
	}
}

func TestExcludingShardFailsClosed(t *testing.T) {
	coord := newCoordinator(t)
	req := request()
	req.Selector.Exclude = []selector.Matcher{{IDs: []string{"node-01"}}}
	_, err := coord.Query(context.Background(), req)
	if err == nil {
		t.Fatal("expected coverage_gap")
	}
	if !strings.Contains(err.Error(), "coverage_gap") {
		t.Fatalf("error = %v", err)
	}
}

func TestAllowPartialReturnsLabelledSubset(t *testing.T) {
	coord := newCoordinator(t)
	req := request()
	req.AllowPartial = true
	req.Selector.Exclude = []selector.Matcher{{IDs: []string{"node-01"}}}
	res, err := coord.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.RowCount != 1 || res.Rows[0][0] != "a" || res.Rows[0][1].(int64) != 3 {
		t.Fatalf("unexpected rows: %#v", res.Rows)
	}
	if res.Coverage.Complete || !res.Partial {
		t.Fatalf("coverage must be incomplete: %+v", res.Coverage)
	}
	if len(res.Excluded) != 1 || res.Excluded[0].ID != "node-01" || res.Excluded[0].Reason != selector.ReasonRequestExclude {
		t.Fatalf("excluded = %#v", res.Excluded)
	}
}

func TestBackendFailureWithPartial(t *testing.T) {
	cat := catalog2(t)
	coord := execute.New(cat)
	schema := merge.Schema{Columns: []string{"k0", "p0"}}
	coord.Register(&fakeBackend{id: "node-01", err: context.DeadlineExceeded})
	coord.Register(&fakeBackend{id: "node-02", schema: schema, rows: []merge.Row{
		{value.Str("a"), value.Int64(3)},
	}})
	req := request()
	req.AllowPartial = true
	res, err := coord.Query(context.Background(), req)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !res.Partial || res.Coverage.Complete {
		t.Fatalf("expected partial result: %+v", res.Coverage)
	}
	if !strings.Contains(strings.Join(res.Warnings, ","), "node-01 failed") {
		t.Fatalf("warnings = %v", res.Warnings)
	}
}

func TestExplainShowsPushdownAndExclusions(t *testing.T) {
	coord := newCoordinator(t)
	req := request()
	req.Selector.Exclude = []selector.Matcher{{Tags: map[string]string{"pool": "legacy"}}}
	expl, err := coord.Explain(context.Background(), req)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(expl.Pushdown) != 2 {
		t.Fatalf("pushdowns = %d, want 2", len(expl.Pushdown))
	}
	for _, p := range expl.Pushdown {
		if !strings.Contains(p.SQL, "GROUP BY") {
			t.Fatalf("pushdown missing GROUP BY: %s", p.SQL)
		}
	}
}

func TestExplainInjectsPartitionScope(t *testing.T) {
	coord := newCoordinator(t)
	req := request()
	req.Scope = execute.Scope{Restricted: true, Partitions: []string{"prod-us", "prod-eu"}}
	expl, err := coord.Explain(context.Background(), req)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(expl.Pushdown) != 2 {
		t.Fatalf("pushdowns = %d", len(expl.Pushdown))
	}
	for _, p := range expl.Pushdown {
		if !strings.Contains(p.SQL, `"partition" IN ('prod-us', 'prod-eu')`) {
			t.Fatalf("scope predicate missing: %s", p.SQL)
		}
	}
}
