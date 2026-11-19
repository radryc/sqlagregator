package clickhouse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

func TestCoordinatorEndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{
			"meta": [{"name":"k0","type":"String"},{"name":"p0","type":"Int64"}],
			"data": [
				{"k0":"api","p0":5},
				{"k0":"db","p0":2}
			],
			"rows": 2
		}`))
	}))
	defer server.Close()

	cat, err := sqlag.LoadCatalogBytes([]byte(`tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
coverage_groups:
  ch: {mode: hash, members: 1}
endpoints:
  - id: ch-1
    backend: clickhouse
    tables: [logs]
    role: shard
    coverage_group: ch
    source: strata.logs
    columns: {timestamp: timestamp, service: service}
`))
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	coord := sqlag.New(cat)
	driver, err := New(Config{EndpointID: "ch-1", URL: server.URL})
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	coord.Register(driver)

	res, err := coord.Query(context.Background(), sqlag.Request{
		SQL:  `SELECT service, count(*) AS n FROM logs GROUP BY service ORDER BY n DESC`,
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if res.RowCount != 2 {
		t.Fatalf("rows = %d", res.RowCount)
	}
	if res.Rows[0][0] != "api" || res.Rows[0][1].(int64) != 5 {
		t.Fatalf("first row = %#v", res.Rows[0])
	}
	if res.Rows[1][0] != "db" {
		t.Fatalf("second row = %#v", res.Rows[1])
	}
}
