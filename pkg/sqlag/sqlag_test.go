package sqlag_test

import (
	"context"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/pkg/sqlag"
)

func TestPublicFacadeFromExternalPackage(t *testing.T) {
	cat, err := sqlag.LoadCatalogBytes([]byte(`tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
coverage_groups:
  shards: {mode: hash, members: 1}
endpoints:
  - id: node-01
    backend: clickhouse
    tables: [logs]
    role: shard
    coverage_group: shards
    source: strata.logs
    columns: {timestamp: timestamp, service: service}
`))
	if err != nil {
		t.Fatalf("LoadCatalogBytes: %v", err)
	}
	coord := sqlag.New(cat)
	expl, err := coord.Explain(context.Background(), sqlag.Request{
		SQL:  `SELECT service, count(*) AS n FROM logs GROUP BY service`,
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
		Selector: sqlag.Selector{
			Exclude: []sqlag.Matcher{{Tags: map[string]string{"pool": "legacy"}}},
		},
	})
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if len(expl.Selected) != 1 || expl.Selected[0] != "node-01" {
		t.Fatalf("selected = %#v", expl.Selected)
	}
	if len(expl.Pushdown) != 1 {
		t.Fatalf("pushdowns = %d", len(expl.Pushdown))
	}
	if !expl.Coverage.Complete {
		t.Fatalf("coverage = %+v", expl.Coverage)
	}
}
