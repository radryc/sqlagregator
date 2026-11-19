package selector_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/selector"
)

func catalog25(t *testing.T) *catalog.Catalog {
	t.Helper()
	var b strings.Builder
	b.WriteString(`tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
coverage_groups:
  monofs-local: {mode: hash, members: 4}
endpoints:
`)
	for i := 1; i <= 25; i++ {
		role := "replica"
		if i <= 4 {
			role = "shard"
		}
		fmt.Fprintf(&b, `  - id: node-%02d
    backend: duckdb-local
    tables: [logs]
    role: %s
    coverage_group: monofs-local
    source: logs
    tags: {pool: legacy}
`, i, role)
	}
	cat, err := catalog.LoadBytes([]byte(b.String()))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func window() catalog.TimeRange {
	return catalog.TimeRange{
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
	}
}

func opts(partial bool) selector.Options {
	return selector.Options{AllowPartial: partial, Signal: "logs", ExcludeReplicasByDefault: true}
}

func TestDefaultExcludesReplicas(t *testing.T) {
	cat := catalog25(t)
	res, err := selector.Resolve(cat, "logs", selector.Selector{}, window(), opts(false))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Shards) != 4 {
		t.Fatalf("shards = %d, want 4", len(res.Shards))
	}
	if len(res.Excluded) != 21 {
		t.Fatalf("excluded = %d, want 21", len(res.Excluded))
	}
	for _, ex := range res.Excluded {
		if ex.Reason != selector.ReasonReplica {
			t.Fatalf("%s excluded as %q, want replica", ex.ID, ex.Reason)
		}
	}
	if !res.Coverage.Complete {
		t.Fatalf("coverage should be complete: %+v", res.Coverage)
	}
}

func TestExcludingReplicaIsHarmless(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{Exclude: []selector.Matcher{{IDs: []string{"node-21", "node-22"}}}}
	res, err := selector.Resolve(cat, "logs", sel, window(), opts(false))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Shards) != 4 || !res.Coverage.Complete {
		t.Fatalf("unexpected resolution: shards=%d complete=%v", len(res.Shards), res.Coverage.Complete)
	}
}

func TestExcludingShardCreatesGap(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{Exclude: []selector.Matcher{{IDs: []string{"node-01"}}}}
	_, err := selector.Resolve(cat, "logs", sel, window(), opts(false))
	if err == nil {
		t.Fatal("expected coverage_gap error")
	}
	var ce *selector.CoverageError
	if !asCoverageError(err, &ce) {
		t.Fatalf("error %v is not a CoverageError", err)
	}
	if ce.Code != "coverage_gap" {
		t.Fatalf("code = %q, want coverage_gap", ce.Code)
	}
	if !contains(ce.Missing, "node-01") {
		t.Fatalf("missing = %v, want node-01", ce.Missing)
	}
}

func TestAllowPartialTurnsGapIntoMetadata(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{Exclude: []selector.Matcher{{IDs: []string{"node-01"}}}}
	res, err := selector.Resolve(cat, "logs", sel, window(), opts(true))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if res.Coverage.Complete {
		t.Fatal("coverage must be incomplete")
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected a partial-result warning")
	}
	if !contains(res.Coverage.MissingEndpoints, "node-01") {
		t.Fatalf("missing endpoints = %v", res.Coverage.MissingEndpoints)
	}
}

func TestIncludeReplicasNeverAddsToSumSet(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{IncludeReplicas: true}
	res, err := selector.Resolve(cat, "logs", sel, window(), opts(false))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Shards) != 4 {
		t.Fatalf("shards = %d, want 4", len(res.Shards))
	}
	if len(res.Replicas) != 21 {
		t.Fatalf("replicas = %d, want 21", len(res.Replicas))
	}
}

func TestMaxIsHardCap(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{Max: 2}
	_, err := selector.Resolve(cat, "logs", sel, window(), opts(false))
	if err == nil {
		t.Fatal("expected exceeds_max")
	}
	var ce *selector.CoverageError
	if !asCoverageError(err, &ce) || ce.Code != "exceeds_max" {
		t.Fatalf("error = %v", err)
	}
}

func TestNoEndpointsLeft(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{Exclude: []selector.Matcher{{IDs: []string{"node-0*", "node-1*", "node-2*"}}}}
	_, err := selector.Resolve(cat, "logs", sel, window(), opts(false))
	if err == nil {
		t.Fatal("expected no_shards")
	}
}

func TestTimeCoverage(t *testing.T) {
	cat := loadTimeCatalog(t, false)
	res, err := selector.Resolve(cat, "logs", selector.Selector{}, catalog.TimeRange{
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
	}, opts(false))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !res.Coverage.Complete {
		t.Fatalf("coverage should be complete: %+v", res.Coverage)
	}
}

func TestTimeOverlapIsRejected(t *testing.T) {
	cat := loadTimeCatalog(t, true)
	_, err := selector.Resolve(cat, "logs", selector.Selector{}, catalog.TimeRange{
		From: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC),
	}, opts(false))
	if err == nil {
		t.Fatal("expected coverage_overlap")
	}
	var ce *selector.CoverageError
	if !asCoverageError(err, &ce) || ce.Code != "coverage_overlap" {
		t.Fatalf("error = %v", err)
	}
}

func loadTimeCatalog(t *testing.T, overlap bool) *catalog.Catalog {
	t.Helper()
	secondFrom := "2026-11-02T00:00:00Z"
	if overlap {
		secondFrom = "2026-11-01T12:00:00Z"
	}
	yaml := fmt.Sprintf(`tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
coverage_groups:
  archive: {mode: time}
endpoints:
  - id: omni
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: archive
    range: {from: "2026-11-01T00:00:00Z", to: "2026-11-02T00:00:00Z"}
    source: logs.default
    columns: {timestamp: "@timestamp"}
  - id: bq
    backend: bigquery
    tables: [logs]
    role: shard
    coverage_group: archive
    range: {from: "%s", to: "2026-11-03T00:00:00Z"}
    source: p.d.logs
    columns: {timestamp: timestamp}
`, secondFrom)
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func asCoverageError(err error, target **selector.CoverageError) bool {
	ce, ok := err.(*selector.CoverageError)
	if ok {
		*target = ce
	}
	return ok
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func TestMinCoverageWithHashShards(t *testing.T) {
	cat := catalog25(t)
	sel := selector.Selector{
		Exclude:     []selector.Matcher{{IDs: []string{"node-01"}}},
		MinCoverage: 0.75,
	}
	res, err := selector.Resolve(cat, "logs", sel, window(), opts(true))
	if err != nil {
		t.Fatalf("3/4 shards should satisfy min_coverage=0.75: %v", err)
	}
	if res.Coverage.Complete {
		t.Fatal("coverage must still be incomplete")
	}

	sel.MinCoverage = 0.9
	_, err = selector.Resolve(cat, "logs", sel, window(), opts(true))
	if err == nil {
		t.Fatal("expected coverage_below_min")
	}
}
