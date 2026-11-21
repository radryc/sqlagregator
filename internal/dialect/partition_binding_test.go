package dialect_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/dialect"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/rewrite"
	"github.com/radryc/sqlagregator/internal/sql"
)

func bindingCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	yaml := `tables:
  logs:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
      - {name: partition, type: string}
  events:
    signal: logs
    time_column: timestamp
    columns:
      - {name: timestamp, type: timestamp}
      - {name: service, type: string}
coverage_groups:
  shards: {mode: hash, members: 8, allow_missing: true}
endpoints:
  - id: omni-attr-logs
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: attribute, path: ["attributes", "guardian.partition"]}
  - id: omni-attr-traces
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: attribute, path: ["resource", "attributes", "guardian.partition"]}
  - id: omni-account
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: account_region, account_field: "@aws.account", region_field: "@aws.region"}
  - id: omni-ds
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: data_source, data_source_field: "@data_source_name"}
  - id: omni-lg
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: log_group, log_group_field: "@logGroupName"}
  - id: omni-fixed
    backend: omni
    tables: [logs]
    role: shard
    coverage_group: shards
    source: logs.default
    columns: {timestamp: "@timestamp"}
    partition: {kind: fixed, values: [a, b]}
  - id: ch-column
    backend: clickhouse
    tables: [logs]
    role: shard
    coverage_group: shards
    source: strata.logs
    columns: {timestamp: timestamp}
  - id: ch-attribute
    backend: clickhouse
    tables: [logs]
    role: shard
    coverage_group: shards
    source: strata.logs
    columns: {timestamp: timestamp}
    partition: {kind: attribute, path: ["attributes", "guardian.partition"]}
  - id: omni-nobind
    backend: omni
    tables: [events]
    role: shard
    coverage_group: shards
    source: events.default
    columns: {timestamp: "@timestamp"}
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func bindingScope(partitions []string, bindings ...catalog.PartitionBindingValue) rewrite.Scope {
	return rewrite.Scope{Restricted: true, Partitions: partitions, Bindings: bindings}
}

func emitFor(t *testing.T, cat *catalog.Catalog, query, endpointID string, scope rewrite.Scope) (string, error) {
	t.Helper()
	sel, err := sql.Parse(query)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, err := plan.Bind(cat, sel)
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	ep := cat.Endpoints[endpointID]
	pushdown, err := rewrite.Build(q, ep, time.Now().Add(-time.Hour), time.Now(), scope)
	if err != nil {
		return "", err
	}
	d, ok := dialect.ForBackend(ep.Backend)
	if !ok {
		t.Fatalf("no dialect for %s", ep.Backend)
	}
	return (&dialect.Emitter{D: d, EP: ep}).Emit(pushdown)
}

func TestAttributeBindingLogsAndTraces(t *testing.T) {
	cat := bindingCatalog(t)
	scope := bindingScope([]string{"a"}, catalog.PartitionBindingValue{
		Partition:  "a",
		Attributes: map[string]string{"guardian.partition": "tenant-a"},
	})

	sqlText, err := emitFor(t, cat, `SELECT service, count(*) AS n FROM logs GROUP BY service`, "omni-attr-logs", scope)
	if err != nil {
		t.Fatalf("emit logs: %v", err)
	}
	if !strings.Contains(sqlText, `attributes['guardian.partition'] IN ('tenant-a')`) {
		t.Fatalf("logs attribute predicate missing: %s", sqlText)
	}

	sqlText, err = emitFor(t, cat, `SELECT service, count(*) AS n FROM logs GROUP BY service`, "omni-attr-traces", scope)
	if err != nil {
		t.Fatalf("emit traces: %v", err)
	}
	if !strings.Contains(sqlText, `resource['attributes']['guardian.partition'] IN ('tenant-a')`) {
		t.Fatalf("traces attribute predicate missing: %s", sqlText)
	}
}

func TestAttributeBindingDefaultsToPartitionName(t *testing.T) {
	cat := bindingCatalog(t)
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-attr-logs", bindingScope([]string{"a"}))
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(sqlText, `attributes['guardian.partition'] IN ('a')`) {
		t.Fatalf("default attribute value missing: %s", sqlText)
	}
}

func TestAccountRegionBinding(t *testing.T) {
	cat := bindingCatalog(t)
	scope := bindingScope([]string{"a"}, catalog.PartitionBindingValue{
		Partition: "a", Account: "111111111111", Region: "us-east-1",
	})
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-account", scope)
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(sqlText, `"@aws.account" = '111111111111'`) ||
		!strings.Contains(sqlText, `"@aws.region" = 'us-east-1'`) {
		t.Fatalf("account_region predicate missing: %s", sqlText)
	}
}

func TestDataSourceAndLogGroupBindings(t *testing.T) {
	cat := bindingCatalog(t)
	scope := bindingScope([]string{"a"}, catalog.PartitionBindingValue{
		Partition: "a", DataSource: "strata-tenant-a", LogGroup: "/strata/tenant-a",
	})
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-ds", scope)
	if err != nil {
		t.Fatalf("emit data_source: %v", err)
	}
	if !strings.Contains(sqlText, `"@data_source_name" IN ('strata-tenant-a')`) {
		t.Fatalf("data_source predicate missing: %s", sqlText)
	}
	sqlText, err = emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-lg", scope)
	if err != nil {
		t.Fatalf("emit log_group: %v", err)
	}
	if !strings.Contains(sqlText, `"@logGroupName" IN ('/strata/tenant-a')`) {
		t.Fatalf("log_group predicate missing: %s", sqlText)
	}
}

func TestFixedBinding(t *testing.T) {
	cat := bindingCatalog(t)
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-fixed", bindingScope([]string{"a", "b"}))
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if strings.Contains(sqlText, "partition IN") {
		t.Fatalf("fixed binding must not add a predicate: %s", sqlText)
	}
	if _, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-fixed", bindingScope([]string{"c"})); !errors.Is(err, rewrite.ErrScopeNotEnforceable) {
		t.Fatalf("fixed mismatch error = %v, want ErrScopeNotEnforceable", err)
	}
}

func TestColumnFallbackAndUnenforceable(t *testing.T) {
	cat := bindingCatalog(t)
	scope := bindingScope([]string{"a", "b"})
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "ch-column", scope)
	if err != nil {
		t.Fatalf("emit column: %v", err)
	}
	if !strings.Contains(sqlText, "`partition` IN ('a', 'b')") {
		t.Fatalf("column fallback missing: %s", sqlText)
	}
	if _, err := emitFor(t, cat, `SELECT count(*) AS n FROM events`, "omni-nobind", scope); !errors.Is(err, rewrite.ErrScopeNotEnforceable) {
		t.Fatalf("no-binding error = %v, want ErrScopeNotEnforceable", err)
	}
}

func TestBindingErrors(t *testing.T) {
	cat := bindingCatalog(t)
	if _, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-account", bindingScope([]string{"a"})); !errors.Is(err, rewrite.ErrScopeNotEnforceable) {
		t.Fatalf("missing account binding error = %v", err)
	}
	if _, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "omni-ds", bindingScope([]string{"a"})); !errors.Is(err, rewrite.ErrScopeNotEnforceable) {
		t.Fatalf("missing data_source binding error = %v", err)
	}
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "ch-attribute", bindingScope([]string{"a"}))
	if err != nil {
		t.Fatalf("attribute binding on ClickHouse: %v", err)
	}
	if !strings.Contains(sqlText, "attributes['guardian.partition'] IN ('a')") {
		t.Fatalf("attribute map predicate missing: %s", sqlText)
	}
}
