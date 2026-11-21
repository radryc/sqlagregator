package dialect_test

import (
	"strings"
	"testing"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/dialect"
	"github.com/radryc/sqlagregator/internal/rewrite"
)

func insightsCatalog(t *testing.T) *catalog.Catalog {
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
endpoints:
  - id: insights
    backend: cloudwatch_logs_insights
    tables: [logs]
    role: shard
    source: "logGroups(logGroupIdentifier: ['/strata/observability'])"
    columns: {timestamp: "@timestamp"}
`
	cat, err := catalog.LoadBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	return cat
}

func TestInsightsSQLDialect(t *testing.T) {
	cat := insightsCatalog(t)
	sqlText, err := emitFor(t, cat, `
		SELECT service,
		       count(*) FILTER (WHERE level = 'ERROR') AS errors,
		       sum(latency) AS total
		FROM logs
		WHERE level = 'ERROR'
		GROUP BY service
		ORDER BY errors DESC
		LIMIT 10`, "insights", rewrite.Scope{})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	for _, want := range []string{
		"`logGroups(logGroupIdentifier: ['/strata/observability'])`",
		"CAST(`service` AS STRING) AS `k0`",
		"SUM(CASE WHEN (`level` = 'ERROR') THEN 1 ELSE 0 END) AS `p0`",
		"SUM(CAST(`latency` AS LONG)) AS `p1`",
		"`@timestamp` >= FROM_UNIXTIME(",
		"`@timestamp` < FROM_UNIXTIME(",
		"GROUP BY CAST(`service` AS STRING)",
	} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("SQL missing %q\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "`@timestamp` >= CAST('") {
		t.Fatalf("Insights @timestamp must not be compared to a CAST literal: %s", sqlText)
	}
	if strings.Contains(sqlText, " FILTER ") {
		t.Fatalf("Insights SQL must not use FILTER: %s", sqlText)
	}
	if strings.Contains(sqlText, "HAVING") || strings.Contains(sqlText, "LIMIT") {
		t.Fatalf("pushdown must not finalize HAVING/LIMIT: %s", sqlText)
	}
}

func TestInsightsTimeRangeUsesUnixTime(t *testing.T) {
	from := time.Date(2026, 11, 20, 12, 0, 0, 123456000, time.UTC)
	to := time.Date(2026, 11, 20, 13, 0, 0, 654321000, time.UTC)
	clause, err := (dialect.CloudWatchLogsInsights{}).TimeRange("`@timestamp`", from, to)
	if err != nil {
		t.Fatalf("time range: %v", err)
	}
	want := "(`@timestamp` >= FROM_UNIXTIME(1795176000.123456) AND `@timestamp` < FROM_UNIXTIME(1795179600.654321))"
	if clause != want {
		t.Fatalf("time range = %q, want %q", clause, want)
	}
}

func TestInsightsTimeBucket(t *testing.T) {
	cat := insightsCatalog(t)
	sqlText, err := emitFor(t, cat, `
		SELECT date_trunc('hour', timestamp) AS hour, count(*) AS n
		FROM logs
		GROUP BY date_trunc('hour', timestamp)`, "insights", rewrite.Scope{})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	want := "CAST(date_format(`@timestamp`, 'yyyy-MM-dd HH:00:00') AS TIMESTAMP)"
	if !strings.Contains(sqlText, want) {
		t.Fatalf("SQL missing time bucket %q\n%s", want, sqlText)
	}
}

func TestInsightsPartitionBindingUsesLogGroup(t *testing.T) {
	cat := insightsCatalog(t)
	ep := cat.Endpoints["insights"]
	ep.Partition = &catalog.PartitionBinding{Kind: "log_group", LogGroupField: "@logGroupName"}
	ep.Binding = &catalog.PartitionBindingValue{
		Partition: "observability",
		LogGroups: []string{"/strata/observability"},
	}
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "insights", rewrite.Scope{
		Restricted: true,
		Partitions: []string{"observability"},
	})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	if !strings.Contains(sqlText, "`@logGroupName` IN ('/strata/observability')") {
		t.Fatalf("log group predicate missing: %s", sqlText)
	}
}

func TestInsightsExpandsBareLogGroupSource(t *testing.T) {
	cat := insightsCatalog(t)
	cat.Endpoints["insights"].Source = "/strata/observability"
	sqlText, err := emitFor(t, cat, `SELECT count(*) AS n FROM logs`, "insights", rewrite.Scope{})
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	want := "FROM `logGroups(logGroupIdentifier: ['/strata/observability'])`"
	if !strings.Contains(sqlText, want) {
		t.Fatalf("SQL missing %q\n%s", want, sqlText)
	}
}

func TestBigQueryQuotesNestedColumns(t *testing.T) {
	if got := (dialect.BigQuery{}).QuoteIdent("jsonPayload.service"); got != "`jsonPayload`.`service`" {
		t.Fatalf("nested ident = %q", got)
	}
	if got := (dialect.BigQuery{}).QuoteIdent("service"); got != "`service`" {
		t.Fatalf("flat ident = %q", got)
	}
}

func TestForBackendAliases(t *testing.T) {
	for _, name := range []string{"monofs_local", "omni", "cloudwatch_logs_insights", "bigquery", "clickhouse"} {
		if _, ok := dialect.ForBackend(name); !ok {
			t.Fatalf("backend %q has no dialect", name)
		}
	}
}
