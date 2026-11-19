package sql_test

import (
	"strings"
	"testing"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/sql"
)

func TestParseAggregateQuery(t *testing.T) {
	sel, err := sql.Parse(`
		SELECT service,
		       count(*) FILTER (WHERE status_code >= 500) AS errors,
		       count(DISTINCT user_id) AS users,
		       date_trunc('hour', timestamp) AS hour
		FROM traces
		WHERE timestamp BETWEEN now() - INTERVAL '1' HOUR AND now()
		  AND path ILIKE '/api/%'
		GROUP BY service, date_trunc('hour', timestamp)
		HAVING count(*) > 100
		ORDER BY errors DESC NULLS LAST
		LIMIT 10 OFFSET 5`)
	if err == nil {
		t.Fatal("INTERVAL should be rejected in v1")
	}
	if !strings.Contains(err.Error(), "INTERVAL") && !strings.Contains(err.Error(), "unexpected token") {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = sel
}

func TestParseSupportedSubset(t *testing.T) {
	sel, err := sql.Parse(`
		SELECT service,
		       count(*) FILTER (WHERE level = 'ERROR') AS errors,
		       date_trunc('hour', timestamp) AS hour
		FROM logs
		WHERE timestamp >= '2026-11-01T00:00:00Z'
		  AND level NOT IN ('debug', 'trace')
		  AND service NOT LIKE 'test-%'
		GROUP BY service, date_trunc('hour', timestamp)
		HAVING count(*) > 10
		ORDER BY errors DESC NULLS LAST
		LIMIT 10 OFFSET 5`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !sel.HasLimit || sel.Limit != 10 || sel.Offset != 5 {
		t.Fatalf("limit/offset = %d/%d", sel.Limit, sel.Offset)
	}
	if len(sel.Items) != 3 || len(sel.GroupBy) != 2 {
		t.Fatalf("items=%d groups=%d", len(sel.Items), len(sel.GroupBy))
	}
	call, ok := sel.Items[1].Expr.(*ast.FuncCall)
	if !ok || call.Filter == nil {
		t.Fatal("FILTER was not parsed")
	}
	if sel.OrderBy[0].NullsFirst == nil || *sel.OrderBy[0].NullsFirst {
		t.Fatal("NULLS LAST was not parsed")
	}
}

func TestParseRejectsJoins(t *testing.T) {
	_, err := sql.Parse(`SELECT a.service FROM logs a JOIN logs b ON a.id = b.id`)
	if err == nil {
		t.Fatal("expected JOIN rejection")
	}
	if !strings.Contains(err.Error(), "JOIN") {
		t.Fatalf("error = %v", err)
	}
}

func TestParseRejectsMultipleStatements(t *testing.T) {
	_, err := sql.Parse(`SELECT count(*) FROM logs; DROP TABLE logs`)
	if err == nil {
		t.Fatal("expected multi-statement rejection")
	}
}

func TestParseStringEscaping(t *testing.T) {
	sel, err := sql.Parse(`SELECT count(*) FROM logs WHERE service = 'o''brien'`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	bin, ok := sel.Where.(*ast.Binary)
	if !ok {
		t.Fatalf("where = %T", sel.Where)
	}
	lit, ok := bin.Y.(*ast.Literal)
	if !ok || lit.Value.S != "o'brien" {
		t.Fatalf("literal = %#v", bin.Y)
	}
}
