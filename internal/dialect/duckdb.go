package dialect

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// DuckDB is the dialect used by MonoFS node-local Parquet views.
type DuckDB struct{}

func (DuckDB) Name() string { return "duckdb" }

func (DuckDB) QuoteIdent(s string) string { return quoteWith(s, `"`, `"`, `"`) }

func (DuckDB) QuoteTable(s string) string { return quotePath(s, `"`, `"`, `"`) }

func (DuckDB) Literal(v value.Value) (string, error) {
	switch v.Kind {
	case value.KNull:
		return "NULL", nil
	case value.KInt:
		return fmt.Sprintf("%d", v.I), nil
	case value.KUint:
		return fmt.Sprintf("%d", v.U), nil
	case value.KFloat:
		return formatFloat(v.F)
	case value.KDecimal:
		return v.D.String(), nil
	case value.KString:
		return singleQuote(v.S), nil
	case value.KBool:
		if v.B {
			return "TRUE", nil
		}
		return "FALSE", nil
	case value.KTimestamp:
		return "TIMESTAMP '" + timestampText(v.T) + "'", nil
	default:
		return "", fmt.Errorf("duckdb: unsupported literal kind %s", v.Kind)
	}
}

func (DuckDB) Now() string { return "now()" }

func (DuckDB) Cast(expr string, t catalog.Type) (string, error) {
	return "CAST(" + expr + " AS " + duckType(t) + ")", nil
}

func (DuckDB) DecimalCast(expr string, scale int32) string {
	return fmt.Sprintf("CAST(%s AS DECIMAL(38,%d))", expr, scale)
}

func (DuckDB) TimeBucket(expr, unit string, n int) (string, error) {
	u, err := normalizeUnit(unit)
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", fmt.Errorf("duckdb: time bucket size must be positive")
	}
	if n == 1 {
		return fmt.Sprintf("date_trunc('%s', %s)", u, expr), nil
	}
	return fmt.Sprintf("time_bucket(INTERVAL '%d %s', %s)", n, u, expr), nil
}

func (DuckDB) NestedField(path []string) (string, error) {
	return "", fmt.Errorf("duckdb: attribute partition bindings are only supported by Omni")
}

func (DuckDB) ILike(expr, pattern string) (string, error) {
	return "(" + expr + " ILIKE " + pattern + ")", nil
}

func (DuckDB) SupportsFilterAgg() bool { return true }

func duckType(t catalog.Type) string {
	switch t {
	case catalog.TypeInt64:
		return "BIGINT"
	case catalog.TypeUint64:
		return "UBIGINT"
	case catalog.TypeFloat64:
		return "DOUBLE"
	case catalog.TypeDecimal:
		return "DECIMAL(38,9)"
	case catalog.TypeBool:
		return "BOOLEAN"
	case catalog.TypeTimestamp:
		return "TIMESTAMP"
	case catalog.TypeJSON:
		return "JSON"
	default:
		return "VARCHAR"
	}
}

func quotePath(s, open, close, escape string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, ".")
	for i := range parts {
		parts[i] = quoteWith(parts[i], open, close, escape)
	}
	return strings.Join(parts, ".")
}
