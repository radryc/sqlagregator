package dialect

import (
	"fmt"
	"strings"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// BigQuery is GoogleSQL.
type BigQuery struct{}

func (BigQuery) Name() string { return "bigquery" }

// QuoteIdent quotes dotted record paths as nested fields
// (`jsonPayload`.`service`) instead of one flat column name.
func (BigQuery) QuoteIdent(s string) string {
	parts := strings.Split(s, ".")
	if len(parts) == 1 {
		return quoteWith(s, "`", "`", "`")
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteWith(part, "`", "`", "`"))
	}
	return strings.Join(quoted, ".")
}

func (BigQuery) QuoteTable(s string) string { return quoteWith(s, "`", "`", "`") }

func (BigQuery) Literal(v value.Value) (string, error) {
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
		return "NUMERIC '" + v.D.String() + "'", nil
	case value.KString:
		return singleQuote(v.S), nil
	case value.KBool:
		if v.B {
			return "TRUE", nil
		}
		return "FALSE", nil
	case value.KTimestamp:
		return "TIMESTAMP '" + v.T.UTC().Format(time.RFC3339Nano) + "'", nil
	default:
		return "", fmt.Errorf("bigquery: unsupported literal kind %s", v.Kind)
	}
}

func (BigQuery) Now() string { return "CURRENT_TIMESTAMP()" }

func (BigQuery) Cast(expr string, t catalog.Type) (string, error) {
	return "CAST(" + expr + " AS " + bqType(t) + ")", nil
}

func (BigQuery) DecimalCast(expr string, scale int32) string {
	// BigQuery NUMERIC is fixed at 38 digits / scale 9.
	return "CAST(" + expr + " AS NUMERIC)"
}

func (BigQuery) TimeBucket(expr, unit string, n int) (string, error) {
	u, err := normalizeUnit(unit)
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", fmt.Errorf("bigquery: time bucket size must be positive")
	}
	if n == 1 {
		return "TIMESTAMP_TRUNC(" + expr + ", " + strings.ToUpper(u) + ")", nil
	}
	secs, ok := unitSeconds(u)
	if !ok {
		return "", fmt.Errorf("bigquery: %d-%s buckets are not supported", n, u)
	}
	step := secs * int64(n)
	return fmt.Sprintf("TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(%s), %d) * %d)", expr, step, step), nil
}

func (BigQuery) NestedField(path []string) (string, error) {
	return "", fmt.Errorf("bigquery: attribute partition bindings are only supported by Omni")
}

func (BigQuery) ILike(expr, pattern string) (string, error) {
	return "(LOWER(" + expr + ") LIKE LOWER(" + pattern + "))", nil
}

func (BigQuery) SupportsFilterAgg() bool { return true }

func bqType(t catalog.Type) string {
	switch t {
	case catalog.TypeInt64, catalog.TypeUint64:
		return "INT64"
	case catalog.TypeFloat64:
		return "FLOAT64"
	case catalog.TypeDecimal:
		return "NUMERIC"
	case catalog.TypeBool:
		return "BOOL"
	case catalog.TypeTimestamp:
		return "TIMESTAMP"
	case catalog.TypeJSON:
		return "JSON"
	default:
		return "STRING"
	}
}
