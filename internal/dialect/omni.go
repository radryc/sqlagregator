package dialect

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// Omni is the CloudWatch Omni SQL dialect. It is schemaless, so the catalog
// must map every logical column to a physical field, and every projected
// field is cast to its canonical type by the emitter's pushdown builder.
type Omni struct{}

func (Omni) Name() string { return "omni" }

func (Omni) QuoteIdent(s string) string { return quoteWith(s, `"`, `"`, `"`) }

// Omni addresses a signal as a single quoted identifier: "logs.default".
func (Omni) QuoteTable(s string) string { return quoteWith(s, `"`, `"`, `"`) }

func (Omni) Literal(v value.Value) (string, error) {
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
		return "", fmt.Errorf("omni: unsupported literal kind %s", v.Kind)
	}
}

func (Omni) Now() string { return "now()" }

func (Omni) Cast(expr string, t catalog.Type) (string, error) {
	return "CAST(" + expr + " AS " + omniType(t) + ")", nil
}

func (Omni) DecimalCast(expr string, scale int32) string {
	return fmt.Sprintf("CAST(%s AS DECIMAL(38,%d))", expr, scale)
}

func (Omni) TimeBucket(expr, unit string, n int) (string, error) {
	u, err := normalizeUnit(unit)
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", fmt.Errorf("omni: time bucket size must be positive")
	}
	if n == 1 {
		return fmt.Sprintf("date_trunc('%s', %s)", u, expr), nil
	}
	return fmt.Sprintf("date_bin(INTERVAL '%d' %s, %s, TIMESTAMP '1970-01-01 00:00:00')",
		n, strings.ToUpper(u), expr), nil
}

func (Omni) NestedField(path []string) (string, error) {
	if len(path) < 2 {
		return "", fmt.Errorf("omni: attribute path needs a map and at least one key")
	}
	var b strings.Builder
	if simpleIdent(path[0]) {
		b.WriteString(path[0])
	} else {
		b.WriteString(quoteWith(path[0], `"`, `"`, `"`))
	}
	for _, key := range path[1:] {
		b.WriteByte('[')
		b.WriteString(singleQuote(key))
		b.WriteByte(']')
	}
	return b.String(), nil
}

func (Omni) ILike(expr, pattern string) (string, error) {
	return "(" + expr + " ILIKE " + pattern + ")", nil
}

func (Omni) SupportsFilterAgg() bool { return true }

func omniType(t catalog.Type) string {
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
		return "VARCHAR"
	default:
		return "VARCHAR"
	}
}
