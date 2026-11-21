package dialect

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// ClickHouse is the shared telemetry dialect.
type ClickHouse struct{}

func (ClickHouse) Name() string { return "clickhouse" }

func (ClickHouse) QuoteIdent(s string) string { return quoteWith(s, "`", "`", "`") }

func (ClickHouse) QuoteTable(s string) string { return quotePath(s, "`", "`", "`") }

func (ClickHouse) Literal(v value.Value) (string, error) {
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
			return "true", nil
		}
		return "false", nil
	case value.KTimestamp:
		return "toDateTime64('" + timestampText(v.T) + "', 6, 'UTC')", nil
	default:
		return "", fmt.Errorf("clickhouse: unsupported literal kind %s", v.Kind)
	}
}

func (ClickHouse) Now() string { return "now64(6)" }

func (ClickHouse) Cast(expr string, t catalog.Type) (string, error) {
	return "CAST(" + expr + " AS " + chType(t) + ")", nil
}

func (ClickHouse) DecimalCast(expr string, scale int32) string {
	return fmt.Sprintf("toDecimal128(%s, %d)", expr, scale)
}

func (ClickHouse) TimeBucket(expr, unit string, n int) (string, error) {
	u, err := normalizeUnit(unit)
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", fmt.Errorf("clickhouse: time bucket size must be positive")
	}
	if n == 1 {
		fn := map[string]string{
			"second": "toStartOfSecond", "minute": "toStartOfMinute", "hour": "toStartOfHour",
			"day": "toStartOfDay", "week": "toStartOfWeek", "month": "toStartOfMonth",
			"quarter": "toStartOfQuarter", "year": "toStartOfYear",
		}[u]
		return fn + "(" + expr + ")", nil
	}
	return fmt.Sprintf("toStartOfInterval(%s, INTERVAL %d %s)", expr, n, strings.ToUpper(u)), nil
}

// NestedField renders a map subscript, e.g. labels['guardian.partition'].
func (ClickHouse) NestedField(path []string) (string, error) {
	if len(path) < 2 {
		return "", fmt.Errorf("clickhouse: attribute path needs a map and at least one key")
	}
	var b strings.Builder
	if simpleIdent(path[0]) {
		b.WriteString(path[0])
	} else {
		b.WriteString(quoteWith(path[0], "`", "`", "`"))
	}
	for _, key := range path[1:] {
		b.WriteByte('[')
		b.WriteString(singleQuote(key))
		b.WriteByte(']')
	}
	return b.String(), nil
}

func (ClickHouse) ILike(expr, pattern string) (string, error) {
	return "(" + expr + " ILIKE " + pattern + ")", nil
}

func (ClickHouse) SupportsFilterAgg() bool { return true }

func chType(t catalog.Type) string {
	switch t {
	case catalog.TypeInt64:
		return "Int64"
	case catalog.TypeUint64:
		return "UInt64"
	case catalog.TypeFloat64:
		return "Float64"
	case catalog.TypeDecimal:
		return "Decimal128(9)"
	case catalog.TypeBool:
		return "Bool"
	case catalog.TypeTimestamp:
		return "DateTime64(6, 'UTC')"
	case catalog.TypeJSON:
		return "String"
	default:
		return "String"
	}
}
