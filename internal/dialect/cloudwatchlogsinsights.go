package dialect

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// CloudWatchLogsInsights emits OpenSearch SQL for the CloudWatch Logs Insights
// SQL endpoint. The log group set lives in the FROM clause:
//
//	FROM `logGroups(logGroupIdentifier: ['/strata/observability'])`
type CloudWatchLogsInsights struct{}

func (CloudWatchLogsInsights) Name() string { return "cloudwatch_logs_insights" }

func (CloudWatchLogsInsights) QuoteIdent(s string) string { return quoteWith(s, "`", "`", "`") }

// QuoteTable wraps the whole log-group source expression in backticks. A bare
// log group name is expanded to the logGroups(...) source expression.
func (CloudWatchLogsInsights) QuoteTable(s string) string {
	trimmed := strings.TrimSpace(strings.Trim(s, "`"))
	if trimmed == "" {
		return quoteWith("", "`", "`", "`")
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "loggroups") {
		return quoteWith(trimmed, "`", "`", "`")
	}
	escaped := strings.ReplaceAll(trimmed, "'", "''")
	return quoteWith("logGroups(logGroupIdentifier: ['"+escaped+"'])", "`", "`", "`")
}

func (CloudWatchLogsInsights) Literal(v value.Value) (string, error) {
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
		return "CAST('" + v.T.UTC().Format(time.RFC3339Nano) + "' AS TIMESTAMP)", nil
	default:
		return "", fmt.Errorf("logs insights: unsupported literal kind %s", v.Kind)
	}
}

func (CloudWatchLogsInsights) Now() string { return "current_timestamp()" }

func (CloudWatchLogsInsights) Cast(expr string, t catalog.Type) (string, error) {
	return "CAST(" + expr + " AS " + insightsType(t) + ")", nil
}

// DecimalCast uses LONG for scale 0 (exact integers). OpenSearch SQL has no
// exact DECIMAL in this endpoint; fractional decimals become DOUBLE.
func (CloudWatchLogsInsights) DecimalCast(expr string, scale int32) string {
	if scale == 0 {
		return "CAST(" + expr + " AS LONG)"
	}
	return "CAST(" + expr + " AS DOUBLE)"
}

func (CloudWatchLogsInsights) TimeBucket(expr, unit string, n int) (string, error) {
	u, err := normalizeUnit(unit)
	if err != nil {
		return "", err
	}
	if n <= 0 {
		return "", fmt.Errorf("logs insights: time bucket size must be positive")
	}
	if n > 1 {
		return "", fmt.Errorf("logs insights: %d-%s buckets are not supported; bucket locally", n, u)
	}
	format := map[string]string{
		"second": "yyyy-MM-dd HH:mm:ss",
		"minute": "yyyy-MM-dd HH:mm:00",
		"hour":   "yyyy-MM-dd HH:00:00",
		"day":    "yyyy-MM-dd 00:00:00",
		"month":  "yyyy-MM-01 00:00:00",
		"year":   "yyyy-01-01 00:00:00",
	}[u]
	if format == "" {
		return "", fmt.Errorf("logs insights: %s buckets are not supported", u)
	}
	return "CAST(date_format(" + expr + ", '" + format + "') AS TIMESTAMP)", nil
}

func (CloudWatchLogsInsights) NestedField(path []string) (string, error) {
	return "", fmt.Errorf("logs insights: attribute partition bindings are not supported; use a log group binding")
}

func (CloudWatchLogsInsights) ILike(expr, pattern string) (string, error) {
	return "(LOWER(" + expr + ") LIKE LOWER(" + pattern + "))", nil
}

// TimeRange renders the query window with FROM_UNIXTIME. Logs Insights SQL
// exposes @timestamp as a string ("2026-11-20 12:05:35.643"), and comparing
// it to CAST(<RFC3339> AS TIMESTAMP) or a TIMESTAMP literal matches zero
// records even though the query reports status Complete. Unix-time bounds
// compare correctly and keep the pushdown bounded.
func (CloudWatchLogsInsights) TimeRange(expr string, from, to time.Time) (string, error) {
	return "(" + expr + " >= FROM_UNIXTIME(" + unixSeconds(from) + ") AND " +
		expr + " < FROM_UNIXTIME(" + unixSeconds(to) + "))", nil
}

func unixSeconds(t time.Time) string {
	// value.Time truncates to microseconds; preserve that precision so a
	// boundary event is not skipped by rounding.
	return strconv.FormatFloat(float64(t.UTC().UnixMicro())/1e6, 'f', -1, 64)
}

// OpenSearch SQL on this endpoint has no FILTER (WHERE ...) aggregate clause;
// the emitter rewrites it to CASE WHEN.
func (CloudWatchLogsInsights) SupportsFilterAgg() bool { return false }

func insightsType(t catalog.Type) string {
	switch t {
	case catalog.TypeInt64, catalog.TypeUint64:
		return "LONG"
	case catalog.TypeFloat64, catalog.TypeDecimal:
		return "DOUBLE"
	case catalog.TypeBool:
		return "BOOLEAN"
	case catalog.TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "STRING"
	}
}
