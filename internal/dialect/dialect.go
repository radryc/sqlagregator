// Package dialect emits backend SQL for the narrow pushdown plan.
package dialect

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/rewrite"
	"github.com/radryc/sqlagregator/internal/value"
)

// Dialect is the per-backend SQL surface.
type Dialect interface {
	Name() string
	QuoteIdent(string) string
	QuoteTable(string) string
	Literal(value.Value) (string, error)
	Now() string
	Cast(expr string, t catalog.Type) (string, error)
	DecimalCast(expr string, scale int32) string
	TimeBucket(expr, unit string, n int) (string, error)
	NestedField(path []string) (string, error)
	ILike(expr, pattern string) (string, error)
	SupportsFilterAgg() bool
}

// TimeRangeFilterer is implemented by dialects that cannot compare their
// timestamp column against the generic timestamp literal. For example,
// CloudWatch Logs Insights SQL exposes @timestamp as a string and silently
// matches zero rows when it is compared to CAST(<RFC3339> AS TIMESTAMP).
type TimeRangeFilterer interface {
	TimeRange(expr string, from, to time.Time) (string, error)
}

// ForBackend maps a registry backend name to a dialect.
func ForBackend(name string) (Dialect, bool) {
	switch strings.ToLower(name) {
	case "duckdb-local", "duckdb", "local", "monofs_local":
		return DuckDB{}, true
	case "clickhouse", "ch":
		return ClickHouse{}, true
	case "bigquery", "bq":
		return BigQuery{}, true
	case "omni", "cloudwatch-omni":
		return Omni{}, true
	case "cloudwatch_logs_insights", "logs_insights", "insights":
		return CloudWatchLogsInsights{}, true
	default:
		return nil, false
	}
}

// Emitter renders a pushdown for one endpoint.
type Emitter struct {
	D  Dialect
	EP *catalog.Endpoint
}

// Emit returns the SQL sent to the backend.
func (e *Emitter) Emit(p *rewrite.Pushdown) (string, error) {
	if e.D == nil {
		return "", fmt.Errorf("dialect: nil dialect")
	}
	var cols []string
	if p.RowMode {
		for _, proj := range p.Projections {
			expr, err := e.Expr(proj.Expr)
			if err != nil {
				return "", err
			}
			expr, err = e.D.Cast(expr, proj.Type)
			if err != nil {
				return "", err
			}
			cols = append(cols, expr+" AS "+e.D.QuoteIdent(proj.Alias))
		}
	} else {
		for i, g := range p.GroupBy {
			expr, err := e.Expr(g)
			if err != nil {
				return "", err
			}
			t := catalog.TypeString
			if i < len(p.GroupTypes) {
				t = p.GroupTypes[i]
			}
			expr, err = e.D.Cast(expr, t)
			if err != nil {
				return "", err
			}
			cols = append(cols, expr+" AS "+e.D.QuoteIdent(p.GroupAliases[i]))
		}
		for _, partial := range p.Partials {
			col, err := e.partial(partial)
			if err != nil {
				return "", err
			}
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("dialect: empty projection")
	}

	var where []string
	timeExpr, err := e.Expr(p.TimeColumn)
	if err != nil {
		return "", err
	}
	if tr, ok := e.D.(TimeRangeFilterer); ok {
		clause, err := tr.TimeRange(timeExpr, p.From, p.To)
		if err != nil {
			return "", err
		}
		where = append(where, clause)
	} else {
		from, err := e.D.Literal(value.Time(p.From))
		if err != nil {
			return "", err
		}
		to, err := e.D.Literal(value.Time(p.To))
		if err != nil {
			return "", err
		}
		where = append(where, timeExpr+" >= "+from, timeExpr+" < "+to)
	}
	for _, f := range p.Filters {
		expr, err := e.Expr(f)
		if err != nil {
			return "", err
		}
		where = append(where, expr)
	}
	if p.Partition != nil {
		expr, err := e.partitionExpr(p.Partition)
		if err != nil {
			return "", err
		}
		if expr != "" {
			where = append(where, expr)
		}
	}

	var b strings.Builder
	b.WriteString("SELECT ")
	b.WriteString(strings.Join(cols, ", "))
	b.WriteString(" FROM ")
	b.WriteString(e.D.QuoteTable(p.Source))
	b.WriteString(" WHERE ")
	b.WriteString(strings.Join(where, " AND "))
	if !p.RowMode && len(p.GroupBy) > 0 {
		group := make([]string, 0, len(p.GroupBy))
		for i, g := range p.GroupBy {
			expr, err := e.Expr(g)
			if err != nil {
				return "", err
			}
			t := catalog.TypeString
			if i < len(p.GroupTypes) {
				t = p.GroupTypes[i]
			}
			expr, err = e.D.Cast(expr, t)
			if err != nil {
				return "", err
			}
			group = append(group, expr)
		}
		b.WriteString(" GROUP BY ")
		b.WriteString(strings.Join(group, ", "))
	}
	return b.String(), nil
}

func (e *Emitter) partitionExpr(f *rewrite.PartitionFilter) (string, error) {
	switch f.Kind {
	case "column":
		column := f.Column
		if column == "" {
			column = "partition"
		}
		return e.inList(e.D.QuoteIdent(column), f.Partitions)
	case "attribute":
		if len(f.Path) == 0 {
			return "", fmt.Errorf("dialect: attribute partition binding has no path")
		}
		expr, err := e.D.NestedField(f.Path)
		if err != nil {
			return "", err
		}
		return e.inList(expr, f.Values)
	case "account_region":
		accountField := f.AccountField
		if accountField == "" {
			accountField = "@aws.account"
		}
		regionField := f.RegionField
		if regionField == "" {
			regionField = "@aws.region"
		}
		var clauses []string
		for _, pair := range f.Pairs {
			var parts []string
			if pair.Account != "" {
				literal, err := e.D.Literal(value.Str(pair.Account))
				if err != nil {
					return "", err
				}
				parts = append(parts, e.D.QuoteIdent(accountField)+" = "+literal)
			}
			if pair.Region != "" {
				literal, err := e.D.Literal(value.Str(pair.Region))
				if err != nil {
					return "", err
				}
				parts = append(parts, e.D.QuoteIdent(regionField)+" = "+literal)
			}
			if len(parts) > 0 {
				clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
			}
		}
		if len(clauses) == 0 {
			return "", fmt.Errorf("dialect: account_region partition binding has no pairs")
		}
		return "(" + strings.Join(clauses, " OR ") + ")", nil
	case "data_source":
		column := f.DataSourceField
		if column == "" {
			column = "@data_source_name"
		}
		return e.inList(e.D.QuoteIdent(column), f.Values)
	case "log_group":
		column := f.LogGroupField
		if column == "" {
			column = "@logGroupName"
		}
		return e.inList(e.D.QuoteIdent(column), f.Values)
	case "fixed":
		return "", nil
	default:
		return "", fmt.Errorf("dialect: unknown partition binding kind %q", f.Kind)
	}
}

func (e *Emitter) inList(expr string, values []string) (string, error) {
	if len(values) == 0 {
		return "", fmt.Errorf("dialect: partition binding has no values")
	}
	literals := make([]string, 0, len(values))
	for _, v := range values {
		literal, err := e.D.Literal(value.Str(v))
		if err != nil {
			return "", err
		}
		literals = append(literals, literal)
	}
	return expr + " IN (" + strings.Join(literals, ", ") + ")", nil
}

func (e *Emitter) partial(p plan.Partial) (string, error) {
	var arg, filter string
	var err error
	if p.Arg != nil {
		arg, err = e.Expr(p.Arg)
		if err != nil {
			return "", err
		}
	}
	if p.Filter != nil {
		filter, err = e.Expr(p.Filter)
		if err != nil {
			return "", err
		}
	}

	var inner string
	switch p.Func {
	case "count":
		if p.Star {
			inner = "COUNT(*)"
		} else {
			// COUNT is type-agnostic; casting here would break COUNT(string_col).
			inner = "COUNT(" + arg + ")"
		}
	case "sum":
		if p.Type == catalog.TypeFloat64 {
			cast, err := e.D.Cast(arg, p.Type)
			if err != nil {
				return "", err
			}
			inner = "SUM(" + cast + ")"
		} else {
			inner = "SUM(" + e.D.DecimalCast(arg, 0) + ")"
		}
	case "min", "max":
		cast, err := e.D.Cast(arg, p.Type)
		if err != nil {
			return "", err
		}
		inner = strings.ToUpper(p.Func) + "(" + cast + ")"
	default:
		return "", fmt.Errorf("dialect: unsupported partial %q", p.Func)
	}

	if filter != "" {
		if e.D.SupportsFilterAgg() {
			inner += " FILTER (WHERE " + filter + ")"
		} else {
			switch p.Func {
			case "count":
				if p.Star {
					inner = "SUM(CASE WHEN " + filter + " THEN 1 ELSE 0 END)"
				} else {
					inner = "COUNT(CASE WHEN " + filter + " THEN " + arg + " END)"
				}
			case "sum":
				sumArg := arg
				if p.Type != catalog.TypeFloat64 {
					sumArg = e.D.DecimalCast(arg, 0)
				}
				inner = "SUM(CASE WHEN " + filter + " THEN " + sumArg + " END)"
			case "min", "max":
				cast, err := e.D.Cast(arg, p.Type)
				if err != nil {
					return "", err
				}
				inner = strings.ToUpper(p.Func) + "(CASE WHEN " + filter + " THEN " + cast + " END)"
			}
		}
	}
	return inner + " AS " + e.D.QuoteIdent(p.Alias), nil
}

// Expr renders a scalar expression.
func (e *Emitter) Expr(x ast.Expr) (string, error) {
	switch v := x.(type) {
	case *ast.ColumnRef:
		return e.ident(v.Name), nil
	case *ast.Literal:
		return e.D.Literal(v.Value)
	case *ast.Unary:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		switch strings.ToUpper(v.Op) {
		case "NOT":
			return "(NOT " + inner + ")", nil
		case "-":
			return "(-" + inner + ")", nil
		}
		return "", fmt.Errorf("dialect: unsupported unary %q", v.Op)
	case *ast.Binary:
		left, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		right, err := e.Expr(v.Y)
		if err != nil {
			return "", err
		}
		op := v.Op
		if op == "!=" {
			op = "<>"
		}
		return "(" + left + " " + op + " " + right + ")", nil
	case *ast.IsNull:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		if v.Not {
			return "(" + inner + " IS NOT NULL)", nil
		}
		return "(" + inner + " IS NULL)", nil
	case *ast.Between:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		low, err := e.Expr(v.Low)
		if err != nil {
			return "", err
		}
		high, err := e.Expr(v.High)
		if err != nil {
			return "", err
		}
		not := ""
		if v.Not {
			not = "NOT "
		}
		return "(" + inner + " " + not + "BETWEEN " + low + " AND " + high + ")", nil
	case *ast.In:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		items := make([]string, 0, len(v.List))
		for _, item := range v.List {
			s, err := e.Expr(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		not := ""
		if v.Not {
			not = "NOT "
		}
		return "(" + inner + " " + not + "IN (" + strings.Join(items, ", ") + "))", nil
	case *ast.Like:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		pattern, err := e.Expr(v.Pattern)
		if err != nil {
			return "", err
		}
		var expr string
		if v.IgnoreCase {
			expr, err = e.D.ILike(inner, pattern)
			if err != nil {
				return "", err
			}
		} else {
			expr = "(" + inner + " LIKE " + pattern + ")"
		}
		if v.Not {
			expr = "(NOT " + expr + ")"
		}
		return expr, nil
	case *ast.Cast:
		inner, err := e.Expr(v.X)
		if err != nil {
			return "", err
		}
		return e.D.Cast(inner, plan.ParseType(v.Type))
	case *ast.FuncCall:
		return e.funcCall(v)
	default:
		return "", fmt.Errorf("dialect: unsupported expression %T", x)
	}
}

func (e *Emitter) funcCall(call *ast.FuncCall) (string, error) {
	name := strings.ToLower(call.Name)
	switch name {
	case "count", "sum", "min", "max", "avg", "stddev", "var_samp", "var_pop":
		return "", fmt.Errorf("dialect: aggregate %q must be rewritten to a partial", name)
	case "now", "current_timestamp":
		return e.D.Now(), nil
	case "date_trunc":
		if len(call.Args) != 2 {
			return "", fmt.Errorf("date_trunc takes (unit, expr)")
		}
		unit, err := literalString(call.Args[0])
		if err != nil {
			return "", err
		}
		expr, err := e.Expr(call.Args[1])
		if err != nil {
			return "", err
		}
		return e.D.TimeBucket(expr, unit, 1)
	case "time_bucket":
		if len(call.Args) != 3 {
			return "", fmt.Errorf("time_bucket takes (n, unit, expr)")
		}
		n, err := literalInt(call.Args[0])
		if err != nil {
			return "", err
		}
		unit, err := literalString(call.Args[1])
		if err != nil {
			return "", err
		}
		expr, err := e.Expr(call.Args[2])
		if err != nil {
			return "", err
		}
		return e.D.TimeBucket(expr, unit, int(n))
	}
	switch name {
	case "lower", "upper", "length", "abs", "round", "floor", "ceil", "coalesce", "concat":
	default:
		return "", fmt.Errorf("dialect: unsupported function %q", call.Name)
	}
	args := make([]string, 0, len(call.Args))
	for _, arg := range call.Args {
		s, err := e.Expr(arg)
		if err != nil {
			return "", err
		}
		args = append(args, s)
	}
	return strings.ToUpper(name) + "(" + strings.Join(args, ", ") + ")", nil
}

func (e *Emitter) ident(name string) string {
	if e.EP != nil {
		if physical, ok := e.EP.Columns[name]; ok && physical != "" {
			return e.D.QuoteIdent(physical)
		}
	}
	return e.D.QuoteIdent(name)
}

func literalString(e ast.Expr) (string, error) {
	lit, ok := e.(*ast.Literal)
	if !ok || lit.Value.Kind != value.KString {
		return "", fmt.Errorf("expected a string literal")
	}
	return lit.Value.S, nil
}

func literalInt(e ast.Expr) (int64, error) {
	lit, ok := e.(*ast.Literal)
	if !ok || lit.Value.Kind != value.KInt {
		return 0, fmt.Errorf("expected an integer literal")
	}
	return lit.Value.I, nil
}

// --- shared helpers --------------------------------------------------------

func quoteWith(s, open, close, escape string) string {
	return open + strings.ReplaceAll(s, close, close+escape) + close
}

func singleQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func formatFloat(f float64) (string, error) {
	if f != f {
		return "", fmt.Errorf("NaN literals are not portable")
	}
	if f > 1.7976931348623157e308 || f < -1.7976931348623157e308 {
		return "", fmt.Errorf("Infinity literals are not portable")
	}
	return strconv.FormatFloat(f, 'g', -1, 64), nil
}

func timestampText(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}

func normalizeUnit(unit string) (string, error) {
	u := strings.ToLower(strings.TrimSpace(unit))
	switch u {
	case "s", "sec", "secs", "second", "seconds":
		return "second", nil
	case "m", "min", "mins", "minute", "minutes":
		return "minute", nil
	case "h", "hr", "hrs", "hour", "hours":
		return "hour", nil
	case "d", "day", "days":
		return "day", nil
	case "w", "week", "weeks":
		return "week", nil
	case "mon", "month", "months":
		return "month", nil
	case "q", "quarter", "quarters":
		return "quarter", nil
	case "y", "year", "years":
		return "year", nil
	default:
		return "", fmt.Errorf("unsupported time bucket unit %q", unit)
	}
}

func unitSeconds(unit string) (int64, bool) {
	switch unit {
	case "second":
		return 1, true
	case "minute":
		return 60, true
	case "hour":
		return 3600, true
	case "day":
		return 86400, true
	case "week":
		return 604800, true
	default:
		return 0, false
	}
}

func simpleIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' || c == '@' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if i > 0 {
			ok = ok || (c >= '0' && c <= '9')
		}
		if !ok {
			return false
		}
	}
	return true
}
