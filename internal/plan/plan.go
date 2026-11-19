// Package plan binds a parsed SELECT to a catalog table and produces the
// pushdown/merge split consumed by the dialects and the merge engine.
package plan

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/sql"
)

// Aggregate is one user-visible aggregate.
type Aggregate struct {
	Alias    string
	Func     string // count | sum | min | max | avg
	Arg      ast.Expr
	Star     bool
	Distinct bool
	Filter   ast.Expr
	Type     catalog.Type
	Partials []Partial
}

// Partial is one column a backend computes.
type Partial struct {
	Alias  string
	Func   string // count | sum | min | max
	Arg    ast.Expr
	Star   bool
	Filter ast.Expr
	Type   catalog.Type
}

// Output expresses the SELECT-list order.
type Output struct {
	Alias string
	Group int      // index into Groups, or -1
	Agg   int      // index into Aggs, or -1
	Expr  ast.Expr // row mode only
}

// Query is a bound, validated SELECT.
type Query struct {
	Table    *catalog.Table
	Select   *ast.Select
	Alias    string
	Aggs     []*Aggregate
	Groups   []ast.Expr
	Output   []Output
	Where    ast.Expr
	Having   ast.Expr
	OrderBy  []ast.SortItem
	Limit    int
	Offset   int
	HasLimit bool
	RowMode  bool
	Distinct bool
}

// Bind validates the AST against the catalog.
func Bind(cat *catalog.Catalog, sel *ast.Select) (*Query, error) {
	table, ok := cat.Table(sel.From.FullName())
	if !ok {
		return nil, &sql.Error{
			Msg:        fmt.Sprintf("unknown table %q", sel.From.FullName()),
			Clause:     "FROM",
			Suggestion: "use a logical table from the catalog (for example logs, traces, metrics)",
		}
	}
	q := &Query{Table: table, Select: sel, HasLimit: sel.HasLimit, Limit: sel.Limit, Offset: sel.Offset, Distinct: sel.Distinct}
	q.Alias = sel.From.Alias
	if q.Alias == "" {
		q.Alias = table.Name
	}

	for _, item := range sel.Items {
		if err := validateColumns(item.Expr, table); err != nil {
			return nil, err
		}
	}
	if err := validateColumns(sel.Where, table); err != nil {
		return nil, err
	}
	for _, g := range sel.GroupBy {
		if err := validateColumns(g, table); err != nil {
			return nil, err
		}
		if containsAggregate(g) {
			return nil, unsupported("GROUP BY", "aggregate functions are not allowed in GROUP BY")
		}
	}
	aliases := map[string]bool{}
	for _, item := range sel.Items {
		if item.Alias != "" {
			aliases[item.Alias] = true
		}
	}
	if err := validateColumnsAllowing(sel.Having, table, aliases); err != nil {
		return nil, err
	}
	for _, o := range sel.OrderBy {
		if err := validateColumnsAllowing(o.Expr, table, aliases); err != nil {
			return nil, err
		}
	}
	if containsAggregate(sel.Where) {
		return nil, unsupported("WHERE", "aggregate functions are not allowed in WHERE; use HAVING")
	}

	aggregating := false
	for _, item := range sel.Items {
		if call, ok := item.Expr.(*ast.FuncCall); ok && isAggregate(call.Name) {
			aggregating = true
		}
	}

	switch {
	case aggregating:
		if err := bindAggregating(q, sel, table); err != nil {
			return nil, err
		}
	case len(sel.GroupBy) > 0:
		if err := bindGroupProjection(q, sel); err != nil {
			return nil, err
		}
	default:
		if err := bindRowMode(q, sel, table); err != nil {
			return nil, err
		}
	}

	q.Where = sel.Where

	groupIndex := map[string]int{}
	for i, g := range q.Groups {
		groupIndex[ast.Canonical(g)] = i
	}
	aggIndex := map[string]int{}
	for i, a := range q.Aggs {
		aggIndex[a.Alias] = i
	}
	outputAliases := map[string]struct{}{}
	for _, o := range q.Output {
		outputAliases[o.Alias] = struct{}{}
	}

	var err error
	if q.Having, err = rewriteExpr(sel.Having, q, groupIndex, aggIndex, outputAliases); err != nil {
		return nil, err
	}
	if q.Having != nil && len(q.Aggs) == 0 && len(q.Groups) == 0 {
		return nil, unsupported("HAVING", "HAVING requires GROUP BY or an aggregate")
	}
	for _, o := range sel.OrderBy {
		expr, err := rewriteExpr(o.Expr, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		q.OrderBy = append(q.OrderBy, ast.SortItem{Expr: expr, Desc: o.Desc, NullsFirst: o.NullsFirst})
	}
	return q, nil
}

func bindAggregating(q *Query, sel *ast.Select, table *catalog.Table) error {
	for _, g := range sel.GroupBy {
		key := ast.Canonical(g)
		if groupContains(q.Groups, key) {
			continue
		}
		q.Groups = append(q.Groups, g)
	}
	groupIndex := map[string]int{}
	for i, g := range q.Groups {
		groupIndex[ast.Canonical(g)] = i
	}

	aggIndex := map[string]int{}
	usedAliases := map[string]bool{}
	nextPartial := 0
	for _, item := range sel.Items {
		if call, ok := item.Expr.(*ast.FuncCall); ok && isAggregate(call.Name) {
			key := ast.Canonical(call)
			idx, exists := aggIndex[key]
			if !exists {
				agg, err := buildAggregate(call, table, &nextPartial)
				if err != nil {
					return err
				}
				agg.Alias = uniqueAlias(item.Alias, generatedAlias(call), usedAliases)
				idx = len(q.Aggs)
				aggIndex[key] = idx
				q.Aggs = append(q.Aggs, agg)
			}
			q.Output = append(q.Output, Output{Alias: q.Aggs[idx].Alias, Group: -1, Agg: idx})
			continue
		}
		gi, ok := groupIndex[ast.Canonical(item.Expr)]
		if !ok {
			return unsupported("SELECT", "every non-aggregate SELECT expression must appear in GROUP BY",
				"add the expression to GROUP BY")
		}
		alias := uniqueAlias(item.Alias, generatedGroupAlias(item.Expr), usedAliases)
		q.Output = append(q.Output, Output{Alias: alias, Group: gi, Agg: -1})
	}
	return nil
}

func bindGroupProjection(q *Query, sel *ast.Select) error {
	for _, g := range sel.GroupBy {
		if groupContains(q.Groups, ast.Canonical(g)) {
			continue
		}
		q.Groups = append(q.Groups, g)
	}
	groupIndex := map[string]int{}
	for i, g := range q.Groups {
		groupIndex[ast.Canonical(g)] = i
	}
	usedAliases := map[string]bool{}
	for _, item := range sel.Items {
		gi, ok := groupIndex[ast.Canonical(item.Expr)]
		if !ok {
			return unsupported("SELECT", "every SELECT expression must appear in GROUP BY",
				"add the expression to GROUP BY or use an aggregate")
		}
		alias := uniqueAlias(item.Alias, generatedGroupAlias(item.Expr), usedAliases)
		q.Output = append(q.Output, Output{Alias: alias, Group: gi, Agg: -1})
	}
	return nil
}

func bindRowMode(q *Query, sel *ast.Select, table *catalog.Table) error {
	q.RowMode = true
	q.Groups = nil
	usedAliases := map[string]bool{}
	for _, item := range sel.Items {
		switch item.Expr.(type) {
		case *ast.ColumnRef, *ast.Literal:
		default:
			return unsupported("SELECT", "row-shaped queries support only plain columns and literals",
				"use passthrough mode for expressions, or run an aggregate query")
		}
		alias := uniqueAlias(item.Alias, generatedGroupAlias(item.Expr), usedAliases)
		q.Output = append(q.Output, Output{Alias: alias, Expr: item.Expr})
	}
	return nil
}

func buildAggregate(call *ast.FuncCall, table *catalog.Table, next *int) (*Aggregate, error) {
	name := strings.ToLower(call.Name)
	if call.Distinct {
		return nil, unsupported("DISTINCT aggregate", "COUNT(DISTINCT ...) is not implemented in this milestone",
			"use passthrough mode, or wait for the cardinality-budget milestone")
	}
	if call.Star && name != "count" {
		return nil, unsupported(name, "only COUNT(*) accepts *")
	}
	agg := &Aggregate{Func: name, Arg: firstArg(call), Star: call.Star, Distinct: call.Distinct, Filter: call.Filter}
	switch name {
	case "count":
		agg.Type = catalog.TypeInt64
		agg.Partials = []Partial{{Alias: partialAlias(next), Func: "count", Arg: agg.Arg, Star: agg.Star, Filter: agg.Filter, Type: catalog.TypeInt64}}
	case "sum", "min", "max":
		if agg.Arg == nil {
			return nil, unsupported(name, name+" requires an argument")
		}
		t, err := TypeOf(agg.Arg, table)
		if err != nil {
			return nil, err
		}
		if name == "sum" && !isNumericType(t) {
			return nil, unsupported("SUM", fmt.Sprintf("SUM is not defined for %s", t))
		}
		agg.Type = t
		agg.Partials = []Partial{{Alias: partialAlias(next), Func: name, Arg: agg.Arg, Filter: agg.Filter, Type: t}}
	case "avg":
		if agg.Arg == nil {
			return nil, unsupported("AVG", "AVG requires an argument")
		}
		t, err := TypeOf(agg.Arg, table)
		if err != nil {
			return nil, err
		}
		if !isNumericType(t) {
			return nil, unsupported("AVG", fmt.Sprintf("AVG is not defined for %s", t))
		}
		if t == catalog.TypeFloat64 {
			agg.Type = catalog.TypeFloat64
		} else {
			agg.Type = catalog.TypeDecimal
		}
		sum := partialAlias(next)
		cnt := partialAlias(next)
		agg.Partials = []Partial{
			{Alias: sum, Func: "sum", Arg: agg.Arg, Filter: agg.Filter, Type: t},
			{Alias: cnt, Func: "count", Arg: agg.Arg, Filter: agg.Filter, Type: catalog.TypeInt64},
		}
	default:
		return nil, unsupported(name, fmt.Sprintf("aggregate %s is not supported", name),
			"supported aggregates: count, sum, min, max, avg")
	}
	return agg, nil
}

func partialAlias(next *int) string {
	alias := fmt.Sprintf("p%d", *next)
	*next = *next + 1
	return alias
}

func firstArg(call *ast.FuncCall) ast.Expr {
	if len(call.Args) == 0 {
		return nil
	}
	return call.Args[0]
}

func isAggregate(name string) bool {
	switch strings.ToLower(name) {
	case "count", "sum", "min", "max", "avg", "stddev", "stddev_pop", "stddev_samp", "var_pop", "var_samp", "variance":
		return true
	}
	return false
}

func containsAggregate(e ast.Expr) bool {
	found := false
	ast.Walk(e, func(x ast.Expr) {
		if call, ok := x.(*ast.FuncCall); ok && isAggregate(call.Name) {
			found = true
		}
	})
	return found
}

func validateColumnsAllowing(e ast.Expr, table *catalog.Table, aliases map[string]bool) error {
	var firstErr error
	ast.Walk(e, func(x ast.Expr) {
		if firstErr != nil {
			return
		}
		ref, ok := x.(*ast.ColumnRef)
		if !ok {
			return
		}
		if aliases[ref.Name] {
			return
		}
		if _, ok := table.Column(ref.Name); !ok {
			firstErr = &sql.Error{
				Msg:        fmt.Sprintf("unknown column %q on table %q", ref.Name, table.Name),
				Clause:     "column",
				Suggestion: "check the catalog column list",
			}
		}
	})
	return firstErr
}

func validateColumns(e ast.Expr, table *catalog.Table) error {
	var firstErr error
	ast.Walk(e, func(x ast.Expr) {
		if firstErr != nil {
			return
		}
		ref, ok := x.(*ast.ColumnRef)
		if !ok {
			return
		}
		if _, ok := table.Column(ref.Name); !ok {
			firstErr = &sql.Error{
				Msg:        fmt.Sprintf("unknown column %q on table %q", ref.Name, table.Name),
				Clause:     "column",
				Suggestion: "check the catalog column list",
			}
		}
	})
	return firstErr
}

func rewriteExpr(e ast.Expr, q *Query, groupIndex, aggIndex map[string]int, outputAliases map[string]struct{}) (ast.Expr, error) {
	if e == nil {
		return nil, nil
	}
	key := ast.Canonical(e)
	if gi, ok := groupIndex[key]; ok {
		return &ast.ColumnRef{Name: groupAlias(gi)}, nil
	}
	if call, ok := e.(*ast.FuncCall); ok && isAggregate(call.Name) {
		alias, ok := aggregateAlias(call, q)
		if !ok {
			return nil, unsupported(call.Name, "aggregate in HAVING/ORDER BY must also appear in SELECT",
				"select the aggregate explicitly")
		}
		return &ast.ColumnRef{Name: alias}, nil
	}
	if q.RowMode {
		for _, out := range q.Output {
			if out.Expr != nil && ast.Canonical(out.Expr) == key {
				return &ast.ColumnRef{Name: out.Alias}, nil
			}
			if ref, ok := e.(*ast.ColumnRef); ok && ref.Name == out.Alias {
				return e, nil
			}
		}
		if ref, ok := e.(*ast.ColumnRef); ok {
			if _, ok := outputAliases[ref.Name]; !ok {
				return nil, unsupported("ORDER BY", fmt.Sprintf("column %q is not projected", ref.Name),
					"add the column to SELECT")
			}
		}
	}

	switch x := e.(type) {
	case *ast.ColumnRef:
		if _, ok := outputAliases[x.Name]; ok {
			return x, nil
		}
		return nil, unsupported("expression", fmt.Sprintf("column %q must appear in GROUP BY or be aggregated", x.Name))
	case *ast.Literal:
		return e, nil
	case *ast.Unary:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.Unary{Op: x.Op, X: inner}, nil
	case *ast.Binary:
		left, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		right, err := rewriteExpr(x.Y, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.Binary{Op: x.Op, X: left, Y: right}, nil
	case *ast.IsNull:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.IsNull{X: inner, Not: x.Not}, nil
	case *ast.Between:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		low, err := rewriteExpr(x.Low, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		high, err := rewriteExpr(x.High, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.Between{X: inner, Low: low, High: high, Not: x.Not}, nil
	case *ast.In:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		list := make([]ast.Expr, 0, len(x.List))
		for _, item := range x.List {
			rewritten, err := rewriteExpr(item, q, groupIndex, aggIndex, outputAliases)
			if err != nil {
				return nil, err
			}
			list = append(list, rewritten)
		}
		return &ast.In{X: inner, List: list, Not: x.Not}, nil
	case *ast.Like:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		pattern, err := rewriteExpr(x.Pattern, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.Like{X: inner, Pattern: pattern, Not: x.Not, IgnoreCase: x.IgnoreCase}, nil
	case *ast.Cast:
		inner, err := rewriteExpr(x.X, q, groupIndex, aggIndex, outputAliases)
		if err != nil {
			return nil, err
		}
		return &ast.Cast{X: inner, Type: x.Type}, nil
	case *ast.FuncCall:
		args := make([]ast.Expr, 0, len(x.Args))
		for _, arg := range x.Args {
			rewritten, err := rewriteExpr(arg, q, groupIndex, aggIndex, outputAliases)
			if err != nil {
				return nil, err
			}
			args = append(args, rewritten)
		}
		return &ast.FuncCall{Name: x.Name, Args: args, Distinct: x.Distinct, Star: x.Star, Filter: x.Filter}, nil
	default:
		return nil, unsupported("expression", fmt.Sprintf("unsupported expression %T", e))
	}
}

func aggregateAlias(call *ast.FuncCall, q *Query) (string, bool) {
	key := ast.Canonical(call)
	for _, agg := range q.Aggs {
		args := []ast.Expr(nil)
		if !agg.Star && agg.Arg != nil {
			args = []ast.Expr{agg.Arg}
		}
		if ast.Canonical(&ast.FuncCall{
			Name:     agg.Func,
			Args:     args,
			Star:     agg.Star,
			Distinct: agg.Distinct,
			Filter:   agg.Filter,
		}) == key {
			return agg.Alias, true
		}
	}
	return "", false
}

func groupAlias(i int) string { return fmt.Sprintf("k%d", i) }

func groupContains(groups []ast.Expr, key string) bool {
	for _, g := range groups {
		if ast.Canonical(g) == key {
			return true
		}
	}
	return false
}

func unsupported(clause, msg string, suggestion ...string) error {
	s := ""
	if len(suggestion) > 0 {
		s = suggestion[0]
	}
	return &sql.Error{Msg: msg, Clause: clause, Suggestion: s}
}

func uniqueAlias(preferred, fallback string, used map[string]bool) string {
	alias := preferred
	if alias == "" {
		alias = fallback
	}
	if alias == "" {
		alias = "col"
	}
	base := alias
	for i := 2; used[alias]; i++ {
		alias = fmt.Sprintf("%s_%d", base, i)
	}
	used[alias] = true
	return alias
}

func generatedAlias(call *ast.FuncCall) string {
	name := strings.ToLower(call.Name)
	if call.Star || len(call.Args) == 0 {
		return name
	}
	if ref, ok := call.Args[0].(*ast.ColumnRef); ok {
		return name + "_" + ref.Name
	}
	return name
}

func generatedGroupAlias(e ast.Expr) string {
	if ref, ok := e.(*ast.ColumnRef); ok {
		return ref.Name
	}
	if call, ok := e.(*ast.FuncCall); ok {
		return strings.ToLower(call.Name)
	}
	return "col"
}
