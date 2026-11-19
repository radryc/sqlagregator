package merge

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/value"
)

type rowCtx struct {
	names map[string]int
	vals  []value.Value
}

func newRowCtx() *rowCtx {
	return &rowCtx{names: map[string]int{}}
}

func (c *rowCtx) set(name string, v value.Value) {
	if i, ok := c.names[name]; ok {
		c.vals[i] = v
		return
	}
	c.names[name] = len(c.vals)
	c.vals = append(c.vals, v)
}

func (c *rowCtx) get(name string) (value.Value, bool) {
	i, ok := c.names[name]
	if !ok {
		return value.NullValue(), false
	}
	return c.vals[i], true
}

func truth(v value.Value) bool {
	switch v.Kind {
	case value.KBool:
		return v.B
	case value.KNull:
		return false
	case value.KInt:
		return v.I != 0
	default:
		return false
	}
}

func eval(e ast.Expr, ctx *rowCtx) (value.Value, error) {
	switch x := e.(type) {
	case nil:
		return value.NullValue(), nil
	case *ast.ColumnRef:
		v, ok := ctx.get(x.Name)
		if !ok {
			return value.NullValue(), fmt.Errorf("merge: unknown output column %q", x.Name)
		}
		return v, nil
	case *ast.Literal:
		return x.Value, nil
	case *ast.Unary:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KNull {
			return v, nil
		}
		switch strings.ToUpper(x.Op) {
		case "NOT":
			return value.Boolean(!truth(v)), nil
		case "-":
			return negate(v)
		}
		return value.NullValue(), fmt.Errorf("merge: unsupported unary operator %q", x.Op)
	case *ast.Binary:
		return evalBinary(x, ctx)
	case *ast.IsNull:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		isNull := v.Kind == value.KNull
		if x.Not {
			isNull = !isNull
		}
		return value.Boolean(isNull), nil
	case *ast.Between:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		lo, err := eval(x.Low, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		hi, err := eval(x.High, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		inRange := compareOps(">=", v, lo) && compareOps("<=", v, hi)
		if v.Kind == value.KNull || lo.Kind == value.KNull || hi.Kind == value.KNull {
			return value.NullValue(), nil
		}
		if x.Not {
			inRange = !inRange
		}
		return value.Boolean(inRange), nil
	case *ast.In:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KNull {
			return value.NullValue(), nil
		}
		found, hasNull := false, false
		for _, item := range x.List {
			iv, err := eval(item, ctx)
			if err != nil {
				return value.NullValue(), err
			}
			if iv.Kind == value.KNull {
				hasNull = true
				continue
			}
			if value.Compare(v, iv) == 0 {
				found = true
				break
			}
		}
		if !found && hasNull {
			return value.NullValue(), nil
		}
		if x.Not {
			found = !found
		}
		return value.Boolean(found), nil
	case *ast.Like:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		p, err := eval(x.Pattern, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KNull || p.Kind == value.KNull {
			return value.NullValue(), nil
		}
		ok := likeMatch(v.AsString(), p.AsString(), x.IgnoreCase)
		if x.Not {
			ok = !ok
		}
		return value.Boolean(ok), nil
	case *ast.Cast:
		v, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		return castValue(v, plan.ParseType(x.Type)), nil
	case *ast.FuncCall:
		return evalFunc(x, ctx)
	default:
		return value.NullValue(), fmt.Errorf("merge: unsupported expression %T", e)
	}
}

func evalBinary(x *ast.Binary, ctx *rowCtx) (value.Value, error) {
	op := strings.ToUpper(x.Op)
	if op == "AND" || op == "OR" {
		a, err := eval(x.X, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		b, err := eval(x.Y, ctx)
		if err != nil {
			return value.NullValue(), err
		}
		return logical(op, a, b), nil
	}
	a, err := eval(x.X, ctx)
	if err != nil {
		return value.NullValue(), err
	}
	b, err := eval(x.Y, ctx)
	if err != nil {
		return value.NullValue(), err
	}
	if op == "=" || op == "!=" || op == "<" || op == "<=" || op == ">" || op == ">=" {
		if a.Kind == value.KNull || b.Kind == value.KNull {
			return value.NullValue(), nil
		}
		ok := false
		switch op {
		case "=":
			ok = value.Compare(a, b) == 0
		case "!=":
			ok = value.Compare(a, b) != 0
		case "<":
			ok = value.Compare(a, b) < 0
		case "<=":
			ok = value.Compare(a, b) <= 0
		case ">":
			ok = value.Compare(a, b) > 0
		case ">=":
			ok = value.Compare(a, b) >= 0
		}
		return value.Boolean(ok), nil
	}
	return arithmetic(op, a, b)
}

func compareOps(op string, a, b value.Value) bool {
	switch op {
	case ">=":
		return value.Compare(a, b) >= 0
	case "<=":
		return value.Compare(a, b) <= 0
	}
	return false
}

func logical(op string, a, b value.Value) value.Value {
	at, bt := tri(a), tri(b)
	if op == "AND" {
		if at == 0 || bt == 0 {
			return value.Boolean(false)
		}
		if at == 2 || bt == 2 {
			return value.NullValue()
		}
		return value.Boolean(true)
	}
	if at == 1 || bt == 1 {
		return value.Boolean(true)
	}
	if at == 2 || bt == 2 {
		return value.NullValue()
	}
	return value.Boolean(false)
}

// tri returns 1 true, 0 false, 2 null.
func tri(v value.Value) int {
	switch v.Kind {
	case value.KNull:
		return 2
	case value.KBool:
		if v.B {
			return 1
		}
		return 0
	case value.KInt:
		if v.I != 0 {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func negate(v value.Value) (value.Value, error) {
	switch v.Kind {
	case value.KInt:
		return value.Int64(-v.I), nil
	case value.KFloat:
		return value.Float64(-v.F), nil
	case value.KDecimal:
		return value.Dec(v.D.Neg()), nil
	default:
		return value.NullValue(), nil
	}
}

func arithmetic(op string, a, b value.Value) (value.Value, error) {
	if a.Kind == value.KNull || b.Kind == value.KNull {
		return value.NullValue(), nil
	}
	switch op {
	case "+":
		if v, ok := value.Add(a, b); ok {
			return v, nil
		}
	case "-":
		nb, _ := negate(b)
		if v, ok := value.Add(a, nb); ok {
			return v, nil
		}
	case "*":
		if a.Kind == value.KFloat || b.Kind == value.KFloat {
			return value.Float64(a.Float64() * b.Float64()), nil
		}
		return value.Dec(a.AsDecimal().Mul(b.AsDecimal())), nil
	case "/":
		if b.Float64() == 0 {
			return value.NullValue(), nil
		}
		if a.Kind == value.KFloat || b.Kind == value.KFloat {
			return value.Float64(a.Float64() / b.Float64()), nil
		}
		d := a.AsDecimal().DivInt(b.Int(), 9)
		if d == nil {
			return value.NullValue(), nil
		}
		return value.Dec(d), nil
	case "%":
		if b.Int() == 0 {
			return value.NullValue(), nil
		}
		return value.Int64(a.Int() % b.Int()), nil
	}
	return value.NullValue(), fmt.Errorf("merge: unsupported numeric operation %s %s %s", a.Kind, op, b.Kind)
}

func evalFunc(x *ast.FuncCall, ctx *rowCtx) (value.Value, error) {
	name := strings.ToLower(x.Name)
	switch name {
	case "lower", "upper":
		if len(x.Args) != 1 {
			return value.NullValue(), fmt.Errorf("%s takes one argument", name)
		}
		v, err := eval(x.Args[0], ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KNull {
			return v, nil
		}
		if name == "lower" {
			return value.Str(strings.ToLower(v.AsString())), nil
		}
		return value.Str(strings.ToUpper(v.AsString())), nil
	case "length":
		if len(x.Args) != 1 {
			return value.NullValue(), fmt.Errorf("length takes one argument")
		}
		v, err := eval(x.Args[0], ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KNull {
			return v, nil
		}
		return value.Int64(int64(len(v.AsString()))), nil
	case "coalesce":
		for _, arg := range x.Args {
			v, err := eval(arg, ctx)
			if err != nil {
				return value.NullValue(), err
			}
			if v.Kind != value.KNull {
				return v, nil
			}
		}
		return value.NullValue(), nil
	case "abs":
		if len(x.Args) != 1 {
			return value.NullValue(), fmt.Errorf("abs takes one argument")
		}
		v, err := eval(x.Args[0], ctx)
		if err != nil {
			return value.NullValue(), err
		}
		if v.Kind == value.KInt && v.I < 0 {
			return value.Int64(-v.I), nil
		}
		if v.Kind == value.KFloat {
			return value.Float64(math.Abs(v.F)), nil
		}
		return v, nil
	case "round":
		if len(x.Args) != 1 {
			return value.NullValue(), fmt.Errorf("round takes one argument")
		}
		v, err := eval(x.Args[0], ctx)
		if err != nil {
			return value.NullValue(), err
		}
		return value.Float64(math.Round(v.Float64())), nil
	case "concat":
		var b strings.Builder
		for _, arg := range x.Args {
			v, err := eval(arg, ctx)
			if err != nil {
				return value.NullValue(), err
			}
			b.WriteString(v.AsString())
		}
		return value.Str(b.String()), nil
	default:
		return value.NullValue(), fmt.Errorf("merge: aggregate %q was not rewritten to a column", x.Name)
	}
}

func castValue(v value.Value, t catalog.Type) value.Value {
	switch t {
	case catalog.TypeInt64:
		return value.Int64(v.Int())
	case catalog.TypeFloat64:
		return value.Float64(v.Float64())
	case catalog.TypeString:
		return value.Str(v.AsString())
	case catalog.TypeBool:
		return value.Boolean(truth(v))
	case catalog.TypeDecimal:
		d := v.AsDecimal()
		if d == nil {
			return value.NullValue()
		}
		return value.Dec(d)
	case catalog.TypeTimestamp:
		return v
	default:
		return v
	}
}

func likeMatch(s, pattern string, ignoreCase bool) bool {
	if ignoreCase {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	return likeDP([]rune(s), []rune(pattern))
}

func likeDP(s, p []rune) bool {
	// dp[j] = pattern prefix of length j matches current string prefix.
	dp := make([]bool, len(p)+1)
	dp[0] = true
	for j := 1; j <= len(p); j++ {
		dp[j] = dp[j-1] && p[j-1] == '%'
	}
	for i := 1; i <= len(s); i++ {
		next := make([]bool, len(p)+1)
		for j := 1; j <= len(p); j++ {
			switch p[j-1] {
			case '%':
				next[j] = next[j-1] || dp[j]
			case '_':
				next[j] = dp[j-1]
			default:
				next[j] = dp[j-1] && s[i-1] == p[j-1]
			}
		}
		dp = next
	}
	return dp[len(p)]
}

func parseTimestamp(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
