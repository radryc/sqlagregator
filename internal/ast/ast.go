// Package ast defines the narrow SQL subset the coordinator accepts.
package ast

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/value"
)

// Expr is a scalar expression.
type Expr interface{ isExpr() }

type ColumnRef struct {
	Table string
	Name  string
}

type Literal struct{ Value value.Value }

type Unary struct {
	Op string // NOT, -
	X  Expr
}

type Binary struct {
	Op   string // AND, OR, =, !=, <, <=, >, >=, +, -, *, /
	X, Y Expr
}

type IsNull struct {
	X   Expr
	Not bool
}

type Between struct {
	X, Low, High Expr
	Not          bool
}

type In struct {
	X    Expr
	List []Expr
	Not  bool
}

type Like struct {
	X, Pattern Expr
	Not        bool
	IgnoreCase bool
}

type FuncCall struct {
	Name     string
	Args     []Expr
	Distinct bool
	Star     bool
	Filter   Expr
}

type Cast struct {
	X    Expr
	Type string
}

func (*ColumnRef) isExpr() {}
func (*Literal) isExpr()   {}
func (*Unary) isExpr()     {}
func (*Binary) isExpr()    {}
func (*IsNull) isExpr()    {}
func (*Between) isExpr()   {}
func (*In) isExpr()        {}
func (*Like) isExpr()      {}
func (*FuncCall) isExpr()  {}
func (*Cast) isExpr()      {}

// Select is the only statement the front end produces.
type Select struct {
	Distinct bool
	Items    []SelectItem
	From     TableRef
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []SortItem
	Limit    int
	HasLimit bool
	Offset   int
}

type SelectItem struct {
	Expr  Expr
	Alias string
}

type TableRef struct {
	Schema string
	Name   string
	Alias  string
}

type SortItem struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool
}

// FullName returns the full table name as written.
func (t TableRef) FullName() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	return t.Name
}

// Canonical serialises an expression so structurally equal expressions match.
// It is used to resolve HAVING/ORDER BY references to group keys and aggregates.
func Canonical(e Expr) string {
	var b strings.Builder
	writeCanonical(&b, e)
	return b.String()
}

func writeCanonical(b *strings.Builder, e Expr) {
	switch x := e.(type) {
	case nil:
		b.WriteString("<nil>")
	case *ColumnRef:
		b.WriteString("col(")
		b.WriteString(x.Table)
		b.WriteByte('.')
		b.WriteString(x.Name)
		b.WriteByte(')')
	case *Literal:
		b.WriteString("lit(")
		b.WriteString(x.Value.Key())
		b.WriteByte(')')
	case *Unary:
		b.WriteString("un(")
		b.WriteString(x.Op)
		b.WriteByte(' ')
		writeCanonical(b, x.X)
		b.WriteByte(')')
	case *Binary:
		b.WriteString("bin(")
		b.WriteString(x.Op)
		b.WriteByte(' ')
		writeCanonical(b, x.X)
		b.WriteByte(' ')
		writeCanonical(b, x.Y)
		b.WriteByte(')')
	case *IsNull:
		fmt.Fprintf(b, "isnull(%t ", x.Not)
		writeCanonical(b, x.X)
		b.WriteByte(')')
	case *Between:
		fmt.Fprintf(b, "between(%t ", x.Not)
		writeCanonical(b, x.X)
		b.WriteByte(' ')
		writeCanonical(b, x.Low)
		b.WriteByte(' ')
		writeCanonical(b, x.High)
		b.WriteByte(')')
	case *In:
		fmt.Fprintf(b, "in(%t ", x.Not)
		writeCanonical(b, x.X)
		for _, item := range x.List {
			b.WriteByte(' ')
			writeCanonical(b, item)
		}
		b.WriteByte(')')
	case *Like:
		fmt.Fprintf(b, "like(%t %t ", x.Not, x.IgnoreCase)
		writeCanonical(b, x.X)
		b.WriteByte(' ')
		writeCanonical(b, x.Pattern)
		b.WriteByte(')')
	case *FuncCall:
		fmt.Fprintf(b, "fn(%s,%t,%t,", strings.ToLower(x.Name), x.Distinct, x.Star)
		for _, arg := range x.Args {
			writeCanonical(b, arg)
			b.WriteByte(',')
		}
		if x.Filter != nil {
			b.WriteString("filter=")
			writeCanonical(b, x.Filter)
		}
		b.WriteByte(')')
	case *Cast:
		b.WriteString("cast(")
		b.WriteString(strings.ToLower(x.Type))
		b.WriteByte(' ')
		writeCanonical(b, x.X)
		b.WriteByte(')')
	default:
		fmt.Fprintf(b, "%T", e)
	}
}

// Walk calls fn for every expression node in depth-first order.
func Walk(e Expr, fn func(Expr)) {
	if e == nil {
		return
	}
	fn(e)
	switch x := e.(type) {
	case *Unary:
		Walk(x.X, fn)
	case *Binary:
		Walk(x.X, fn)
		Walk(x.Y, fn)
	case *IsNull:
		Walk(x.X, fn)
	case *Between:
		Walk(x.X, fn)
		Walk(x.Low, fn)
		Walk(x.High, fn)
	case *In:
		Walk(x.X, fn)
		for _, item := range x.List {
			Walk(item, fn)
		}
	case *Like:
		Walk(x.X, fn)
		Walk(x.Pattern, fn)
	case *FuncCall:
		for _, arg := range x.Args {
			Walk(arg, fn)
		}
		Walk(x.Filter, fn)
	case *Cast:
		Walk(x.X, fn)
	}
}
