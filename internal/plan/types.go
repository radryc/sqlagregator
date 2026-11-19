package plan

import (
	"fmt"
	"strings"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/value"
)

// TypeOf infers the canonical type of an expression.
func TypeOf(e ast.Expr, table *catalog.Table) (catalog.Type, error) {
	switch x := e.(type) {
	case *ast.ColumnRef:
		col, ok := table.Column(x.Name)
		if !ok {
			return "", fmt.Errorf("unknown column %q", x.Name)
		}
		return col.Type, nil
	case *ast.Literal:
		return typeOfValue(x.Value), nil
	case *ast.Unary:
		return TypeOf(x.X, table)
	case *ast.Binary:
		switch strings.ToUpper(x.Op) {
		case "AND", "OR", "=", "!=", "<", "<=", ">", ">=":
			return catalog.TypeBool, nil
		default:
			lt, err := TypeOf(x.X, table)
			if err != nil {
				return "", err
			}
			rt, err := TypeOf(x.Y, table)
			if err != nil {
				return "", err
			}
			return promote(lt, rt), nil
		}
	case *ast.IsNull, *ast.Between, *ast.In, *ast.Like:
		return catalog.TypeBool, nil
	case *ast.Cast:
		return ParseType(x.Type), nil
	case *ast.FuncCall:
		name := strings.ToLower(x.Name)
		switch name {
		case "date_trunc", "time_bucket", "now":
			return catalog.TypeTimestamp, nil
		case "lower", "upper", "concat":
			return catalog.TypeString, nil
		case "length":
			return catalog.TypeInt64, nil
		case "coalesce":
			if len(x.Args) == 0 {
				return "", fmt.Errorf("coalesce requires arguments")
			}
			return TypeOf(x.Args[0], table)
		case "abs", "round", "floor", "ceil":
			if len(x.Args) == 0 {
				return "", fmt.Errorf("%s requires an argument", name)
			}
			return TypeOf(x.Args[0], table)
		case "count":
			return catalog.TypeInt64, nil
		case "avg", "stddev", "stddev_pop", "var_pop", "var_samp", "variance":
			return catalog.TypeFloat64, nil
		case "sum", "min", "max":
			if len(x.Args) == 0 {
				return "", fmt.Errorf("%s requires an argument", name)
			}
			return TypeOf(x.Args[0], table)
		default:
			return "", fmt.Errorf("unknown function %q", x.Name)
		}
	default:
		return "", fmt.Errorf("cannot infer type of %T", e)
	}
}

// ParseType maps a SQL type name to a canonical type.
func ParseType(name string) catalog.Type {
	n := strings.ToLower(strings.TrimSpace(name))
	if i := strings.IndexByte(n, '('); i >= 0 {
		n = n[:i]
	}
	switch n {
	case "int", "integer", "int32", "int64", "bigint", "long", "smallint":
		return catalog.TypeInt64
	case "uint", "uint64", "ubigint":
		return catalog.TypeUint64
	case "float", "float64", "double", "double precision", "real":
		return catalog.TypeFloat64
	case "decimal", "numeric", "bignumeric":
		return catalog.TypeDecimal
	case "bool", "boolean":
		return catalog.TypeBool
	case "timestamp", "timestamptz", "datetime", "datetime64":
		return catalog.TypeTimestamp
	case "json", "jsonb":
		return catalog.TypeJSON
	default:
		return catalog.TypeString
	}
}

func typeOfValue(v value.Value) catalog.Type {
	switch v.Kind {
	case value.KInt:
		return catalog.TypeInt64
	case value.KUint:
		return catalog.TypeUint64
	case value.KFloat:
		return catalog.TypeFloat64
	case value.KDecimal:
		return catalog.TypeDecimal
	case value.KBool:
		return catalog.TypeBool
	case value.KTimestamp:
		return catalog.TypeTimestamp
	default:
		return catalog.TypeString
	}
}

func isNumericType(t catalog.Type) bool {
	switch t {
	case catalog.TypeInt64, catalog.TypeUint64, catalog.TypeFloat64, catalog.TypeDecimal:
		return true
	}
	return false
}

func promote(a, b catalog.Type) catalog.Type {
	if a == catalog.TypeFloat64 || b == catalog.TypeFloat64 {
		return catalog.TypeFloat64
	}
	if a == catalog.TypeDecimal || b == catalog.TypeDecimal {
		return catalog.TypeDecimal
	}
	if a == catalog.TypeUint64 || b == catalog.TypeUint64 {
		return catalog.TypeUint64
	}
	return catalog.TypeInt64
}
