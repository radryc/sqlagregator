package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/value"
)

// Parse parses the supported SELECT subset into an AST.
func Parse(input string) (*ast.Select, error) {
	toks, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	sel, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if p.acceptOp(";") {
		// one trailing semicolon is fine
	}
	if p.cur().kind != tokEOF {
		return nil, unsupported(p.cur().pos, "statement", fmt.Sprintf("unexpected token %q", p.cur().text),
			"only a single SELECT statement is supported")
	}
	return sel, nil
}

type parser struct {
	toks []token
	pos  int
}

func (p *parser) cur() token { return p.toks[p.pos] }

func (p *parser) peek(n int) token {
	if p.pos+n >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[p.pos+n]
}

func (p *parser) next() token {
	t := p.toks[p.pos]
	if p.pos < len(p.toks)-1 {
		p.pos++
	}
	return t
}

func (p *parser) acceptOp(op string) bool {
	if p.cur().kind == tokOp && p.cur().text == op {
		p.pos++
		return true
	}
	return false
}

func (p *parser) peekOp(op string) bool {
	return p.cur().kind == tokOp && p.cur().text == op
}

func (p *parser) expectOp(op string) error {
	if p.acceptOp(op) {
		return nil
	}
	return errAt(p.cur().pos, fmt.Sprintf("expected %q, found %q", op, p.cur().text))
}

func (p *parser) acceptKeyword(kw string) bool {
	if p.cur().isKeyword(kw) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) peekKeyword(kw string) bool { return p.cur().isKeyword(kw) }

func (p *parser) expectKeyword(kw string) error {
	if p.acceptKeyword(kw) {
		return nil
	}
	return errAt(p.cur().pos, fmt.Sprintf("expected %s, found %q", kw, p.cur().text))
}

func (p *parser) expectIdent() (string, error) {
	if p.cur().kind != tokIdent {
		return "", errAt(p.cur().pos, fmt.Sprintf("expected identifier, found %q", p.cur().text))
	}
	return p.next().text, nil
}

func (p *parser) parseSelect() (*ast.Select, error) {
	if err := p.expectKeyword("SELECT"); err != nil {
		return nil, unsupported(p.cur().pos, "statement", "query must start with SELECT", "only SELECT statements are supported")
	}
	sel := &ast.Select{Distinct: p.acceptKeyword("DISTINCT")}
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		sel.Items = append(sel.Items, item)
		if !p.acceptOp(",") {
			break
		}
	}
	if len(sel.Items) == 0 {
		return nil, errAt(p.cur().pos, "SELECT list cannot be empty")
	}
	if !p.acceptKeyword("FROM") {
		return nil, unsupported(p.cur().pos, "FROM", "FROM is required",
			"name a logical table such as logs, traces, or metrics")
	}
	table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	sel.From = table
	if p.peekOp(",") {
		return nil, unsupported(p.cur().pos, "FROM", "multiple FROM items are not supported",
			"the generic dialect supports one table per query")
	}
	switch {
	case p.peekKeyword("JOIN"), p.peekKeyword("LEFT"), p.peekKeyword("RIGHT"),
		p.peekKeyword("INNER"), p.peekKeyword("FULL"), p.peekKeyword("CROSS"):
		return nil, unsupported(p.cur().pos, "JOIN", "joins are not supported in the generic dialect",
			"run the query in passthrough mode on a single engine")
	}

	for {
		switch {
		case p.acceptKeyword("WHERE"):
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			sel.Where = expr
		case p.acceptKeyword("GROUP"):
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			list, err := p.parseExprList("GROUP BY")
			if err != nil {
				return nil, err
			}
			sel.GroupBy = list
		case p.acceptKeyword("HAVING"):
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			sel.Having = expr
		case p.acceptKeyword("ORDER"):
			if err := p.expectKeyword("BY"); err != nil {
				return nil, err
			}
			items, err := p.parseOrderBy()
			if err != nil {
				return nil, err
			}
			sel.OrderBy = items
		case p.acceptKeyword("LIMIT"):
			n, err := p.parseInt()
			if err != nil {
				return nil, err
			}
			sel.Limit = n
			sel.HasLimit = true
			if p.acceptKeyword("OFFSET") {
				off, err := p.parseInt()
				if err != nil {
					return nil, err
				}
				sel.Offset = off
			}
		case p.peekKeyword("UNION"), p.peekKeyword("EXCEPT"), p.peekKeyword("INTERSECT"):
			return nil, unsupported(p.cur().pos, "set operation", "set operations are not supported",
				"only a single SELECT is supported in v1")
		case p.peekKeyword("QUALIFY"):
			return nil, unsupported(p.cur().pos, "QUALIFY", "QUALIFY is not supported",
				"window functions require passthrough mode")
		default:
			return sel, nil
		}
	}
}

func (p *parser) parseSelectItem() (ast.SelectItem, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return ast.SelectItem{}, err
	}
	item := ast.SelectItem{Expr: expr}
	if p.acceptKeyword("AS") {
		alias, err := p.expectIdent()
		if err != nil {
			return ast.SelectItem{}, err
		}
		item.Alias = alias
	} else if p.cur().kind == tokIdent && !isAliasStop(p.cur()) {
		item.Alias = p.next().text
	}
	return item, nil
}

func (p *parser) parseTableRef() (ast.TableRef, error) {
	tok := p.cur()
	if tok.kind != tokIdent {
		return ast.TableRef{}, errAt(tok.pos, fmt.Sprintf("expected table name, found %q", tok.text))
	}
	p.next()
	ref := ast.TableRef{Name: tok.text}
	if !tok.quoted {
		for p.acceptOp(".") {
			part, err := p.expectIdent()
			if err != nil {
				return ast.TableRef{}, err
			}
			ref.Schema = ref.Name
			ref.Name = part
		}
	}
	if p.acceptKeyword("AS") {
		alias, err := p.expectIdent()
		if err != nil {
			return ast.TableRef{}, err
		}
		ref.Alias = alias
	} else if p.cur().kind == tokIdent && !isAliasStop(p.cur()) {
		ref.Alias = p.next().text
	}
	return ref, nil
}

func (p *parser) parseExprList(clause string) ([]ast.Expr, error) {
	var out []ast.Expr
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, expr)
		if !p.acceptOp(",") {
			break
		}
	}
	if len(out) == 0 {
		return nil, errAt(p.cur().pos, clause+" list cannot be empty")
	}
	return out, nil
}

func (p *parser) parseOrderBy() ([]ast.SortItem, error) {
	var out []ast.SortItem
	for {
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		item := ast.SortItem{Expr: expr}
		if p.acceptKeyword("DESC") {
			item.Desc = true
		} else {
			p.acceptKeyword("ASC")
		}
		if p.acceptKeyword("NULLS") {
			first := false
			switch {
			case p.acceptKeyword("FIRST"):
				first = true
			case p.acceptKeyword("LAST"):
				first = false
			default:
				return nil, errAt(p.cur().pos, "expected FIRST or LAST after NULLS")
			}
			item.NullsFirst = &first
		}
		out = append(out, item)
		if !p.acceptOp(",") {
			break
		}
	}
	return out, nil
}

func (p *parser) parseInt() (int, error) {
	tok := p.cur()
	if tok.kind != tokNumber || strings.ContainsAny(tok.text, ".eE") {
		return 0, errAt(tok.pos, fmt.Sprintf("expected integer, found %q", tok.text))
	}
	n, err := strconv.Atoi(tok.text)
	if err != nil || n < 0 {
		return 0, errAt(tok.pos, "invalid non-negative integer "+tok.text)
	}
	p.next()
	return n, nil
}

// --- expressions -----------------------------------------------------------

func (p *parser) parseExpr() (ast.Expr, error) { return p.parseOr() }

func (p *parser) parseOr() (ast.Expr, error) {
	x, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("OR") {
		y, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		x = &ast.Binary{Op: "OR", X: x, Y: y}
	}
	return x, nil
}

func (p *parser) parseAnd() (ast.Expr, error) {
	x, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.acceptKeyword("AND") {
		y, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		x = &ast.Binary{Op: "AND", X: x, Y: y}
	}
	return x, nil
}

func (p *parser) parseNot() (ast.Expr, error) {
	if p.acceptKeyword("NOT") {
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &ast.Unary{Op: "NOT", X: x}, nil
	}
	return p.parseComparison()
}

func (p *parser) parseComparison() (ast.Expr, error) {
	x, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.acceptOp("="):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "=", X: x, Y: y}
		case p.acceptOp("!="), p.acceptOp("<>"):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "!=", X: x, Y: y}
		case p.acceptOp("<="):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "<=", X: x, Y: y}
		case p.acceptOp(">="):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: ">=", X: x, Y: y}
		case p.acceptOp("<"):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "<", X: x, Y: y}
		case p.acceptOp(">"):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: ">", X: x, Y: y}
		case p.acceptKeyword("ILIKE"):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Like{X: x, Pattern: y, IgnoreCase: true}
		case p.acceptKeyword("LIKE"):
			y, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			x = &ast.Like{X: x, Pattern: y}
		case p.acceptKeyword("IN"):
			expr, err := p.parseIn(x, false)
			if err != nil {
				return nil, err
			}
			x = expr
		case p.acceptKeyword("BETWEEN"):
			expr, err := p.parseBetween(x, false)
			if err != nil {
				return nil, err
			}
			x = expr
		case p.acceptKeyword("IS"):
			not := p.acceptKeyword("NOT")
			if !p.acceptKeyword("NULL") {
				return nil, unsupported(p.cur().pos, "IS", "only IS [NOT] NULL is supported", "")
			}
			x = &ast.IsNull{X: x, Not: not}
		case p.acceptKeyword("NOT"):
			switch {
			case p.acceptKeyword("LIKE"):
				y, err := p.parseAdditive()
				if err != nil {
					return nil, err
				}
				x = &ast.Like{X: x, Pattern: y, Not: true}
			case p.acceptKeyword("ILIKE"):
				y, err := p.parseAdditive()
				if err != nil {
					return nil, err
				}
				x = &ast.Like{X: x, Pattern: y, Not: true, IgnoreCase: true}
			case p.acceptKeyword("IN"):
				expr, err := p.parseIn(x, true)
				if err != nil {
					return nil, err
				}
				x = expr
			case p.acceptKeyword("BETWEEN"):
				expr, err := p.parseBetween(x, true)
				if err != nil {
					return nil, err
				}
				x = expr
			default:
				return nil, errAt(p.cur().pos, "expected LIKE, ILIKE, IN, or BETWEEN after NOT")
			}
		default:
			return x, nil
		}
	}
}

func (p *parser) parseIn(x ast.Expr, not bool) (ast.Expr, error) {
	if p.peekKeyword("SELECT") {
		return nil, unsupported(p.cur().pos, "IN (subquery)", "subqueries are not supported",
			"pass explicit values or use passthrough mode")
	}
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	var list []ast.Expr
	for {
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		list = append(list, e)
		if !p.acceptOp(",") {
			break
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &ast.In{X: x, List: list, Not: not}, nil
}

func (p *parser) parseBetween(x ast.Expr, not bool) (ast.Expr, error) {
	low, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("AND"); err != nil {
		return nil, err
	}
	high, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}
	return &ast.Between{X: x, Low: low, High: high, Not: not}, nil
}

func (p *parser) parseAdditive() (ast.Expr, error) {
	x, err := p.parseMultiplicative()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.acceptOp("+"):
			y, err := p.parseMultiplicative()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "+", X: x, Y: y}
		case p.acceptOp("-"):
			y, err := p.parseMultiplicative()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "-", X: x, Y: y}
		default:
			return x, nil
		}
	}
}

func (p *parser) parseMultiplicative() (ast.Expr, error) {
	x, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		switch {
		case p.acceptOp("*"):
			y, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "*", X: x, Y: y}
		case p.acceptOp("/"):
			y, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "/", X: x, Y: y}
		case p.acceptOp("%"):
			y, err := p.parseUnary()
			if err != nil {
				return nil, err
			}
			x = &ast.Binary{Op: "%", X: x, Y: y}
		default:
			return x, nil
		}
	}
}

func (p *parser) parseUnary() (ast.Expr, error) {
	if p.acceptOp("-") {
		x, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &ast.Unary{Op: "-", X: x}, nil
	}
	if p.acceptOp("+") {
		return p.parseUnary()
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (ast.Expr, error) {
	tok := p.cur()
	switch tok.kind {
	case tokNumber:
		p.next()
		if strings.ContainsAny(tok.text, ".eE") {
			d, ok := value.NewDecimalFromString(tok.text)
			if !ok {
				return nil, errAt(tok.pos, "invalid number "+tok.text)
			}
			return &ast.Literal{Value: value.Dec(d)}, nil
		}
		i, err := strconv.ParseInt(tok.text, 10, 64)
		if err != nil {
			d, ok := value.NewDecimalFromString(tok.text)
			if !ok {
				return nil, errAt(tok.pos, "invalid number "+tok.text)
			}
			return &ast.Literal{Value: value.Dec(d)}, nil
		}
		return &ast.Literal{Value: value.Int64(i)}, nil
	case tokString:
		p.next()
		return &ast.Literal{Value: value.Str(tok.text)}, nil
	case tokIdent:
		switch {
		case tok.isKeyword("TRUE"):
			p.next()
			return &ast.Literal{Value: value.Boolean(true)}, nil
		case tok.isKeyword("FALSE"):
			p.next()
			return &ast.Literal{Value: value.Boolean(false)}, nil
		case tok.isKeyword("NULL"):
			p.next()
			return &ast.Literal{Value: value.NullValue()}, nil
		case tok.isKeyword("CAST"):
			return p.parseCast()
		case tok.isKeyword("INTERVAL"):
			return nil, unsupported(tok.pos, "INTERVAL",
				"INTERVAL literals are not supported in the generic dialect",
				"pass from/to bounds on the request instead of embedding them in SQL")
		}
		if p.peek(1).kind == tokOp && p.peek(1).text == "(" {
			return p.parseCall()
		}
		p.next()
		ref := &ast.ColumnRef{Name: tok.text}
		if !tok.quoted && p.acceptOp(".") {
			part, err := p.expectIdent()
			if err != nil {
				return nil, err
			}
			ref.Table = tok.text
			ref.Name = part
		}
		return ref, nil
	case tokOp:
		if tok.text == "(" {
			p.next()
			x, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expectOp(")"); err != nil {
				return nil, err
			}
			return x, nil
		}
	}
	return nil, errAt(tok.pos, fmt.Sprintf("unexpected token %q", tok.text))
}

func (p *parser) parseCall() (ast.Expr, error) {
	nameTok := p.next()
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	call := &ast.FuncCall{Name: nameTok.text}
	if p.acceptKeyword("DISTINCT") {
		call.Distinct = true
	}
	if p.acceptOp("*") {
		call.Star = true
	} else if !p.peekOp(")") {
		for {
			arg, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			call.Args = append(call.Args, arg)
			if !p.acceptOp(",") {
				break
			}
		}
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("FILTER") {
		if err := p.expectOp("("); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("WHERE"); err != nil {
			return nil, err
		}
		filter, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectOp(")"); err != nil {
			return nil, err
		}
		call.Filter = filter
	}
	return call, nil
}

func (p *parser) parseCast() (ast.Expr, error) {
	p.next() // CAST
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	x, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if err := p.expectKeyword("AS"); err != nil {
		return nil, err
	}
	typ, err := p.parseTypeName()
	if err != nil {
		return nil, err
	}
	if err := p.expectOp(")"); err != nil {
		return nil, err
	}
	return &ast.Cast{X: x, Type: typ}, nil
}

func (p *parser) parseTypeName() (string, error) {
	name, err := p.expectIdent()
	if err != nil {
		return "", err
	}
	if !p.acceptOp("(") {
		return name, nil
	}
	var parts []string
	for {
		tok := p.cur()
		if tok.kind != tokNumber {
			return "", errAt(tok.pos, "expected type precision")
		}
		p.next()
		parts = append(parts, tok.text)
		if p.acceptOp(",") {
			continue
		}
		break
	}
	if err := p.expectOp(")"); err != nil {
		return "", err
	}
	return name + "(" + strings.Join(parts, ",") + ")", nil
}

var aliasStop = map[string]struct{}{
	"FROM": {}, "WHERE": {}, "GROUP": {}, "HAVING": {}, "ORDER": {}, "LIMIT": {},
	"OFFSET": {}, "UNION": {}, "EXCEPT": {}, "INTERSECT": {}, "JOIN": {}, "LEFT": {},
	"RIGHT": {}, "FULL": {}, "INNER": {}, "CROSS": {}, "ON": {}, "AS": {}, "ASC": {},
	"DESC": {}, "NULLS": {}, "FILTER": {},
}

func isAliasStop(t token) bool {
	if t.kind != tokIdent || t.quoted {
		return false
	}
	_, ok := aliasStop[strings.ToUpper(t.text)]
	return ok
}
