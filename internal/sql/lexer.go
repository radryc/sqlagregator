package sql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenKind int

const (
	tokEOF tokenKind = iota
	tokIdent
	tokString
	tokNumber
	tokOp
)

type token struct {
	kind   tokenKind
	text   string
	pos    int
	quoted bool
}

func (t token) isKeyword(kw string) bool {
	return t.kind == tokIdent && !t.quoted && strings.EqualFold(t.text, kw)
}

// Error is a structured parse or validation error.
type Error struct {
	Pos        int
	Msg        string
	Clause     string
	Suggestion string
}

func (e *Error) Error() string {
	var b strings.Builder
	if e.Pos > 0 {
		fmt.Fprintf(&b, "position %d: ", e.Pos)
	}
	b.WriteString(e.Msg)
	if e.Clause != "" {
		fmt.Fprintf(&b, " [%s]", e.Clause)
	}
	if e.Suggestion != "" {
		fmt.Fprintf(&b, " — %s", e.Suggestion)
	}
	return b.String()
}

func errAt(pos int, msg string) *Error { return &Error{Pos: pos, Msg: msg} }

func unsupported(pos int, clause, msg, suggestion string) *Error {
	return &Error{Pos: pos, Msg: msg, Clause: clause, Suggestion: suggestion}
}

func lex(input string) ([]token, error) {
	var out []token
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(input) && input[i+1] == '-':
			for i < len(input) && input[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(input) && input[i+1] == '*':
			start := i
			i += 2
			for i+1 < len(input) && !(input[i] == '*' && input[i+1] == '/') {
				i++
			}
			if i+1 >= len(input) {
				return nil, errAt(start, "unterminated block comment")
			}
			i += 2
		case c == '\'':
			start := i
			i++
			var b strings.Builder
			for {
				if i >= len(input) {
					return nil, errAt(start, "unterminated string literal")
				}
				if input[i] == '\'' {
					if i+1 < len(input) && input[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(input[i])
				i++
			}
			out = append(out, token{kind: tokString, text: b.String(), pos: start})
		case c == '"' || c == '`':
			quote := c
			start := i
			i++
			var b strings.Builder
			for {
				if i >= len(input) {
					return nil, errAt(start, "unterminated quoted identifier")
				}
				if input[i] == quote {
					if i+1 < len(input) && input[i+1] == quote {
						b.WriteByte(quote)
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(input[i])
				i++
			}
			out = append(out, token{kind: tokIdent, text: b.String(), pos: start, quoted: true})
		case isDigit(c) || (c == '.' && i+1 < len(input) && isDigit(input[i+1])):
			start := i
			seenDot := false
			for i < len(input) {
				if isDigit(input[i]) {
					i++
					continue
				}
				if input[i] == '.' && !seenDot {
					seenDot = true
					i++
					continue
				}
				break
			}
			if i < len(input) && (input[i] == 'e' || input[i] == 'E') {
				j := i + 1
				if j < len(input) && (input[j] == '+' || input[j] == '-') {
					j++
				}
				if j < len(input) && isDigit(input[j]) {
					i = j
					for i < len(input) && isDigit(input[i]) {
						i++
					}
				}
			}
			out = append(out, token{kind: tokNumber, text: input[start:i], pos: start})
		case isIdentStart(c):
			start := i
			i++
			for i < len(input) && isIdentPart(input[i]) {
				i++
			}
			out = append(out, token{kind: tokIdent, text: input[start:i], pos: start})
		default:
			if i+1 < len(input) {
				two := input[i : i+2]
				switch two {
				case "<=", ">=", "!=", "<>", "::", "||":
					out = append(out, token{kind: tokOp, text: two, pos: i})
					i += 2
					continue
				}
			}
			switch c {
			case '=', '<', '>', '+', '-', '*', '/', '%', '(', ')', ',', '.', ';':
				out = append(out, token{kind: tokOp, text: string(c), pos: i})
				i++
			default:
				r, _ := utf8.DecodeRuneInString(input[i:])
				return nil, errAt(i, fmt.Sprintf("unexpected character %q", r))
			}
		}
	}
	out = append(out, token{kind: tokEOF, pos: len(input)})
	return out, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || c == '@' || c == '$' || unicode.IsLetter(rune(c))
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || isDigit(c) || c == '#'
}
