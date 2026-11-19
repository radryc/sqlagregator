// Package value defines the canonical typed values that cross the driver
// boundary. Every backend result is normalised into these types before the
// merge engine sees it.
package value

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Kind is the canonical value kind.
type Kind uint8

const (
	KNull Kind = iota
	KInt
	KUint
	KFloat
	KDecimal
	KString
	KBool
	KTimestamp
)

func (k Kind) String() string {
	switch k {
	case KNull:
		return "null"
	case KInt:
		return "int64"
	case KUint:
		return "uint64"
	case KFloat:
		return "float64"
	case KDecimal:
		return "decimal"
	case KString:
		return "string"
	case KBool:
		return "bool"
	case KTimestamp:
		return "timestamp"
	default:
		return "unknown"
	}
}

// Decimal is an exact base-10 number: Unscaled * 10^-Scale.
type Decimal struct {
	Unscaled *big.Int
	Scale    int32
}

// Value is the tagged union used on the hot path. It is deliberately a value
// type: no boxing, no interface calls.
type Value struct {
	Kind Kind
	I    int64
	U    uint64
	F    float64
	S    string
	B    bool
	T    time.Time
	D    *Decimal
}

// NullValue returns SQL NULL.
func NullValue() Value { return Value{Kind: KNull} }

func Int64(v int64) Value   { return Value{Kind: KInt, I: v} }
func Uint64(v uint64) Value { return Value{Kind: KUint, U: v} }
func Float64(v float64) Value {
	return Value{Kind: KFloat, F: v}
}
func Str(v string) Value        { return Value{Kind: KString, S: v} }
func Boolean(v bool) Value      { return Value{Kind: KBool, B: v} }
func Dec(d *Decimal) Value      { return Value{Kind: KDecimal, D: d} }
func Time(v time.Time) Value    { return Value{Kind: KTimestamp, T: v.UTC().Truncate(time.Microsecond)} }
func (v Value) IsNull() bool    { return v.Kind == KNull }
func (v Value) IsNumeric() bool { return isNumeric(v.Kind) }

// NewDecimalFromInt64 returns a scale-0 decimal.
func NewDecimalFromInt64(i int64) *Decimal {
	return &Decimal{Unscaled: big.NewInt(i), Scale: 0}
}

// NewDecimalFromString parses a base-10 decimal string.
func NewDecimalFromString(s string) (*Decimal, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		f, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
		if err != nil {
			return nil, false
		}
		if neg {
			f.Neg(f)
		}
		return NewDecimalFromString(f.Text('f', 9))
	}
	scale := int32(0)
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		scale = int32(len(s) - dot - 1)
		s = s[:dot] + s[dot+1:]
	}
	if s == "" {
		return nil, false
	}
	unscaled, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, false
	}
	if neg {
		unscaled.Neg(unscaled)
	}
	return &Decimal{Unscaled: unscaled, Scale: scale}, true
}

// Add returns d+o with aligned scales.
func (d *Decimal) Add(o *Decimal) *Decimal {
	if d == nil {
		return o.clone()
	}
	if o == nil {
		return d.clone()
	}
	scale := d.Scale
	if o.Scale > scale {
		scale = o.Scale
	}
	a := d.rescaleUp(scale)
	b := o.rescaleUp(scale)
	return &Decimal{Unscaled: new(big.Int).Add(a, b), Scale: scale}
}

// Cmp compares d and o.
func (d *Decimal) Cmp(o *Decimal) int {
	if d == nil || o == nil {
		switch {
		case d == nil && o == nil:
			return 0
		case d == nil:
			return -1
		default:
			return 1
		}
	}
	scale := d.Scale
	if o.Scale > scale {
		scale = o.Scale
	}
	return d.rescaleUp(scale).Cmp(o.rescaleUp(scale))
}

// DivInt divides d by n, producing resultScale decimal places.
func (d *Decimal) DivInt(n int64, resultScale int32) *Decimal {
	if d == nil || n == 0 {
		return nil
	}
	unscaled := new(big.Int).Set(d.Unscaled)
	if shift := resultScale - d.Scale; shift > 0 {
		unscaled.Mul(unscaled, pow10(shift))
	} else if shift < 0 {
		unscaled.Quo(unscaled, pow10(-shift))
	}
	unscaled.Quo(unscaled, big.NewInt(n))
	return &Decimal{Unscaled: unscaled, Scale: resultScale}
}

// Float64 converts d to a float64.
func (d *Decimal) Float64() float64 {
	if d == nil || d.Unscaled == nil {
		return 0
	}
	f := new(big.Float).SetInt(d.Unscaled)
	if d.Scale > 0 {
		f.Quo(f, new(big.Float).SetInt(pow10(d.Scale)))
	}
	out, _ := f.Float64()
	return out
}

// String renders d as a plain decimal string.
func (d *Decimal) String() string {
	if d == nil || d.Unscaled == nil {
		return "0"
	}
	s := d.Unscaled.String()
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	scale := int(d.Scale)
	if scale > 0 {
		if len(s) <= scale {
			s = strings.Repeat("0", scale-len(s)+1) + s
		}
		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}
	if neg {
		s = "-" + s
	}
	return s
}

func (d *Decimal) clone() *Decimal {
	if d == nil {
		return nil
	}
	return &Decimal{Unscaled: new(big.Int).Set(d.Unscaled), Scale: d.Scale}
}

// rescaleUp multiplies to a larger scale. It is only valid for scale >= d.Scale.
func (d *Decimal) rescaleUp(scale int32) *big.Int {
	out := new(big.Int).Set(d.Unscaled)
	if scale > d.Scale {
		out.Mul(out, pow10(scale-d.Scale))
	}
	return out
}

func pow10(n int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
}

// Key returns a collision-free grouping key. The result is used directly as a
// map key.
func (v Value) Key() string {
	var b strings.Builder
	b.WriteByte(byte(v.Kind))
	switch v.Kind {
	case KNull:
	case KInt:
		writeUint64(&b, uint64(v.I))
	case KUint:
		writeUint64(&b, v.U)
	case KFloat:
		writeUint64(&b, math.Float64bits(v.F))
	case KDecimal:
		if v.D == nil || v.D.Unscaled == nil {
			b.WriteByte(0)
			break
		}
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(v.D.Scale))
		b.Write(buf[:])
		b.Write(v.D.Unscaled.Bytes())
	case KString:
		writeLenBytes(&b, []byte(v.S))
	case KBool:
		if v.B {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
	case KTimestamp:
		writeUint64(&b, uint64(v.T.UnixMicro()))
	}
	return b.String()
}

func writeUint64(b *strings.Builder, u uint64) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], u)
	b.Write(buf[:])
}

func writeLenBytes(b *strings.Builder, p []byte) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(p)))
	b.Write(buf[:])
	b.Write(p)
}

// Compare imposes a deterministic total order. NULL sorts before any value;
// NULLS FIRST/LAST is applied by the caller.
func Compare(a, b Value) int {
	if a.Kind == KNull || b.Kind == KNull {
		switch {
		case a.Kind == KNull && b.Kind == KNull:
			return 0
		case a.Kind == KNull:
			return -1
		default:
			return 1
		}
	}
	if isNumeric(a.Kind) && isNumeric(b.Kind) {
		return compareNumeric(a, b)
	}
	if a.Kind != b.Kind {
		if a.Kind < b.Kind {
			return -1
		}
		return 1
	}
	switch a.Kind {
	case KString:
		return strings.Compare(a.S, b.S)
	case KBool:
		switch {
		case a.B == b.B:
			return 0
		case !a.B:
			return -1
		default:
			return 1
		}
	case KTimestamp:
		switch {
		case a.T.Before(b.T):
			return -1
		case a.T.After(b.T):
			return 1
		default:
			return 0
		}
	}
	return 0
}

func isNumeric(k Kind) bool {
	return k == KInt || k == KUint || k == KFloat || k == KDecimal
}

func compareNumeric(a, b Value) int {
	if a.Kind == KFloat || b.Kind == KFloat {
		af, bf := a.Float64(), b.Float64()
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	if a.Kind == KDecimal || b.Kind == KDecimal {
		return a.asDecimal().Cmp(b.asDecimal())
	}
	if a.Kind == KInt && a.I < 0 {
		return -1
	}
	if b.Kind == KInt && b.I < 0 {
		return 1
	}
	au, bu := a.Uint(), b.Uint()
	switch {
	case au < bu:
		return -1
	case au > bu:
		return 1
	default:
		return 0
	}
}

// Add adds two numeric values. NULL acts as the additive identity for SUM.
func Add(a, b Value) (Value, bool) {
	if a.Kind == KNull {
		return b, true
	}
	if b.Kind == KNull {
		return a, true
	}
	if !isNumeric(a.Kind) || !isNumeric(b.Kind) {
		return Value{}, false
	}
	if a.Kind == KFloat || b.Kind == KFloat {
		return Float64(a.Float64() + b.Float64()), true
	}
	if a.Kind == KDecimal || b.Kind == KDecimal {
		return Dec(a.asDecimal().Add(b.asDecimal())), true
	}
	if a.Kind == KUint || b.Kind == KUint {
		if (a.Kind == KInt && a.I < 0) || (b.Kind == KInt && b.I < 0) {
			return Dec(a.asDecimal().Add(b.asDecimal())), true
		}
		au, bu := a.Uint(), b.Uint()
		sum := au + bu
		if sum < au {
			return Dec(bigIntDecimal(new(big.Int).Add(new(big.Int).SetUint64(au), new(big.Int).SetUint64(bu)))), true
		}
		return Uint64(sum), true
	}
	sum := a.I + b.I
	if (b.I > 0 && sum < a.I) || (b.I < 0 && sum > a.I) {
		return Dec(NewDecimalFromInt64(a.I).Add(NewDecimalFromInt64(b.I))), true
	}
	return Int64(sum), true
}

// asDecimal converts a numeric value to an exact decimal.
func bigIntDecimal(b *big.Int) *Decimal {
	return &Decimal{Unscaled: b, Scale: 0}
}

func (v Value) asDecimal() *Decimal {
	switch v.Kind {
	case KInt:
		return NewDecimalFromInt64(v.I)
	case KUint:
		return (&Decimal{Unscaled: new(big.Int).SetUint64(v.U)})
	case KDecimal:
		if v.D == nil {
			return NewDecimalFromInt64(0)
		}
		return v.D
	case KFloat:
		d, ok := NewDecimalFromString(strconvFormatFloat(v.F))
		if !ok {
			return NewDecimalFromInt64(0)
		}
		return d
	default:
		return NewDecimalFromInt64(0)
	}
}

// AsDecimal exports asDecimal for the merge engine.
func (v Value) AsDecimal() *Decimal { return v.asDecimal() }

func strconvFormatFloat(f float64) string {
	return fmt.Sprintf("%.17g", f)
}

// Float64 converts v to float64.
func (v Value) Float64() float64 {
	switch v.Kind {
	case KInt:
		return float64(v.I)
	case KUint:
		return float64(v.U)
	case KFloat:
		return v.F
	case KDecimal:
		return v.D.Float64()
	default:
		return 0
	}
}

// Uint converts v to uint64, clamping negatives to zero.
func (v Value) Uint() uint64 {
	switch v.Kind {
	case KInt:
		if v.I < 0 {
			return 0
		}
		return uint64(v.I)
	case KUint:
		return v.U
	case KFloat:
		if v.F < 0 {
			return 0
		}
		return uint64(v.F)
	case KDecimal:
		if v.D == nil || v.D.Unscaled.Sign() < 0 {
			return 0
		}
		return v.D.Unscaled.Uint64()
	default:
		return 0
	}
}

// Int converts v to int64.
func (v Value) Int() int64 {
	switch v.Kind {
	case KInt:
		return v.I
	case KUint:
		if v.U > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(v.U)
	case KFloat:
		return int64(v.F)
	case KDecimal:
		if v.D == nil {
			return 0
		}
		return int64(v.D.Float64())
	default:
		return 0
	}
}

// ToAny converts to a JSON-friendly value. Decimals stay exact via json.Number.
func (v Value) ToAny() any {
	switch v.Kind {
	case KNull:
		return nil
	case KInt:
		return v.I
	case KUint:
		return v.U
	case KFloat:
		return v.F
	case KDecimal:
		return json.Number(v.D.String())
	case KString:
		return v.S
	case KBool:
		return v.B
	case KTimestamp:
		return v.T
	default:
		return nil
	}
}

// ParseAny normalises a driver value into the canonical representation.
func ParseAny(x any) (Value, error) {
	switch t := x.(type) {
	case nil:
		return NullValue(), nil
	case Value:
		return t, nil
	case int:
		return Int64(int64(t)), nil
	case int8:
		return Int64(int64(t)), nil
	case int16:
		return Int64(int64(t)), nil
	case int32:
		return Int64(int64(t)), nil
	case int64:
		return Int64(t), nil
	case uint:
		return Uint64(uint64(t)), nil
	case uint8:
		return Uint64(uint64(t)), nil
	case uint16:
		return Uint64(uint64(t)), nil
	case uint32:
		return Uint64(uint64(t)), nil
	case uint64:
		return Uint64(t), nil
	case float32:
		return Float64(float64(t)), nil
	case float64:
		return Float64(t), nil
	case bool:
		return Boolean(t), nil
	case string:
		return Str(t), nil
	case []byte:
		return Str(string(t)), nil
	case time.Time:
		return Time(t), nil
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return Int64(i), nil
		}
		if d, ok := NewDecimalFromString(t.String()); ok {
			return Dec(d), nil
		}
		return Value{}, fmt.Errorf("cannot parse json number %q", t.String())
	default:
		return Value{}, fmt.Errorf("unsupported value type %T", x)
	}
}

// AsString renders a value as text using SQL-ish rules.
func (v Value) AsString() string {
	switch v.Kind {
	case KNull:
		return ""
	case KInt:
		return strconv.FormatInt(v.I, 10)
	case KUint:
		return strconv.FormatUint(v.U, 10)
	case KFloat:
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case KDecimal:
		return v.D.String()
	case KString:
		return v.S
	case KBool:
		if v.B {
			return "true"
		}
		return "false"
	case KTimestamp:
		return v.T.UTC().Format(time.RFC3339Nano)
	default:
		return ""
	}
}

// Mul returns d*o.
func (d *Decimal) Mul(o *Decimal) *Decimal {
	if d == nil || o == nil {
		return nil
	}
	return &Decimal{Unscaled: new(big.Int).Mul(d.Unscaled, o.Unscaled), Scale: d.Scale + o.Scale}
}

// Neg returns -d.
func (d *Decimal) Neg() *Decimal {
	if d == nil {
		return nil
	}
	return &Decimal{Unscaled: new(big.Int).Neg(d.Unscaled), Scale: d.Scale}
}
