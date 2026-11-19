package value_test

import (
	"math"
	"testing"

	"github.com/radryc/sqlagregator/internal/value"
)

func TestAddOverflowPromotesToDecimal(t *testing.T) {
	sum, ok := value.Add(value.Int64(math.MaxInt64), value.Int64(1))
	if !ok {
		t.Fatal("add failed")
	}
	if sum.Kind != value.KDecimal {
		t.Fatalf("kind = %s, want decimal", sum.Kind)
	}
	if got := sum.AsString(); got != "9223372036854775808" {
		t.Fatalf("sum = %s", got)
	}
}

func TestNullIsAdditiveIdentity(t *testing.T) {
	sum, ok := value.Add(value.NullValue(), value.Int64(7))
	if !ok || sum.Kind != value.KInt || sum.I != 7 {
		t.Fatalf("sum = %#v ok=%v", sum, ok)
	}
}

func TestDecimalDivision(t *testing.T) {
	// 15 / 3 = 5.000000000
	d := value.NewDecimalFromInt64(15).DivInt(3, 9)
	if got := d.String(); got != "5.000000000" {
		t.Fatalf("division = %s", got)
	}
}

func TestCompareAndKey(t *testing.T) {
	if value.Compare(value.Int64(1), value.Float64(1.5)) >= 0 {
		t.Fatal("1 should be less than 1.5")
	}
	if value.Compare(value.NullValue(), value.Int64(-1)) >= 0 {
		t.Fatal("NULL must sort before values")
	}
	if value.Int64(1).Key() == value.Uint64(1).Key() {
		t.Fatal("int64 and uint64 keys must differ")
	}
	if value.Str("a").Key() != value.Str("a").Key() {
		t.Fatal("string keys must be stable")
	}
}

func TestParseAny(t *testing.T) {
	cases := []any{nil, int64(1), 2.5, "x", true}
	for _, in := range cases {
		if _, err := value.ParseAny(in); err != nil {
			t.Fatalf("ParseAny(%T): %v", in, err)
		}
	}
}
