package clickhouse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

func TestQueryDecodesTypedJSON(t *testing.T) {
	var gotQuery, gotReadonly, gotMaxRows string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		gotReadonly = r.URL.Query().Get("readonly")
		gotMaxRows = r.URL.Query().Get("max_result_rows")
		if r.Header.Get("X-ClickHouse-User") != "tester" {
			t.Errorf("missing user header")
		}
		_, _ = w.Write([]byte(`{
			"meta": [
				{"name":"k0","type":"String"},
				{"name":"p0","type":"Int64"},
				{"name":"p1","type":"Nullable(Float64)"},
				{"name":"p2","type":"Decimal(38, 9)"},
				{"name":"p3","type":"DateTime64(6, 'UTC')"},
				{"name":"p4","type":"UInt64"},
				{"name":"p5","type":"Bool"}
			],
			"data": [{
				"k0":"a",
				"p0":9007199254740993,
				"p1":null,
				"p2":"5.000000000",
				"p3":"2026-11-01 00:00:00.000000",
				"p4":18446744073709551615,
				"p5":true
			}],
			"rows": 1
		}`))
	}))
	defer server.Close()

	d, err := New(Config{
		EndpointID: "ch-1",
		URL:        server.URL,
		Database:   "strata",
		User:       "tester",
		MaxRows:    1000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stream, err := d.Query(context.Background(), sqlag.PartialRequest{
		SQL:    `SELECT "k0", COUNT(*) AS "p0" FROM "strata"."logs" GROUP BY "k0"`,
		Schema: []string{"k0", "p0", "p1", "p2", "p3", "p4", "p5"},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(gotQuery, "FORMAT JSON") {
		t.Fatalf("query missing FORMAT JSON: %s", gotQuery)
	}
	if gotReadonly != "1" {
		t.Fatalf("readonly = %q", gotReadonly)
	}
	if gotMaxRows != "1000" {
		t.Fatalf("max_result_rows = %q", gotMaxRows)
	}
	if len(stream.Rows) != 1 {
		t.Fatalf("rows = %d", len(stream.Rows))
	}
	row := stream.Rows[0]
	if row[0].AsString() != "a" {
		t.Fatalf("k0 = %q", row[0].AsString())
	}
	if row[1].Int() != 9007199254740993 {
		t.Fatalf("int64 precision lost: %d", row[1].Int())
	}
	if !row[2].IsNull() {
		t.Fatalf("p1 = %#v, want NULL", row[2])
	}
	if row[3].AsString() != "5.000000000" {
		t.Fatalf("decimal = %s", row[3].AsString())
	}
	if row[3].Kind != sqlag.KindDecimal {
		t.Fatalf("decimal kind = %s", row[3].Kind)
	}
	if row[4].T.UTC().Format("2006-01-02 15:04:05") != "2026-11-01 00:00:00" {
		t.Fatalf("timestamp = %v", row[4].T)
	}
	if row[5].U != 18446744073709551615 {
		t.Fatalf("uint64 = %d", row[5].U)
	}
	if row[6].Kind != sqlag.KindBool || !row[6].B {
		t.Fatalf("bool = %#v", row[6])
	}
}

func TestQueryMissingColumnFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"meta":[{"name":"k0","type":"String"}],"data":[{"k0":"a"}],"rows":1}`))
	}))
	defer server.Close()
	d, err := New(Config{EndpointID: "ch", URL: server.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = d.Query(context.Background(), sqlag.PartialRequest{SQL: "SELECT 1", Schema: []string{"k0", "p0"}})
	if err == nil || !strings.Contains(err.Error(), "missing column") {
		t.Fatalf("error = %v", err)
	}
}

func TestQueryHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Code: 62. DB::Exception: syntax error", http.StatusBadRequest)
	}))
	defer server.Close()
	d, err := New(Config{EndpointID: "ch", URL: server.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = d.Query(context.Background(), sqlag.PartialRequest{SQL: "SELECT 1"})
	if err == nil || !strings.Contains(err.Error(), "http 400") {
		t.Fatalf("error = %v", err)
	}
}

func TestBaseType(t *testing.T) {
	cases := map[string]string{
		"Nullable(UInt64)":                            "UInt64",
		"LowCardinality(Nullable(String))":            "String",
		"SimpleAggregateFunction(sum, Decimal128(9))": "Decimal128(9)",
	}
	for in, want := range cases {
		got, _ := baseType(in)
		if got != want {
			t.Errorf("baseType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecodeValueRejectsUnsupported(t *testing.T) {
	var raw json.RawMessage = []byte(`[1,2]`)
	if _, err := decodeValue(raw, "Array(UInt8)"); err == nil {
		t.Fatal("expected unsupported type error")
	}
}
