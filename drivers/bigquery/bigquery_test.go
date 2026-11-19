package bigquery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

func newTestDriver(t *testing.T, handler http.Handler) *Driver {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	driver, err := New(context.Background(), Config{
		EndpointID:  "bq-1",
		ProjectID:   "proj",
		Endpoint:    server.URL,
		DisableAuth: true,
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return driver
}

func TestQueryDecodesBigQueryTypes(t *testing.T) {
	driver := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"kind":"bigquery#queryResponse",
			"jobComplete":true,
			"jobReference":{"jobId":"job-1","projectId":"proj"},
			"schema":{"fields":[
				{"name":"k0","type":"STRING"},
				{"name":"p0","type":"INTEGER"},
				{"name":"p1","type":"NUMERIC"},
				{"name":"p2","type":"TIMESTAMP"},
				{"name":"p3","type":"BOOLEAN"},
				{"name":"p4","type":"FLOAT"}
			]},
			"rows":[{"f":[
				{"v":"api"},{"v":"42"},{"v":"5.000000000"},
				{"v":"1792519316.123456"},{"v":"true"},{"v":"1.5"}
			]}],
			"totalRows":"1"
		}`))
	}))
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT service, count(*) FROM logs GROUP BY service",
		Schema: []string{"k0", "p0", "p1", "p2", "p3", "p4"},
		Types: []sqlag.Type{
			sqlag.TypeString, sqlag.TypeInt64, sqlag.TypeDecimal,
			sqlag.TypeTimestamp, sqlag.TypeBool, sqlag.TypeFloat64,
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	row := stream.Rows[0]
	if row[0].AsString() != "api" {
		t.Fatalf("k0 = %q", row[0].AsString())
	}
	if row[1].Int() != 42 {
		t.Fatalf("p0 = %d", row[1].Int())
	}
	if row[2].AsString() != "5.000000000" {
		t.Fatalf("p1 = %s", row[2].AsString())
	}
	if row[3].T.Unix() != 1792519316 {
		t.Fatalf("p2 = %v", row[3].T)
	}
	if !row[4].B {
		t.Fatalf("p3 = %#v", row[4])
	}
	if row[5].Float64() != 1.5 {
		t.Fatalf("p4 = %v", row[5].Float64())
	}
}

func TestQueryPaginates(t *testing.T) {
	var getCalls atomic.Int32
	driver := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{
				"jobComplete":true,
				"jobReference":{"jobId":"job-1","projectId":"proj"},
				"schema":{"fields":[{"name":"k0","type":"STRING"},{"name":"p0","type":"INTEGER"}]},
				"rows":[{"f":[{"v":"a"},{"v":"1"}]}],
				"pageToken":"page-2",
				"totalRows":"2"
			}`))
			return
		}
		getCalls.Add(1)
		_, _ = w.Write([]byte(`{
			"jobComplete":true,
			"schema":{"fields":[{"name":"k0","type":"STRING"},{"name":"p0","type":"INTEGER"}]},
			"rows":[{"f":[{"v":"b"},{"v":"2"}]}]
		}`))
	}))
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT 1",
		Schema: []string{"k0", "p0"},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(stream.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(stream.Rows))
	}
	if getCalls.Load() != 1 {
		t.Fatalf("page calls = %d, want 1", getCalls.Load())
	}
}

func TestQueryWaitsForJob(t *testing.T) {
	var polls atomic.Int32
	driver := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"jobComplete":false,"jobReference":{"jobId":"job-1","projectId":"proj"}}`))
			return
		}
		if polls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"jobComplete":false}`))
			return
		}
		_, _ = w.Write([]byte(`{
			"jobComplete":true,
			"schema":{"fields":[{"name":"p0","type":"INTEGER"}]},
			"rows":[{"f":[{"v":"7"}]}]
		}`))
	}))
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT 1",
		Schema: []string{"p0"},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(stream.Rows) != 1 || stream.Rows[0][0].Int() != 7 {
		t.Fatalf("rows = %#v", stream.Rows)
	}
}

func TestQueryErrorsAndMissingColumns(t *testing.T) {
	driver := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors":[{"message":"bad query"}],"jobComplete":false}`))
	}))
	if _, err := driver.Query(context.Background(), sqlag.PartialRequest{SQL: "SELECT 1"}); err == nil || !strings.Contains(err.Error(), "bad query") {
		t.Fatalf("error = %v", err)
	}

	driver = newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"jobComplete":true,
			"jobReference":{"jobId":"job-1"},
			"schema":{"fields":[{"name":"p0","type":"INTEGER"}]},
			"rows":[{"f":[{"v":"1"}]}]
		}`))
	}))
	if _, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL: "SELECT 1", Schema: []string{"p0", "p1"},
	}); err == nil || !strings.Contains(err.Error(), "missing column") {
		t.Fatalf("error = %v", err)
	}
}

func TestDecodeCell(t *testing.T) {
	v, err := decodeCell("2026-11-01", "DATE")
	if err != nil || !v.T.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("date = %#v, %v", v, err)
	}
	if _, err := decodeCell([]any{"x"}, "REPEATED"); err == nil {
		t.Fatal("expected repeated-field error")
	}
}
