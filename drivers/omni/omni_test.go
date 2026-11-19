package omni

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchomni"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchomni/types"
	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

type fakeAPI struct {
	pages      []*cloudwatchomni.GetTelemetryQueryResultsOutput
	index      int
	query      string
	nextTokens []string
}

func (f *fakeAPI) StartTelemetryQuerySession(context.Context, *cloudwatchomni.StartTelemetryQuerySessionInput, ...func(*cloudwatchomni.Options)) (*cloudwatchomni.StartTelemetryQuerySessionOutput, error) {
	return &cloudwatchomni.StartTelemetryQuerySessionOutput{SessionId: aws.String("session-1")}, nil
}

func (f *fakeAPI) StartTelemetryQuery(_ context.Context, in *cloudwatchomni.StartTelemetryQueryInput, _ ...func(*cloudwatchomni.Options)) (*cloudwatchomni.StartTelemetryQueryOutput, error) {
	f.query = aws.ToString(in.QueryString)
	return &cloudwatchomni.StartTelemetryQueryOutput{QueryId: aws.String("query-1"), SessionId: in.SessionId}, nil
}

func (f *fakeAPI) GetTelemetryQueryResults(_ context.Context, in *cloudwatchomni.GetTelemetryQueryResultsInput, _ ...func(*cloudwatchomni.Options)) (*cloudwatchomni.GetTelemetryQueryResultsOutput, error) {
	f.nextTokens = append(f.nextTokens, aws.ToString(in.NextToken))
	if f.index >= len(f.pages) {
		return f.pages[len(f.pages)-1], nil
	}
	out := f.pages[f.index]
	f.index++
	return out, nil
}

func TestQueryDecodesStringRows(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchomni.GetTelemetryQueryResultsOutput{{
		Status: types.QueryStatusComplete,
		Rows: []map[string]string{{
			"k0": "checkout",
			"p0": "42",
			"p1": "5.000000000",
			"p2": "2026-11-01T00:00:00Z",
			"p3": "true",
		}},
	}}}
	driver, err := New(context.Background(), Config{EndpointID: "omni-1", API: api})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    `SELECT "service" AS "k0", COUNT(*) AS "p0" FROM "logs.default" WHERE "@timestamp" >= TIMESTAMP '2026-11-01 00:00:00'`,
		Schema: []string{"k0", "p0", "p1", "p2", "p3"},
		Types: []sqlag.Type{
			sqlag.TypeString, sqlag.TypeInt64, sqlag.TypeDecimal,
			sqlag.TypeTimestamp, sqlag.TypeBool,
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if api.query == "" {
		t.Fatal("query string was not sent")
	}
	row := stream.Rows[0]
	if row[0].AsString() != "checkout" {
		t.Fatalf("k0 = %q", row[0].AsString())
	}
	if row[1].Int() != 42 {
		t.Fatalf("p0 = %d", row[1].Int())
	}
	if row[2].AsString() != "5.000000000" {
		t.Fatalf("p1 = %s", row[2].AsString())
	}
	if !row[3].T.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("p2 = %v", row[3].T)
	}
	if !row[4].B {
		t.Fatalf("p3 = %#v", row[4])
	}
}

func TestQueryPaginates(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchomni.GetTelemetryQueryResultsOutput{
		{Status: types.QueryStatusComplete, Rows: []map[string]string{{"k0": "a", "p0": "1"}}, NextToken: aws.String("t1")},
		{Status: types.QueryStatusComplete, Rows: []map[string]string{{"k0": "b", "p0": "2"}}},
	}}
	driver, err := New(context.Background(), Config{EndpointID: "omni-1", API: api})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT 1",
		Schema: []string{"k0", "p0"},
		Types:  []sqlag.Type{sqlag.TypeString, sqlag.TypeInt64},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(stream.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(stream.Rows))
	}
	if len(api.nextTokens) != 2 || api.nextTokens[0] != "" || api.nextTokens[1] != "t1" {
		t.Fatalf("next tokens = %#v", api.nextTokens)
	}
}

func TestQueryPollsWhileRunning(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchomni.GetTelemetryQueryResultsOutput{
		{Status: types.QueryStatusRunning},
		{Status: types.QueryStatusComplete, Rows: []map[string]string{{"p0": "1"}}},
	}}
	driver, err := New(context.Background(), Config{EndpointID: "omni-1", API: api})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT 1",
		Schema: []string{"p0"},
		Types:  []sqlag.Type{sqlag.TypeInt64},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(stream.Rows) != 1 || stream.Rows[0][0].Int() != 1 {
		t.Fatalf("rows = %#v", stream.Rows)
	}
}

func TestQueryFailedStatus(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchomni.GetTelemetryQueryResultsOutput{{Status: types.QueryStatusFailed}}}
	driver, err := New(context.Background(), Config{EndpointID: "omni-1", API: api})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := driver.Query(context.Background(), sqlag.PartialRequest{SQL: "SELECT 1"}); err == nil {
		t.Fatal("expected failed status error")
	}
}

func TestDecodeTextNullAndString(t *testing.T) {
	if v, err := decodeText("", sqlag.TypeInt64); err != nil || !v.IsNull() {
		t.Fatalf("empty int = %#v, %v", v, err)
	}
	if v, err := decodeText("null", sqlag.TypeDecimal); err != nil || !v.IsNull() {
		t.Fatalf("null decimal = %#v, %v", v, err)
	}
	if v, err := decodeText("", sqlag.TypeString); err != nil || v.Kind != sqlag.KindString {
		t.Fatalf("empty string = %#v, %v", v, err)
	}
}
