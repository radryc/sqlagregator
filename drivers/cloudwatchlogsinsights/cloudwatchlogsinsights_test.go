package cloudwatchlogsinsights

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

// fakeAPI returns a scripted sequence of GetQueryResults responses.
type fakeAPI struct {
	started   *cloudwatchlogs.StartQueryInput
	pages     []*cloudwatchlogs.GetQueryResultsOutput
	calls     int
	startErr  error
	resultErr error
}

func (f *fakeAPI) StartQuery(_ context.Context, in *cloudwatchlogs.StartQueryInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
	f.started = in
	if f.startErr != nil {
		return nil, f.startErr
	}
	return &cloudwatchlogs.StartQueryOutput{QueryId: aws.String("q-1")}, nil
}

func (f *fakeAPI) GetQueryResults(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	if f.resultErr != nil {
		return nil, f.resultErr
	}
	if f.calls >= len(f.pages) {
		return f.pages[len(f.pages)-1], nil
	}
	page := f.pages[f.calls]
	f.calls++
	return page, nil
}

func field(name, value string) types.ResultField {
	return types.ResultField{Field: aws.String(name), Value: aws.String(value)}
}

func TestQueryStartsSQLAndDecodesRows(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchlogs.GetQueryResultsOutput{{
		Status: types.QueryStatusComplete,
		Results: [][]types.ResultField{
			{field("k0", "api"), field("p0", "3"), field("p1", "12.5")},
			{field("k0", "worker"), field("p0", "2"), field("p1", "4.25")},
		},
	}}}
	driver, err := New(context.Background(), Config{EndpointID: "insights", API: api})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT x FROM `logGroups(logGroupIdentifier: ['/strata/observability'])`",
		From:   from,
		To:     to,
		Schema: []string{"k0", "p0", "p1"},
		Types:  []sqlag.Type{sqlag.TypeString, sqlag.TypeInt64, sqlag.TypeFloat64},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := aws.ToString(api.started.QueryString); got == "" {
		t.Fatal("empty query string")
	}
	if api.started.QueryLanguage != types.QueryLanguageSql {
		t.Fatalf("language = %v", api.started.QueryLanguage)
	}
	if *api.started.StartTime != from.Unix() || *api.started.EndTime != to.Unix() {
		t.Fatalf("range = %d..%d", *api.started.StartTime, *api.started.EndTime)
	}
	if len(api.started.LogGroupNames) != 0 || len(api.started.LogGroupIdentifiers) != 0 {
		t.Fatal("SQL queries must name log groups in the statement, not in the request")
	}
	if stream.EndpointID != "insights" || len(stream.Rows) != 2 {
		t.Fatalf("stream = %#v", stream)
	}
	if stream.Rows[0][0].S != "api" || stream.Rows[0][1].I != 3 {
		t.Fatalf("row0 = %#v", stream.Rows[0])
	}
	if stream.Rows[1][2].F != 4.25 {
		t.Fatalf("row1 = %#v", stream.Rows[1])
	}
}

func TestQueryPollsUntilComplete(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchlogs.GetQueryResultsOutput{
		{Status: types.QueryStatusRunning},
		{Status: types.QueryStatusScheduled},
		{
			Status:  types.QueryStatusComplete,
			Results: [][]types.ResultField{{field("p0", "7")}},
		},
	}}
	driver, err := New(context.Background(), Config{EndpointID: "insights", API: api})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	stream, err := driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT count(*) AS p0 FROM x",
		From:   time.Now().Add(-time.Hour),
		To:     time.Now(),
		Schema: []string{"p0"},
		Types:  []sqlag.Type{sqlag.TypeInt64},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(stream.Rows) != 1 || stream.Rows[0][0].I != 7 {
		t.Fatalf("rows = %#v", stream.Rows)
	}
}

func TestQueryFailsClosed(t *testing.T) {
	from := time.Now().Add(-time.Hour)
	to := time.Now()
	cases := []struct {
		name string
		api  *fakeAPI
		req  sqlag.PartialRequest
	}{
		{
			name: "failed status",
			api:  &fakeAPI{pages: []*cloudwatchlogs.GetQueryResultsOutput{{Status: types.QueryStatusFailed}}},
			req:  sqlag.PartialRequest{SQL: "SELECT 1", From: from, To: to},
		},
		{
			name: "missing time range",
			api:  &fakeAPI{},
			req:  sqlag.PartialRequest{SQL: "SELECT 1"},
		},
		{
			name: "empty statement",
			api:  &fakeAPI{},
			req:  sqlag.PartialRequest{From: from, To: to},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			driver, err := New(context.Background(), Config{EndpointID: "insights", API: tc.api})
			if err != nil {
				t.Fatalf("new: %v", err)
			}
			if _, err := driver.Query(context.Background(), tc.req); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestQueryRejectsMissingColumn(t *testing.T) {
	api := &fakeAPI{pages: []*cloudwatchlogs.GetQueryResultsOutput{{
		Status:  types.QueryStatusComplete,
		Results: [][]types.ResultField{{field("p0", "1")}},
	}}}
	driver, err := New(context.Background(), Config{EndpointID: "insights", API: api})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT p0, p1 FROM x",
		From:   time.Now().Add(-time.Hour),
		To:     time.Now(),
		Schema: []string{"p0", "p1"},
		Types:  []sqlag.Type{sqlag.TypeInt64, sqlag.TypeInt64},
	})
	if err == nil {
		t.Fatal("expected missing-column error")
	}
}

func TestQueryExceedsMaxRows(t *testing.T) {
	rows := make([][]types.ResultField, 0, 3)
	for i := 0; i < 3; i++ {
		rows = append(rows, []types.ResultField{field("p0", "1")})
	}
	api := &fakeAPI{pages: []*cloudwatchlogs.GetQueryResultsOutput{{
		Status:  types.QueryStatusComplete,
		Results: rows,
	}}}
	driver, err := New(context.Background(), Config{EndpointID: "insights", API: api, MaxRows: 2})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:    "SELECT p0 FROM x",
		From:   time.Now().Add(-time.Hour),
		To:     time.Now(),
		Schema: []string{"p0"},
		Types:  []sqlag.Type{sqlag.TypeInt64},
	})
	if err == nil {
		t.Fatal("expected max-row error")
	}
}

func TestStartQueryErrorIsWrapped(t *testing.T) {
	api := &fakeAPI{startErr: errors.New("access denied")}
	driver, err := New(context.Background(), Config{EndpointID: "insights", API: api})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = driver.Query(context.Background(), sqlag.PartialRequest{
		SQL:  "SELECT 1",
		From: time.Now().Add(-time.Hour),
		To:   time.Now(),
	})
	if err == nil || !errors.Is(err, api.startErr) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRequiresEndpointID(t *testing.T) {
	if _, err := New(context.Background(), Config{API: &fakeAPI{}}); err == nil {
		t.Fatal("expected endpoint id error")
	}
}
