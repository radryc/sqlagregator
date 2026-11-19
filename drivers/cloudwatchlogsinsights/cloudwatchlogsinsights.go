// Package cloudwatchlogsinsights implements the execute.Backend driver for
// CloudWatch Logs Insights using the OpenSearch SQL query language.
package cloudwatchlogsinsights

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

// API is the subset of the CloudWatch Logs client the driver uses. It is an
// interface so tests can exercise polling/decoding without AWS.
type API interface {
	StartQuery(context.Context, *cloudwatchlogs.StartQueryInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error)
	GetQueryResults(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error)
}

// Config configures one Logs Insights endpoint driver.
type Config struct {
	EndpointID string
	// Region and Profile are used when API is nil.
	Region  string
	Profile string
	// Endpoint overrides the service endpoint (testing/private links).
	Endpoint string
	// AssumeRoleARN, when set, is assumed after the base config loads so one
	// router can query logs in another account.
	AssumeRoleARN string
	// Timeout bounds the whole query.
	Timeout time.Duration
	// MaxRows bounds a partial result. Zero uses 10000 (the SQL single-page
	// ceiling).
	MaxRows int
	// API is optional and primarily used by tests.
	API API
}

// Driver executes OpenSearch SQL against CloudWatch Logs Insights.
type Driver struct {
	id      string
	maxRows int
	timeout time.Duration
	api     API
}

// New loads AWS configuration (unless an API is injected) and returns a driver.
func New(ctx context.Context, cfg Config) (*Driver, error) {
	if cfg.EndpointID == "" {
		return nil, fmt.Errorf("logs insights: endpoint id is required")
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 10_000
	}
	if cfg.API == nil {
		loadOpts := make([]func(*awsconfig.LoadOptions) error, 0, 2)
		if cfg.Region != "" {
			loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
		}
		if cfg.Profile != "" {
			loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.Profile))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("logs insights: load AWS config: %w", err)
		}
		if role := strings.TrimSpace(cfg.AssumeRoleARN); role != "" {
			awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), role))
		}
		cfg.API = cloudwatchlogs.NewFromConfig(awsCfg, func(options *cloudwatchlogs.Options) {
			if cfg.Endpoint != "" {
				options.BaseEndpoint = aws.String(cfg.Endpoint)
			}
		})
	}
	return &Driver{id: cfg.EndpointID, maxRows: cfg.MaxRows, timeout: cfg.Timeout, api: cfg.API}, nil
}

// ID returns the registry endpoint ID.
func (d *Driver) ID() string { return d.id }

// Query runs the pushdown and returns canonical typed rows.
func (d *Driver) Query(ctx context.Context, req sqlag.PartialRequest) (*sqlag.Stream, error) {
	statement := strings.TrimSpace(req.SQL)
	if statement == "" {
		return nil, fmt.Errorf("logs insights: empty statement")
	}
	if req.From.IsZero() || req.To.IsZero() {
		return nil, fmt.Errorf("logs insights: a time range is required")
	}
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	started, err := d.api.StartQuery(ctx, &cloudwatchlogs.StartQueryInput{
		QueryString:   aws.String(statement),
		StartTime:     aws.Int64(req.From.Unix()),
		EndTime:       aws.Int64(req.To.Unix()),
		QueryLanguage: types.QueryLanguageSql,
		// For OpenSearch SQL the log groups are named inside the query string
		// (FROM logGroups(...)); do not set LogGroupNames/Identifiers here.
	})
	if err != nil {
		return nil, fmt.Errorf("logs insights: start query: %w", err)
	}
	queryID := aws.ToString(started.QueryId)
	if queryID == "" {
		return nil, fmt.Errorf("logs insights: start query returned no query id")
	}
	rows, err := d.collect(ctx, queryID)
	if err != nil {
		return nil, err
	}
	return d.toStream(req, rows)
}

func (d *Driver) collect(ctx context.Context, queryID string) ([]map[string]string, error) {
	var out []map[string]string
	var token *string
	for {
		page, err := d.api.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{
			QueryId:   aws.String(queryID),
			NextToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("logs insights: get results: %w", err)
		}
		switch page.Status {
		case types.QueryStatusComplete:
			out = append(out, flatten(page.Results)...)
			if len(out) > d.maxRows {
				return nil, fmt.Errorf("logs insights: result exceeded max rows (%d)", d.maxRows)
			}
			if page.NextToken == nil || *page.NextToken == "" {
				return out, nil
			}
			token = page.NextToken
		case types.QueryStatusFailed, types.QueryStatusCancelled, types.QueryStatusTimeout, types.QueryStatusUnknown:
			return nil, fmt.Errorf("logs insights: query %s ended with status %s", queryID, page.Status)
		default:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func flatten(rows [][]types.ResultField) []map[string]string {
	out := make([]map[string]string, 0, len(rows))
	for _, fields := range rows {
		row := make(map[string]string, len(fields))
		for _, field := range fields {
			row[aws.ToString(field.Field)] = aws.ToString(field.Value)
		}
		out = append(out, row)
	}
	return out
}

func (d *Driver) toStream(req sqlag.PartialRequest, rows []map[string]string) (*sqlag.Stream, error) {
	columns := req.Schema
	if len(columns) == 0 && len(rows) > 0 {
		for name := range rows[0] {
			columns = append(columns, name)
		}
	}
	types := req.Types
	if len(types) != len(columns) {
		types = make([]sqlag.Type, len(columns))
		for i := range types {
			types[i] = sqlag.TypeString
		}
	}
	present := map[string]bool{}
	for _, row := range rows {
		for name := range row {
			present[name] = true
		}
	}
	if len(rows) > 0 {
		for _, name := range columns {
			if !present[name] {
				return nil, fmt.Errorf("logs insights: partial result is missing column %q", name)
			}
		}
	}
	out := make([]sqlag.Row, 0, len(rows))
	for _, row := range rows {
		values := make(sqlag.Row, len(columns))
		for i, name := range columns {
			decoded, err := decodeText(row[name], types[i])
			if err != nil {
				return nil, fmt.Errorf("logs insights: column %q: %w", name, err)
			}
			values[i] = decoded
		}
		out = append(out, values)
	}
	return &sqlag.Stream{EndpointID: d.id, Schema: sqlag.Schema{Columns: columns}, Rows: out}, nil
}

func decodeText(text string, typ sqlag.Type) (sqlag.Value, error) {
	trimmed := strings.TrimSpace(text)
	if typ != sqlag.TypeString && typ != sqlag.TypeJSON {
		if trimmed == "" || strings.EqualFold(trimmed, "null") {
			return sqlag.Null(), nil
		}
	}
	switch typ {
	case sqlag.TypeInt64:
		i, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Int64(i), nil
	case sqlag.TypeUint64:
		u, err := strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Uint64(u), nil
	case sqlag.TypeFloat64:
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Float64(f), nil
	case sqlag.TypeDecimal:
		d, ok := sqlag.Decimal(trimmed)
		if !ok {
			return sqlag.Value{}, fmt.Errorf("invalid decimal %q", text)
		}
		return d, nil
	case sqlag.TypeBool:
		switch strings.ToLower(trimmed) {
		case "true", "1":
			return sqlag.Boolean(true), nil
		case "false", "0":
			return sqlag.Boolean(false), nil
		default:
			return sqlag.Value{}, fmt.Errorf("invalid bool %q", text)
		}
	case sqlag.TypeTimestamp:
		t, err := parseTimestamp(trimmed)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	default:
		return sqlag.Str(text), nil
	}
}

func parseTimestamp(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}
