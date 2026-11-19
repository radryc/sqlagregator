// Package omni implements the execute.Backend driver for CloudWatch Omni
// telemetry SQL. The SDK handles the CBOR wire protocol and SigV4 signing.
package omni

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchomni"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchomni/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

// API is the subset of the CloudWatch Omni client the driver uses. It is an
// interface so tests can exercise polling and paging without CBOR.
type API interface {
	StartTelemetryQuerySession(context.Context, *cloudwatchomni.StartTelemetryQuerySessionInput, ...func(*cloudwatchomni.Options)) (*cloudwatchomni.StartTelemetryQuerySessionOutput, error)
	StartTelemetryQuery(context.Context, *cloudwatchomni.StartTelemetryQueryInput, ...func(*cloudwatchomni.Options)) (*cloudwatchomni.StartTelemetryQueryOutput, error)
	GetTelemetryQueryResults(context.Context, *cloudwatchomni.GetTelemetryQueryResultsInput, ...func(*cloudwatchomni.Options)) (*cloudwatchomni.GetTelemetryQueryResultsOutput, error)
}

// Config configures one Omni endpoint driver.
type Config struct {
	EndpointID string
	// Region and Profile are used when API is nil.
	Region  string
	Profile string
	// Endpoint overrides the service endpoint (testing or private links).
	Endpoint string
	// AssumeRoleARN, when set, is assumed after the base config loads. It lets
	// one router read CloudWatch in another account.
	AssumeRoleARN string
	// SessionName is shown in Omni sessions. Defaults to sqlag-<endpoint>.
	SessionName string
	// MaxRows bounds a partial result. Zero uses 200000.
	MaxRows int
	// Timeout bounds the whole query. Zero uses the caller's deadline.
	Timeout time.Duration
	// API is optional and primarily used by tests.
	API API
}

// Driver executes pushdown SQL against CloudWatch Omni.
type Driver struct {
	id          string
	sessionName string
	maxRows     int
	timeout     time.Duration
	api         API

	mu        sync.Mutex
	sessionID string
}

var sessionSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

// New loads AWS configuration (unless an API is injected) and returns a driver.
func New(ctx context.Context, cfg Config) (*Driver, error) {
	if cfg.EndpointID == "" {
		return nil, fmt.Errorf("omni: endpoint id is required")
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 200_000
	}
	if cfg.SessionName == "" {
		cfg.SessionName = "sqlag-" + cfg.EndpointID
	}
	cfg.SessionName = sessionSanitizer.ReplaceAllString(cfg.SessionName, "-")
	if len(cfg.SessionName) > 128 {
		cfg.SessionName = cfg.SessionName[:128]
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
			return nil, fmt.Errorf("omni: load AWS config: %w", err)
		}
		if role := strings.TrimSpace(cfg.AssumeRoleARN); role != "" {
			awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(awsCfg), role))
		}
		cfg.API = cloudwatchomni.NewFromConfig(awsCfg, func(options *cloudwatchomni.Options) {
			if cfg.Endpoint != "" {
				options.BaseEndpoint = aws.String(cfg.Endpoint)
			}
		})
	}
	return &Driver{
		id:          cfg.EndpointID,
		sessionName: cfg.SessionName,
		maxRows:     cfg.MaxRows,
		timeout:     cfg.Timeout,
		api:         cfg.API,
	}, nil
}

// ID returns the registry endpoint ID.
func (d *Driver) ID() string { return d.id }

// Query runs the pushdown and returns canonical typed rows.
func (d *Driver) Query(ctx context.Context, req sqlag.PartialRequest) (*sqlag.Stream, error) {
	statement := strings.TrimSpace(req.SQL)
	if statement == "" {
		return nil, fmt.Errorf("omni: empty statement")
	}
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	sessionID, err := d.session(ctx)
	if err != nil {
		return nil, err
	}
	started, err := d.api.StartTelemetryQuery(ctx, &cloudwatchomni.StartTelemetryQueryInput{
		QueryString: aws.String(statement),
		SessionId:   aws.String(sessionID),
	})
	if err != nil {
		// A session can expire between queries. Recreate it once.
		d.clearSession()
		if sessionID, sessionErr := d.session(ctx); sessionErr == nil {
			started, err = d.api.StartTelemetryQuery(ctx, &cloudwatchomni.StartTelemetryQueryInput{
				QueryString: aws.String(statement),
				SessionId:   aws.String(sessionID),
			})
		}
		if err != nil {
			return nil, fmt.Errorf("omni: start query: %w", err)
		}
	}
	queryID := aws.ToString(started.QueryId)
	if queryID == "" {
		return nil, fmt.Errorf("omni: start query returned no query id")
	}
	rows, err := d.collect(ctx, queryID)
	if err != nil {
		return nil, err
	}
	return d.toStream(req, rows)
}

func (d *Driver) session(ctx context.Context) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessionID != "" {
		return d.sessionID, nil
	}
	created, err := d.api.StartTelemetryQuerySession(ctx, &cloudwatchomni.StartTelemetryQuerySessionInput{
		SessionName: aws.String(d.sessionName),
	})
	if err != nil {
		return "", fmt.Errorf("omni: start session: %w", err)
	}
	id := aws.ToString(created.SessionId)
	if id == "" {
		return "", fmt.Errorf("omni: start session returned no session id")
	}
	d.sessionID = id
	return id, nil
}

func (d *Driver) clearSession() {
	d.mu.Lock()
	d.sessionID = ""
	d.mu.Unlock()
}

func (d *Driver) collect(ctx context.Context, queryID string) ([]map[string]string, error) {
	const pageSize int32 = 1000
	var (
		rows  []map[string]string
		token *string
	)
	for {
		out, err := d.api.GetTelemetryQueryResults(ctx, &cloudwatchomni.GetTelemetryQueryResultsInput{
			QueryId:    aws.String(queryID),
			MaxResults: aws.Int32(pageSize),
			NextToken:  token,
		})
		if err != nil {
			return nil, fmt.Errorf("omni: get results: %w", err)
		}
		switch out.Status {
		case types.QueryStatusComplete:
			rows = append(rows, out.Rows...)
			if len(rows) > d.maxRows {
				return nil, fmt.Errorf("omni: partial result exceeded max rows (%d)", d.maxRows)
			}
			if out.NextToken == nil || *out.NextToken == "" {
				return rows, nil
			}
			token = out.NextToken
		case types.QueryStatusFailed:
			return nil, fmt.Errorf("omni: query %s failed", queryID)
		case types.QueryStatusCancelled:
			return nil, fmt.Errorf("omni: query %s was cancelled", queryID)
		default:
			if len(rows) > 0 {
				// Never mix a completed page with a running retry; that would
				// duplicate rows in the merge engine.
				return nil, fmt.Errorf("omni: query status regressed to %s after results", out.Status)
			}
			// Running: poll until the first complete page.
			token = nil
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
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
	present := make(map[string]bool, len(columns))
	for _, row := range rows {
		for name := range row {
			present[name] = true
		}
	}
	if len(rows) > 0 {
		for _, name := range columns {
			if !present[name] {
				return nil, fmt.Errorf("omni: partial result is missing column %q", name)
			}
		}
	}
	out := make([]sqlag.Row, 0, len(rows))
	for _, row := range rows {
		values := make(sqlag.Row, len(columns))
		for i, name := range columns {
			text, ok := row[name]
			if !ok {
				values[i] = sqlag.Null()
				continue
			}
			decoded, err := decodeText(text, types[i])
			if err != nil {
				return nil, fmt.Errorf("omni: column %q: %w", name, err)
			}
			values[i] = decoded
		}
		out = append(out, values)
	}
	return &sqlag.Stream{
		EndpointID: d.id,
		Schema:     sqlag.Schema{Columns: columns},
		Rows:       out,
	}, nil
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
		case "true", "1", "t":
			return sqlag.Boolean(true), nil
		case "false", "0", "f":
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
