// Package bigquery implements the execute.Backend driver for GoogleSQL over
// the BigQuery REST API. Using the REST client (rather than the storage API)
// keeps paging and result decoding explicit and testable.
package bigquery

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

// Config configures one BigQuery endpoint driver.
type Config struct {
	EndpointID string
	// ProjectID is the billing project used for jobs.
	ProjectID string
	// Location is the dataset location, for example "US" or "EU".
	Location string
	// CredentialsFile is optional; empty uses Application Default Credentials.
	CredentialsFile string
	// Endpoint overrides the API base (emulators/tests).
	Endpoint string
	// DisableAuth is for emulators/tests only.
	DisableAuth bool
	// HTTPClient is optional and primarily used by tests.
	HTTPClient *http.Client
	// MaxRows bounds a partial result. Zero uses 200000.
	MaxRows int
	// MaxBytesBilled caps query cost. Zero leaves it unset.
	MaxBytesBilled int64
	// Timeout bounds the whole query. Zero uses the caller's deadline.
	Timeout time.Duration
	// Service is optional and primarily used by tests.
	Service *bigquery.Service
}

// Driver executes pushdown SQL against BigQuery.
type Driver struct {
	id       string
	project  string
	location string
	maxRows  int
	maxBytes int64
	timeout  time.Duration
	svc      *bigquery.Service
}

const pageSize = 1000

// New creates a BigQuery service and driver.
func New(ctx context.Context, cfg Config) (*Driver, error) {
	if cfg.EndpointID == "" {
		return nil, fmt.Errorf("bigquery: endpoint id is required")
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("bigquery: project id is required")
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 200_000
	}
	if cfg.Service == nil {
		opts := []option.ClientOption{}
		if cfg.DisableAuth {
			opts = append(opts, option.WithoutAuthentication())
		}
		if cfg.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		}
		if cfg.HTTPClient != nil {
			opts = append(opts, option.WithHTTPClient(cfg.HTTPClient))
		}
		if cfg.Endpoint != "" {
			endpoint := cfg.Endpoint
			if !strings.HasSuffix(endpoint, "/") {
				endpoint += "/"
			}
			opts = append(opts, option.WithEndpoint(endpoint))
		}
		svc, err := bigquery.NewService(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("bigquery: new service: %w", err)
		}
		cfg.Service = svc
	}
	return &Driver{
		id:       cfg.EndpointID,
		project:  cfg.ProjectID,
		location: cfg.Location,
		maxRows:  cfg.MaxRows,
		maxBytes: cfg.MaxBytesBilled,
		timeout:  cfg.Timeout,
		svc:      cfg.Service,
	}, nil
}

// ID returns the registry endpoint ID.
func (d *Driver) ID() string { return d.id }

// Query runs the pushdown and returns canonical typed rows.
func (d *Driver) Query(ctx context.Context, req sqlag.PartialRequest) (*sqlag.Stream, error) {
	statement := strings.TrimSpace(req.SQL)
	if statement == "" {
		return nil, fmt.Errorf("bigquery: empty statement")
	}
	if d.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.timeout)
		defer cancel()
	}
	request := &bigquery.QueryRequest{
		Query:         statement,
		UseLegacySql:  googleapi.Bool(false),
		Location:      d.location,
		MaxResults:    pageSize,
		UseQueryCache: googleapi.Bool(true),
	}
	if d.maxBytes > 0 {
		request.MaximumBytesBilled = d.maxBytes
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			request.JobTimeoutMs = remaining.Milliseconds()
		}
	}
	response, err := d.svc.Jobs.Query(d.project, request).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("bigquery: query: %w", err)
	}
	if len(response.Errors) > 0 {
		return nil, fmt.Errorf("bigquery: %s", formatErrors(response.Errors))
	}
	jobID := ""
	if response.JobReference != nil {
		jobID = response.JobReference.JobId
	}
	var (
		schema *bigquery.TableSchema
		rows   []*bigquery.TableRow
		token  string
	)
	if response.JobComplete {
		schema, rows, token = response.Schema, response.Rows, response.PageToken
	} else {
		if jobID == "" {
			return nil, fmt.Errorf("bigquery: query did not complete and returned no job id")
		}
		schema, rows, token, err = d.wait(ctx, jobID)
		if err != nil {
			return nil, err
		}
	}
	if schema == nil {
		return nil, fmt.Errorf("bigquery: result has no schema")
	}
	for token != "" {
		page, err := d.svc.Jobs.GetQueryResults(d.project, jobID).
			Context(ctx).
			Location(d.location).
			PageToken(token).
			MaxResults(pageSize).
			Do()
		if err != nil {
			return nil, fmt.Errorf("bigquery: page: %w", err)
		}
		if len(page.Errors) > 0 {
			return nil, fmt.Errorf("bigquery: %s", formatErrors(page.Errors))
		}
		rows = append(rows, page.Rows...)
		if len(rows) > d.maxRows {
			return nil, fmt.Errorf("bigquery: partial result exceeded max rows (%d)", d.maxRows)
		}
		token = page.PageToken
	}
	return toStream(d.id, req.Schema, schema, rows)
}

func (d *Driver) wait(ctx context.Context, jobID string) (*bigquery.TableSchema, []*bigquery.TableRow, string, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, "", ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
		page, err := d.svc.Jobs.GetQueryResults(d.project, jobID).
			Context(ctx).
			Location(d.location).
			MaxResults(pageSize).
			Do()
		if err != nil {
			return nil, nil, "", fmt.Errorf("bigquery: poll: %w", err)
		}
		if len(page.Errors) > 0 {
			return nil, nil, "", fmt.Errorf("bigquery: %s", formatErrors(page.Errors))
		}
		if page.JobComplete {
			return page.Schema, page.Rows, page.PageToken, nil
		}
	}
}

func toStream(endpointID string, expected []string, schema *bigquery.TableSchema, rows []*bigquery.TableRow) (*sqlag.Stream, error) {
	columns := make([]string, 0, len(schema.Fields))
	types := make([]string, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		columns = append(columns, field.Name)
		types = append(types, field.Type)
	}
	have := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		have[name] = struct{}{}
	}
	for _, name := range expected {
		if _, ok := have[name]; !ok {
			return nil, fmt.Errorf("bigquery: partial result is missing column %q", name)
		}
	}
	out := make([]sqlag.Row, 0, len(rows))
	for _, row := range rows {
		if len(row.F) != len(columns) {
			return nil, fmt.Errorf("bigquery: row has %d cells for %d columns", len(row.F), len(columns))
		}
		values := make(sqlag.Row, len(columns))
		for i, cell := range row.F {
			if cell == nil || cell.V == nil {
				values[i] = sqlag.Null()
				continue
			}
			decoded, err := decodeCell(cell.V, types[i])
			if err != nil {
				return nil, fmt.Errorf("bigquery: column %q (%s): %w", columns[i], types[i], err)
			}
			values[i] = decoded
		}
		out = append(out, values)
	}
	return &sqlag.Stream{
		EndpointID: endpointID,
		Schema:     sqlag.Schema{Columns: columns},
		Rows:       out,
	}, nil
}

func decodeCell(raw any, typ string) (sqlag.Value, error) {
	switch strings.ToUpper(typ) {
	case "INTEGER", "INT64":
		i, err := toInt64(raw)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Int64(i), nil
	case "FLOAT", "FLOAT64":
		f, err := toFloat64(raw)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Float64(f), nil
	case "BOOLEAN", "BOOL":
		b, err := toBool(raw)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Boolean(b), nil
	case "NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL":
		d, ok := sqlag.Decimal(toText(raw))
		if !ok {
			return sqlag.Value{}, fmt.Errorf("invalid decimal %q", toText(raw))
		}
		return d, nil
	case "TIMESTAMP":
		t, err := parseUnixSeconds(toText(raw))
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	case "DATE":
		t, err := time.Parse("2006-01-02", toText(raw))
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	case "DATETIME":
		t, err := parseDateTime(toText(raw))
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	case "RECORD", "STRUCT", "REPEATED":
		return sqlag.Value{}, fmt.Errorf("nested/repeated fields are not supported")
	default:
		return sqlag.Str(toText(raw)), nil
	}
}

func toText(raw any) string {
	switch v := raw.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case jsonNumber:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

// jsonNumber avoids importing encoding/json just for the interface shape.
type jsonNumber interface{ String() string }

func toInt64(raw any) (int64, error) {
	switch v := raw.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case float64:
		return int64(v), nil
	case jsonNumber:
		return strconv.ParseInt(v.String(), 10, 64)
	default:
		return 0, fmt.Errorf("expected integer, got %T", raw)
	}
}

func toFloat64(raw any) (float64, error) {
	switch v := raw.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	case jsonNumber:
		return strconv.ParseFloat(v.String(), 64)
	default:
		return 0, fmt.Errorf("expected float, got %T", raw)
	}
}

func toBool(raw any) (bool, error) {
	switch v := raw.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(v) {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		}
	}
	return false, fmt.Errorf("expected bool, got %T", raw)
}

func parseUnixSeconds(s string) (time.Time, error) {
	seconds, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, err
	}
	whole := math.Floor(seconds)
	nanos := (seconds - whole) * 1e9
	return time.Unix(int64(whole), int64(nanos)).UTC(), nil
}

func parseDateTime(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05", "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid datetime %q", s)
}

func formatErrors(errors []*bigquery.ErrorProto) string {
	parts := make([]string, 0, len(errors))
	for _, e := range errors {
		if e == nil {
			continue
		}
		parts = append(parts, e.Message)
	}
	if len(parts) == 0 {
		return "query failed"
	}
	return strings.Join(parts, "; ")
}
