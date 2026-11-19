// Package clickhouse implements the execute.Backend driver for ClickHouse's
// HTTP interface. It is deliberately standard-library only, matching MonoFS's
// existing ClickHouse client.
package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	sqlag "github.com/radryc/sqlagregator/pkg/sqlag"
)

// Config configures one ClickHouse endpoint driver.
type Config struct {
	// EndpointID is the registry ID this driver serves.
	EndpointID string
	// URL is the ClickHouse HTTP endpoint, for example http://clickhouse:8123.
	URL string
	// Database is optional; leave empty when emitted SQL fully qualifies tables.
	Database string
	User     string
	Password string
	// Timeout bounds one HTTP request. Zero uses 20s.
	Timeout time.Duration
	// MaxRows bounds a partial result. Zero uses 200000. Overflow is an error,
	// never a silent truncation.
	MaxRows int
	// HTTPClient is optional and primarily used by tests.
	HTTPClient *http.Client
}

// Driver executes pushdown SQL against ClickHouse.
type Driver struct {
	id       string
	baseURL  string
	database string
	user     string
	password string
	maxRows  int
	client   *http.Client
}

// New validates the configuration and returns a driver.
func New(cfg Config) (*Driver, error) {
	if cfg.EndpointID == "" {
		return nil, fmt.Errorf("clickhouse: endpoint id is required")
	}
	raw := strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	if raw == "" {
		return nil, fmt.Errorf("clickhouse: url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("clickhouse: url must be an http(s) endpoint with a host")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 20 * time.Second
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 200_000
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	return &Driver{
		id:       cfg.EndpointID,
		baseURL:  raw,
		database: cfg.Database,
		user:     cfg.User,
		password: cfg.Password,
		maxRows:  cfg.MaxRows,
		client:   client,
	}, nil
}

// ID returns the registry endpoint ID.
func (d *Driver) ID() string { return d.id }

// Query runs the pushdown and returns canonical typed rows.
func (d *Driver) Query(ctx context.Context, req sqlag.PartialRequest) (*sqlag.Stream, error) {
	statement := strings.TrimSpace(req.SQL)
	if statement == "" {
		return nil, fmt.Errorf("clickhouse: empty statement")
	}
	// ClickHouse requires FORMAT after the statement.
	if !strings.HasSuffix(strings.ToUpper(statement), "FORMAT JSON") {
		statement += "\nFORMAT JSON"
	}
	params := url.Values{}
	params.Set("query", statement)
	if d.database != "" {
		params.Set("database", d.database)
	}
	params.Set("readonly", "1")
	params.Set("output_format_json_quote_64bit_integers", "0")
	params.Set("output_format_json_quote_decimals", "1")
	params.Set("max_result_rows", strconv.Itoa(d.maxRows))
	params.Set("result_overflow_mode", "throw")
	params.Set("max_execution_time", strconv.Itoa(int(d.requestTimeout(ctx).Seconds())+1))

	endpoint := d.baseURL + "/?" + params.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: build request: %w", err)
	}
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if d.user != "" {
		request.Header.Set("X-ClickHouse-User", d.user)
	}
	if d.password != "" {
		request.Header.Set("X-ClickHouse-Key", d.password)
	}
	response, err := d.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return nil, fmt.Errorf("clickhouse: http %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}

	var payload struct {
		Meta []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"meta"`
		Data []map[string]json.RawMessage `json:"data"`
		Rows int                          `json:"rows"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 256<<20))
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("clickhouse: decode result: %w", err)
	}
	if len(payload.Meta) == 0 {
		return nil, fmt.Errorf("clickhouse: result has no column metadata")
	}
	columns := make([]string, len(payload.Meta))
	types := make([]string, len(payload.Meta))
	for i, meta := range payload.Meta {
		columns[i] = meta.Name
		types[i] = meta.Type
	}
	if err := validateSchema(req.Schema, columns); err != nil {
		return nil, err
	}
	if payload.Rows > d.maxRows {
		return nil, fmt.Errorf("clickhouse: result exceeded max rows (%d > %d)", payload.Rows, d.maxRows)
	}

	rows := make([]sqlag.Row, 0, len(payload.Data))
	for _, raw := range payload.Data {
		row := make(sqlag.Row, len(columns))
		for i, column := range columns {
			v, ok := raw[column]
			if !ok {
				return nil, fmt.Errorf("clickhouse: row is missing column %q", column)
			}
			parsed, err := decodeValue(v, types[i])
			if err != nil {
				return nil, fmt.Errorf("clickhouse: column %q (%s): %w", column, types[i], err)
			}
			row[i] = parsed
		}
		rows = append(rows, row)
	}
	return &sqlag.Stream{
		EndpointID: d.id,
		Schema:     sqlag.Schema{Columns: columns},
		Rows:       rows,
	}, nil
}

func (d *Driver) requestTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			return remaining
		}
	}
	return 20 * time.Second
}

func validateSchema(expected, got []string) error {
	if len(expected) == 0 {
		return nil
	}
	have := make(map[string]struct{}, len(got))
	for _, name := range got {
		have[name] = struct{}{}
	}
	for _, name := range expected {
		if _, ok := have[name]; !ok {
			return fmt.Errorf("clickhouse: partial result is missing column %q", name)
		}
	}
	return nil
}

// decodeValue converts one ClickHouse JSON value using its declared type.
func decodeValue(raw json.RawMessage, typ string) (sqlag.Value, error) {
	text := strings.TrimSpace(string(raw))
	if text == "null" || text == "" {
		return sqlag.Null(), nil
	}
	base, nullable := baseType(typ)
	_ = nullable

	switch {
	case isIntType(base):
		if isJSONString(text) {
			text = unquote(text)
		}
		i, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Int64(i), nil
	case isUintType(base):
		if isJSONString(text) {
			text = unquote(text)
		}
		u, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Uint64(u), nil
	case isFloatType(base):
		if isJSONString(text) {
			text = unquote(text)
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Float64(f), nil
	case strings.HasPrefix(base, "Decimal"):
		if isJSONString(text) {
			text = unquote(text)
		}
		d, ok := sqlag.Decimal(text)
		if !ok {
			return sqlag.Value{}, fmt.Errorf("invalid decimal %q", text)
		}
		return d, nil
	case base == "Bool" || base == "Boolean":
		switch text {
		case "true", "1", `"true"`, `"1"`:
			return sqlag.Boolean(true), nil
		case "false", "0", `"false"`, `"0"`:
			return sqlag.Boolean(false), nil
		default:
			return sqlag.Value{}, fmt.Errorf("invalid bool %q", text)
		}
	case strings.HasPrefix(base, "DateTime64"), base == "DateTime":
		s := unquoteIfString(text)
		t, err := parseTime(s)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	case base == "Date", base == "Date32":
		s := unquoteIfString(text)
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return sqlag.Value{}, err
		}
		return sqlag.Timestamp(t), nil
	case base == "String", strings.HasPrefix(base, "FixedString"),
		strings.HasPrefix(base, "Enum"):
		return sqlag.Str(unquoteIfString(text)), nil
	case base == "UUID", base == "IPv4", base == "IPv6":
		return sqlag.Str(unquoteIfString(text)), nil
	default:
		return sqlag.Value{}, fmt.Errorf("unsupported ClickHouse type %q", typ)
	}
}

// baseType strips Nullable/LowCardinality/SimpleAggregateFunction wrappers.
func baseType(t string) (string, bool) {
	nullable := false
	for {
		t = strings.TrimSpace(t)
		switch {
		case strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")"):
			nullable = true
			t = strings.TrimSuffix(strings.TrimPrefix(t, "Nullable("), ")")
		case strings.HasPrefix(t, "LowCardinality(") && strings.HasSuffix(t, ")"):
			t = strings.TrimSuffix(strings.TrimPrefix(t, "LowCardinality("), ")")
		case strings.HasPrefix(t, "SimpleAggregateFunction(") && strings.HasSuffix(t, ")"):
			inner := strings.TrimSuffix(strings.TrimPrefix(t, "SimpleAggregateFunction("), ")")
			if comma := strings.IndexByte(inner, ','); comma >= 0 {
				t = inner[comma+1:]
			} else {
				t = inner
			}
		default:
			return t, nullable
		}
	}
}

func isIntType(t string) bool {
	switch t {
	case "Int8", "Int16", "Int32", "Int64", "Int128", "Int256":
		return true
	}
	return false
}

func isUintType(t string) bool {
	switch t {
	case "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256":
		return true
	}
	return false
}

func isFloatType(t string) bool {
	return t == "Float32" || t == "Float64"
}

func isJSONString(s string) bool {
	return strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)
}

func unquote(s string) string {
	if out, err := strconv.Unquote(s); err == nil {
		return out
	}
	return strings.Trim(s, `"`)
}

func unquoteIfString(s string) string {
	if isJSONString(s) {
		return unquote(s)
	}
	return s
}

func parseTime(s string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
}
