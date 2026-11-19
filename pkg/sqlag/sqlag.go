// Package sqlag is the stable public API for the federated SQL coordinator.
// It wraps the internal planner/merge/execute packages so other modules (for
// example MonoFS) can embed the coordinator without importing internal paths.
package sqlag

import (
	"context"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/execute"
	"github.com/radryc/sqlagregator/internal/merge"
	"github.com/radryc/sqlagregator/internal/selector"
	"github.com/radryc/sqlagregator/internal/value"
)

// Catalog and registry types.
type (
	Catalog         = catalog.Catalog
	Table           = catalog.Table
	Column          = catalog.Column
	Endpoint        = catalog.Endpoint
	Role            = catalog.Role
	Capabilities    = catalog.Capabilities
	CoverageGroup   = catalog.CoverageGroup
	BackendDefaults = catalog.BackendDefaults
	Defaults        = catalog.Defaults
	TimeRange       = catalog.TimeRange
	Type            = catalog.Type
)

const (
	RoleShard   = catalog.RoleShard
	RoleReplica = catalog.RoleReplica

	TypeInt64     = catalog.TypeInt64
	TypeUint64    = catalog.TypeUint64
	TypeFloat64   = catalog.TypeFloat64
	TypeDecimal   = catalog.TypeDecimal
	TypeString    = catalog.TypeString
	TypeBool      = catalog.TypeBool
	TypeTimestamp = catalog.TypeTimestamp
	TypeJSON      = catalog.TypeJSON
)

// NewCatalog assembles and validates a registry.
func NewCatalog(
	tables map[string]*Table,
	endpoints []*Endpoint,
	groups map[string]*CoverageGroup,
	backends map[string]*BackendDefaults,
	defaults Defaults,
) (*Catalog, error) {
	return catalog.New(tables, endpoints, groups, backends, defaults)
}

// LoadCatalog reads a YAML catalog.
func LoadCatalog(path string) (*Catalog, error) { return catalog.LoadFile(path) }

// LoadCatalogBytes parses a YAML catalog.
func LoadCatalogBytes(data []byte) (*Catalog, error) { return catalog.LoadBytes(data) }

// Selection types.
type (
	Matcher      = selector.Matcher
	Selector     = selector.Selector
	CoverageInfo = selector.CoverageInfo
	Exclusion    = selector.Exclusion
)

// PartitionBinding describes how an endpoint constrains itself to the visible
// partitions.
type PartitionBinding = catalog.PartitionBinding

// PartitionBindingValue maps a visible partition to native identifiers.
type PartitionBindingValue = catalog.PartitionBindingValue

// Value and stream types.
type (
	Value  = value.Value
	Kind   = value.Kind
	Schema = merge.Schema
	Row    = merge.Row
	Stream = merge.Stream
)

// Kind constants mirror the canonical value kinds.
const (
	KindNull      = value.KNull
	KindInt       = value.KInt
	KindUint      = value.KUint
	KindFloat     = value.KFloat
	KindDecimal   = value.KDecimal
	KindString    = value.KString
	KindBool      = value.KBool
	KindTimestamp = value.KTimestamp
)

// Stream construction helpers.
func Str(s string) Value          { return value.Str(s) }
func Int64(i int64) Value         { return value.Int64(i) }
func Uint64(u uint64) Value       { return value.Uint64(u) }
func Float64(f float64) Value     { return value.Float64(f) }
func Boolean(b bool) Value        { return value.Boolean(b) }
func Timestamp(t time.Time) Value { return value.Time(t) }
func Null() Value                 { return value.NullValue() }

// Decimal parses an exact base-10 value.
func Decimal(s string) (Value, bool) {
	d, ok := value.NewDecimalFromString(s)
	if !ok {
		return value.Value{}, false
	}
	return value.Dec(d), true
}

// ResolveOptions are resolution inputs that are not part of the selector.
type ResolveOptions struct {
	AllowPartial             bool
	Signal                   string
	ExcludeReplicasByDefault bool
}

// Resolution is the execution set produced by Resolve.
type Resolution struct {
	Shards   []*Endpoint
	Replicas []*Endpoint
	Excluded []Exclusion
	Coverage CoverageInfo
	Warnings []string
}

// Resolve applies a selector and validates coverage.
func Resolve(cat *Catalog, table string, sel Selector, window TimeRange, opts ResolveOptions) (*Resolution, error) {
	res, err := selector.Resolve(cat, table, sel, window, selector.Options{
		AllowPartial:             opts.AllowPartial,
		Signal:                   opts.Signal,
		ExcludeReplicasByDefault: opts.ExcludeReplicasByDefault,
	})
	if err != nil {
		return nil, err
	}
	return &Resolution{
		Shards:   res.Shards,
		Replicas: res.Replicas,
		Excluded: res.Excluded,
		Coverage: res.Coverage,
		Warnings: res.Warnings,
	}, nil
}

// Scope is the caller's authorized partition scope.
type Scope struct {
	Restricted bool
	Partitions []string
	Bindings   []PartitionBindingValue
}

// PartialRequest is what a backend receives.
type PartialRequest struct {
	Endpoint *Endpoint
	Dataset  string
	SQL      string
	From, To time.Time
	Schema   []string
	Types    []Type
	Scope    Scope
}

// Backend executes one pushdown.
type Backend interface {
	ID() string
	Query(ctx context.Context, req PartialRequest) (*Stream, error)
}

// Request is one federated query.
type Request struct {
	SQL          string
	From, To     time.Time
	Limit        int
	Selector     Selector
	AllowPartial bool
	Scope        Scope
}

// BackendStats is per-endpoint execution metadata.
type BackendStats struct {
	ID         string `json:"id"`
	Backend    string `json:"backend"`
	Rows       int    `json:"rows"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// Result is the JSON-facing query result.
type Result struct {
	Columns  []string       `json:"columns"`
	Rows     [][]any        `json:"rows"`
	RowCount int            `json:"row_count"`
	Coverage CoverageInfo   `json:"coverage"`
	Backends []BackendStats `json:"backends"`
	Excluded []Exclusion    `json:"excluded"`
	Warnings []string       `json:"warnings,omitempty"`
	Partial  bool           `json:"partial"`
	Mode     string         `json:"mode"`
}

// PushdownExplain is one emitted statement.
type PushdownExplain struct {
	Endpoint string `json:"endpoint"`
	Backend  string `json:"backend"`
	Source   string `json:"source"`
	SQL      string `json:"sql"`
}

// MergeExplain describes the local post-processing.
type MergeExplain struct {
	RowMode    bool     `json:"row_mode"`
	GroupKeys  int      `json:"group_keys"`
	Aggregates []string `json:"aggregates,omitempty"`
	Having     bool     `json:"having"`
	OrderBy    int      `json:"order_by"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

// Explanation is the output of EXPLAIN.
type Explanation struct {
	Mode     string            `json:"mode"`
	Selected []string          `json:"selected"`
	Excluded []Exclusion       `json:"excluded"`
	Coverage CoverageInfo      `json:"coverage"`
	Pushdown []PushdownExplain `json:"pushdown"`
	Merge    MergeExplain      `json:"merge"`
	Warnings []string          `json:"warnings,omitempty"`
}

// Coordinator owns the catalog and backend drivers.
type Coordinator struct {
	inner *execute.Coordinator
}

// New creates a coordinator.
func New(cat *Catalog) *Coordinator {
	return &Coordinator{inner: execute.New(cat)}
}

// Register adds a backend driver.
func (c *Coordinator) Register(b Backend) {
	c.inner.Register(backendAdapter{b: b})
}

// Query executes a federated query.
func (c *Coordinator) Query(ctx context.Context, req Request) (*Result, error) {
	res, err := c.inner.Query(ctx, execute.Request{
		SQL:          req.SQL,
		From:         req.From,
		To:           req.To,
		Limit:        req.Limit,
		Selector:     req.Selector,
		AllowPartial: req.AllowPartial,
		Scope: execute.Scope{
			Restricted: req.Scope.Restricted,
			Partitions: req.Scope.Partitions,
			Bindings:   req.Scope.Bindings,
		},
	})
	if err != nil {
		return nil, err
	}
	out := &Result{
		Columns:  res.Columns,
		Rows:     res.Rows,
		RowCount: res.RowCount,
		Coverage: res.Coverage,
		Excluded: res.Excluded,
		Warnings: res.Warnings,
		Partial:  res.Partial,
		Mode:     res.Mode,
	}
	for _, b := range res.Backends {
		out.Backends = append(out.Backends, BackendStats{
			ID: b.ID, Backend: b.Backend, Rows: b.Rows, DurationMs: b.DurationMs, Error: b.Error,
		})
	}
	return out, nil
}

// Explain plans a query without executing it.
func (c *Coordinator) Explain(ctx context.Context, req Request) (*Explanation, error) {
	expl, err := c.inner.Explain(ctx, execute.Request{
		SQL:          req.SQL,
		From:         req.From,
		To:           req.To,
		Limit:        req.Limit,
		Selector:     req.Selector,
		AllowPartial: req.AllowPartial,
		Scope: execute.Scope{
			Restricted: req.Scope.Restricted,
			Partitions: req.Scope.Partitions,
			Bindings:   req.Scope.Bindings,
		},
	})
	if err != nil {
		return nil, err
	}
	out := &Explanation{
		Mode:     expl.Mode,
		Selected: expl.Selected,
		Excluded: expl.Excluded,
		Coverage: expl.Coverage,
		Warnings: expl.Warnings,
		Merge: MergeExplain{
			RowMode:    expl.Merge.RowMode,
			GroupKeys:  expl.Merge.GroupKeys,
			Aggregates: expl.Merge.Aggregates,
			Having:     expl.Merge.Having,
			OrderBy:    expl.Merge.OrderBy,
			Limit:      expl.Merge.Limit,
			Offset:     expl.Merge.Offset,
		},
	}
	for _, p := range expl.Pushdown {
		out.Pushdown = append(out.Pushdown, PushdownExplain{
			Endpoint: p.Endpoint, Backend: p.Backend, Source: p.Source, SQL: p.SQL,
		})
	}
	return out, nil
}

type backendAdapter struct{ b Backend }

func (a backendAdapter) ID() string { return a.b.ID() }

func (a backendAdapter) Query(ctx context.Context, req execute.PartialRequest) (*merge.Stream, error) {
	return a.b.Query(ctx, PartialRequest{
		Endpoint: req.Endpoint,
		Dataset:  req.Dataset,
		SQL:      req.SQL,
		From:     req.From,
		To:       req.To,
		Schema:   req.Schema,
		Types:    req.Types,
		Scope:    Scope{Restricted: req.Scope.Restricted, Partitions: req.Scope.Partitions, Bindings: req.Scope.Bindings},
	})
}
