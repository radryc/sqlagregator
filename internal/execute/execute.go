// Package execute is the query coordinator: parse, bind, select endpoints,
// scatter pushdowns, merge partials.
package execute

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/dialect"
	"github.com/radryc/sqlagregator/internal/merge"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/rewrite"
	"github.com/radryc/sqlagregator/internal/selector"
	"github.com/radryc/sqlagregator/internal/sql"
)

// Request is one federated query.
type Request struct {
	SQL          string
	From, To     time.Time
	Limit        int
	Selector     selector.Selector
	AllowPartial bool
	Scope        Scope
}

// PartitionBindingValue is the canonical mapping from a partition to native
// identifiers.
type PartitionBindingValue = catalog.PartitionBindingValue

// Scope is the caller's authorized partition scope.
type Scope struct {
	Restricted bool                    `json:"restricted"`
	Partitions []string                `json:"partitions,omitempty"`
	Bindings   []PartitionBindingValue `json:"bindings,omitempty"`
}

// PartialRequest is what a backend receives.
type PartialRequest struct {
	Endpoint *catalog.Endpoint
	Dataset  string
	SQL      string
	From, To time.Time
	Schema   []string
	Types    []catalog.Type
	Scope    Scope
}

// Backend executes one pushdown.
type Backend interface {
	ID() string
	Query(ctx context.Context, req PartialRequest) (*merge.Stream, error)
}

// Coordinator owns the catalog and the backend drivers.
type Coordinator struct {
	Catalog  *catalog.Catalog
	Backends map[string]Backend
}

// New creates a coordinator.
func New(cat *catalog.Catalog) *Coordinator {
	return &Coordinator{Catalog: cat, Backends: map[string]Backend{}}
}

// Register adds a backend driver.
func (c *Coordinator) Register(b Backend) { c.Backends[b.ID()] = b }

// Result is the JSON-facing result.
type Result struct {
	Columns  []string              `json:"columns"`
	Rows     [][]any               `json:"rows"`
	RowCount int                   `json:"row_count"`
	Coverage selector.CoverageInfo `json:"coverage"`
	Backends []BackendStats        `json:"backends"`
	Excluded []selector.Exclusion  `json:"excluded"`
	Warnings []string              `json:"warnings,omitempty"`
	Partial  bool                  `json:"partial"`
	Mode     string                `json:"mode"`
}

// BackendStats is per-endpoint execution metadata.
type BackendStats struct {
	ID         string `json:"id"`
	Backend    string `json:"backend"`
	Rows       int    `json:"rows"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// Explanation is the output of EXPLAIN.
type Explanation struct {
	Mode     string                `json:"mode"`
	Selected []string              `json:"selected"`
	Excluded []selector.Exclusion  `json:"excluded"`
	Coverage selector.CoverageInfo `json:"coverage"`
	Pushdown []PushdownExplain     `json:"pushdown"`
	Merge    MergeExplain          `json:"merge"`
	Warnings []string              `json:"warnings,omitempty"`
}

type PushdownExplain struct {
	Endpoint string `json:"endpoint"`
	Backend  string `json:"backend"`
	Source   string `json:"source"`
	SQL      string `json:"sql"`
}

type MergeExplain struct {
	RowMode    bool     `json:"row_mode"`
	GroupKeys  int      `json:"group_keys"`
	Aggregates []string `json:"aggregates,omitempty"`
	Having     bool     `json:"having"`
	OrderBy    int      `json:"order_by"`
	Limit      int      `json:"limit,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

type prepared struct {
	query    *plan.Query
	resolved *selector.Resolution
	entries  []pushdownEntry
	from, to time.Time
	scope    Scope
}

type pushdownEntry struct {
	endpoint *catalog.Endpoint
	sql      string
	source   string
}

// Explain validates and plans a query without executing it.
func (c *Coordinator) Explain(ctx context.Context, req Request) (*Explanation, error) {
	p, err := c.prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	expl := &Explanation{
		Mode:     modeFor(p),
		Excluded: p.resolved.Excluded,
		Coverage: p.resolved.Coverage,
		Warnings: p.resolved.Warnings,
		Merge: MergeExplain{
			RowMode: p.query.RowMode,
			Having:  p.query.Having != nil,
			OrderBy: len(p.query.OrderBy),
			Limit:   p.query.Limit,
			Offset:  p.query.Offset,
		},
	}
	expl.Merge.GroupKeys = len(p.query.Groups)
	for _, agg := range p.query.Aggs {
		expl.Merge.Aggregates = append(expl.Merge.Aggregates, agg.Alias)
	}
	for _, ep := range p.resolved.Shards {
		expl.Selected = append(expl.Selected, ep.ID)
	}
	for _, e := range p.entries {
		expl.Pushdown = append(expl.Pushdown, PushdownExplain{
			Endpoint: e.endpoint.ID,
			Backend:  e.endpoint.Backend,
			Source:   e.source,
			SQL:      e.sql,
		})
	}
	return expl, nil
}

// Query executes a federated query.
func (c *Coordinator) Query(ctx context.Context, req Request) (*Result, error) {
	p, err := c.prepare(ctx, req)
	if err != nil {
		return nil, err
	}
	outcomes := make([]outcome, len(p.entries))
	sem := make(chan struct{}, max(1, len(p.entries)))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i, entry := range p.entries {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, entry pushdownEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			outcomes[i] = c.runOne(runCtx, entry, p)
			if outcomes[i].err != nil && !req.AllowPartial {
				cancel()
			}
		}(i, entry)
	}
	wg.Wait()

	streams := make([]merge.Stream, 0, len(outcomes))
	stats := make([]BackendStats, 0, len(outcomes))
	warnings := append([]string(nil), p.resolved.Warnings...)
	coverage := p.resolved.Coverage
	var firstErr error
	for _, out := range outcomes {
		stats = append(stats, out.stat)
		if out.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("endpoint %s: %w", out.stat.ID, out.err)
			}
			coverage.Complete = false
			coverage.MissingEndpoints = append(coverage.MissingEndpoints, out.stat.ID)
			warnings = append(warnings, fmt.Sprintf("endpoint %s failed: %v", out.stat.ID, out.err))
			continue
		}
		if out.stream != nil {
			streams = append(streams, *out.stream)
		}
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].EndpointID < streams[j].EndpointID })
	if firstErr != nil && (!req.AllowPartial || len(streams) == 0) {
		return nil, firstErr
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].ID < stats[j].ID })

	merged, err := merge.Run(p.query, streams)
	if err != nil {
		return nil, err
	}
	rows := make([][]any, len(merged.Rows))
	for i, row := range merged.Rows {
		vals := make([]any, len(row))
		for j, v := range row {
			vals[j] = v.ToAny()
		}
		rows[i] = vals
	}
	return &Result{
		Columns:  merged.Columns,
		Rows:     rows,
		RowCount: len(rows),
		Coverage: coverage,
		Backends: stats,
		Excluded: p.resolved.Excluded,
		Warnings: warnings,
		Partial:  !coverage.Complete,
		Mode:     modeFor(p),
	}, nil
}

type outcome struct {
	stream *merge.Stream
	stat   BackendStats
	err    error
}

func (c *Coordinator) runOne(ctx context.Context, entry pushdownEntry, p *prepared) outcome {
	stat := BackendStats{ID: entry.endpoint.ID, Backend: entry.endpoint.Backend}
	started := time.Now()
	backend, ok := c.Backends[entry.endpoint.ID]
	if !ok {
		stat.DurationMs = time.Since(started).Milliseconds()
		return outcome{stat: stat, err: fmt.Errorf("no backend registered for endpoint %q", entry.endpoint.ID)}
	}
	schema := expectedSchema(p.query)
	stream, err := backend.Query(ctx, PartialRequest{
		Endpoint: entry.endpoint,
		Dataset:  p.query.Table.Name,
		SQL:      entry.sql,
		From:     p.from,
		To:       p.to,
		Schema:   schema,
		Types:    expectedTypes(p.query),
		Scope:    p.scope,
	})
	stat.DurationMs = time.Since(started).Milliseconds()
	if err != nil {
		return outcome{stat: stat, err: err}
	}
	if stream == nil {
		return outcome{stat: stat, err: fmt.Errorf("backend returned no stream")}
	}
	if stream.EndpointID == "" {
		stream.EndpointID = entry.endpoint.ID
	}
	stat.Rows = len(stream.Rows)
	return outcome{stream: stream, stat: stat}
}

func expectedTypes(q *plan.Query) []catalog.Type {
	out := make([]catalog.Type, 0, len(q.Groups)+4)
	for _, g := range q.Groups {
		t, err := plan.TypeOf(g, q.Table)
		if err != nil {
			t = catalog.TypeString
		}
		out = append(out, t)
	}
	for _, agg := range q.Aggs {
		for _, p := range agg.Partials {
			switch {
			case p.Func == "count":
				out = append(out, catalog.TypeInt64)
			case p.Func == "sum" && p.Type != catalog.TypeFloat64:
				out = append(out, catalog.TypeDecimal)
			default:
				out = append(out, p.Type)
			}
		}
	}
	return out
}

func expectedSchema(q *plan.Query) []string {
	if q.RowMode {
		out := make([]string, 0, len(q.Output))
		for _, o := range q.Output {
			out = append(out, o.Alias)
		}
		return out
	}
	out := make([]string, 0, len(q.Groups)+4)
	for i := range q.Groups {
		out = append(out, fmt.Sprintf("k%d", i))
	}
	for _, agg := range q.Aggs {
		for _, p := range agg.Partials {
			out = append(out, p.Alias)
		}
	}
	return out
}

func (c *Coordinator) prepare(ctx context.Context, req Request) (*prepared, error) {
	if c == nil || c.Catalog == nil {
		return nil, fmt.Errorf("coordinator has no catalog")
	}
	sel, err := sql.Parse(req.SQL)
	if err != nil {
		return nil, err
	}
	query, err := plan.Bind(c.Catalog, sel)
	if err != nil {
		return nil, err
	}
	from, to := req.From, req.To
	if to.IsZero() {
		to = time.Now().UTC()
	}
	if from.IsZero() {
		from = to.Add(-time.Hour)
	}
	if !to.After(from) {
		return nil, fmt.Errorf("from must be before to")
	}
	if req.Limit > 0 {
		if !query.HasLimit || req.Limit < query.Limit {
			query.Limit = req.Limit
			query.HasLimit = true
		}
	}
	resolved, err := selector.Resolve(c.Catalog, query.Table.Name, req.Selector, catalog.TimeRange{From: from, To: to}, selector.Options{
		AllowPartial:             req.AllowPartial,
		Signal:                   query.Table.Signal,
		ExcludeReplicasByDefault: c.Catalog.ExcludeReplicasByDefault(),
	})
	if err != nil {
		return nil, err
	}
	p := &prepared{query: query, resolved: resolved, from: from, to: to, scope: req.Scope}
	for _, ep := range resolved.Shards {
		d, ok := dialect.ForBackend(ep.Backend)
		if !ok {
			return nil, fmt.Errorf("endpoint %q: no dialect for backend %q", ep.ID, ep.Backend)
		}
		pushdown, err := rewrite.Build(query, ep, from, to, rewrite.Scope{
			Restricted: req.Scope.Restricted,
			Partitions: req.Scope.Partitions,
			Bindings:   req.Scope.Bindings,
		})
		if err != nil {
			return nil, err
		}
		emitter := &dialect.Emitter{D: d, EP: ep}
		statement, err := emitter.Emit(pushdown)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", ep.ID, err)
		}
		p.entries = append(p.entries, pushdownEntry{endpoint: ep, sql: statement, source: pushdown.Source})
	}
	return p, nil
}

func modeFor(p *prepared) string {
	if len(p.entries) == 1 {
		return "passthrough"
	}
	return "federated"
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
