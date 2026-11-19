// Package rewrite builds the pushdown plan sent to one endpoint.
package rewrite

import (
	"errors"
	"fmt"
	"time"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/plan"
)

// ErrScopeNotEnforceable reports that a restricted query selected an endpoint
// whose partition binding cannot express the visible partitions. Callers must
// surface this as PermissionDenied, never as an empty result.
var ErrScopeNotEnforceable = errors.New("scope_not_enforceable")

// Projection is a row-mode output column.
type Projection struct {
	Alias string
	Expr  ast.Expr
	Type  catalog.Type
}

// AccountRegionPair maps one visible partition to an AWS account/region.
type AccountRegionPair struct {
	Account string
	Region  string
}

// PartitionFilter is the structured partition constraint for one endpoint.
// The emitter renders it per dialect; the coordinator never accepts raw SQL.
type PartitionFilter struct {
	Kind string

	Column string
	Path   []string

	AccountField    string
	RegionField     string
	DataSourceField string
	LogGroupField   string

	Partitions []string
	Pairs      []AccountRegionPair
	Values     []string
}

// Pushdown is the per-endpoint SELECT. It contains group keys and partial
// aggregates only; HAVING, final ORDER BY, LIMIT and OFFSET stay local.
type Pushdown struct {
	Source       string
	Backend      string
	From, To     time.Time
	TimeColumn   ast.Expr
	GroupBy      []ast.Expr
	GroupAliases []string
	GroupTypes   []catalog.Type
	Partials     []plan.Partial
	Projections  []Projection
	Filters      []ast.Expr
	Partition    *PartitionFilter
	RowMode      bool
}

// Scope is the caller's authorized partition scope. When restricted, the
// coordinator injects the predicate (or refuses an endpoint that cannot
// enforce it).
type Scope struct {
	Restricted bool
	Partitions []string
	Bindings   []catalog.PartitionBindingValue
}

// Build creates the pushdown for one endpoint.
func Build(q *plan.Query, ep *catalog.Endpoint, from, to time.Time, scope Scope) (*Pushdown, error) {
	if q == nil || q.Table == nil {
		return nil, fmt.Errorf("rewrite: nil query")
	}
	source := q.Table.Name
	if ep != nil && ep.Source != "" {
		source = ep.Source
	}
	p := &Pushdown{
		Source:     source,
		From:       from,
		To:         to,
		TimeColumn: &ast.ColumnRef{Name: q.Table.TimeColumn},
		Filters:    nil,
		RowMode:    q.RowMode,
	}
	if scope.Restricted {
		filter, err := buildPartitionFilter(q, ep, scope)
		if err != nil {
			return nil, err
		}
		p.Partition = filter
	}
	if q.Where != nil {
		p.Filters = append(p.Filters, q.Where)
	}
	if ep != nil {
		p.Backend = ep.Backend
	}
	if q.RowMode {
		for _, out := range q.Output {
			if out.Expr == nil {
				return nil, fmt.Errorf("rewrite: row mode output %q has no expression", out.Alias)
			}
			t, err := plan.TypeOf(out.Expr, q.Table)
			if err != nil {
				return nil, fmt.Errorf("rewrite: %w", err)
			}
			p.Projections = append(p.Projections, Projection{Alias: out.Alias, Expr: out.Expr, Type: t})
		}
		return p, nil
	}
	for i, g := range q.Groups {
		if i >= 63 {
			return nil, fmt.Errorf("rewrite: too many group keys (max 63)")
		}
		t, err := plan.TypeOf(g, q.Table)
		if err != nil {
			return nil, fmt.Errorf("rewrite: %w", err)
		}
		p.GroupBy = append(p.GroupBy, g)
		p.GroupAliases = append(p.GroupAliases, fmt.Sprintf("k%d", i))
		p.GroupTypes = append(p.GroupTypes, t)
	}
	for _, agg := range q.Aggs {
		p.Partials = append(p.Partials, agg.Partials...)
	}
	if len(p.Partials) == 0 && len(p.GroupBy) == 0 {
		return nil, fmt.Errorf("rewrite: query has no aggregates and is not row mode")
	}
	return p, nil
}

func buildPartitionFilter(q *plan.Query, ep *catalog.Endpoint, scope Scope) (*PartitionFilter, error) {
	if len(scope.Partitions) == 0 {
		return nil, scopeError(ep, "restricted scope has no visible partitions")
	}
	binding := (*catalog.PartitionBinding)(nil)
	if ep != nil {
		binding = ep.Partition
	}
	if binding == nil {
		if _, ok := q.Table.Column("partition"); ok {
			// Backward-compatible default for endpoints that predate bindings.
			binding = &catalog.PartitionBinding{Kind: "column", Column: "partition"}
		} else if ep != nil && ep.Caps.EnforcesPartitionScope {
			return nil, nil
		} else {
			return nil, scopeError(ep, "endpoint cannot enforce partition scope")
		}
	}
	filter := &PartitionFilter{
		Kind:            binding.Kind,
		Column:          binding.Column,
		Path:            append([]string(nil), binding.Path...),
		AccountField:    binding.AccountField,
		RegionField:     binding.RegionField,
		DataSourceField: binding.DataSourceField,
		LogGroupField:   binding.LogGroupField,
	}
	switch binding.Kind {
	case "column":
		if filter.Column == "" {
			filter.Column = "partition"
		}
		filter.Partitions = append([]string(nil), scope.Partitions...)
		return filter, nil
	case "attribute":
		if len(binding.Path) < 2 {
			return nil, scopeError(ep, "attribute binding needs a map and a key")
		}
		values := attributeValues(endpointBinding(ep), scope, binding.Path)
		filter.Values = values
		return filter, nil
	case "account_region":
		pairs, err := accountRegionPairs(endpointBinding(ep), scope)
		if err != nil {
			return nil, err
		}
		filter.Pairs = pairs
		return filter, nil
	case "data_source":
		values, err := bindingValues(endpointBinding(ep), scope, func(v catalog.PartitionBindingValue) string { return v.DataSource })
		if err != nil {
			return nil, err
		}
		filter.Values = values
		return filter, nil
	case "log_group":
		values, err := logGroupValues(endpointBinding(ep), scope)
		if err != nil {
			return nil, err
		}
		filter.Values = values
		return filter, nil
	case "fixed":
		for _, partition := range scope.Partitions {
			if !containsString(binding.Values, partition) {
				return nil, scopeError(ep, fmt.Sprintf("partition %q is outside the endpoint's fixed values", partition))
			}
		}
		return nil, nil
	default:
		return nil, scopeError(ep, fmt.Sprintf("unknown partition binding kind %q", binding.Kind))
	}
}

func endpointBinding(ep *catalog.Endpoint) *catalog.PartitionBindingValue {
	if ep == nil {
		return nil
	}
	return ep.Binding
}

func attributeValues(epBinding *catalog.PartitionBindingValue, scope Scope, path []string) []string {
	key := path[len(path)-1]
	if epBinding != nil {
		if v, ok := epBinding.Attributes[key]; ok && v != "" {
			return []string{v}
		}
		if epBinding.Partition != "" {
			return []string{epBinding.Partition}
		}
	}
	byPartition := make(map[string]catalog.PartitionBindingValue, len(scope.Bindings))
	for _, binding := range scope.Bindings {
		byPartition[binding.Partition] = binding
	}
	out := make([]string, 0, len(scope.Partitions))
	for _, partition := range scope.Partitions {
		value := partition
		if binding, ok := byPartition[partition]; ok {
			if v, ok := binding.Attributes[key]; ok && v != "" {
				value = v
			}
		}
		out = append(out, value)
	}
	return out
}

func accountRegionPairs(epBinding *catalog.PartitionBindingValue, scope Scope) ([]AccountRegionPair, error) {
	if epBinding != nil {
		if epBinding.Account == "" && epBinding.Region == "" {
			return nil, fmt.Errorf("%w: endpoint binding has no account/region", ErrScopeNotEnforceable)
		}
		return []AccountRegionPair{{Account: epBinding.Account, Region: epBinding.Region}}, nil
	}
	byPartition := map[string][]AccountRegionPair{}
	for _, binding := range scope.Bindings {
		if binding.Account == "" && binding.Region == "" {
			continue
		}
		byPartition[binding.Partition] = append(byPartition[binding.Partition], AccountRegionPair{
			Account: binding.Account,
			Region:  binding.Region,
		})
	}
	var out []AccountRegionPair
	seen := map[string]bool{}
	for _, partition := range scope.Partitions {
		pairs := byPartition[partition]
		if len(pairs) == 0 {
			return nil, fmt.Errorf("%w: partition %q has no account/region binding", ErrScopeNotEnforceable, partition)
		}
		for _, pair := range pairs {
			key := pair.Account + "\x00" + pair.Region
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, pair)
		}
	}
	return out, nil
}

func bindingValues(epBinding *catalog.PartitionBindingValue, scope Scope, pick func(catalog.PartitionBindingValue) string) ([]string, error) {
	if epBinding != nil {
		value := pick(*epBinding)
		if value == "" {
			return nil, fmt.Errorf("%w: endpoint binding has an empty value", ErrScopeNotEnforceable)
		}
		return []string{value}, nil
	}
	byPartition := make(map[string]catalog.PartitionBindingValue, len(scope.Bindings))
	for _, binding := range scope.Bindings {
		byPartition[binding.Partition] = binding
	}
	out := make([]string, 0, len(scope.Partitions))
	for _, partition := range scope.Partitions {
		binding, ok := byPartition[partition]
		if !ok {
			return nil, fmt.Errorf("%w: partition %q has no binding value", ErrScopeNotEnforceable, partition)
		}
		value := pick(binding)
		if value == "" {
			return nil, fmt.Errorf("%w: partition %q has an empty binding value", ErrScopeNotEnforceable, partition)
		}
		out = append(out, value)
	}
	return out, nil
}

func logGroupValues(epBinding *catalog.PartitionBindingValue, scope Scope) ([]string, error) {
	if epBinding != nil {
		if len(epBinding.LogGroups) > 0 {
			return append([]string(nil), epBinding.LogGroups...), nil
		}
		if epBinding.LogGroup != "" {
			return []string{epBinding.LogGroup}, nil
		}
		return nil, fmt.Errorf("%w: endpoint binding has no log groups", ErrScopeNotEnforceable)
	}
	byPartition := make(map[string]catalog.PartitionBindingValue, len(scope.Bindings))
	for _, binding := range scope.Bindings {
		byPartition[binding.Partition] = binding
	}
	out := make([]string, 0, len(scope.Partitions))
	for _, partition := range scope.Partitions {
		binding, ok := byPartition[partition]
		if !ok {
			return nil, fmt.Errorf("%w: partition %q has no log group binding", ErrScopeNotEnforceable, partition)
		}
		switch {
		case len(binding.LogGroups) > 0:
			out = append(out, binding.LogGroups...)
		case binding.LogGroup != "":
			out = append(out, binding.LogGroup)
		default:
			return nil, fmt.Errorf("%w: partition %q has an empty log group binding", ErrScopeNotEnforceable, partition)
		}
	}
	return out, nil
}

func scopeError(ep *catalog.Endpoint, message string) error {
	if ep != nil && ep.ID != "" {
		return fmt.Errorf("%w: endpoint %q: %s", ErrScopeNotEnforceable, ep.ID, message)
	}
	return fmt.Errorf("%w: %s", ErrScopeNotEnforceable, message)
}

func containsString(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}
