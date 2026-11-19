// Package selector resolves a request-level selector against the endpoint
// registry and validates coverage before any query is planned.
package selector

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/radryc/sqlagregator/internal/catalog"
)

// Matcher selects endpoints. All populated fields must match (AND); an empty
// matcher matches every endpoint.
type Matcher struct {
	IDs      []string          `json:"ids,omitempty"`
	Backends []string          `json:"backends,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
	Roles    []string          `json:"roles,omitempty"`
	Groups   []string          `json:"groups,omitempty"`
}

// Selector is the request-level endpoint selection policy.
type Selector struct {
	Include         []Matcher `json:"include,omitempty"`
	Exclude         []Matcher `json:"exclude,omitempty"`
	TreatAsReplica  []Matcher `json:"treat_as_replica,omitempty"`
	IncludeReplicas bool      `json:"include_replicas,omitempty"`
	Max             int       `json:"max,omitempty"`
	MinCoverage     float64   `json:"min_coverage,omitempty"`
}

// Options are resolution inputs that do not belong to the request selector.
type Options struct {
	AllowPartial             bool
	Signal                   string
	ExcludeReplicasByDefault bool
}

// Exclusion records why an endpoint was not used.
type Exclusion struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

const (
	ReasonReplica           = "replica"
	ReasonRequestExclude    = "request_exclude"
	ReasonNotIncluded       = "not_included"
	ReasonMissingCapability = "missing_capability"
	ReasonDisabled          = "disabled"
	ReasonUnhealthy         = "unhealthy"
	ReasonExceedsMax        = "exceeds_max"
)

// GroupCoverage is per coverage group.
type GroupCoverage struct {
	Group    string              `json:"group"`
	Mode     string              `json:"mode"`
	Selected int                 `json:"selected"`
	Required int                 `json:"required"`
	Complete bool                `json:"complete"`
	Missing  []string            `json:"missing,omitempty"`
	Gaps     []catalog.TimeRange `json:"gaps,omitempty"`
}

// CoverageInfo describes whether the selected shards cover the query window.
type CoverageInfo struct {
	Complete         bool                `json:"complete"`
	Covered          []catalog.TimeRange `json:"covered,omitempty"`
	Gaps             []catalog.TimeRange `json:"gaps,omitempty"`
	MissingEndpoints []string            `json:"missing_endpoints,omitempty"`
	Groups           []GroupCoverage     `json:"groups,omitempty"`
}

// Resolution is the execution set.
type Resolution struct {
	Shards   []*catalog.Endpoint `json:"-"`
	Replicas []*catalog.Endpoint `json:"-"`
	Excluded []Exclusion         `json:"excluded"`
	Coverage CoverageInfo        `json:"coverage"`
	Warnings []string            `json:"warnings,omitempty"`
}

// CoverageError is a hard, structured selection failure.
type CoverageError struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Missing []string            `json:"missing,omitempty"`
	Gaps    []catalog.TimeRange `json:"gaps,omitempty"`
}

func (e *CoverageError) Error() string {
	msg := e.Code + ": " + e.Message
	if len(e.Missing) > 0 {
		msg += " (missing: " + strings.Join(e.Missing, ", ") + ")"
	}
	return msg
}

// Resolve applies the selector and validates coverage.
func Resolve(cat *catalog.Catalog, table string, sel Selector, window catalog.TimeRange, opts Options) (*Resolution, error) {
	if !window.To.After(window.From) {
		return nil, &CoverageError{Code: "invalid_window", Message: "from must be before to"}
	}
	endpoints := cat.EndpointsFor(table)
	if len(endpoints) == 0 {
		return nil, &CoverageError{Code: "no_endpoints", Message: fmt.Sprintf("no endpoints serve table %q", table)}
	}

	res := &Resolution{Excluded: []Exclusion{}}
	var shards, replicas []*catalog.Endpoint

	for _, ep := range endpoints {
		role := effectiveRole(ep, sel.TreatAsReplica)
		reason := rejectReason(ep, role, sel, cat.ExcludeReplicasByDefault(), opts.Signal)
		if reason != "" {
			res.Excluded = append(res.Excluded, Exclusion{ID: ep.ID, Reason: reason})
			continue
		}
		if role == catalog.RoleReplica {
			replicas = append(replicas, ep)
			continue
		}
		shards = append(shards, ep)
	}

	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].ID < replicas[j].ID })
	sort.Slice(res.Excluded, func(i, j int) bool { return res.Excluded[i].ID < res.Excluded[j].ID })

	if len(shards) == 0 {
		return nil, &CoverageError{
			Code:    "no_shards",
			Message: "selector left no shard endpoints; refusing to return an empty aggregate",
		}
	}
	if sel.Max > 0 && len(shards) > sel.Max {
		return nil, &CoverageError{
			Code:    "exceeds_max",
			Message: fmt.Sprintf("selector matched %d shards, max is %d", len(shards), sel.Max),
		}
	}

	cov, err := coverage(cat, table, shards, window)
	if err != nil {
		return nil, err
	}
	res.Shards, res.Replicas, res.Coverage = shards, replicas, cov

	if sel.MinCoverage > 0 {
		fraction := coverageFraction(cov, shards, window)
		if fraction+1e-9 < sel.MinCoverage {
			return nil, &CoverageError{
				Code:    "coverage_below_min",
				Message: fmt.Sprintf("coverage %.3f is below min_coverage %.3f", fraction, sel.MinCoverage),
				Missing: cov.MissingEndpoints,
				Gaps:    cov.Gaps,
			}
		}
	}
	if !cov.Complete {
		if !opts.AllowPartial {
			return nil, &CoverageError{
				Code:    "coverage_gap",
				Message: "selected endpoints do not cover the requested window",
				Missing: cov.MissingEndpoints,
				Gaps:    cov.Gaps,
			}
		}
		res.Warnings = append(res.Warnings,
			"partial result: coverage is incomplete; see coverage.missing_endpoints and coverage.gaps")
	}
	return res, nil
}

func effectiveRole(ep *catalog.Endpoint, treat []Matcher) catalog.Role {
	if matchesAny(treat, ep, ep.Role) {
		return catalog.RoleReplica
	}
	if ep.Role == "" {
		return catalog.RoleShard
	}
	return ep.Role
}

func rejectReason(ep *catalog.Endpoint, role catalog.Role, sel Selector, excludeReplicas bool, signal string) string {
	if ep.Disabled {
		return ReasonDisabled
	}
	if len(sel.Include) > 0 && !matchesAny(sel.Include, ep, role) {
		return ReasonNotIncluded
	}
	if matchesAny(sel.Exclude, ep, role) {
		return ReasonRequestExclude
	}
	if role == catalog.RoleReplica && !sel.IncludeReplicas && excludeReplicas {
		return ReasonReplica
	}
	if signal != "" && !ep.Caps.SupportsSignal(signal) {
		return ReasonMissingCapability
	}
	return ""
}

func matchesAny(matchers []Matcher, ep *catalog.Endpoint, role catalog.Role) bool {
	for _, m := range matchers {
		if m.matches(ep, role) {
			return true
		}
	}
	return false
}

func (m Matcher) matches(ep *catalog.Endpoint, role catalog.Role) bool {
	if len(m.IDs) > 0 && !globAny(m.IDs, ep.ID) {
		return false
	}
	if len(m.Backends) > 0 && !globAny(m.Backends, ep.Backend) {
		return false
	}
	if len(m.Roles) > 0 && !globAny(m.Roles, string(role)) {
		return false
	}
	if len(m.Groups) > 0 && !globAny(m.Groups, ep.CoverageGroup) {
		return false
	}
	for k, pattern := range m.Tags {
		value, ok := ep.Tags[k]
		if !ok || !glob(pattern, value) {
			return false
		}
	}
	return true
}

func globAny(patterns []string, value string) bool {
	for _, p := range patterns {
		if glob(p, value) {
			return true
		}
	}
	return false
}

func glob(pattern, value string) bool {
	if pattern == value {
		return true
	}
	ok, err := path.Match(pattern, value)
	return err == nil && ok
}

func coverage(cat *catalog.Catalog, table string, shards []*catalog.Endpoint, window catalog.TimeRange) (CoverageInfo, error) {
	byGroup := map[string][]*catalog.Endpoint{}
	for _, ep := range shards {
		byGroup[ep.CoverageGroup] = append(byGroup[ep.CoverageGroup], ep)
	}
	names := make([]string, 0, len(byGroup))
	for name := range byGroup {
		names = append(names, name)
	}
	sort.Strings(names)

	info := CoverageInfo{Complete: true}
	var covered []catalog.TimeRange
	for _, name := range names {
		group := cat.Group(name)
		members := byGroup[name]
		gc := GroupCoverage{
			Group:    name,
			Mode:     group.Mode,
			Selected: len(members),
			Required: len(members),
		}
		switch group.Mode {
		case "hash", "replica":
			required := group.Members
			if required <= 0 {
				required = shardCount(cat, table, name)
			}
			gc.Required = required
			gc.Complete = len(members) >= required
			if !gc.Complete {
				gc.Missing = missingMembers(cat, table, name, members, required)
				info.Complete = false
				info.MissingEndpoints = append(info.MissingEndpoints, gc.Missing...)
			}
		case "time":
			intervals := make([]catalog.TimeRange, 0, len(members))
			for _, ep := range members {
				r := window
				if ep.Range != nil {
					r = *ep.Range
				}
				intervals = append(intervals, r)
			}
			sort.Slice(intervals, func(i, j int) bool { return intervals[i].From.Before(intervals[j].From) })
			for i := 1; i < len(intervals); i++ {
				if intervals[i].From.Before(intervals[i-1].To) {
					return info, &CoverageError{
						Code: "coverage_overlap",
						Message: fmt.Sprintf("shards in group %q overlap in time near %s; refusing to double count",
							name, intervals[i].From.Format(time.RFC3339)),
					}
				}
			}
			covered = append(covered, intervals...)
			merged := mergeIntervals(intervals)
			gaps := subtract(window, merged)
			gc.Gaps = gaps
			if len(gaps) > 0 {
				gc.Complete = false
				info.Complete = false
				info.Gaps = append(info.Gaps, gaps...)
			}
		default:
			return info, &CoverageError{Code: "unknown_group_mode", Message: "unknown coverage mode " + group.Mode}
		}
		info.Groups = append(info.Groups, gc)
	}
	info.Covered = mergeIntervals(covered)
	if info.Covered == nil {
		info.Covered = []catalog.TimeRange{window}
	}
	return info, nil
}

func shardCount(cat *catalog.Catalog, table, group string) int {
	n := 0
	for _, ep := range cat.EndpointsFor(table) {
		if ep.Disabled {
			continue
		}
		if ep.Role == catalog.RoleShard && ep.CoverageGroup == group {
			n++
		}
	}
	return n
}

func missingMembers(cat *catalog.Catalog, table, group string, selected []*catalog.Endpoint, required int) []string {
	have := make(map[string]struct{}, len(selected))
	for _, ep := range selected {
		have[ep.ID] = struct{}{}
	}
	var missing []string
	for _, ep := range cat.EndpointsFor(table) {
		if ep.Disabled || ep.Role != catalog.RoleShard || ep.CoverageGroup != group {
			continue
		}
		if _, ok := have[ep.ID]; !ok {
			missing = append(missing, ep.ID)
		}
	}
	if len(missing) == 0 && required > len(selected) {
		missing = append(missing, fmt.Sprintf("(%d of %d shards selected)", len(selected), required))
	}
	sort.Strings(missing)
	return missing
}

func coverageFraction(cov CoverageInfo, shards []*catalog.Endpoint, window catalog.TimeRange) float64 {
	total := window.To.Sub(window.From)
	if total <= 0 {
		return 1
	}
	if len(cov.Groups) == 0 {
		return 1
	}
	worst := 1.0
	for _, g := range cov.Groups {
		var f float64
		switch g.Mode {
		case "hash", "replica":
			if g.Required <= 0 {
				f = 1
			} else {
				f = float64(g.Selected) / float64(g.Required)
			}
		case "time":
			gap := time.Duration(0)
			for _, r := range g.Gaps {
				if r.To.After(r.From) {
					gap += r.To.Sub(r.From)
				}
			}
			f = 1 - float64(gap)/float64(total)
		default:
			f = 1
		}
		if f < worst {
			worst = f
		}
	}
	return worst
}

func mergeIntervals(in []catalog.TimeRange) []catalog.TimeRange {
	if len(in) == 0 {
		return nil
	}
	sorted := append([]catalog.TimeRange(nil), in...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].From.Before(sorted[j].From) })
	out := []catalog.TimeRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &out[len(out)-1]
		if !r.From.After(last.To) {
			if r.To.After(last.To) {
				last.To = r.To
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

func subtract(window catalog.TimeRange, covered []catalog.TimeRange) []catalog.TimeRange {
	var gaps []catalog.TimeRange
	cursor := window.From
	for _, r := range covered {
		if r.To.Before(window.From) || r.From.After(window.To) {
			continue
		}
		if r.From.After(cursor) {
			gaps = append(gaps, catalog.TimeRange{From: cursor, To: r.From})
		}
		if r.To.After(cursor) {
			cursor = r.To
		}
	}
	if cursor.Before(window.To) {
		gaps = append(gaps, catalog.TimeRange{From: cursor, To: window.To})
	}
	return gaps
}
