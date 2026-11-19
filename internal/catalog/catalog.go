// Package catalog holds the logical tables, canonical column types, and the
// endpoint registry that the planner and selector operate on.
package catalog

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Type is the canonical column type shared by every backend.
type Type string

const (
	TypeInt64     Type = "int64"
	TypeUint64    Type = "uint64"
	TypeFloat64   Type = "float64"
	TypeDecimal   Type = "decimal"
	TypeString    Type = "string"
	TypeBool      Type = "bool"
	TypeTimestamp Type = "timestamp"
	TypeJSON      Type = "json"
)

// Column is one logical column.
type Column struct {
	Name     string `yaml:"name" json:"name"`
	Type     Type   `yaml:"type" json:"type"`
	Nullable bool   `yaml:"nullable" json:"nullable"`
}

// Table is a logical table exposed to users (logs, traces, metrics, ...).
type Table struct {
	Name       string   `yaml:"-" json:"name"`
	Signal     string   `yaml:"signal" json:"signal"`
	TimeColumn string   `yaml:"time_column" json:"time_column"`
	Columns    []Column `yaml:"columns" json:"columns"`

	index map[string]Column
}

// Role describes whether an endpoint is load-bearing or redundant.
type Role string

const (
	RoleShard   Role = "shard"
	RoleReplica Role = "replica"
)

// Capabilities describes what an endpoint can answer.
type Capabilities struct {
	Signals                []string `yaml:"signals" json:"signals,omitempty"`
	EnforcesPartitionScope bool     `yaml:"enforces_partition_scope" json:"enforces_partition_scope"`
	MaxRows                int      `yaml:"max_rows" json:"max_rows,omitempty"`
}

// SupportsSignal reports whether the endpoint can answer the signal.
func (c Capabilities) SupportsSignal(signal string) bool {
	if signal == "" || len(c.Signals) == 0 {
		return true
	}
	for _, s := range c.Signals {
		if s == signal {
			return true
		}
	}
	return false
}

// PartitionBinding describes how a placement constrains itself to the
// caller's visible partitions.
type PartitionBinding struct {
	Kind string `yaml:"kind" json:"kind"`

	Column string   `yaml:"column,omitempty" json:"column,omitempty"`
	Path   []string `yaml:"path,omitempty" json:"path,omitempty"`

	AccountField    string `yaml:"account_field,omitempty" json:"account_field,omitempty"`
	RegionField     string `yaml:"region_field,omitempty" json:"region_field,omitempty"`
	DataSourceField string `yaml:"data_source_field,omitempty" json:"data_source_field,omitempty"`
	LogGroupField   string `yaml:"log_group_field,omitempty" json:"log_group_field,omitempty"`

	Values []string `yaml:"values,omitempty" json:"values,omitempty"`
}

// PartitionBindingValue maps one visible partition to the native identifiers a
// backend stores.
type PartitionBindingValue struct {
	Partition  string            `json:"partition"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Account    string            `json:"account,omitempty"`
	Region     string            `json:"region,omitempty"`
	DataSource string            `json:"data_source,omitempty"`
	LogGroup   string            `json:"log_group,omitempty"`
	LogGroups  []string          `json:"log_groups,omitempty"`
}

// Endpoint is one queryable physical backend.
type Endpoint struct {
	ID            string                 `yaml:"id" json:"id"`
	Backend       string                 `yaml:"backend" json:"backend"`
	Tables        []string               `yaml:"tables" json:"tables"`
	Role          Role                   `yaml:"role" json:"role"`
	CoverageGroup string                 `yaml:"coverage_group" json:"coverage_group"`
	ReplicaOf     string                 `yaml:"replica_of" json:"replica_of,omitempty"`
	Range         *TimeRange             `yaml:"range" json:"range,omitempty"`
	Source        string                 `yaml:"source" json:"source"`
	Tags          map[string]string      `yaml:"tags" json:"tags,omitempty"`
	Columns       map[string]string      `yaml:"columns" json:"columns,omitempty"`
	Partition     *PartitionBinding      `yaml:"partition,omitempty" json:"partition,omitempty"`
	Binding       *PartitionBindingValue `yaml:"-" json:"binding,omitempty"`
	Caps          Capabilities           `yaml:"caps" json:"caps"`
	Disabled      bool                   `yaml:"disabled" json:"disabled"`
}

// Serves reports whether the endpoint serves the logical table.
func (e *Endpoint) Serves(table string) bool {
	if len(e.Tables) == 0 {
		return true
	}
	for _, t := range e.Tables {
		if t == table {
			return true
		}
	}
	return false
}

// TimeRange is a half-open [From, To) interval.
type TimeRange struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// UnmarshalYAML accepts RFC3339 strings for from/to.
func (r *TimeRange) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		From string `yaml:"from"`
		To   string `yaml:"to"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	from, err := time.Parse(time.RFC3339, raw.From)
	if err != nil {
		return fmt.Errorf("range.from: %w", err)
	}
	to, err := time.Parse(time.RFC3339, raw.To)
	if err != nil {
		return fmt.Errorf("range.to: %w", err)
	}
	r.From, r.To = from.UTC(), to.UTC()
	return nil
}

// CoverageGroup describes how members of a group partition the data.
type CoverageGroup struct {
	// Mode is one of "hash" (disjoint key shards, every member required),
	// "time" (disjoint time ranges) or "replica" (redundant copies).
	Mode    string `yaml:"mode" json:"mode"`
	Members int    `yaml:"members" json:"members,omitempty"`
	// AllowMissing lets Members declare an expected cluster size that is
	// larger than the currently registered endpoints (for example when a node
	// is down). The gap is reported at query time instead of failing startup.
	AllowMissing bool `yaml:"allow_missing" json:"allow_missing,omitempty"`
}

// BackendDefaults are inherited by endpoints of the same backend kind.
type BackendDefaults struct {
	Signals                []string `yaml:"signals" json:"signals,omitempty"`
	EnforcesPartitionScope bool     `yaml:"enforces_partition_scope" json:"enforces_partition_scope"`
}

// Defaults are deployment-wide selection defaults.
type Defaults struct {
	ExcludeReplicas *bool `yaml:"exclude_replicas" json:"exclude_replicas,omitempty"`
}

// Catalog is the loaded, validated registry.
type Catalog struct {
	Tables        map[string]*Table
	Endpoints     map[string]*Endpoint
	Groups        map[string]*CoverageGroup
	Backends      map[string]*BackendDefaults
	Defaults      Defaults
	endpointOrder []string
}

type config struct {
	Tables         map[string]*Table           `yaml:"tables"`
	Endpoints      []*Endpoint                 `yaml:"endpoints"`
	CoverageGroups map[string]*CoverageGroup   `yaml:"coverage_groups"`
	Backends       map[string]*BackendDefaults `yaml:"backends"`
	Defaults       Defaults                    `yaml:"defaults"`
}

// LoadFile reads and validates a YAML catalog.
func LoadFile(path string) (*Catalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read catalog: %w", err)
	}
	return LoadBytes(data)
}

// LoadBytes parses and validates a YAML catalog.
func LoadBytes(data []byte) (*Catalog, error) {
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse catalog: %w", err)
	}
	return New(cfg.Tables, cfg.Endpoints, cfg.CoverageGroups, cfg.Backends, cfg.Defaults)
}

// New assembles and validates a catalog from programmatic inputs. It is used
// by the public facade and by services that discover endpoints at runtime.
func New(
	tables map[string]*Table,
	endpoints []*Endpoint,
	groups map[string]*CoverageGroup,
	backends map[string]*BackendDefaults,
	defaults Defaults,
) (*Catalog, error) {
	cat := &Catalog{
		Tables:    tables,
		Endpoints: make(map[string]*Endpoint, len(endpoints)),
		Groups:    groups,
		Backends:  backends,
		Defaults:  defaults,
	}
	if cat.Tables == nil {
		cat.Tables = map[string]*Table{}
	}
	if cat.Groups == nil {
		cat.Groups = map[string]*CoverageGroup{}
	}
	if cat.Backends == nil {
		cat.Backends = map[string]*BackendDefaults{}
	}
	for name, table := range cat.Tables {
		table.Name = name
		table.index = make(map[string]Column, len(table.Columns))
		for _, col := range table.Columns {
			table.index[col.Name] = col
		}
	}
	for _, ep := range endpoints {
		if ep == nil {
			continue
		}
		if _, exists := cat.Endpoints[ep.ID]; exists {
			return nil, fmt.Errorf("duplicate endpoint id %q", ep.ID)
		}
		cat.Endpoints[ep.ID] = ep
		cat.endpointOrder = append(cat.endpointOrder, ep.ID)
	}
	sort.Strings(cat.endpointOrder)
	if err := cat.finalize(); err != nil {
		return nil, err
	}
	return cat, nil
}

func (c *Catalog) finalize() error {
	for _, ep := range c.Endpoints {
		if ep.ID == "" {
			return fmt.Errorf("endpoint id is required")
		}
		if ep.Backend == "" {
			return fmt.Errorf("endpoint %q: backend is required", ep.ID)
		}
		if ep.Role == "" {
			ep.Role = RoleShard
		}
		switch ep.Role {
		case RoleShard, RoleReplica:
		default:
			return fmt.Errorf("endpoint %q: unknown role %q", ep.ID, ep.Role)
		}
		for _, t := range ep.Tables {
			if _, ok := c.Tables[t]; !ok {
				return fmt.Errorf("endpoint %q: unknown table %q", ep.ID, t)
			}
		}
		if ep.Source == "" {
			if len(ep.Tables) > 1 {
				return fmt.Errorf("endpoint %q: source is required when it serves multiple tables", ep.ID)
			}
			if len(ep.Tables) == 1 {
				ep.Source = ep.Tables[0]
			}
		}
		if defaults, ok := c.Backends[ep.Backend]; ok {
			if len(ep.Caps.Signals) == 0 {
				ep.Caps.Signals = append([]string(nil), defaults.Signals...)
			}
			ep.Caps.EnforcesPartitionScope = ep.Caps.EnforcesPartitionScope || defaults.EnforcesPartitionScope
		}
		if ep.CoverageGroup == "" && ep.Role == RoleReplica {
			ep.CoverageGroup = ep.ReplicaOf
		}
		if ep.CoverageGroup == "" {
			ep.CoverageGroup = ep.Backend
		}
		if _, ok := c.Groups[ep.CoverageGroup]; !ok {
			c.Groups[ep.CoverageGroup] = inferGroup(ep)
		}
		if ep.Partition != nil {
			switch ep.Partition.Kind {
			case "column", "attribute", "account_region", "data_source", "log_group", "fixed":
				ep.Caps.EnforcesPartitionScope = true
			default:
				return fmt.Errorf("endpoint %q: unknown partition binding kind %q", ep.ID, ep.Partition.Kind)
			}
		}
	}
	for name, table := range c.Tables {
		if table.TimeColumn == "" {
			return fmt.Errorf("table %q: time_column is required", name)
		}
		if _, ok := table.index[table.TimeColumn]; !ok {
			return fmt.Errorf("table %q: time_column %q is not in columns", name, table.TimeColumn)
		}
	}
	for name, group := range c.Groups {
		switch group.Mode {
		case "hash", "time", "replica":
		default:
			return fmt.Errorf("coverage group %q: unknown mode %q", name, group.Mode)
		}
		if group.Mode == "hash" && group.Members > 0 && !group.AllowMissing {
			members := 0
			for _, ep := range c.Endpoints {
				if !ep.Disabled && ep.Role == RoleShard && ep.CoverageGroup == name {
					members++
				}
			}
			if members < group.Members {
				return fmt.Errorf("coverage group %q declares %d members but only %d shard endpoints exist",
					name, group.Members, members)
			}
		}
	}
	return nil
}

func inferGroup(ep *Endpoint) *CoverageGroup {
	if ep.Role == RoleReplica {
		return &CoverageGroup{Mode: "replica"}
	}
	if ep.Range != nil {
		return &CoverageGroup{Mode: "time"}
	}
	return &CoverageGroup{Mode: "hash"}
}

// Table looks up a logical table, accepting "schema.name" by last segment.
func (c *Catalog) Table(name string) (*Table, bool) {
	if t, ok := c.Tables[name]; ok {
		return t, true
	}
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		t, ok := c.Tables[name[i+1:]]
		return t, ok
	}
	return nil, false
}

// Column looks up a column in a table.
func (t *Table) Column(name string) (Column, bool) {
	if t == nil {
		return Column{}, false
	}
	col, ok := t.index[name]
	return col, ok
}

// EndpointsFor returns endpoints serving the table, sorted by ID.
func (c *Catalog) EndpointsFor(table string) []*Endpoint {
	out := make([]*Endpoint, 0, len(c.Endpoints))
	for _, id := range c.endpointOrder {
		ep := c.Endpoints[id]
		if ep.Serves(table) {
			out = append(out, ep)
		}
	}
	return out
}

// Group returns a coverage group.
func (c *Catalog) Group(name string) *CoverageGroup {
	if g, ok := c.Groups[name]; ok {
		return g
	}
	return &CoverageGroup{Mode: "hash"}
}

// ExcludeReplicasByDefault reports the deployment default.
func (c *Catalog) ExcludeReplicasByDefault() bool {
	if c.Defaults.ExcludeReplicas == nil {
		return true
	}
	return *c.Defaults.ExcludeReplicas
}

// IDs returns every endpoint ID in stable order.
func (c *Catalog) IDs() []string {
	return append([]string(nil), c.endpointOrder...)
}
