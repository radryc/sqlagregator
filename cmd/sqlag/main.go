// Command sqlag inspects a catalog and explains federated query plans.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	bigquerydriver "github.com/radryc/sqlagregator/drivers/bigquery"
	"github.com/radryc/sqlagregator/drivers/clickhouse"
	omnidriver "github.com/radryc/sqlagregator/drivers/omni"
	"github.com/radryc/sqlagregator/pkg/sqlag"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "backends":
		err = runBackends(os.Args[2:])
	case "explain":
		err = runExplain(os.Args[2:])
	case "query":
		err = runQuery(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `sqlag — federated SQL coordinator (scaffold)

Usage:
  sqlag backends -config catalog.yaml [-table logs]
  sqlag explain  -config catalog.yaml -q "SELECT ..." [flags]
  sqlag query    -config catalog.yaml -clickhouse-url URL -q "SELECT ..." [flags]

Explain flags:
  -from / -to RFC3339 or YYYY-MM-DD
  -limit N                 cap rows after merge
  -exclude id1,id2         exclude endpoint IDs
  -exclude-tag k=v         exclude by tag (repeatable)
  -treat-as-replica ids    mark endpoints redundant before coverage
  -treat-replica-tag k=v   mark by tag (repeatable)
  -include-replicas        add replicas to the failover set (never summed)
  -max N                   hard cap on selected shards
  -min-coverage F          required covered fraction (0..1)
  -allow-partial           return partial results with coverage metadata

Query flags (additionally):
  -clickhouse-url URL      ClickHouse HTTP endpoint (one cluster)
  -clickhouse-database DB
  -clickhouse-user USER
  -clickhouse-password PASS
  -bigquery-project ID     BigQuery billing project
  -bigquery-location LOC
  -bigquery-credentials FILE
  -bigquery-endpoint URL
  -bigquery-disable-auth   emulator/test only
  -omni-region REGION
  -omni-profile PROFILE
  -omni-endpoint URL
  -omni-session NAME
  -timeout 20s             total query budget
`)
}

func runBackends(args []string) error {
	fs := flag.NewFlagSet("backends", flag.ExitOnError)
	configPath := fs.String("config", "", "catalog YAML")
	table := fs.String("table", "logs", "logical table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cat, err := loadCatalog(*configPath)
	if err != nil {
		return err
	}
	tbl, ok := cat.Table(*table)
	if !ok {
		return fmt.Errorf("unknown table %q", *table)
	}
	from, to := time.Now().UTC().Add(-time.Hour), time.Now().UTC()
	res, err := sqlag.Resolve(cat, tbl.Name, sqlag.Selector{}, sqlag.TimeRange{From: from, To: to}, sqlag.ResolveOptions{
		AllowPartial:             true,
		Signal:                   tbl.Signal,
		ExcludeReplicasByDefault: cat.ExcludeReplicasByDefault(),
	})
	if err != nil {
		return err
	}
	selected := map[string]bool{}
	for _, ep := range res.Shards {
		selected[ep.ID] = true
	}
	for _, ep := range res.Replicas {
		selected[ep.ID] = true
	}
	reason := map[string]string{}
	for _, ex := range res.Excluded {
		reason[ex.ID] = ex.Reason
	}
	type view struct {
		ID       string            `json:"id"`
		Backend  string            `json:"backend"`
		Role     string            `json:"role"`
		Group    string            `json:"coverage_group"`
		Tags     map[string]string `json:"tags,omitempty"`
		Selected bool              `json:"selected"`
		Reason   string            `json:"reason,omitempty"`
	}
	var out []view
	for _, ep := range cat.EndpointsFor(tbl.Name) {
		out = append(out, view{
			ID:       ep.ID,
			Backend:  ep.Backend,
			Role:     string(ep.Role),
			Group:    ep.CoverageGroup,
			Tags:     ep.Tags,
			Selected: selected[ep.ID],
			Reason:   reason[ep.ID],
		})
	}
	return printJSON(map[string]any{
		"table":     tbl.Name,
		"endpoints": out,
		"coverage":  res.Coverage,
	})
}

func runExplain(args []string) error {
	fs := flag.NewFlagSet("explain", flag.ExitOnError)
	configPath := fs.String("config", "", "catalog YAML")
	query := fs.String("q", "", "generic SQL")
	from := fs.String("from", "", "RFC3339 or YYYY-MM-DD")
	to := fs.String("to", "", "RFC3339 or YYYY-MM-DD")
	limit := fs.Int("limit", 0, "row cap")
	exclude := fs.String("exclude", "", "comma-separated endpoint IDs")
	treatIDs := fs.String("treat-as-replica", "", "comma-separated endpoint IDs")
	includeReplicas := fs.Bool("include-replicas", false, "include replicas in the failover set")
	maxShards := fs.Int("max", 0, "max selected shards")
	minCoverage := fs.Float64("min-coverage", 0, "required covered fraction")
	allowPartial := fs.Bool("allow-partial", false, "allow partial results")
	partitions := fs.String("partitions", "", "comma-separated authorized partitions")
	restricted := fs.Bool("restricted", false, "enforce a restricted partition scope")
	var excludeTags, treatTags stringList
	fs.Var(&excludeTags, "exclude-tag", "exclude by tag k=v (repeatable)")
	fs.Var(&treatTags, "treat-replica-tag", "treat by tag k=v as replica (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*query) == "" {
		return fmt.Errorf("-q is required")
	}
	cat, err := loadCatalog(*configPath)
	if err != nil {
		return err
	}
	req := sqlag.Request{
		SQL:          *query,
		Limit:        *limit,
		AllowPartial: *allowPartial,
		Selector: sqlag.Selector{
			IncludeReplicas: *includeReplicas,
			Max:             *maxShards,
			MinCoverage:     *minCoverage,
		},
	}
	if *from != "" {
		req.From, err = parseTime(*from)
		if err != nil {
			return fmt.Errorf("-from: %w", err)
		}
	}
	if *to != "" {
		req.To, err = parseTime(*to)
		if err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}
	req.Scope = sqlag.Scope{Restricted: *restricted}
	if *partitions != "" {
		req.Scope.Partitions = splitCSV(*partitions)
	}
	if *exclude != "" {
		req.Selector.Exclude = append(req.Selector.Exclude, sqlag.Matcher{IDs: splitCSV(*exclude)})
	}
	for _, tag := range excludeTags {
		m, err := tagMatcher(tag)
		if err != nil {
			return fmt.Errorf("-exclude-tag: %w", err)
		}
		req.Selector.Exclude = append(req.Selector.Exclude, m)
	}
	if *treatIDs != "" {
		req.Selector.TreatAsReplica = append(req.Selector.TreatAsReplica, sqlag.Matcher{IDs: splitCSV(*treatIDs)})
	}
	for _, tag := range treatTags {
		m, err := tagMatcher(tag)
		if err != nil {
			return fmt.Errorf("-treat-replica-tag: %w", err)
		}
		req.Selector.TreatAsReplica = append(req.Selector.TreatAsReplica, m)
	}
	coord := sqlag.New(cat)
	expl, err := coord.Explain(context.Background(), req)
	if err != nil {
		return err
	}
	return printJSON(expl)
}

func loadCatalog(path string) (*sqlag.Catalog, error) {
	if path == "" {
		return nil, fmt.Errorf("-config is required")
	}
	return sqlag.LoadCatalog(path)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("expected RFC3339 or YYYY-MM-DD, got %q", s)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func tagMatcher(s string) (sqlag.Matcher, error) {
	key, value, ok := strings.Cut(s, "=")
	if !ok || key == "" {
		return sqlag.Matcher{}, fmt.Errorf("expected k=v, got %q", s)
	}
	return sqlag.Matcher{Tags: map[string]string{key: value}}, nil
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func runQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	configPath := fs.String("config", "", "catalog YAML")
	query := fs.String("q", "", "generic SQL")
	from := fs.String("from", "", "RFC3339 or YYYY-MM-DD")
	to := fs.String("to", "", "RFC3339 or YYYY-MM-DD")
	limit := fs.Int("limit", 0, "row cap")
	allowPartial := fs.Bool("allow-partial", false, "allow partial results")
	partitions := fs.String("partitions", "", "comma-separated authorized partitions")
	restricted := fs.Bool("restricted", false, "enforce a restricted partition scope")
	exclude := fs.String("exclude", "", "comma-separated endpoint IDs")
	includeReplicas := fs.Bool("include-replicas", false, "include replicas in the failover set")
	var excludeTags stringList
	fs.Var(&excludeTags, "exclude-tag", "exclude by tag k=v (repeatable)")
	chURL := fs.String("clickhouse-url", "", "ClickHouse HTTP endpoint")
	chDB := fs.String("clickhouse-database", "", "ClickHouse database")
	chUser := fs.String("clickhouse-user", "", "ClickHouse user")
	chPass := fs.String("clickhouse-password", "", "ClickHouse password")
	bqProject := fs.String("bigquery-project", "", "BigQuery billing project")
	bqLocation := fs.String("bigquery-location", "", "BigQuery location")
	bqCreds := fs.String("bigquery-credentials", "", "BigQuery credentials file")
	bqEndpoint := fs.String("bigquery-endpoint", "", "BigQuery endpoint override")
	bqDisableAuth := fs.Bool("bigquery-disable-auth", false, "disable BigQuery auth (emulator only)")
	omniRegion := fs.String("omni-region", "", "CloudWatch Omni region")
	omniProfile := fs.String("omni-profile", "", "AWS shared config profile")
	omniEndpoint := fs.String("omni-endpoint", "", "CloudWatch Omni endpoint override")
	omniSession := fs.String("omni-session", "", "CloudWatch Omni session name")
	timeout := fs.Duration("timeout", 20*time.Second, "total query budget")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*query) == "" {
		return fmt.Errorf("-q is required")
	}
	cat, err := loadCatalog(*configPath)
	if err != nil {
		return err
	}
	coord := sqlag.New(cat)
	registered := 0
	for _, id := range cat.IDs() {
		ep := cat.Endpoints[id]
		if ep.Disabled {
			continue
		}
		switch strings.ToLower(ep.Backend) {
		case "clickhouse", "ch":
			if *chURL == "" {
				return fmt.Errorf("endpoint %s needs -clickhouse-url", ep.ID)
			}
			driver, err := clickhouse.New(clickhouse.Config{
				EndpointID: ep.ID,
				URL:        *chURL,
				Database:   *chDB,
				User:       *chUser,
				Password:   *chPass,
				Timeout:    *timeout,
			})
			if err != nil {
				return fmt.Errorf("endpoint %s: %w", ep.ID, err)
			}
			coord.Register(driver)
			registered++
		case "bigquery", "bq":
			if *bqProject == "" {
				return fmt.Errorf("endpoint %s needs -bigquery-project", ep.ID)
			}
			driver, err := bigquerydriver.New(context.Background(), bigquerydriver.Config{
				EndpointID:      ep.ID,
				ProjectID:       *bqProject,
				Location:        *bqLocation,
				CredentialsFile: *bqCreds,
				Endpoint:        *bqEndpoint,
				DisableAuth:     *bqDisableAuth,
				Timeout:         *timeout,
			})
			if err != nil {
				return fmt.Errorf("endpoint %s: %w", ep.ID, err)
			}
			coord.Register(driver)
			registered++
		case "omni", "cloudwatch-omni":
			driver, err := omnidriver.New(context.Background(), omnidriver.Config{
				EndpointID:  ep.ID,
				Region:      *omniRegion,
				Profile:     *omniProfile,
				Endpoint:    *omniEndpoint,
				SessionName: *omniSession,
				Timeout:     *timeout,
			})
			if err != nil {
				return fmt.Errorf("endpoint %s: %w", ep.ID, err)
			}
			coord.Register(driver)
			registered++
		}
	}
	if registered == 0 {
		return fmt.Errorf("no supported endpoints in the catalog")
	}

	req := sqlag.Request{
		SQL:          *query,
		Limit:        *limit,
		AllowPartial: *allowPartial,
		Selector:     sqlag.Selector{IncludeReplicas: *includeReplicas},
		Scope:        sqlag.Scope{Restricted: *restricted},
	}
	if *partitions != "" {
		req.Scope.Partitions = splitCSV(*partitions)
	}
	if *exclude != "" {
		req.Selector.Exclude = append(req.Selector.Exclude, sqlag.Matcher{IDs: splitCSV(*exclude)})
	}
	for _, tag := range excludeTags {
		m, err := tagMatcher(tag)
		if err != nil {
			return fmt.Errorf("-exclude-tag: %w", err)
		}
		req.Selector.Exclude = append(req.Selector.Exclude, m)
	}
	if *from != "" {
		req.From, err = parseTime(*from)
		if err != nil {
			return fmt.Errorf("-from: %w", err)
		}
	}
	if *to != "" {
		req.To, err = parseTime(*to)
		if err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	res, err := coord.Query(ctx, req)
	if err != nil {
		return err
	}
	return printJSON(res)
}
