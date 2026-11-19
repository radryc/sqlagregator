// Package merge implements the pure-Go two-phase aggregation engine.
package merge

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/radryc/sqlagregator/internal/ast"
	"github.com/radryc/sqlagregator/internal/catalog"
	"github.com/radryc/sqlagregator/internal/plan"
	"github.com/radryc/sqlagregator/internal/value"
)

// Schema names the columns of a partial stream.
type Schema struct {
	Columns []string
}

// Row is one partial row.
type Row []value.Value

// Stream is one backend's partial result.
type Stream struct {
	EndpointID string
	Schema     Schema
	Rows       []Row
}

// Result is the merged output.
type Result struct {
	Columns []string
	Rows    []Row
}

// Run merges partial streams according to the bound query.
func Run(q *plan.Query, streams []Stream) (*Result, error) {
	if q.RowMode {
		return runRows(q, streams)
	}
	return runAggregates(q, streams)
}

func runAggregates(q *plan.Query, streams []Stream) (*Result, error) {
	groupNames := make([]string, len(q.Groups))
	for i := range q.Groups {
		groupNames[i] = fmt.Sprintf("k%d", i)
	}
	partialNames := make([]string, 0, 4)
	for _, agg := range q.Aggs {
		for _, p := range agg.Partials {
			partialNames = append(partialNames, p.Alias)
		}
	}

	type indexed struct {
		stream Stream
		groups map[string]int
		parts  map[string]int
	}
	inputs := make([]indexed, 0, len(streams))
	for _, s := range streams {
		idx := make(map[string]int, len(s.Schema.Columns))
		for i, name := range s.Schema.Columns {
			idx[name] = i
		}
		in := indexed{stream: s, groups: map[string]int{}, parts: map[string]int{}}
		for _, name := range groupNames {
			i, ok := idx[name]
			if !ok {
				return nil, fmt.Errorf("endpoint %s: partial result is missing group column %q", s.EndpointID, name)
			}
			in.groups[name] = i
		}
		for _, name := range partialNames {
			i, ok := idx[name]
			if !ok {
				return nil, fmt.Errorf("endpoint %s: partial result is missing column %q", s.EndpointID, name)
			}
			in.parts[name] = i
		}
		inputs = append(inputs, in)
	}

	buckets := map[string]*bucket{}
	for _, in := range inputs {
		for _, row := range in.stream.Rows {
			group := make([]value.Value, len(groupNames))
			var kb strings.Builder
			for i, name := range groupNames {
				v := row[in.groups[name]]
				group[i] = v
				appendKey(&kb, v.Key())
			}
			key := kb.String()
			b, ok := buckets[key]
			if !ok {
				b = &bucket{key: key, group: group, accs: newAccumulators(q)}
				buckets[key] = b
			}
			for ai, agg := range q.Aggs {
				for _, p := range agg.Partials {
					b.accs[ai].addPartial(p.Func, row[in.parts[p.Alias]])
				}
			}
		}
	}

	if len(q.Groups) == 0 && len(buckets) == 0 {
		buckets[""] = &bucket{accs: newAccumulators(q)}
	}

	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	columns := make([]string, len(q.Output))
	for i, out := range q.Output {
		columns[i] = out.Alias
	}

	rows := make([]Row, 0, len(keys))
	contexts := make([]*rowCtx, 0, len(keys))
	for _, key := range keys {
		b := buckets[key]
		final := make([]value.Value, len(q.Aggs))
		for i, agg := range q.Aggs {
			final[i] = b.accs[i].finalize(agg)
		}
		out := make([]value.Value, len(q.Output))
		for i, item := range q.Output {
			switch {
			case item.Group >= 0:
				out[i] = b.group[item.Group]
			case item.Agg >= 0:
				out[i] = final[item.Agg]
			default:
				out[i] = value.NullValue()
			}
		}
		rows = append(rows, out)
		ctx := newRowCtx()
		for i, item := range q.Output {
			ctx.set(item.Alias, out[i])
		}
		for i, g := range groupNames {
			ctx.set(g, b.group[i])
		}
		contexts = append(contexts, ctx)
	}

	kept, keptCtx, err := applyHaving(q, rows, contexts)
	if err != nil {
		return nil, err
	}
	return sortAndLimit(q, columns, kept, keptCtx)
}

func applyHaving(q *plan.Query, rows []Row, contexts []*rowCtx) ([]Row, []*rowCtx, error) {
	if q.Having == nil {
		return rows, contexts, nil
	}
	kept := make([]Row, 0, len(rows))
	keptCtx := make([]*rowCtx, 0, len(contexts))
	for i, row := range rows {
		v, err := eval(q.Having, contexts[i])
		if err != nil {
			return nil, nil, err
		}
		if truth(v) {
			kept = append(kept, row)
			keptCtx = append(keptCtx, contexts[i])
		}
	}
	return kept, keptCtx, nil
}

func sortAndLimit(q *plan.Query, columns []string, rows []Row, contexts []*rowCtx) (*Result, error) {
	if len(q.OrderBy) > 0 {
		idx := make([]int, len(rows))
		for i := range idx {
			idx[i] = i
		}
		var evalErr error
		sort.SliceStable(idx, func(i, j int) bool {
			if evalErr != nil {
				return false
			}
			a, b := contexts[idx[i]], contexts[idx[j]]
			for _, item := range q.OrderBy {
				av, err := eval(item.Expr, a)
				if err != nil {
					evalErr = err
					return false
				}
				bv, err := eval(item.Expr, b)
				if err != nil {
					evalErr = err
					return false
				}
				if less(av, bv, item) {
					return true
				}
				if less(bv, av, item) {
					return false
				}
			}
			return false
		})
		if evalErr != nil {
			return nil, evalErr
		}
		sortedRows := make([]Row, len(rows))
		sortedCtx := make([]*rowCtx, len(rows))
		for i, at := range idx {
			sortedRows[i] = rows[at]
			sortedCtx[i] = contexts[at]
		}
		rows, contexts = sortedRows, sortedCtx
	}
	rows = slice(q, rows)
	return &Result{Columns: columns, Rows: rows}, nil
}

func slice(q *plan.Query, rows []Row) []Row {
	start := q.Offset
	if start > len(rows) {
		start = len(rows)
	}
	end := len(rows)
	if q.HasLimit && start+q.Limit < end {
		end = start + q.Limit
	}
	return rows[start:end]
}

func less(a, b value.Value, item ast.SortItem) bool {
	if a.Kind == value.KNull || b.Kind == value.KNull {
		if a.Kind == value.KNull && b.Kind == value.KNull {
			return false
		}
		nullsFirst := item.Desc
		if item.NullsFirst != nil {
			nullsFirst = *item.NullsFirst
		}
		return a.Kind == value.KNull && nullsFirst
	}
	cmp := value.Compare(a, b)
	if item.Desc {
		cmp = -cmp
	}
	return cmp < 0
}

type bucket struct {
	key   string
	group []value.Value
	accs  []*accumulator
}

func newAccumulators(q *plan.Query) []*accumulator {
	out := make([]*accumulator, len(q.Aggs))
	for i, agg := range q.Aggs {
		out[i] = &accumulator{typ: agg.Type, fn: agg.Func}
	}
	return out
}

type accumulator struct {
	fn    string
	typ   catalog.Type
	count int64
	sum   value.Value
	has   bool
	fsum  float64
	fcomp float64
	ext   value.Value
	hasEx bool
}

func (a *accumulator) addPartial(fn string, v value.Value) {
	if v.Kind == value.KNull {
		return
	}
	switch fn {
	case "count":
		a.count += v.Int()
	case "sum":
		if a.typ == catalog.TypeFloat64 || v.Kind == value.KFloat {
			addFloat(&a.fsum, &a.fcomp, v.Float64())
			a.has = true
			return
		}
		if !a.has {
			a.sum = v
		} else {
			a.sum, _ = value.Add(a.sum, v)
		}
		a.has = true
	case "min":
		if !a.hasEx || value.Compare(v, a.ext) < 0 {
			a.ext = v
			a.hasEx = true
		}
	case "max":
		if !a.hasEx || value.Compare(v, a.ext) > 0 {
			a.ext = v
			a.hasEx = true
		}
	}
}

func (a *accumulator) finalize(agg *plan.Aggregate) value.Value {
	switch agg.Func {
	case "count":
		return value.Int64(a.count)
	case "sum":
		if !a.has {
			return value.NullValue()
		}
		if a.typ == catalog.TypeFloat64 {
			return value.Float64(a.fsum + a.fcomp)
		}
		return a.sum
	case "min", "max":
		if !a.hasEx {
			return value.NullValue()
		}
		return a.ext
	case "avg":
		if a.count == 0 {
			return value.NullValue()
		}
		if a.typ == catalog.TypeFloat64 {
			return value.Float64((a.fsum + a.fcomp) / float64(a.count))
		}
		d := a.sum.AsDecimal().DivInt(a.count, 9)
		if d == nil {
			return value.NullValue()
		}
		return value.Dec(d)
	default:
		return value.NullValue()
	}
}

func addFloat(sum, comp *float64, x float64) {
	t := *sum + x
	if math.Abs(*sum) >= math.Abs(x) {
		*comp += (*sum - t) + x
	} else {
		*comp += (x - t) + *sum
	}
	*sum = t
}

func appendKey(b *strings.Builder, key string) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(key)))
	b.Write(buf[:])
	b.WriteString(key)
}

func runRows(q *plan.Query, streams []Stream) (*Result, error) {
	columns := make([]string, len(q.Output))
	for i, out := range q.Output {
		columns[i] = out.Alias
	}
	rows := make([]Row, 0)
	contexts := make([]*rowCtx, 0)
	seen := map[string]struct{}{}
	for _, s := range streams {
		idx := map[string]int{}
		for i, name := range s.Schema.Columns {
			idx[name] = i
		}
		for _, row := range s.Rows {
			out := make([]value.Value, len(q.Output))
			ctx := newRowCtx()
			for i, item := range q.Output {
				j, ok := idx[item.Alias]
				if !ok {
					return nil, fmt.Errorf("endpoint %s: row result is missing column %q", s.EndpointID, item.Alias)
				}
				out[i] = row[j]
				ctx.set(item.Alias, out[i])
			}
			if q.Distinct {
				var kb strings.Builder
				for _, v := range out {
					appendKey(&kb, v.Key())
				}
				if _, ok := seen[kb.String()]; ok {
					continue
				}
				seen[kb.String()] = struct{}{}
			}
			rows = append(rows, out)
			contexts = append(contexts, ctx)
		}
	}
	return sortAndLimit(q, columns, rows, contexts)
}
