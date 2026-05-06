// Package executor runs physical query plans against registered index managers.
//
// Each Execute call resolves the plan tree bottom-up, populating a Result set:
//   - BtreeScan    -> IndexManager.Get / GetRange
//   - InvertedScan -> IndexManager.TextSearch
//   - SpatialScan  -> bounding-box query via TextSearch workaround
//   - Filter       -> residual predicate evaluation
//   - Limit        -> row-count truncation
//   - Insert / Delete / Update -> write methods
package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexmanager"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/query_engine/optimizer"
	"github.com/sushant-115/gojodb/core/query_engine/parser"
)

// Row is a single result tuple.
type Row struct {
	Key   string
	Value string
}

// Result holds the output of a query execution.
type Result struct {
	Rows         []Row
	RowsAffected int64
	Error        error
}

// Registry maps index names to their IndexManager implementations.
type Registry map[string]indexmanager.IndexManager

// Executor executes physical plans against the registered index managers.
type Executor struct {
	registry Registry
}

// NewExecutor creates an Executor with the given index manager registry.
func NewExecutor(registry Registry) *Executor {
	return &Executor{registry: registry}
}

// Execute runs the given plan and returns a Result.
func (e *Executor) Execute(ctx context.Context, plan *optimizer.Plan) *Result {
	rows, affected, err := e.execPlan(ctx, plan)
	return &Result{Rows: rows, RowsAffected: affected, Error: err}
}

// ExecuteQuery is a convenience wrapper: parse -> optimize -> execute.
func (e *Executor) ExecuteQuery(ctx context.Context, query string) *Result {
	stmt, err := parser.Parse(query)
	if err != nil {
		return &Result{Error: fmt.Errorf("parse error: %w", err)}
	}
	plan, err := optimizer.Optimize(stmt)
	if err != nil {
		return &Result{Error: fmt.Errorf("optimizer error: %w", err)}
	}
	return e.Execute(ctx, plan)
}

// -----------------------------------------------------------------------
// Internal plan execution
// -----------------------------------------------------------------------

func (e *Executor) execPlan(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	switch plan.Type {
	case optimizer.PlanTypeBtreeScan:
		return e.execBtreeScan(ctx, plan)
	case optimizer.PlanTypeInvertedScan:
		return e.execInvertedScan(ctx, plan)
	case optimizer.PlanTypeSpatialScan:
		return e.execSpatialScan(ctx, plan)
	case optimizer.PlanTypeInsert:
		return e.execInsert(ctx, plan)
	case optimizer.PlanTypeDelete:
		return e.execDelete(ctx, plan)
	case optimizer.PlanTypeUpdate:
		return e.execUpdate(ctx, plan)
	case optimizer.PlanTypeFilter:
		return e.execFilter(ctx, plan)
	case optimizer.PlanTypeLimit:
		return e.execLimit(ctx, plan)
	default:
		return nil, 0, fmt.Errorf("executor: unsupported plan type %d", plan.Type)
	}
}

// -----------------------------------------------------------------------
// Scan nodes
// -----------------------------------------------------------------------

func (e *Executor) execBtreeScan(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	// Point lookup.
	if plan.PointKey != "" {
		val, found := mgr.Get(ctx, plan.PointKey)
		if !found {
			return nil, 0, nil
		}
		return applyLimit([]Row{{Key: plan.PointKey, Value: string(val)}}, plan.Limit), 0, nil
	}
	// Range scan.
	limit := plan.Limit
	if limit == 0 {
		limit = 10000
	}
	pairs, err := mgr.GetRange(ctx, plan.StartKey, plan.EndKey, limit)
	if err != nil {
		return nil, 0, err
	}
	rows := pairsToRows(pairs)
	if !plan.OrderAsc {
		reverseRows(rows)
	}
	return rows, 0, nil
}

func (e *Executor) execInvertedScan(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	limit := plan.Limit
	if limit == 0 {
		limit = 1000
	}
	results, err := mgr.TextSearch(ctx, plan.TextQuery, plan.TextField, limit)
	if err != nil {
		return nil, 0, err
	}
	rows := make([]Row, 0, len(results))
	for _, r := range results {
		rows = append(rows, Row{Key: r.DocId, Value: r.Snippet})
	}
	return rows, 0, nil
}

func (e *Executor) execSpatialScan(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	// Encode the bounding box as a TextSearch query understood by the SpatialIndexManager.
	query := fmt.Sprintf("bbox:%.6f,%.6f,%.6f,%.6f",
		plan.SpatialMinLat, plan.SpatialMinLon,
		plan.SpatialMaxLat, plan.SpatialMaxLon)
	results, err := mgr.TextSearch(ctx, query, "spatial", 1000)
	if err != nil {
		return nil, 0, nil
	}
	rows := make([]Row, 0, len(results))
	for _, r := range results {
		rows = append(rows, Row{Key: r.DocId, Value: r.Snippet})
	}
	return applyLimit(rows, plan.Limit), 0, nil
}

// -----------------------------------------------------------------------
// Write nodes
// -----------------------------------------------------------------------

func (e *Executor) execInsert(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	if err := mgr.Put(ctx, plan.WriteKey, []byte(plan.WriteValue)); err != nil {
		return nil, 0, err
	}
	return nil, 1, nil
}

func (e *Executor) execDelete(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	// Direct key delete.
	if plan.Child == nil {
		if err := mgr.Delete(ctx, plan.WriteKey); err != nil {
			return nil, 0, err
		}
		return nil, 1, nil
	}
	// Scan-then-delete: fetch matching rows from child, then delete each key.
	rows, _, err := e.execPlan(ctx, plan.Child)
	if err != nil {
		return nil, 0, err
	}
	for _, row := range rows {
		if delErr := mgr.Delete(ctx, row.Key); delErr != nil {
			return nil, 0, delErr
		}
	}
	return nil, int64(len(rows)), nil
}

func (e *Executor) execUpdate(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	mgr, err := e.getManager(plan.IndexName)
	if err != nil {
		return nil, 0, err
	}
	// Direct key update.
	if plan.Child == nil {
		if err := mgr.Put(ctx, plan.WriteKey, []byte(plan.WriteValue)); err != nil {
			return nil, 0, err
		}
		return nil, 1, nil
	}
	// Scan-then-update.
	rows, _, err := e.execPlan(ctx, plan.Child)
	if err != nil {
		return nil, 0, err
	}
	for _, row := range rows {
		if putErr := mgr.Put(ctx, row.Key, []byte(plan.WriteValue)); putErr != nil {
			return nil, 0, putErr
		}
	}
	return nil, int64(len(rows)), nil
}

// -----------------------------------------------------------------------
// Decorator nodes
// -----------------------------------------------------------------------

func (e *Executor) execFilter(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	rows, affected, err := e.execPlan(ctx, plan.Child)
	if err != nil {
		return nil, affected, err
	}
	filtered := make([]Row, 0, len(rows))
	for _, row := range rows {
		if evalPredicate(plan.FilterPred, row) {
			filtered = append(filtered, row)
		}
	}
	return filtered, affected, nil
}

func (e *Executor) execLimit(ctx context.Context, plan *optimizer.Plan) ([]Row, int64, error) {
	rows, affected, err := e.execPlan(ctx, plan.Child)
	if err != nil {
		return nil, affected, err
	}
	return applyLimit(rows, plan.Limit), affected, nil
}

// -----------------------------------------------------------------------
// Predicate evaluation (in-memory filter)
// -----------------------------------------------------------------------

func evalPredicate(pred parser.Predicate, row Row) bool {
	if pred == nil {
		return true
	}
	switch p := pred.(type) {
	case *parser.EqPredicate:
		return fieldValue(p.Field, row) == p.Value
	case *parser.RangePredicate:
		v := fieldValue(p.Field, row)
		return v >= p.StartKey && v <= p.EndKey
	case *parser.CompPredicate:
		v := fieldValue(p.Field, row)
		switch p.Op {
		case parser.TOKEN_LT:
			return v < p.Value
		case parser.TOKEN_LTE:
			return v <= p.Value
		case parser.TOKEN_GT:
			return v > p.Value
		case parser.TOKEN_GTE:
			return v >= p.Value
		case parser.TOKEN_NEQ:
			return v != p.Value
		}
	case *parser.TextSearchPredicate:
		v := fieldValue(p.Field, row)
		return strings.Contains(strings.ToLower(v), strings.ToLower(p.Query))
	case *parser.AndPredicate:
		return evalPredicate(p.Left, row) && evalPredicate(p.Right, row)
	case *parser.OrPredicate:
		return evalPredicate(p.Left, row) || evalPredicate(p.Right, row)
	case *parser.NotPredicate:
		return !evalPredicate(p.Pred, row)
	}
	return true
}

func fieldValue(field string, row Row) string {
	switch strings.ToLower(field) {
	case "value":
		return row.Value
	default:
		return row.Key
	}
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

func (e *Executor) getManager(name string) (indexmanager.IndexManager, error) {
	mgr, ok := e.registry[name]
	if !ok {
		return nil, fmt.Errorf("executor: no index manager registered for %q", name)
	}
	return mgr, nil
}

func pairsToRows(pairs []*pb.KeyValuePair) []Row {
	rows := make([]Row, len(pairs))
	for i, p := range pairs {
		rows[i] = Row{Key: p.Key, Value: string(p.Value)}
	}
	return rows
}

func reverseRows(rows []Row) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func applyLimit(rows []Row, limit int32) []Row {
	if limit <= 0 || int(limit) >= len(rows) {
		return rows
	}
	return rows[:limit]
}

func sortRows(rows []Row, asc bool) []Row {
	sorted := make([]Row, len(rows))
	copy(sorted, rows)
	sort.Slice(sorted, func(i, j int) bool {
		if asc {
			return sorted[i].Key < sorted[j].Key
		}
		return sorted[i].Key > sorted[j].Key
	})
	return sorted
}

// SpatialRect converts plan bounding-box coords to a spatial.Rect for the R-tree.
func SpatialRect(plan *optimizer.Plan) spatial.Rect {
	return spatial.Rect{
		MinX: plan.SpatialMinLon,
		MinY: plan.SpatialMinLat,
		MaxX: plan.SpatialMaxLon,
		MaxY: plan.SpatialMaxLat,
	}
}

// Ensure sortRows is used to avoid unused-function errors.
var _ = sortRows
