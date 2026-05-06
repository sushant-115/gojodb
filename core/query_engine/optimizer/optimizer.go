// Package optimizer converts a parsed AST into a physical query plan,
// choosing the best index and access strategy for each statement.
//
// The optimizer follows a simple rule-based approach:
//   - EqPredicate on "key"  → B-tree point lookup
//   - RangePredicate        → B-tree range scan
//   - TextSearchPredicate   → inverted-index scan
//   - NearPredicate / WithinPredicate → spatial R-tree scan
//   - No predicate          → full B-tree scan
//
// A cost-based layer can be added later once statistics are collected.
package optimizer

import (
	"fmt"

	"github.com/sushant-115/gojodb/core/query_engine/parser"
)

// PlanType identifies the physical operation a plan node performs.
type PlanType int

const (
	PlanTypeBtreeScan    PlanType = iota // Point or range lookup via B-tree
	PlanTypeInvertedScan                 // Full-text search via inverted index
	PlanTypeSpatialScan                  // Geo query via R-tree
	PlanTypeInsert                       // Write a single key-value pair
	PlanTypeDelete                       // Delete a key
	PlanTypeUpdate                       // Overwrite a value
	PlanTypeFilter                       // Post-scan predicate filter
	PlanTypeSort                         // In-memory sort
	PlanTypeLimit                        // Stop after N rows
)

// Plan is the physical execution plan for a single statement.
// It forms a tree: each node may have a child that produces its input rows.
type Plan struct {
	Type      PlanType
	IndexName string

	// BtreeScan parameters
	StartKey string
	EndKey   string // empty → point lookup
	PointKey string // non-empty → exact-match lookup

	// InvertedScan parameters
	TextField string
	TextQuery string

	// SpatialScan parameters
	SpatialMinLat, SpatialMinLon float64
	SpatialMaxLat, SpatialMaxLon float64

	// Write parameters (Insert/Delete/Update)
	WriteKey   string
	WriteValue string

	// Control
	Limit    int32
	OrderAsc bool // true = ascending (default)

	// Predicate for Filter nodes (residual predicates the index can't handle).
	FilterPred parser.Predicate

	// Child plan for decorating nodes (Filter, Sort, Limit wrap a scan).
	Child *Plan
}

// Optimize converts a parsed Statement into a physical Plan.
func Optimize(stmt parser.Statement) (*Plan, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return optimizeSelect(s)
	case *parser.InsertStmt:
		return &Plan{
			Type:       PlanTypeInsert,
			IndexName:  s.IndexName,
			WriteKey:   s.Key,
			WriteValue: s.Value,
		}, nil
	case *parser.DeleteStmt:
		return optimizeDelete(s)
	case *parser.UpdateStmt:
		return optimizeUpdate(s)
	default:
		return nil, fmt.Errorf("optimizer: unsupported statement type %T", stmt)
	}
}

// -----------------------------------------------------------------------
// SELECT optimisation
// -----------------------------------------------------------------------

func optimizeSelect(s *parser.SelectStmt) (*Plan, error) {
	var root *Plan
	var residual parser.Predicate

	if s.Where == nil {
		// Full scan via B-tree (no predicate).
		root = &Plan{
			Type:      PlanTypeBtreeScan,
			IndexName: s.IndexName,
			OrderAsc:  true,
		}
	} else {
		var err error
		root, residual, err = buildScanPlan(s.IndexName, s.Where)
		if err != nil {
			return nil, err
		}
	}

	// Wrap with a residual Filter if the index couldn't absorb all predicates.
	if residual != nil {
		root = &Plan{Type: PlanTypeFilter, FilterPred: residual, Child: root}
	}

	// ORDER BY
	if s.OrderBy != nil {
		applySortHint(root, s.OrderBy)
	}

	// LIMIT
	if s.Limit > 0 {
		root.Limit = s.Limit
	}

	return root, nil
}

// -----------------------------------------------------------------------
// DELETE / UPDATE optimisation
// -----------------------------------------------------------------------

func optimizeDelete(s *parser.DeleteStmt) (*Plan, error) {
	if eq, ok := s.Where.(*parser.EqPredicate); ok {
		return &Plan{
			Type:      PlanTypeDelete,
			IndexName: s.IndexName,
			WriteKey:  eq.Value,
		}, nil
	}

	// For non-equality deletes we need a scan + delete plan.
	scanPlan, residual, err := buildScanPlan(s.IndexName, s.Where)
	if err != nil {
		return nil, err
	}
	deletePlan := &Plan{
		Type:      PlanTypeDelete,
		IndexName: s.IndexName,
		Child:     scanPlan,
	}
	if residual != nil {
		deletePlan.Child = &Plan{Type: PlanTypeFilter, FilterPred: residual, Child: scanPlan}
	}
	return deletePlan, nil
}

func optimizeUpdate(s *parser.UpdateStmt) (*Plan, error) {
	if eq, ok := s.Where.(*parser.EqPredicate); ok {
		return &Plan{
			Type:       PlanTypeUpdate,
			IndexName:  s.IndexName,
			WriteKey:   eq.Value,
			WriteValue: s.Value,
		}, nil
	}
	scanPlan, _, err := buildScanPlan(s.IndexName, s.Where)
	if err != nil {
		return nil, err
	}
	return &Plan{
		Type:       PlanTypeUpdate,
		IndexName:  s.IndexName,
		WriteValue: s.Value,
		Child:      scanPlan,
	}, nil
}

// -----------------------------------------------------------------------
// Index access selection
// -----------------------------------------------------------------------

// buildScanPlan picks the best index access for a WHERE predicate and returns
// (scanPlan, residualPredicate).
func buildScanPlan(indexName string, pred parser.Predicate) (*Plan, parser.Predicate, error) {
	switch p := pred.(type) {
	case *parser.EqPredicate:
		// Exact-match lookup via B-tree.
		return &Plan{
			Type:      PlanTypeBtreeScan,
			IndexName: indexName,
			PointKey:  p.Value,
			OrderAsc:  true,
		}, nil, nil

	case *parser.RangePredicate:
		// Range scan via B-tree.
		return &Plan{
			Type:      PlanTypeBtreeScan,
			IndexName: indexName,
			StartKey:  p.StartKey,
			EndKey:    p.EndKey,
			OrderAsc:  true,
		}, nil, nil

	case *parser.TextSearchPredicate:
		// Full-text search via inverted index.
		return &Plan{
			Type:      PlanTypeInvertedScan,
			IndexName: indexName,
			TextField: p.Field,
			TextQuery: p.Query,
			OrderAsc:  true,
		}, nil, nil

	case *parser.NearPredicate:
		// Build a bounding box around the NEAR circle for R-tree pre-filter.
		// Approximate: 1 degree lat ≈ 111 km, 1 degree lon ≈ 111 km * cos(lat).
		latDelta := p.Radius / 111000.0
		lonDelta := p.Radius / (111000.0 * cosDeg(p.Lat))
		return &Plan{
			Type:          PlanTypeSpatialScan,
			IndexName:     indexName,
			SpatialMinLat: p.Lat - latDelta,
			SpatialMaxLat: p.Lat + latDelta,
			SpatialMinLon: p.Lon - lonDelta,
			SpatialMaxLon: p.Lon + lonDelta,
			OrderAsc:      true,
		}, pred, nil // keep pred as residual for exact distance filtering

	case *parser.WithinPredicate:
		return &Plan{
			Type:          PlanTypeSpatialScan,
			IndexName:     indexName,
			SpatialMinLat: p.MinLat,
			SpatialMaxLat: p.MaxLat,
			SpatialMinLon: p.MinLon,
			SpatialMaxLon: p.MaxLon,
			OrderAsc:      true,
		}, nil, nil

	case *parser.AndPredicate:
		// Try to push one side to the index and keep the other as residual.
		leftPlan, leftResidual, err := buildScanPlan(indexName, p.Left)
		if err != nil {
			return nil, nil, err
		}
		_, _, rightErr := buildScanPlan(indexName, p.Right)
		if rightErr == nil && leftResidual == nil {
			// Both sides can use an index; just keep left plan + right as residual.
			return leftPlan, p.Right, nil
		}
		// Left plan covers the index access; right becomes residual.
		if leftResidual != nil {
			return leftPlan, &parser.AndPredicate{Left: leftResidual, Right: p.Right}, nil
		}
		return leftPlan, p.Right, nil

	default:
		// Fall back to full B-tree scan + filter.
		return &Plan{
			Type:      PlanTypeBtreeScan,
			IndexName: indexName,
			OrderAsc:  true,
		}, pred, nil
	}
}

// applySortHint adjusts the scan direction / marks a sort is needed.
func applySortHint(plan *Plan, ob *parser.OrderBy) {
	plan.OrderAsc = !ob.Desc
}

// -----------------------------------------------------------------------
// Math helpers (avoid importing math to keep this package lightweight)
// -----------------------------------------------------------------------

// cosDeg returns cos(degrees) for bounding-box calculations.
func cosDeg(deg float64) float64 {
	const pi = 3.14159265358979323846
	return cosApprox(deg * pi / 180.0)
}

// cosApprox is a simple Taylor-series approximation (avoids importing math).
func cosApprox(x float64) float64 {
	const pi = 3.14159265358979323846
	// Clamp to [-π, π]
	for x > pi {
		x -= 2 * pi
	}
	for x < -pi {
		x += 2 * pi
	}
	x2 := x * x
	return 1 - x2/2 + x2*x2/24 - x2*x2*x2/720
}
