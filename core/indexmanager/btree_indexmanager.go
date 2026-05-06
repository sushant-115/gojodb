package indexmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/sushant-115/gojodb/api/proto"
	"github.com/sushant-115/gojodb/core/indexing/btree"
	"github.com/sushant-115/gojodb/core/indexing/spatial"
	"github.com/sushant-115/gojodb/core/write_engine/wal"
	internaltelemetry "github.com/sushant-115/gojodb/internal/telemetry"
	"github.com/sushant-115/gojodb/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type BTreeIndexManager struct {
	mu        sync.RWMutex
	tree      *btree.BTree[string, string]
	latestLSN uint64 // Local LSN for this index
	// For snapshotting (in-memory dummy for now, needs disk persistence)
	snapshotData map[string][]byte // snapshotID -> serialized data
	tracer       trace.Tracer
	metrics      *internaltelemetry.GrpcGatewayMetrics
	serviceName  string
}

func NewBTreeIndexManager(tree *btree.BTree[string, string], tel *telemetry.Telemetry) *BTreeIndexManager {
	grpcMetrics, err := internaltelemetry.NewGrpcGatewayMetrics(tel.Meter)
	if err != nil {
		fmt.Println("failed to create gRPC metrics:", err)
	}
	return &BTreeIndexManager{
		tree:         tree,
		snapshotData: make(map[string][]byte),
		tracer:       tel.Tracer,
		metrics:      grpcMetrics,
		serviceName:  "btree_indexmanager",
	}
}

func (m *BTreeIndexManager) Name() string { return "btree" }

func (m *BTreeIndexManager) Put(ctx context.Context, key string, value []byte) error {
	metricCtx, span, startTime := m.StartMetricsAndTrace(ctx, "Put")
	var statusCode otelcodes.Code = otelcodes.Ok
	defer func() {
		m.EndMetricsAndTrace(metricCtx, span, startTime, "Put", statusCode)
	}()

	err := m.tree.Insert(key, string(value), 0)
	if err != nil {
		statusCode = otelcodes.Error
		return err
	}
	m.latestLSN++
	return nil
}

func (m *BTreeIndexManager) Get(ctx context.Context, key string) ([]byte, bool) {
	metricCtx, span, startTime := m.StartMetricsAndTrace(ctx, "Get")
	var statusCode otelcodes.Code = otelcodes.Ok
	defer func() {
		m.EndMetricsAndTrace(metricCtx, span, startTime, "Get", statusCode)
	}()

	value, found, err := m.tree.Search(key)
	if err != nil {
		statusCode = otelcodes.Error
	}
	return []byte(value), found
}

func (m *BTreeIndexManager) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.tree.Delete(key, 0)
	if err == nil {
		m.latestLSN++
	}
	return err
}

// Assuming btree.BTree has a ScanRange equivalent, or implementing here.
func (m *BTreeIndexManager) GetRange(ctx context.Context, startKey, endKey string, limit int32) ([]*pb.KeyValuePair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var results []*pb.KeyValuePair
	// This is a dummy implementation based on a simple map.
	// A real B-tree would have efficient range iteration.

	if strings.TrimSpace(startKey) == "" || strings.TrimSpace(endKey) == "" {
		//resp = Response{Status: "ERROR", Message: fmt.Sprintf("Invalid startKey = '%s'and endKey = '%s'", req.StartKey, req.EndKey)}
	}
	var iterator btree.BTreeIterator[string, string]
	var err error
	if startKey == "*" || endKey == "*" {
		iterator, err = m.tree.FullScan()
	} else {
		iterator, err = m.tree.Iterator(startKey, endKey)
	}
	if err != nil {
		//resp = Response{Status: "ERROR", Message: fmt.Sprintf("Failed to create iterator: %v", err)}
	} else {
		for {
			key, val, isNext, iterErr := iterator.Next()
			if iterErr != nil || !isNext {
				// log.Println("ITERATOR NEXT: ", isNext, iterErr)
				break
			}
			results = append(results, &pb.KeyValuePair{Key: key, Value: []byte(val)})
		}
		iterator.Close()
	}

	// for k := range m.tree.Iterator(startKey, endKey) { // Assuming GetAll returns a map[string][]byte for snapshot
	// 	keys = append(keys, k)
	// }
	sort.SliceStable(results, func(i int, j int) bool { return results[i].Key > results[j].Key }) // Sort keys for proper range behavior
	return results, nil
}

// TextSearch is not applicable for a pure B-tree.
func (m *BTreeIndexManager) TextSearch(ctx context.Context, query, indexName string, limit int32) ([]*pb.TextSearchResult, error) {
	return nil, fmt.Errorf("TextSearch not supported on BTreeIndexManager")
}

// AddDocument is not applicable for B-tree.
func (m *BTreeIndexManager) AddDocument(ctx context.Context, docID string, content string) error {
	return fmt.Errorf("AddDocument not supported on BTreeIndexManager")
}

// InsertSpatial is not applicable for B-tree.
func (m *BTreeIndexManager) InsertSpatial(ctx context.Context, rect spatial.Rect, dataID string) error {
	return fmt.Errorf("InsertSpatial not supported on BTreeIndexManager")
}

// DeleteSpatial is not applicable for B-tree.
func (m *BTreeIndexManager) DeleteSpatial(ctx context.Context, dataID string) error {
	return fmt.Errorf("DeleteSpatial not supported on BTreeIndexManager")
}

// PrepareSnapshot for BTreeIndexManager (dummy in-memory serialization).
func (m *BTreeIndexManager) PrepareSnapshot(ctx context.Context) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// In a real B-tree, you'd serialize its internal structure to disk.
	// For this example, we'll serialize all its content (keys and values).
	allData := make(map[string][]byte) // Assuming BTree has a method to get all data
	snapshotBytes, err := json.Marshal(struct {
		Data map[string][]byte `json:"data"`
		LSN  uint64            `json:"lsn"`
	}{
		Data: allData,
		LSN:  m.latestLSN,
	})
	if err != nil {
		return "", fmt.Errorf("failed to marshal btree snapshot: %v", err)
	}
	snapshotID := fmt.Sprintf("btree-snapshot-%d", time.Now().UnixNano())
	m.snapshotData[snapshotID] = snapshotBytes
	log.Printf("BTreeIndexManager: Prepared snapshot %s with LSN %d", snapshotID, m.latestLSN)
	return snapshotID, nil
}

// StreamSnapshot for BTreeIndexManager.
func (m *BTreeIndexManager) StreamSnapshot(ctx context.Context, snapshotID string, chunkChan chan []byte) error {
	defer close(chunkChan)
	m.mu.RLock()
	snapshotBytes, ok := m.snapshotData[snapshotID] // Get from in-memory map for dummy
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("btree snapshot ID %s not found", snapshotID)
	}

	chunkSize := 4 * 1024 // 4KB chunks
	for i := 0; i < len(snapshotBytes); i += chunkSize {
		end := i + chunkSize
		if end > len(snapshotBytes) {
			end = len(snapshotBytes)
		}
		chunk := snapshotBytes[i:end]
		select {
		case chunkChan <- chunk:
			// Chunk sent
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending btree snapshot chunk")
		}
	}
	log.Printf("BTreeIndexManager: Streamed snapshot %s successfully", snapshotID)
	return nil
}

// ApplySnapshot for BTreeIndexManager.
func (m *BTreeIndexManager) ApplySnapshot(ctx context.Context, snapshotID string, chunkChan <-chan []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var receivedBytes []byte
	for chunk := range chunkChan {
		receivedBytes = append(receivedBytes, chunk...)
	}

	var snapshotContent struct {
		Data map[string][]byte `json:"data"`
		LSN  uint64            `json:"lsn"`
	}
	if err := json.Unmarshal(receivedBytes, &snapshotContent); err != nil {
		return fmt.Errorf("failed to unmarshal btree snapshot: %v", err)
	}

	// Rebuild the B-tree from the snapshot data
	// m.tree = btree.NewBTree() // Clear existing
	// for k, v := range snapshotContent.Data {
	// 	m.tree.Put(k, v) // Re-populate
	// }
	m.latestLSN = snapshotContent.LSN
	log.Printf("BTreeIndexManager: Applied snapshot %s, LSN %d", snapshotID, m.latestLSN)
	return nil
}

func (m *BTreeIndexManager) GetLatestLSN() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.latestLSN
}

// ApplyLogRecord for BTreeIndexManager.
func (m *BTreeIndexManager) ApplyLogRecord(ctx context.Context, entry *wal.LogRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.LogType != wal.LogTypeBtree {
		return fmt.Errorf("BTreeIndexManager cannot apply log entry for index type %v", entry.LogType)
	}
	switch entry.Type {
	// case "PUT":
	// 	err := m.tree.Put(entry.Key, entry.Value)
	// 	if err == nil {
	// 		m.latestLSN++ // Increment LSN upon successful application
	// 	}
	// 	return err
	// case "DELETE":
	// 	err := m.tree.Delete(entry.Key)
	// 	if err == nil {
	// 		m.latestLSN++ // Increment LSN upon successful application
	// 	}
	// 	return err
	default:
		return fmt.Errorf("unsupported log entry type for BTreeIndexManager: %v", entry.Type)
	}
}

// StartMetricsAndTrace begins the telemetry recording for a gRPC method.
// It returns a new context, the trace span, and the start time.
func (s *BTreeIndexManager) StartMetricsAndTrace(ctx context.Context, fullMethodName string) (context.Context, trace.Span, time.Time) {
	startTime := time.Now()

	// Increment active RPCs and started counter
	s.metrics.ActiveRpcsUpDownCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))
	s.metrics.RpcsStartedCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Start a new trace span
	ctx, span := s.tracer.Start(ctx, fullMethodName, trace.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	return ctx, span, startTime
}

// EndMetricsAndTrace completes the telemetry recording for a gRPC method.
func (s *BTreeIndexManager) EndMetricsAndTrace(ctx context.Context, span trace.Span, startTime time.Time, fullMethodName string, statusCode otelcodes.Code) {
	latency := time.Since(startTime).Milliseconds()

	// Set span status based on the final gRPC code
	if statusCode != otelcodes.Ok {
		span.SetStatus(otelcodes.Error, statusCode.String())
	} else {
		span.SetStatus(otelcodes.Ok, "Success")
	}
	span.End()

	// Decrement active RPCs
	s.metrics.ActiveRpcsUpDownCounter.Add(ctx, -1, metric.WithAttributes(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
	))

	// Define attributes for the completed RPC.
	metricAttributes := attribute.NewSet(
		attribute.String("grpc.service", s.serviceName),
		attribute.String("grpc.method", fullMethodName),
		attribute.String("grpc.code", statusCode.String()),
	)

	// Record latency and increment handled counter
	s.metrics.RpcLatencyHistogram.Record(ctx, latency, metric.WithAttributeSet(metricAttributes))
	s.metrics.RpcsHandledCounter.Add(ctx, 1, metric.WithAttributeSet(metricAttributes))
}
