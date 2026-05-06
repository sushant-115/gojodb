package internaltelemetry

import (
	"go.opentelemetry.io/otel/metric"
	// Your telemetry library
)

// GrpcGatewayMetrics holds all the metric instruments for the gRPC gateway.
type GrpcGatewayMetrics struct {
	RpcsStartedCounter      metric.Int64Counter
	RpcsHandledCounter      metric.Int64Counter
	RpcLatencyHistogram     metric.Int64Histogram
	ActiveRpcsUpDownCounter metric.Int64UpDownCounter
	// You could also add msgSentCounter and msgReceivedCounter for streaming RPCs
}

// NewGrpcGatewayMetrics creates and registers all the metrics for the gRPC gateway.
func NewGrpcGatewayMetrics(meter metric.Meter) (*GrpcGatewayMetrics, error) {
	rpcsStartedCounter, err := meter.Int64Counter(
		"gojodb.grpc.server.started_total",
		metric.WithDescription("Total number of RPCs started."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	rpcsHandledCounter, err := meter.Int64Counter(
		"gojodb.grpc.server.handled_total",
		metric.WithDescription("Total number of RPCs completed."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	rpcLatencyHistogram, err := meter.Int64Histogram(
		"gojodb.grpc.server.duration",
		metric.WithDescription("The latency of RPCs."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	activeRpcsUpDownCounter, err := meter.Int64UpDownCounter(
		"gojodb.grpc.server.active_rpcs",
		metric.WithDescription("Number of active RPCs."),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return &GrpcGatewayMetrics{
		RpcsStartedCounter:      rpcsStartedCounter,
		RpcsHandledCounter:      rpcsHandledCounter,
		RpcLatencyHistogram:     rpcLatencyHistogram,
		ActiveRpcsUpDownCounter: activeRpcsUpDownCounter,
	}, nil
}
