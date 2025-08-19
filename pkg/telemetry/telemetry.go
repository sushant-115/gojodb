// Package telemetry provides a standardized, one-stop-shop for setting up
// OpenTelemetry for the GojoDB project, including metrics and tracing.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

// Config holds all the configuration for the telemetry system.
type Config struct {
	// Enabled toggles the entire telemetry system on or off.
	Enabled bool `yaml:"enabled"`
	// ServiceName is the name of the service that will appear in traces and metrics.
	ServiceName string `yaml:"service_name"`
	// PrometheusPort is the port on which to expose the /metrics endpoint.
	PrometheusPort int `yaml:"prometheus_port"`
	// TraceSampleRatio is the fraction of traces to sample (e.g., 0.01 for 1%).
	// Defaults to 1.0 (always sample) if not set or invalid.
	TraceSampleRatio float64 `yaml:"trace_sample_ratio"`
}

// Telemetry represents the active telemetry components.
type Telemetry struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	Tracer         trace.Tracer
	Meter          metric.Meter
}

// ShutdownFunc is a function that gracefully shuts down the telemetry providers.
type ShutdownFunc func(ctx context.Context) error

// New initializes the OpenTelemetry SDK for metrics and tracing.
// It sets up a Prometheus exporter for metrics. It returns a Telemetry struct
// containing the active components and a shutdown function.
func New(config Config) (*Telemetry, ShutdownFunc, error) {
	if !config.Enabled {
		// If telemetry is disabled, return no-op providers.
		return &Telemetry{
			TracerProvider: nil,
			MeterProvider:  nil,
			Tracer:         nooptrace.NewTracerProvider().Tracer(""),
			Meter:          noop.NewMeterProvider().Meter(""),
		}, func(ctx context.Context) error { return nil }, nil
	}

	// --- General OpenTelemetry Setup ---
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(config.ServiceName),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// --- Metrics Setup (Prometheus) ---
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)

	// Expose the Prometheus metrics endpoint.
	go func() {
		addr := fmt.Sprintf(":%d", config.PrometheusPort)
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(addr, nil); err != nil {
			otel.Handle(fmt.Errorf("prometheus http server failed: %w", err))
		}
	}()

	// --- Tracing Setup ---
	// Set a default sampling ratio if not provided or invalid.
	sampleRatio := config.TraceSampleRatio
	if sampleRatio <= 0 || sampleRatio > 1 {
		sampleRatio = 1.0 // Default to always sampling
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// Use the ratio-based sampler for production use.
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sampleRatio)),
	)

	// Set the global providers.
	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// Create the main tracer and meter for the application.
	tracer := tracerProvider.Tracer(config.ServiceName)
	meter := meterProvider.Meter(config.ServiceName)

	tel := &Telemetry{
		TracerProvider: tracerProvider,
		MeterProvider:  meterProvider,
		Tracer:         tracer,
		Meter:          meter,
	}

	// The shutdown function ensures all buffered telemetry is exported.
	shutdown := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown tracer provider: %w", err)
		}
		if err := meterProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown meter provider: %w", err)
		}
		return nil
	}

	return tel, shutdown, nil
}
