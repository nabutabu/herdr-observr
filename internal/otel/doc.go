// Package otel initializes the OpenTelemetry metrics SDK for herdr-observr
// (plan 3.1). It builds the resource from the environment and constructs a
// MeterProvider whose OTLP/gRPC exporter reads its endpoint, TLS, headers,
// and timeout from the standard OTEL_EXPORTER_OTLP_* variables, so endpoint
// selection never needs code-level config.
//
// Privacy constraint: only process metadata leaves through this path. Nothing
// here adds pane content or agent transcripts to resources, metrics, or
// attributes.
package otel
