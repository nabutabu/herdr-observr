// Package otel initializes the OpenTelemetry SDKs for herdr-observr (plan
// 3.1). It builds the resource from the environment and constructs
// MeterProvider / LoggerProvider / TracerProvider whose OTLP/gRPC exporters
// read their endpoint, TLS, headers, and timeout from the standard
// OTEL_EXPORTER_OTLP_* variables, so endpoint selection never needs
// code-level config — extended by the 4.2 plugin config file (internal/config),
// whose explicitly-set keys override env as exporter options and resource
// attributes.
//
// Privacy constraint: only process metadata leaves through this path. Nothing
// here adds pane content or agent transcripts to resources, metrics, events,
// or attributes.
package otel
