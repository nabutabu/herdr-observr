package otel

import (
	"context"
	"testing"
	"time"

	"github.com/nabutabu/herdr-observr/internal/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

// fullConfig returns a 4.2 config with every exporter key set: insecure
// explicitly false (TLS), plus endpoint/timeout/headers. Each key maps to
// exactly one exporter option, and the false-insecure suppresses the plaintext
// pin.
func fullConfig() *config.Config {
	cfg := testConfig()
	cfg.OTLP.Insecure = ptr(false)
	cfg.OTLP.Endpoint = ptr("collector:4317")
	cfg.OTLP.Timeout = ptr(500 * time.Millisecond)
	cfg.OTLP.Headers = ptr("X-Key=v")
	return cfg
}

// optionCounter adapts each signal's *OTLPOptions helper, which return
// different option slice types, to a plain count for the shared table below.
type optionCounter func(*config.Config) int

func TestExporterOptionsMapping(t *testing.T) {
	tests := []struct {
		name string
		fn   optionCounter
	}{
		{"metric", func(c *config.Config) int { return len(metricOTLPOptions(c)) }},
		{"log", func(c *config.Config) int { return len(logOTLPOptions(c)) }},
		{"trace", func(c *config.Config) int { return len(traceOTLPOptions(c)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// nil config: exactly the historical plaintext WithInsecure() pin —
			// zero behavior change for installs with no plugin config file.
			if n := tt.fn(nil); n != 1 {
				t.Errorf("options(nil) = %d, want exactly 1 (plaintext pin)", n)
			}
			// Endpoint-only config: the pin stays (insecure defaults true) and
			// the endpoint adds one more option.
			endpointOnly := testConfig()
			endpointOnly.OTLP.Endpoint = ptr("collector:4317")
			if n := tt.fn(endpointOnly); n != 2 {
				t.Errorf("options(endpoint-only) = %d, want 2 (pin + endpoint)", n)
			}
			// Full config with insecure=false: three options, and no pin.
			if n := tt.fn(fullConfig()); n != 3 {
				t.Errorf("options(full) = %d, want 3 (endpoint+timeout+headers, no pin)", n)
			}
		})
	}
}

// TestOTLPConfigSmokeBuildNew exercises that each signal's exporter
// constructor accepts the option list a full config produces — a mapping that
// is pure here but consumed by the SDK.
func TestOTLPConfigSmokeBuildNew(t *testing.T) {
	ctx := context.Background()
	cfg := fullConfig()
	tests := []struct {
		name  string
		build func() error
	}{
		{"metric", func() error {
			_, err := otlpmetricgrpc.New(ctx, metricOTLPOptions(cfg)...)
			return err
		}},
		{"log", func() error {
			_, err := otlploggrpc.New(ctx, logOTLPOptions(cfg)...)
			return err
		}},
		{"trace", func() error {
			_, err := otlptracegrpc.New(ctx, traceOTLPOptions(cfg)...)
			return err
		}},
	}
	for _, tt := range tests {
		if err := tt.build(); err != nil {
			t.Errorf("%s exporter build from full config: %v", tt.name, err)
		}
	}
}
