package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseEnv(t *testing.T) {
	src := `
# comment line
   # indented comment
export OTEL_SERVICE_NAME=from-export
OTEL_SERVICE_NAME="quoted value"
  EMPTY_KEY=
PLAIN=value
OTEL_EXPORTER_OTLP_ENDPOINT='http://collector:4317'
malformed-no-equals
=empty-key
    spaced   =   padded   
`
	got, err := ParseEnv(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ParseEnv: %v", err)
	}

	want := map[string]string{
		"OTEL_SERVICE_NAME":           "quoted value", // later line wins for the same key
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
		"PLAIN":                       "value",
		"EMPTY_KEY":                   "",
		"spaced":                      "padded",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("ParseEnv %q = %q, want %q", k, got[k], v)
		}
	}
	for _, banned := range []string{"malformed-no-equals", "=empty-key"} {
		if _, ok := got[banned]; ok {
			t.Errorf("malformed/empty-key line %q must be skipped, got %v", banned, got)
		}
	}
}

func TestParseCommaKV(t *testing.T) {
	attrs := ParseResourceAttributes("service.name=svc, x=y , malformed")
	if len(attrs) != 2 { // malformed entry skipped with a warning
		t.Fatalf("ParseResourceAttributes = %v, want 2 keys (malformed skipped)", attrs)
	}
	if attrs["service.name"] != "svc" || attrs["x"] != "y" {
		t.Errorf("ParseResourceAttributes = %v", attrs)
	}

	headers := ParseHeaders("Authorization=Bearer token, X-Tenant=acme")
	if len(headers) != 2 || headers["Authorization"] != "Bearer token" || headers["X-Tenant"] != "acme" {
		t.Errorf("ParseHeaders = %v", headers)
	}
}

func TestFromPairsPresence(t *testing.T) {
	// An explicit false must be distinguishable from "unset" (nil), which is
	// the whole point of the pointer fields.
	pairs := map[string]string{
		KeyOtlpEndpoint:      "collector:4317",
		KeyOtlpInsecure:      "false",
		KeyOtlpTimeout:       "750ms",
		KeyOtlpHeaders:       "X-Key=v",
		KeyServiceName:       "svc",
		KeyResourceAttrs:     "a=b",
		KeyMachineID:         "override-uuid",
		KeyEmitPerWsGauges:   "false",
		"UNKNOWN_FUTURE_KEY": "ignored",
	}
	cfg := FromPairs(pairs)

	if cfg.OTLP.Endpoint == nil || *cfg.OTLP.Endpoint != "collector:4317" {
		t.Errorf("endpoint = %v, want collector:4317", cfg.OTLP.Endpoint)
	}
	if cfg.OTLP.Insecure == nil || *cfg.OTLP.Insecure != false {
		t.Errorf("insecure = %v, want explicit false", cfg.OTLP.Insecure)
	}
	if cfg.OTLP.InsecureDefault() {
		t.Error("InsecureDefault() = true, want false when key set to false")
	}
	if cfg.OTLP.Timeout == nil || *cfg.OTLP.Timeout != 750*time.Millisecond {
		t.Errorf("timeout = %v, want 750ms", cfg.OTLP.Timeout)
	}
	if cfg.OTLP.Headers == nil || *cfg.OTLP.Headers != "X-Key=v" {
		t.Errorf("headers = %v", cfg.OTLP.Headers)
	}
	if cfg.ServiceName == nil || *cfg.ServiceName != "svc" {
		t.Errorf("service name = %v", cfg.ServiceName)
	}
	if cfg.MachineID == nil || *cfg.MachineID != "override-uuid" {
		t.Errorf("machine id = %v", cfg.MachineID)
	}
	if cfg.EmitPerWorkspaceGauges == nil || *cfg.EmitPerWorkspaceGauges != false {
		t.Errorf("emit per-workspace gauges = %v, want explicit false", cfg.EmitPerWorkspaceGauges)
	}
	if cfg.EmitPerWorkspaceGaugesDefault() {
		t.Error("EmitPerWorkspaceGaugesDefault() = true, want false")
	}
}

func TestFromPairsDefaults(t *testing.T) {
	cfg := FromPairs(map[string]string{KeyOtlpEndpoint: "collector:4317"})
	if !cfg.OTLP.InsecureDefault() {
		t.Error("InsecureDefault() = false, want the historic plaintext default true")
	}
	if !cfg.EmitPerWorkspaceGaugesDefault() {
		t.Error("EmitPerWorkspaceGaugesDefault() = false, want default true")
	}
	if cfg.OTLP.Timeout != nil || cfg.MachineID != nil || cfg.ServiceName != nil {
		t.Errorf("unset keys must stay nil: %+v", cfg)
	}
}

func TestFromPairsIgnoredBadBools(t *testing.T) {
	// Bad booleans/durations must be skipped with a warning, not poisoned.
	cfg := FromPairs(map[string]string{
		KeyOtlpInsecure:    "not-a-bool",
		KeyEmitPerWsGauges: "yes",
		KeyOtlpTimeout:     "three shakes",
	})
	if cfg.OTLP.Insecure != nil {
		t.Errorf("insecure = %v, want nil after invalid value", cfg.OTLP.Insecure)
	}
	if cfg.EmitPerWorkspaceGauges != nil {
		t.Errorf("gauges = %v, want nil after invalid value", cfg.EmitPerWorkspaceGauges)
	}
	if cfg.OTLP.Timeout != nil {
		t.Errorf("timeout = %v, want nil after invalid value", cfg.OTLP.Timeout)
	}
}

func TestLoadWithoutConfigDir(t *testing.T) {
	t.Setenv(ConfigDirEnvVar, "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load without config dir: %v", err)
	}
	if cfg == nil {
		t.Fatal("Load returned nil config")
	}
	if cfg.OTLP.Endpoint != nil {
		t.Errorf("endpoint = %v, want nil with no config dir", cfg.OTLP.Endpoint)
	}
}

func TestLoadAbsentFile(t *testing.T) {
	t.Setenv(ConfigDirEnvVar, t.TempDir()) // exists but has no .env
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with empty config dir: %v", err)
	}
	if cfg == nil {
		t.Fatal("Load returned nil config")
	}
	if cfg.MachineID != nil {
		t.Errorf("machine id = %v, want nil with no file", cfg.MachineID)
	}
}

func TestLoadFromConfigDir(t *testing.T) {
	dir := t.TempDir()
	content := strings.Join([]string{
		"# herdr-observr sample config",
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317",
		"HERDR_OBSRVR_MACHINE_ID=config-file-uuid",
		"HERDR_OBSRVR_EMIT_PER_WORKSPACE_GAUGES=false",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(content), 0600); err != nil {
		t.Fatalf("writing .env: %v", err)
	}
	t.Setenv(ConfigDirEnvVar, dir)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.OTLP.Endpoint == nil || *cfg.OTLP.Endpoint != "http://collector:4317" {
		t.Errorf("endpoint = %v", cfg.OTLP.Endpoint)
	}
	if cfg.MachineID == nil || *cfg.MachineID != "config-file-uuid" {
		t.Errorf("machine id = %v", cfg.MachineID)
	}
	if cfg.EmitPerWorkspaceGauges == nil || *cfg.EmitPerWorkspaceGauges {
		t.Errorf("gauges = %v, want explicit false", cfg.EmitPerWorkspaceGauges)
	}
}

func TestLoadFileReadError(t *testing.T) {
	// Config dir exists but is a file, so opening <dir>/.env fails; Load must
	// surface an error, not panic, and main decides to warn and continue.
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, []byte("x"), 0600); err != nil {
		t.Fatalf("seeding blocker: %v", err)
	}
	t.Setenv(ConfigDirEnvVar, filepath.Join(block, "sub"))

	if _, err := Load(); err == nil {
		t.Error("Load with unreadable dir = nil error, want error")
	}
}
