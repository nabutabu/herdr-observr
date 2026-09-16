// Package config reads herdr-observr's own configuration. Plan 4.2: instead of
// raw env vars (which force users to touch shell profiles), the plugin reads a
// .env file from HERDR_PLUGIN_CONFIG_DIR on startup — the config-file location
// and format herdr's docs recommend for plugins ("Put user-editable config such
// as .env files under HERDR_PLUGIN_CONFIG_DIR"). The plugin owns the file
// format and lifecycle; herdr only creates the directory and injects the env
// var.
//
// Precedence per key: the config file wins when the key is present in it;
// keys absent from the file fall back to the process environment, then to
// built-in defaults (which the OTel SDK derives from OTEL_EXPORTER_OTLP_* when
// the plugin passes no explicit exporter option).
//
// The parser is a strict documented subset (.env KEY=VALUE); malformed input
// degrades per-entry with a warning and never fails the process.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// ConfigDirEnvVar is the directory herdr injects for plugin-scoped config.
	// Startup/runtime commands receive it; herdr creates the directory and
	// seeds it from any legacy plugin config location.
	ConfigDirEnvVar = "HERDR_PLUGIN_CONFIG_DIR"

	// FileName is the config file inside the config dir.
	FileName = ".env"

	// Recognized config keys. The OTEL_* keys use the standard OTel names so
	// users move familiar settings into the file unchanged; the HERDR_OBSRVR_*
	// keys are plugin-specific and namespaced to avoid colliding with a key
	// herdr or another plugin might inject.
	KeyOtlpEndpoint    = "OTEL_EXPORTER_OTLP_ENDPOINT"
	KeyOtlpInsecure    = "OTEL_EXPORTER_OTLP_INSECURE"
	KeyOtlpTimeout     = "OTEL_EXPORTER_OTLP_TIMEOUT"
	KeyOtlpHeaders     = "OTEL_EXPORTER_OTLP_HEADERS"
	KeyServiceName     = "OTEL_SERVICE_NAME"
	KeyResourceAttrs   = "OTEL_RESOURCE_ATTRIBUTES"
	KeyMachineID       = "HERDR_OBSRVR_MACHINE_ID"
	KeyEmitPerWsGauges = "HERDR_OBSRVR_EMIT_PER_WORKSPACE_GAUGES"
)

// Config holds the plugin's resolved configuration. Every field is
// presence-aware (pointer): nil means the key was not set in the config file,
// so the process environment and built-in defaults apply; a non-nil value is
// authoritative (the config file is the higher-precedence source).
type Config struct {
	// OTLP holds exporter settings. Endpoint/Timeout/Headers default to the
	// OTel SDK's env-driven values when nil; Insecure defaults to true when
	// nil (the plugin targets plaintext local collectors).
	OTLP OTLP

	// ServiceName overrides service.name (OTEL_SERVICE_NAME).
	ServiceName *string
	// ResourceAttributes overrides the key=value pairs carried in
	// OTEL_RESOURCE_ATTRIBUTES.
	ResourceAttributes *string
	// MachineID overrides the persisted herdr.machine.id from
	// HERDR_PLUGIN_STATE_DIR (0.6 / 4.3).
	MachineID *string
	// EmitPerWorkspaceGauges controls whether the 3.6/3.7 gauges emit
	// per-workspace datapoints. Defaults to true.
	EmitPerWorkspaceGauges *bool
}

// OTLP is the exporter-side subset of the plugin config.
type OTLP struct {
	// Endpoint is the collector base URL. When nil, the SDK's
	// OTEL_EXPORTER_OTLP_ENDPOINT / host:port default applies.
	Endpoint *string
	// Insecure controls plaintext transport. When nil, InsecureDefault()
	// reports true (the plugin's historical hard-coded WithInsecure()).
	Insecure *bool
	// Timeout bounds a single export attempt. When nil, the SDK's
	// OTEL_EXPORTER_OTLP_TIMEOUT default applies.
	Timeout *time.Duration
	// Headers are additional gRPC metadata on every export. When nil, the
	// SDK's OTEL_EXPORTER_OTLP_HEADERS default applies.
	Headers *string
}

// InsecureDefault returns the effective insecure value: the config file when
// set, else the hard-coded plaintext default that predates 4.2. Setting
// OTEL_EXPORTER_OTLP_INSECURE=false in the config file (not a shell env var —
// see the exporter-option precedence note in internal/otel/meter.go) switches
// the exporter to TLS.
func (o *OTLP) InsecureDefault() bool {
	if o.Insecure != nil {
		return *o.Insecure
	}
	return true
}

// EmitPerWorkspaceGaugesDefault returns the effective value of the cardinality
// flag, defaulting to true (dashboards and the 3.7 workspace-concurrency
// mission metric depend on the per-workspace dimension).
func (c *Config) EmitPerWorkspaceGaugesDefault() bool {
	if c.EmitPerWorkspaceGauges == nil {
		return true
	}
	return *c.EmitPerWorkspaceGauges
}

// Load reads the plugin config file from HERDR_PLUGIN_CONFIG_DIR and returns
// the resolved config. An unset env var or absent file produce an empty Config
// (identical to the pre-4.2 env-only behavior). A file-level read error is
// returned for the caller to warn about; content-level problems (malformed
// lines, unknown keys) are handled in ParseEnv/FromPairs as warn-and-skip.
func Load() (*Config, error) {
	dir := os.Getenv(ConfigDirEnvVar)
	if dir == "" {
		return &Config{}, nil
	}
	path := filepath.Join(dir, FileName)
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return &Config{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("opening config file %s: %w", path, err)
	}
	defer f.Close()
	pairs, perr := ParseEnv(f)
	if perr != nil {
		return nil, fmt.Errorf("parsing config file %s: %w", path, perr)
	}
	slog.Info("plugin config loaded", "path", path)
	return FromPairs(pairs), nil
}

// FromPairs builds a Config from a parsed key→value map, keeping presence
// information (a set key yields a non-nil pointer) and skipping unknown or
// unparseable keys with a warning so a stray entry can never fail the daemon.
func FromPairs(pairs map[string]string) *Config {
	cfg := &Config{}
	for k, v := range pairs {
		switch k {
		case KeyOtlpEndpoint:
			cfg.OTLP.Endpoint = strp(v)
		case KeyOtlpInsecure:
			if b, ok := parseBool(k, v); ok {
				cfg.OTLP.Insecure = boolp(b)
			}
		case KeyOtlpTimeout:
			d, err := time.ParseDuration(v)
			if err != nil {
				slog.Warn("config: invalid duration; ignoring", "key", k, "value", v, "error", err)
				continue
			}
			cfg.OTLP.Timeout = &d
		case KeyOtlpHeaders:
			cfg.OTLP.Headers = strp(v)
		case KeyServiceName:
			cfg.ServiceName = strp(v)
		case KeyResourceAttrs:
			cfg.ResourceAttributes = strp(v)
		case KeyMachineID:
			cfg.MachineID = strp(v)
		case KeyEmitPerWsGauges:
			if b, ok := parseBool(k, v); ok {
				cfg.EmitPerWorkspaceGauges = boolp(b)
			}
		default:
			slog.Warn("config: unknown key; ignoring", "key", k)
		}
	}
	return cfg
}

func parseBool(key, v string) (bool, bool) {
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		slog.Warn("config: invalid boolean; ignoring", "key", key, "value", v, "error", err)
		return false, false
	}
	return b, true
}

func strp(s string) *string {
	v := s
	return &v
}

func boolp(b bool) *bool {
	return &b
}
