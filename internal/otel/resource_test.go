package otel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/nabutabu/herdr-observr/internal/config"
	"github.com/nabutabu/herdr-observr/internal/machineid"
	"go.opentelemetry.io/otel/sdk/resource"
)

// setCleanEnv gives each test hermetic env vars for BuildResource so the suite
// doesn't depend on a developer's shell or CI environment.
func setCleanEnv(t *testing.T) {
	t.Helper()
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("HERDR_PLUGIN_STATE_DIR", "")
}

func attrMap(t *testing.T, res *resource.Resource) map[string]string {
	t.Helper()
	m := map[string]string{}
	for _, kv := range res.Attributes() {
		m[string(kv.Key)] = kv.Value.AsString()
	}
	return m
}

func TestBuildResourceDefaultsServiceName(t *testing.T) {
	setCleanEnv(t)

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	if got := ServiceName(res); got != DefaultServiceName {
		t.Errorf("service.name = %q, want %q", got, DefaultServiceName)
	}
}

func TestBuildResourceHonorsOTELServiceName(t *testing.T) {
	// OTEL_SERVICE_NAME must win over a service.name in
	// OTEL_RESOURCE_ATTRIBUTES, per the env precedence rules.
	setCleanEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "explicit")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=env-name,x=y")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	m := attrMap(t, res)
	if m["service.name"] != "explicit" {
		t.Errorf("service.name = %q, want %q", m["service.name"], "explicit")
	}
	if m["x"] != "y" {
		t.Errorf("x = %q, want %q", m["x"], "y")
	}
}

func TestBuildResourceHonorsEnvServiceName(t *testing.T) {
	setCleanEnv(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=env-name")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	if got := ServiceName(res); got != "env-name" {
		t.Errorf("service.name = %q, want %q", got, "env-name")
	}
}

func TestBuildResourceKeepsValidAttrsOnPartialError(t *testing.T) {
	// A malformed entry (no '=') is ErrPartialResource, which BuildResource
	// downgrades to a warning while keeping the valid pairs.
	setCleanEnv(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "valid=kept,malformed")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource must not fail on a partial env resource: %v", err)
	}
	m := attrMap(t, res)
	if m["valid"] != "kept" {
		t.Errorf("valid = %q, want %q", m["valid"], "kept")
	}
	if got := ServiceName(res); got != DefaultServiceName {
		t.Errorf("service.name = %q, want default %q", got, DefaultServiceName)
	}
}

// --- 3.2 machine-attribute tests ---

func TestBuildResourceAttachesMachineIDAndHostname(t *testing.T) {
	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", t.TempDir())

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	m := attrMap(t, res)
	if mid := m[machineIDKey]; mid == "" {
		t.Error("herdr.machine.id missing")
	} else if _, err := uuid.Parse(mid); err != nil {
		t.Errorf("herdr.machine.id = %q, not a valid UUID: %v", mid, err)
	}
	if hn := m[machineHostnameKey]; hn == "" {
		t.Error("herdr.machine.hostname missing")
	}
}

func TestBuildResourcePersistedMachineIDStableAcrossCalls(t *testing.T) {
	setCleanEnv(t)
	dir := t.TempDir()

	id, err := machineid.LoadOrCreate(dir)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	t.Setenv("HERDR_PLUGIN_STATE_DIR", dir)
	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	got, ok := ResourceAttribute(res, machineIDKey)
	if !ok || got != id {
		t.Errorf("herdr.machine.id = %q (ok=%v), want persisted %q", got, ok, id)
	}
}

func TestBuildResourceOmitsMachineIDWhenStateDirUnset(t *testing.T) {
	setCleanEnv(t)
	// HERDR_PLUGIN_STATE_DIR deliberately left empty (already set by setCleanEnv).

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	m := attrMap(t, res)
	if _, ok := m[machineIDKey]; ok {
		t.Error("herdr.machine.id present when HERDR_PLUGIN_STATE_DIR was unset")
	}
}

func TestBuildResourcePersistedMachineIDWinsOverEnvAttr(t *testing.T) {
	dir := t.TempDir()
	seed := uuid.New().String()
	if err := os.WriteFile(filepath.Join(dir, "machine-id"), []byte(seed), 0600); err != nil {
		t.Fatalf("seeding machine-id file: %v", err)
	}

	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", dir)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "herdr.machine.id=should-lose")

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	got, _ := ResourceAttribute(res, machineIDKey)
	if got != seed {
		t.Errorf("herdr.machine.id = %q, want persisted %q", got, seed)
	}
}

func TestBuildResourceSurvivesStoreFailure(t *testing.T) {
	// Point the state dir at a path blocked by a file: MkdirAll will fail,
	// causing LoadOrCreate to return an error. The resource must still build
	// without machine.id (degrade per-attribute, never the whole pipeline).
	setCleanEnv(t)
	dir := t.TempDir()
	block := filepath.Join(dir, "block")
	if err := os.WriteFile(block, []byte("x"), 0600); err != nil {
		t.Fatalf("seeding blocker file: %v", err)
	}
	t.Setenv("HERDR_PLUGIN_STATE_DIR", filepath.Join(block, "sub"))

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource must not fail on store error: %v", err)
	}
	m := attrMap(t, res)
	if _, ok := m[machineIDKey]; ok {
		t.Error("herdr.machine.id present despite store failure")
	}
}

func TestBuildResourceHostnameAlwaysPresent(t *testing.T) {
	// hostname must be present even when HERDR_PLUGIN_STATE_DIR is unset
	setCleanEnv(t)

	res, err := BuildResource(context.Background())
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	got, ok := ResourceAttribute(res, machineHostnameKey)
	if !ok || got == "" {
		t.Errorf("herdr.machine.hostname present=%v, value=%q; want non-empty", ok, got)
	}
	expected, _ := os.Hostname()
	if got != expected {
		t.Errorf("herdr.machine.hostname = %q, want %q", got, expected)
	}
}

// --- 4.2 config-file override tests ---

func testConfig() *config.Config {
	return &config.Config{OTLP: config.OTLP{}}
}

func TestBuildResourceConfigOverridesServiceName(t *testing.T) {
	// 4.2 precedence: a service.name/OTEL_SERVICE_NAME set in the config .env
	// must win over the process env and the default.
	setCleanEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "env-svc")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.name=attrs-svc")

	cfg := testConfig()
	cfg.ServiceName = ptr("cfg-svc")
	res, err := BuildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	if got := ServiceName(res); got != "cfg-svc" {
		t.Errorf("service.name = %q, want cfg-svc", got)
	}
}

func TestBuildResourceConfigAttrOverridesEnvAttr(t *testing.T) {
	// A config-file resource attribute must override the same key from the env.
	setCleanEnv(t)
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "mode=env,shared=env")

	cfg := testConfig()
	cfg.ResourceAttributes = ptr("mode=cfg,shared=cfg")
	res, err := BuildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	m := attrMap(t, res)
	if m["mode"] != "cfg" || m["shared"] != "cfg" {
		t.Errorf("mode/shared = %q/%q, want cfg/cfg", m["mode"], m["shared"])
	}
}

func TestBuildResourceMachineIDOverrideWins(t *testing.T) {
	// HERDR_OBSRVR_MACHINE_ID from the config file beats even the persisted
	// herdr.machine.id.
	dir := t.TempDir()
	setCleanEnv(t)
	t.Setenv("HERDR_PLUGIN_STATE_DIR", dir)

	cfg := testConfig()
	cfg.MachineID = ptr("config-file-uuid")
	res, err := BuildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	got, _ := ResourceAttribute(res, machineIDKey)
	if got != "config-file-uuid" {
		t.Errorf("herdr.machine.id = %q, want the config-file override", got)
	}
}

func TestBuildResourceNilConfigKeepsEnvBehavior(t *testing.T) {
	setCleanEnv(t)
	t.Setenv("OTEL_SERVICE_NAME", "env-svc")
	res, err := BuildResource(context.Background(), nil)
	if err != nil {
		t.Fatalf("BuildResource: %v", err)
	}
	if got := ServiceName(res); got != "env-svc" {
		t.Errorf("service.name = %q, want env-svc", got)
	}
}

func ptr[T any](v T) *T { return &v }
