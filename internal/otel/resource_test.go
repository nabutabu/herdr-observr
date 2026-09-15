package otel

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/resource"
)

func attrMap(t *testing.T, res *resource.Resource) map[string]string {
	t.Helper()
	m := map[string]string{}
	for _, kv := range res.Attributes() {
		m[string(kv.Key)] = kv.Value.AsString()
	}
	return m
}

func TestBuildResourceDefaultsServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")

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
	t.Setenv("OTEL_SERVICE_NAME", "")
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
	t.Setenv("OTEL_SERVICE_NAME", "")
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
