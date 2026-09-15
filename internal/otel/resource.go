package otel

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

// DefaultServiceName is the service.name used when neither OTEL_SERVICE_NAME
// nor a service.name in OTEL_RESOURCE_ATTRIBUTES provides one (3.1).
const DefaultServiceName = "herdr-telemetry"

// serviceNameKey is the resource attribute key for the service name. Kept as
// a plain key rather than a semconv import to avoid coupling this package to
// a specific semantic-conventions version.
const serviceNameKey = "service.name"

// BuildResource constructs the process's OTel resource from the environment:
//
//   - OTEL_SERVICE_NAME — set as service.name, taking precedence over any
//     service.name carried in OTEL_RESOURCE_ATTRIBUTES (the FromEnv detector
//     merges the env-name last).
//   - OTEL_RESOURCE_ATTRIBUTES — any user attributes (comma-separated
//     key=value pairs).
//   - telemetry.sdk.* metadata from the SDK.
//
// When neither source supplies a service.name, DefaultServiceName is applied.
// Phase 3.2 appends herdr.machine.id / herdr.machine.hostname here.
//
// A malformed OTEL_RESOURCE_ATTRIBUTES entry returns ErrPartialResource; the
// valid attributes are kept and the partial resource is returned, with the
// error downgraded to a warning — the spec treats that as non-fatal.
func BuildResource(ctx context.Context) (*resource.Resource, error) {
	res, err := resource.New(ctx, resource.WithFromEnv(), resource.WithTelemetrySDK())
	if err != nil {
		// Most commonly ErrPartialResource (malformed env attribute) or
		// ErrSchemaURLConflict. Both leave a usable partial resource.
		if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
			slog.Warn("partial OTel resource from environment", "error", err)
			err = nil
		} else {
			return res, err
		}
	}

	if v, ok := ResourceAttribute(res, serviceNameKey); !ok || v == "" {
		if res, err = resource.Merge(res, resource.NewSchemaless(attribute.String(serviceNameKey, DefaultServiceName))); err != nil {
			return res, err
		}
	}
	return res, nil
}

// ResourceAttribute returns the string value of key in res, or ok=false when
// res is nil or the key is absent.
func ResourceAttribute(res *resource.Resource, key string) (string, bool) {
	if res == nil {
		return "", false
	}
	v, ok := res.Set().Value(attribute.Key(key))
	if !ok {
		return "", false
	}
	return v.AsString(), true
}

// ServiceName returns the resolved service.name of res, falling back to
// DefaultServiceName when absent. Used for logging what the exporter will
// identify itself as.
func ServiceName(res *resource.Resource) string {
	if v, ok := ResourceAttribute(res, serviceNameKey); ok {
		return v
	}
	return DefaultServiceName
}
