package otel

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/nabutabu/herdr-observr/internal/config"
	"github.com/nabutabu/herdr-observr/internal/machineid"
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

// Attribute keys for the machine identity attached by BuildResource (3.2).
// herdr.machine.id is the persisted plugin-generated UUID (never hostname,
// never a Herdr-native id); herdr.machine.hostname is a secondary display
// attribute only.
const (
	machineIDKey       = "herdr.machine.id"
	machineHostnameKey = "herdr.machine.hostname"

	// stateDirEnvVar is where the plugin persists herdr.machine.id (0.6, 4.3).
	// When unset (e.g. standalone dev runs), machine.id is omitted rather than
	// derived from hostname or a Herdr id.
	stateDirEnvVar = "HERDR_PLUGIN_STATE_DIR"
)

// BuildResource constructs the process's OTel resource from the environment
// and, when a plugin config file exists (4.2, from HERDR_PLUGIN_CONFIG_DIR),
// from that config file on top — config-file values win over env values:
//
//   - OTEL_SERVICE_NAME — set as service.name, taking precedence over any
//     service.name carried in OTEL_RESOURCE_ATTRIBUTES (the FromEnv detector
//     merges the env-name last). A service.name in the config file wins over
//     both.
//   - OTEL_RESOURCE_ATTRIBUTES — any user attributes (comma-separated
//     key=value pairs). Same-key config-file attributes win.
//   - telemetry.sdk.* metadata from the SDK.
//   - herdr.machine.id — the persisted UUID from HERDR_PLUGIN_STATE_DIR (3.2),
//     or the HERDR_OBSRVR_MACHINE_ID config-file override when present (which
//     wins over the persisted and env values); omitted with a warning when the
//     store is unavailable and no override exists.
//   - herdr.machine.hostname — the local hostname (3.2), merged when readable;
//     omitted with a warning otherwise.
//
// When neither the config file nor any env source supplies a service.name,
// DefaultServiceName is applied. Machine-identity failures degrade
// per-attribute (warn and omit), never failing the resource as a whole.
//
// A malformed OTEL_RESOURCE_ATTRIBUTES entry returns ErrPartialResource; the
// valid attributes are kept and the partial resource is returned, with the
// error downgraded to a warning — the spec treats that as non-fatal.
func BuildResource(ctx context.Context, cfg ...*config.Config) (*resource.Resource, error) {
	c := firstConfig(cfg)

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

	var attrs []attribute.KeyValue

	if hn, herr := os.Hostname(); herr == nil && hn != "" {
		attrs = append(attrs, attribute.String(machineHostnameKey, hn))
	} else {
		slog.Warn("hostname unavailable; herdr.machine.hostname omitted", "error", herr)
	}

	// Config-file service.name and resource attributes are merged last (below)
	// so they beat any same-key env value (4.2 precedence: file > env).
	if c != nil && c.ServiceName != nil && *c.ServiceName != "" {
		attrs = append(attrs, attribute.String(serviceNameKey, *c.ServiceName))
	}
	if c != nil && c.ResourceAttributes != nil {
		for k, v := range config.ParseResourceAttributes(*c.ResourceAttributes) {
			attrs = append(attrs, attribute.String(k, v))
		}
	}

	if c != nil && c.MachineID != nil && *c.MachineID != "" {
		// A config-file override is authoritative: the user has pinned the
		// machine identity, so the persisted UUID and any env collision both
		// lose to it.
		attrs = append(attrs, attribute.String(machineIDKey, *c.MachineID))
	} else {
		stateDir := os.Getenv(stateDirEnvVar)
		if stateDir == "" {
			slog.Warn(stateDirEnvVar + " unset; herdr.machine.id omitted (multi-machine grouping unavailable)")
		} else {
			id, merr := machineid.LoadOrCreate(stateDir)
			if merr == nil {
				attrs = append(attrs, attribute.String(machineIDKey, id))
			} else {
				slog.Warn("machine id unavailable; herdr.machine.id omitted", "error", merr)
			}
		}
	}

	// Merge last: NewSchemaless has no schema URL (no conflict) and its
	// attributes win over same-key env attrs via last-value-wins, so the
	// persisted id / config-file override always overrides an
	// OTEL_RESOURCE_ATTRIBUTES collision.
	if len(attrs) > 0 {
		if res, err = resource.Merge(res, resource.NewSchemaless(attrs...)); err != nil {
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

// firstConfig picks the first entry of a variadic config argument, or nil when
// none was passed. Keeping the argument variadic lets the many existing test
// call sites compile unchanged (nil still means "env-only"), while production
// passes the loaded plugin config.
func firstConfig(cfg []*config.Config) *config.Config {
	if len(cfg) == 0 {
		return nil
	}
	return cfg[0]
}
