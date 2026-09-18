package config

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// ParseEnv parses a minimal KEY=VALUE dotenv document. It is deliberately a
// small, documented subset of shell dotenv syntax — enough for a user-editable
// plugin config file without dragging in shell parsing semantics:
//
//   - Blank lines are ignored.
//   - Lines whose first non-space character is '#' are comments.
//   - An optional "export " prefix is tolerated and stripped.
//   - Values wrapped in matching single or double quotes have their surrounding
//     quotes removed.
//   - A key may not be empty.
//
// Malformed lines (no '=') cannot fail the whole config: they are skipped with
// a warning (Phase 5's degrade-gracefully rule — a typo in one line must never
// kill the daemon). The returned map never contains empty keys.
func ParseEnv(r io.Reader) (map[string]string, error) {
	sc := bufio.NewScanner(r)
	out := map[string]string{}
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "export "); ok {
			line = strings.TrimSpace(rest)
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			slog.Warn("config: skipping malformed line", "line", lineNo)
			continue
		}
		k = strings.TrimSpace(k)
		if k == "" {
			slog.Warn("config: skipping line with empty key", "line", lineNo)
			continue
		}
		v = unquote(strings.TrimSpace(v))
		out[k] = v
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	return out, nil
}

// unquote strips a single layer of matching single or double quotes from v.
func unquote(v string) string {
	if len(v) < 2 {
		return v
	}
	if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
		return v[1 : len(v)-1]
	}
	return v
}

// parseCommaKV parses the comma-separated key=value syntax shared by
// OTEL_RESOURCE_ATTRIBUTES and OTEL_EXPORTER_OTLP_HEADERS (which use the same
// wire encoding). Malformed entries are skipped with a warning, never fatal
// (Phase 5). Values keep trailing/leading space trimmed.
func parseCommaKV(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(k) == "" {
			slog.Warn("config: skipping malformed key=value entry", "entry", part)
			continue
		}
		out[strings.TrimSpace(k)] = unquote(strings.TrimSpace(v))
	}
	return out
}

// ParseResourceAttributes parses OTEL_RESOURCE_ATTRIBUTES syntax into
// key→value pairs.
func ParseResourceAttributes(s string) map[string]string {
	return parseCommaKV(s)
}

// ParseHeaders parses OTEL_EXPORTER_OTLP_HEADERS syntax into key→value pairs.
func ParseHeaders(s string) map[string]string {
	return parseCommaKV(s)
}
