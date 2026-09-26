// Package config centralizes how every gokraft binary reads its settings.
//
// Design principle: 12-factor configuration. Every knob is an environment
// variable with a sensible default, so the same binary runs unchanged on a
// laptop (docker-compose) or in Kubernetes (ConfigMap/Secret env vars). There
// is no config file to mount and no flags to remember. `MustLoad*` helpers
// panic on genuinely invalid input (e.g. a non-numeric port) because a
// misconfigured process should fail loudly at startup, not limp along.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the env var value or the provided default.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns the env var parsed as an int, or the default if unset. It panics
// if the value is set but unparseable — a typo in a port number should stop the
// process immediately.
func Int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic("config: " + key + " must be an integer, got " + v)
	}
	return n
}

// Duration parses a Go duration string (e.g. "500ms", "30s") from an env var.
func Duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic("config: " + key + " must be a duration, got " + v)
	}
	return d
}

// Bool parses common truthy strings ("1", "true", "yes").
func Bool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

// StringSlice parses a comma-separated env var into a slice, trimming spaces.
// Used for things like Kafka broker lists ("broker1:9092,broker2:9092").
func StringSlice(key string, def []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
