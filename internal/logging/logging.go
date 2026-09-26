// Package logging provides a single, consistent structured logger for all
// gokraft binaries, built on Go's standard library slog.
//
// Why slog and not zap/zerolog? For an observability platform it is fitting
// that our own logs are structured JSON (machine-parseable) and could, in
// principle, be scraped by gokraft itself. slog ships in the standard library
// (Go 1.21+), needs no dependency, and is fast enough. We wrap it only to fix
// the output format and expose a component-scoped helper so every log line
// carries which service emitted it.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a JSON logger for the given component (e.g. "collector",
// "logwriter"). The level is read from the LOG_LEVEL env var (debug/info/warn/
// error), defaulting to info.
func New(component string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler).With("component", component)
}
