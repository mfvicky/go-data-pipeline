package logger

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

func New(out io.Writer, level, format string) (*slog.Logger, error) {
	if out == nil {
		return nil, fmt.Errorf("logger output is required")
	}
	var parsed slog.Level
	switch strings.ToLower(level) {
	case "debug":
		parsed = slog.LevelDebug
	case "", "info":
		parsed = slog.LevelInfo
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		return nil, fmt.Errorf("LOG_LEVEL must be debug, info, warn or error")
	}
	options := &slog.HandlerOptions{Level: parsed}
	switch strings.ToLower(format) {
	case "", "json":
		return slog.New(slog.NewJSONHandler(out, options)), nil
	case "text":
		return slog.New(slog.NewTextHandler(out, options)), nil
	default:
		return nil, fmt.Errorf("LOG_FORMAT must be json or text")
	}
}
