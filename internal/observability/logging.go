package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// InitLogger initializes the global slog logger with JSON formatting and specified level.
func InitLogger(levelStr string) *slog.Logger {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

// LogAttrs returns standard structured attributes for workflow logging.
func WorkflowAttrs(workflowID, workflowType, workflowVersion string) []slog.Attr {
	return []slog.Attr{
		slog.String("workflow_id", workflowID),
		slog.String("workflow_type", workflowType),
		slog.String("workflow_version", workflowVersion),
	}
}

// TaskAttrs returns standard structured attributes for activity/compensation task logging.
func TaskAttrs(taskID string, commandIndex int, activityType string, attempt int, leaseGen int64) []slog.Attr {
	return []slog.Attr{
		slog.String("task_id", taskID),
		slog.Int("command_index", commandIndex),
		slog.String("activity_type", activityType),
		slog.Int("attempt", attempt),
		slog.Int64("lease_generation", leaseGen),
	}
}

// ContextLogger returns a logger attached with request context if any.
func ContextLogger(ctx context.Context) *slog.Logger {
	return slog.Default()
}
