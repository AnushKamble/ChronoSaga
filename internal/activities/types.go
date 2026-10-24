package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"chronosaga/internal/workflow"
)

var (
	ErrStaleLease      = errors.New("stale lease generation: completion rejected")
	ErrTaskNotFound    = errors.New("task not found")
	ErrHandlerNotFound = errors.New("activity handler not found")
)

// NonRetryableError marks an error that should not trigger further retries.
type NonRetryableError struct {
	Code    string
	Message string
}

func (e *NonRetryableError) Error() string {
	return fmt.Sprintf("non-retryable error [%s]: %s", e.Code, e.Message)
}

// NewNonRetryableError creates a new NonRetryableError.
func NewNonRetryableError(code, message string) *NonRetryableError {
	return &NonRetryableError{Code: code, Message: message}
}

// ActivityHandler is the signature for activity implementations.
// Receives context with deadline, serialized input, and the stable engine idempotency key.
type ActivityHandler func(ctx context.Context, input json.RawMessage, idempotencyKey string) (json.RawMessage, error)

// Task represents an operational activity task row.
type Task struct {
	TaskID           string
	WorkflowID       string
	CommandIndex     int
	TaskKind         string
	ActivityType     string
	InputPayload     json.RawMessage
	InputHash        string
	RetryPolicy      workflow.ActivityOptions
	IdempotencyKey   string
	Status           string
	Attempt          int
	MaxAttempts      int
	AvailableAt      time.Time
	LeaseOwner       *string
	LeaseExpiresAt   *time.Time
	LeaseGeneration  int64
	LastErrorCode    *string
	LastErrorMessage *string
	ResultPayload    json.RawMessage
}
