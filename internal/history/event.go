package history

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Standard Event Type constants
const (
	EventWorkflowStarted                = "WorkflowStarted"
	EventWorkflowCompleted              = "WorkflowCompleted"
	EventWorkflowFailed                 = "WorkflowFailed"
	EventWorkflowNondeterminismDetected = "WorkflowNondeterminismDetected"
	EventActivityScheduled              = "ActivityScheduled"
	EventActivityAttemptStarted         = "ActivityAttemptStarted"
	EventActivityAttemptFailed          = "ActivityAttemptFailed"
	EventActivityRetryScheduled         = "ActivityRetryScheduled"
	EventActivityCompleted              = "ActivityCompleted"
	EventActivityFailedFinal            = "ActivityFailedFinal"
	EventCompensationRegistered         = "CompensationRegistered"
	EventWorkflowCompensationStarted    = "WorkflowCompensationStarted"
	EventCompensationAttemptStarted     = "CompensationAttemptStarted"
	EventCompensationCompleted          = "CompensationCompleted"
	EventCompensationFailed             = "CompensationFailed"
	EventCompensationRetryRequested     = "CompensationRetryRequested"
	EventWorkflowCompensated            = "WorkflowCompensated"
	EventTimerStarted                   = "TimerStarted"
	EventTimerFired                     = "TimerFired"
)

// Event represents an immutable recorded history entry.
type Event struct {
	EventID       string          `json:"event_id"`
	WorkflowID    string          `json:"workflow_id"`
	SequenceNo    int64           `json:"sequence_no"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	CommandIndex  *int            `json:"command_index,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}

// NewEvent instantiates an event with a new UUID and timestamp.
func NewEvent(workflowID, eventType string, seq int64, cmdIdx *int, payload any) (*Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event payload: %w", err)
	}

	return &Event{
		EventID:       uuid.NewString(),
		WorkflowID:    workflowID,
		SequenceNo:    seq,
		EventType:     eventType,
		SchemaVersion: 1,
		CommandIndex:  cmdIdx,
		Payload:       raw,
		CreatedAt:     time.Now().UTC(),
	}, nil
}

// --- Typed Event Payloads ---

type WorkflowStartedPayload struct {
	WorkflowType        string          `json:"workflow_type"`
	WorkflowVersion     string          `json:"workflow_version"`
	Input               json.RawMessage `json:"input"`
	StartIdempotencyKey string          `json:"start_idempotency_key"`
}

type WorkflowCompletedPayload struct {
	Output json.RawMessage `json:"output"`
}

type WorkflowFailedPayload struct {
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Terminal     bool   `json:"terminal"`
}

type WorkflowNondeterminismDetectedPayload struct {
	CommandIndex        int    `json:"command_index"`
	ExpectedCommandType string `json:"expected_command_type"`
	ExpectedName        string `json:"expected_name"`
	ExpectedHash        string `json:"expected_hash"`
	ActualCommandType   string `json:"actual_command_type"`
	ActualName          string `json:"actual_name"`
	ActualHash          string `json:"actual_hash"`
}

type ActivityScheduledPayload struct {
	TaskID         string          `json:"task_id"`
	ActivityType   string          `json:"activity_type"`
	Input          json.RawMessage `json:"input"`
	InputHash      string          `json:"input_hash"`
	RetryPolicy    json.RawMessage `json:"retry_policy"`
	IdempotencyKey string          `json:"idempotency_key"`
}

type ActivityAttemptStartedPayload struct {
	TaskID          string `json:"task_id"`
	ActivityType    string `json:"activity_type"`
	Attempt         int    `json:"attempt"`
	LeaseOwner      string `json:"lease_owner"`
	LeaseGeneration int64  `json:"lease_generation"`
}

type ActivityAttemptFailedPayload struct {
	TaskID       string `json:"task_id"`
	ActivityType string `json:"activity_type"`
	Attempt      int    `json:"attempt"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

type ActivityRetryScheduledPayload struct {
	TaskID       string    `json:"task_id"`
	ActivityType string    `json:"activity_type"`
	Attempt      int       `json:"attempt"`
	NextAttempt  int       `json:"next_attempt"`
	AvailableAt  time.Time `json:"available_at"`
	BackoffMs    int64     `json:"backoff_ms"`
}

type ActivityCompletedPayload struct {
	TaskID       string          `json:"task_id"`
	ActivityType string          `json:"activity_type"`
	Result       json.RawMessage `json:"result"`
}

type ActivityFailedFinalPayload struct {
	TaskID        string `json:"task_id"`
	ActivityType  string `json:"activity_type"`
	TotalAttempts int    `json:"total_attempts"`
	ErrorCode     string `json:"error_code"`
	ErrorMessage  string `json:"error_message"`
}

type CompensationRegisteredPayload struct {
	CompensationID      string          `json:"compensation_id"`
	OrderIndex          int             `json:"order_index"`
	ForwardCommandIndex int             `json:"forward_command_index"`
	CompensationType    string          `json:"compensation_type"`
	Input               json.RawMessage `json:"input"`
	InputHash           string          `json:"input_hash"`
	IdempotencyKey      string          `json:"idempotency_key"`
}

type WorkflowCompensationStartedPayload struct {
	TriggerErrorCode   string `json:"trigger_error_code"`
	TotalCompensations int    `json:"total_compensations"`
}

type CompensationAttemptStartedPayload struct {
	CompensationID   string `json:"compensation_id"`
	CompensationType string `json:"compensation_type"`
	Attempt          int    `json:"attempt"`
	LeaseOwner       string `json:"lease_owner"`
	LeaseGeneration  int64  `json:"lease_generation"`
}

type CompensationCompletedPayload struct {
	CompensationID   string          `json:"compensation_id"`
	CompensationType string          `json:"compensation_type"`
	Result           json.RawMessage `json:"result"`
}

type CompensationFailedPayload struct {
	CompensationID   string `json:"compensation_id"`
	CompensationType string `json:"compensation_type"`
	Attempts         int    `json:"attempts"`
	ErrorCode        string `json:"error_code"`
	ErrorMessage     string `json:"error_message"`
}

type CompensationRetryRequestedPayload struct {
	CompensationID string `json:"compensation_id"`
	RequestedBy    string `json:"requested_by"`
}

type WorkflowCompensatedPayload struct {
	CompletedCompensations int `json:"completed_compensations"`
}

type TimerStartedPayload struct {
	TimerID    string    `json:"timer_id"`
	DurationMs int64     `json:"duration_ms"`
	DueAt      time.Time `json:"due_at"`
}

type TimerFiredPayload struct {
	TimerID string    `json:"timer_id"`
	FiredAt time.Time `json:"fired_at"`
}
