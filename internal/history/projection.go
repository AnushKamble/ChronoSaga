package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Workflow status constants
const (
	StatusRunning            = "RUNNING"
	StatusWaitingActivity    = "WAITING_ACTIVITY"
	StatusWaitingTimer       = "WAITING_TIMER"
	StatusCompensating       = "COMPENSATING"
	StatusCompleted          = "COMPLETED"
	StatusFailed             = "FAILED"
	StatusCompensated        = "COMPENSATED"
	StatusCompensationFailed = "COMPENSATION_FAILED"
	StatusNondeterministic   = "NONDETERMINISTIC"
)

// WorkflowRun represents a projection row in workflow_runs.
type WorkflowRun struct {
	WorkflowID            string          `json:"workflow_id"`
	WorkflowType          string          `json:"workflow_type"`
	WorkflowVersion       string          `json:"workflow_version"`
	Status                string          `json:"status"`
	InputJSON             json.RawMessage `json:"input_json"`
	OutputJSON            json.RawMessage `json:"output_json,omitempty"`
	ErrorCode             *string         `json:"error_code,omitempty"`
	ErrorMessage          *string         `json:"error_message,omitempty"`
	LastEventSequence     int64           `json:"last_event_sequence"`
	StartIdempotencyScope string          `json:"start_idempotency_scope"`
	StartIdempotencyKey   string          `json:"start_idempotency_key"`
	LeaseOwner            *string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt        *time.Time      `json:"lease_expires_at,omitempty"`
	LeaseGeneration       int64           `json:"lease_generation"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
	CompletedAt           *time.Time      `json:"completed_at,omitempty"`
}

// StartWorkflowParams contains inputs for creating a new workflow run.
type StartWorkflowParams struct {
	WorkflowID       string
	WorkflowType     string
	WorkflowVersion  string
	Input            json.RawMessage
	IdempotencyScope string
	IdempotencyKey   string
}

// CreateWorkflowRun atomically creates the workflow_runs projection and appends WorkflowStarted event.
// Enforces start idempotency: if the same (scope, idempotency_key) was already used, returns existing run and isExisting = true.
func (s *Store) CreateWorkflowRun(ctx context.Context, params StartWorkflowParams) (run *WorkflowRun, isExisting bool, err error) {
	if params.IdempotencyScope == "" {
		params.IdempotencyScope = "default"
	}
	if params.WorkflowID == "" {
		params.WorkflowID = uuid.NewString()
	}

	err = s.db.WithTx(ctx, func(tx *sql.Tx) error {
		// 1. Check if run with same idempotency key exists
		existingQuery := `
			SELECT workflow_id, workflow_type, workflow_version, status, input_json, output_json,
			       error_code, error_message, last_event_sequence, start_idempotency_scope,
			       start_idempotency_key, created_at, updated_at, completed_at
			FROM workflow_runs
			WHERE start_idempotency_scope = $1 AND start_idempotency_key = $2;`

		var existing WorkflowRun
		var rawInput, rawOutput []byte
		scanErr := tx.QueryRowContext(ctx, existingQuery, params.IdempotencyScope, params.IdempotencyKey).Scan(
			&existing.WorkflowID,
			&existing.WorkflowType,
			&existing.WorkflowVersion,
			&existing.Status,
			&rawInput,
			&rawOutput,
			&existing.ErrorCode,
			&existing.ErrorMessage,
			&existing.LastEventSequence,
			&existing.StartIdempotencyScope,
			&existing.StartIdempotencyKey,
			&existing.CreatedAt,
			&existing.UpdatedAt,
			&existing.CompletedAt,
		)

		if scanErr == nil {
			existing.InputJSON = json.RawMessage(rawInput)
			if len(rawOutput) > 0 {
				existing.OutputJSON = json.RawMessage(rawOutput)
			}
			run = &existing
			isExisting = true
			return nil
		} else if !errors.Is(scanErr, sql.ErrNoRows) {
			return fmt.Errorf("failed checking start idempotency: %w", scanErr)
		}

		// 2. Insert new workflow run row
		insertRunQuery := `
			INSERT INTO workflow_runs (
				workflow_id, workflow_type, workflow_version, status, input_json,
				last_event_sequence, start_idempotency_scope, start_idempotency_key,
				created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, 0, $6, $7, NOW(), NOW())
			RETURNING created_at, updated_at;`

		var createdAt, updatedAt time.Time
		if err := tx.QueryRowContext(ctx, insertRunQuery,
			params.WorkflowID,
			params.WorkflowType,
			params.WorkflowVersion,
			StatusRunning,
			params.Input,
			params.IdempotencyScope,
			params.IdempotencyKey,
		).Scan(&createdAt, &updatedAt); err != nil {
			return fmt.Errorf("failed to insert workflow_run: %w", err)
		}

		// 3. Atomically append WorkflowStarted event
		startPayload := WorkflowStartedPayload{
			WorkflowType:        params.WorkflowType,
			WorkflowVersion:     params.WorkflowVersion,
			Input:               params.Input,
			StartIdempotencyKey: params.IdempotencyKey,
		}
		ev, err := s.AppendEventInTx(ctx, tx, params.WorkflowID, EventWorkflowStarted, nil, startPayload)
		if err != nil {
			return fmt.Errorf("failed to append WorkflowStarted event: %w", err)
		}

		run = &WorkflowRun{
			WorkflowID:            params.WorkflowID,
			WorkflowType:          params.WorkflowType,
			WorkflowVersion:       params.WorkflowVersion,
			Status:                StatusRunning,
			InputJSON:             params.Input,
			LastEventSequence:     ev.SequenceNo,
			StartIdempotencyScope: params.IdempotencyScope,
			StartIdempotencyKey:   params.IdempotencyKey,
			CreatedAt:             createdAt,
			UpdatedAt:             updatedAt,
		}
		isExisting = false
		s.metrics.IncWorkflowsStarted()
		return nil
	})

	return run, isExisting, err
}

// GetWorkflowRun loads a workflow run projection by ID.
func (s *Store) GetWorkflowRun(ctx context.Context, workflowID string) (*WorkflowRun, error) {
	query := `
		SELECT workflow_id, workflow_type, workflow_version, status, input_json, output_json,
		       error_code, error_message, last_event_sequence, start_idempotency_scope,
		       start_idempotency_key, lease_owner, lease_expires_at, lease_generation,
		       created_at, updated_at, completed_at
		FROM workflow_runs
		WHERE workflow_id = $1;`

	var run WorkflowRun
	var rawInput, rawOutput []byte
	err := s.db.QueryRowContext(ctx, query, workflowID).Scan(
		&run.WorkflowID,
		&run.WorkflowType,
		&run.WorkflowVersion,
		&run.Status,
		&rawInput,
		&rawOutput,
		&run.ErrorCode,
		&run.ErrorMessage,
		&run.LastEventSequence,
		&run.StartIdempotencyScope,
		&run.StartIdempotencyKey,
		&run.LeaseOwner,
		&run.LeaseExpiresAt,
		&run.LeaseGeneration,
		&run.CreatedAt,
		&run.UpdatedAt,
		&run.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrWorkflowNotFound
		}
		return nil, fmt.Errorf("failed to get workflow run: %w", err)
	}

	run.InputJSON = json.RawMessage(rawInput)
	if len(rawOutput) > 0 {
		run.OutputJSON = json.RawMessage(rawOutput)
	}

	return &run, nil
}

// RebuildProjectionFromHistory reads the full event stream and reconstructs the WorkflowRun state in-memory.
func (s *Store) RebuildProjectionFromHistory(ctx context.Context, workflowID string) (*WorkflowRun, error) {
	events, err := s.GetAllHistory(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, ErrWorkflowNotFound
	}

	rebuilt := &WorkflowRun{
		WorkflowID: workflowID,
		Status:     StatusRunning,
	}

	for _, ev := range events {
		rebuilt.LastEventSequence = ev.SequenceNo

		switch ev.EventType {
		case EventWorkflowStarted:
			var p WorkflowStartedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				rebuilt.WorkflowType = p.WorkflowType
				rebuilt.WorkflowVersion = p.WorkflowVersion
				rebuilt.InputJSON = p.Input
				rebuilt.StartIdempotencyKey = p.StartIdempotencyKey
			}
			rebuilt.Status = StatusRunning

		case EventActivityScheduled:
			rebuilt.Status = StatusWaitingActivity

		case EventActivityCompleted:
			rebuilt.Status = StatusRunning

		case EventActivityFailedFinal:
			rebuilt.Status = StatusRunning

		case EventTimerStarted:
			rebuilt.Status = StatusWaitingTimer

		case EventTimerFired:
			rebuilt.Status = StatusRunning

		case EventWorkflowCompensationStarted:
			rebuilt.Status = StatusCompensating

		case EventCompensationFailed:
			rebuilt.Status = StatusCompensationFailed

		case EventCompensationRetryRequested:
			rebuilt.Status = StatusCompensating

		case EventWorkflowCompensated:
			rebuilt.Status = StatusCompensated
			completedAt := ev.CreatedAt
			rebuilt.CompletedAt = &completedAt

		case EventWorkflowCompleted:
			rebuilt.Status = StatusCompleted
			var p WorkflowCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				rebuilt.OutputJSON = p.Output
			}
			completedAt := ev.CreatedAt
			rebuilt.CompletedAt = &completedAt

		case EventWorkflowFailed:
			rebuilt.Status = StatusFailed
			var p WorkflowFailedPayload
			if err := json.Unmarshal(ev.Payload, &p); err == nil {
				rebuilt.ErrorCode = &p.ErrorCode
				rebuilt.ErrorMessage = &p.ErrorMessage
			}
			completedAt := ev.CreatedAt
			rebuilt.CompletedAt = &completedAt

		case EventWorkflowNondeterminismDetected:
			rebuilt.Status = StatusNondeterministic
			completedAt := ev.CreatedAt
			rebuilt.CompletedAt = &completedAt
		}
	}

	return rebuilt, nil
}
