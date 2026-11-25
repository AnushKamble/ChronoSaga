package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/persistence"
	"github.com/google/uuid"
)

// ActivityOptions specifies execution and retry parameters for an activity.
type ActivityOptions struct {
	MaxAttempts            int           `json:"max_attempts"`
	InitialBackoff         time.Duration `json:"initial_backoff"`
	MaxBackoff             time.Duration `json:"max_backoff"`
	NonRetryableErrorCodes []string      `json:"non_retryable_error_codes,omitempty"`
}

// DefaultActivityOptions returns standard options with bounded retries.
func DefaultActivityOptions() ActivityOptions {
	return ActivityOptions{
		MaxAttempts:    3,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     10 * time.Second,
	}
}

// Command kinds
const (
	CommandKindActivity     = "ACTIVITY"
	CommandKindCompensation = "COMPENSATION"
	CommandKindTimer        = "TIMER"
)

// RecordedCommand represents a command resolved from history events.
type RecordedCommand struct {
	CommandIndex int
	Kind         string
	Name         string
	Hash         string
	Scheduled    bool
	Completed    bool
	Failed       bool
	Result       json.RawMessage
	ErrorCode    string
	ErrorMessage string
}

// WorkflowContext provides the deterministic API exposed to workflow functions.
type WorkflowContext struct {
	ctx              context.Context
	workflowID       string
	workflowType     string
	workflowVersion  string
	commandIndex     int
	recordedCommands map[int]*RecordedCommand
	db               *persistence.DB
	store            *history.Store
	registeredComps  int
}

// NewWorkflowContext creates a context populated with historical recorded commands.
func NewWorkflowContext(
	ctx context.Context,
	workflowID, workflowType, workflowVersion string,
	recordedCommands map[int]*RecordedCommand,
	db *persistence.DB,
	store *history.Store,
) *WorkflowContext {
	return &WorkflowContext{
		ctx:              ctx,
		workflowID:       workflowID,
		workflowType:     workflowType,
		workflowVersion:  workflowVersion,
		commandIndex:     0,
		recordedCommands: recordedCommands,
		db:               db,
		store:            store,
	}
}

// WorkflowID returns the workflow run identifier.
func (c *WorkflowContext) WorkflowID() string {
	return c.workflowID
}

// CurrentCommandIndex returns the current replay command index.
func (c *WorkflowContext) CurrentCommandIndex() int {
	return c.commandIndex
}

// ExecuteActivity requests execution of an activity.
// If already completed in history, decodes recorded result into resultTarget without running handler.
// If pending, suspends replay. If unseen, persists scheduling event and task, then suspends.
func (c *WorkflowContext) ExecuteActivity(name string, input any, opts ActivityOptions, resultTarget any) error {
	idx := c.commandIndex
	c.commandIndex++

	hash, err := CanonicalHash(CommandKindActivity, name, input, opts)
	if err != nil {
		return fmt.Errorf("failed computing activity canonical hash: %w", err)
	}

	rec, exists := c.recordedCommands[idx]
	if exists {
		// Command match check
		if rec.Kind != CommandKindActivity || rec.Name != name || rec.Hash != hash {
			return &NondeterminismError{
				CommandIndex:        idx,
				ExpectedCommandType: rec.Kind,
				ExpectedName:        rec.Name,
				ExpectedHash:        rec.Hash,
				ActualCommandType:   CommandKindActivity,
				ActualName:          name,
				ActualHash:          hash,
				Message:             "command parameters deviate from recorded history",
			}
		}

		if rec.Completed {
			if resultTarget != nil && len(rec.Result) > 0 {
				if err := json.Unmarshal(rec.Result, resultTarget); err != nil {
					return fmt.Errorf("failed decoding recorded activity result: %w", err)
				}
			}
			return nil
		}

		if rec.Failed {
			return NewActivityError(rec.ErrorCode, rec.ErrorMessage, true)
		}

		// Scheduled but not yet resolved -> suspend replay
		return errSuspended
	}

	// First time seeing this command: schedule atomically
	taskID := uuid.NewString()
	idempotencyKey := fmt.Sprintf("cs-act-%s-%d", c.workflowID, idx)

	inputJSON, err := CanonicalJSON(input)
	if err != nil {
		return err
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	err = c.db.WithTx(c.ctx, func(tx *sql.Tx) error {
		// 1. Append ActivityScheduled event
		schedPayload := history.ActivityScheduledPayload{
			TaskID:         taskID,
			ActivityType:   name,
			Input:          json.RawMessage(inputJSON),
			InputHash:      hash,
			RetryPolicy:    json.RawMessage(optsJSON),
			IdempotencyKey: idempotencyKey,
		}
		if _, err := c.store.AppendEventInTx(c.ctx, tx, c.workflowID, history.EventActivityScheduled, &idx, schedPayload); err != nil {
			return fmt.Errorf("failed appending ActivityScheduled event: %w", err)
		}

		// 2. Insert into activity_tasks queue
		insertTaskQuery := `
			INSERT INTO activity_tasks (
				task_id, workflow_id, command_index, task_kind, activity_type,
				input_payload, input_hash, retry_policy, idempotency_key,
				status, attempt, max_attempts, available_at, lease_generation,
				created_at, updated_at
			) VALUES ($1, $2, $3, 'FORWARD', $4, $5, $6, $7, $8, 'PENDING', 0, $9, NOW(), 0, NOW(), NOW());`

		maxAttempts := opts.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}

		if _, err := tx.ExecContext(c.ctx, insertTaskQuery,
			taskID,
			c.workflowID,
			idx,
			name,
			inputJSON,
			hash,
			optsJSON,
			idempotencyKey,
			maxAttempts,
		); err != nil {
			return fmt.Errorf("failed inserting activity task: %w", err)
		}

		// 3. Update workflow_runs status to WAITING_ACTIVITY
		updateRunQuery := `
			UPDATE workflow_runs
			SET status = $1, updated_at = NOW()
			WHERE workflow_id = $2;`
		if _, err := tx.ExecContext(c.ctx, updateRunQuery, history.StatusWaitingActivity, c.workflowID); err != nil {
			return fmt.Errorf("failed updating workflow status to WAITING_ACTIVITY: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed scheduling activity: %w", err)
	}

	return errSuspended
}

// RegisterCompensation records a compensating action for a successful forward step.
func (c *WorkflowContext) RegisterCompensation(name string, input any, opts ActivityOptions) error {
	idx := c.commandIndex
	c.commandIndex++
	c.registeredComps++
	orderIdx := c.registeredComps

	hash, err := CanonicalHash(CommandKindCompensation, name, input, opts)
	if err != nil {
		return fmt.Errorf("failed computing compensation canonical hash: %w", err)
	}

	rec, exists := c.recordedCommands[idx]
	if exists {
		if rec.Kind != CommandKindCompensation || rec.Name != name || rec.Hash != hash {
			return &NondeterminismError{
				CommandIndex:        idx,
				ExpectedCommandType: rec.Kind,
				ExpectedName:        rec.Name,
				ExpectedHash:        rec.Hash,
				ActualCommandType:   CommandKindCompensation,
				ActualName:          name,
				ActualHash:          hash,
				Message:             "compensation parameters deviate from recorded history",
			}
		}
		return nil
	}

	// First time seeing this compensation: record atomically
	compID := uuid.NewString()
	idempotencyKey := fmt.Sprintf("cs-comp-%s-%d", c.workflowID, orderIdx)

	inputJSON, err := CanonicalJSON(input)
	if err != nil {
		return err
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	err = c.db.WithTx(c.ctx, func(tx *sql.Tx) error {
		// 1. Append CompensationRegistered event
		regPayload := history.CompensationRegisteredPayload{
			CompensationID:      compID,
			OrderIndex:          orderIdx,
			ForwardCommandIndex: idx,
			CompensationType:    name,
			Input:               json.RawMessage(inputJSON),
			InputHash:           hash,
			IdempotencyKey:      idempotencyKey,
		}
		if _, err := c.store.AppendEventInTx(c.ctx, tx, c.workflowID, history.EventCompensationRegistered, &idx, regPayload); err != nil {
			return fmt.Errorf("failed appending CompensationRegistered event: %w", err)
		}

		// 2. Insert into compensation_tasks table
		insertCompQuery := `
			INSERT INTO compensation_tasks (
				compensation_id, workflow_id, order_index, forward_command_index,
				compensation_type, input_payload, input_hash, retry_policy,
				idempotency_key, status, attempt, max_attempts, available_at,
				lease_generation, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING', 0, $10, NOW(), 0, NOW(), NOW());`

		maxAttempts := opts.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}

		if _, err := tx.ExecContext(c.ctx, insertCompQuery,
			compID,
			c.workflowID,
			orderIdx,
			idx,
			name,
			inputJSON,
			hash,
			optsJSON,
			idempotencyKey,
			maxAttempts,
		); err != nil {
			return fmt.Errorf("failed inserting compensation task: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed registering compensation: %w", err)
	}

	return nil
}

// Sleep registers a durable timer in PostgreSQL and suspends replay.
// On replay after timer maturity, Sleep sees TimerFired in history and returns immediately.
func (c *WorkflowContext) Sleep(duration time.Duration) error {
	idx := c.commandIndex
	c.commandIndex++

	hash, err := CanonicalHash(CommandKindTimer, "sleep", duration.Milliseconds(), nil)
	if err != nil {
		return fmt.Errorf("failed computing timer canonical hash: %w", err)
	}

	rec, exists := c.recordedCommands[idx]
	if exists {
		if rec.Kind != CommandKindTimer || rec.Hash != hash {
			return &NondeterminismError{
				CommandIndex:        idx,
				ExpectedCommandType: rec.Kind,
				ExpectedName:        rec.Name,
				ExpectedHash:        rec.Hash,
				ActualCommandType:   CommandKindTimer,
				ActualName:          "sleep",
				ActualHash:          hash,
				Message:             "timer duration deviates from recorded history",
			}
		}

		if rec.Completed {
			return nil
		}

		return errSuspended
	}

	// First time seeing this timer: persist atomically
	timerID := uuid.NewString()
	dueAt := time.Now().UTC().Add(duration)

	err = c.db.WithTx(c.ctx, func(tx *sql.Tx) error {
		timerPayload := history.TimerStartedPayload{
			TimerID:    timerID,
			DurationMs: duration.Milliseconds(),
			DueAt:      dueAt,
		}
		if _, err := c.store.AppendEventInTx(c.ctx, tx, c.workflowID, history.EventTimerStarted, &idx, timerPayload); err != nil {
			return fmt.Errorf("failed appending TimerStarted event: %w", err)
		}

		insertTimerQuery := `
			INSERT INTO timer_tasks (
				timer_id, workflow_id, command_index, duration_ms, due_at, status, created_at
			) VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW());`
		if _, err := tx.ExecContext(c.ctx, insertTimerQuery, timerID, c.workflowID, idx, duration.Milliseconds(), dueAt); err != nil {
			return fmt.Errorf("failed inserting timer task: %w", err)
		}

		updateRunQuery := `
			UPDATE workflow_runs
			SET status = $1, updated_at = NOW()
			WHERE workflow_id = $2;`
		if _, err := tx.ExecContext(c.ctx, updateRunQuery, history.StatusWaitingTimer, c.workflowID); err != nil {
			return fmt.Errorf("failed updating workflow status to WAITING_TIMER: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed starting durable timer: %w", err)
	}

	return errSuspended
}
