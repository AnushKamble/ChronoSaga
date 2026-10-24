package activities

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
	"chronosaga/internal/workflow"
)

// TaskQueue manages durable task claims, completions, and lease fencing.
type TaskQueue struct {
	db      *persistence.DB
	store   *history.Store
	metrics *observability.Metrics
}

// NewTaskQueue creates a new TaskQueue instance.
func NewTaskQueue(db *persistence.DB, store *history.Store, metrics *observability.Metrics) *TaskQueue {
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	return &TaskQueue{
		db:      db,
		store:   store,
		metrics: metrics,
	}
}

// ClaimTask atomically selects and locks one available task using FOR UPDATE SKIP LOCKED.
// It assigns lease ownership and increments lease_generation.
func (q *TaskQueue) ClaimTask(ctx context.Context, workerID string, leaseDuration time.Duration) (*Task, error) {
	var task Task
	var rawInput, rawRetry []byte
	var claimed bool

	err := q.db.WithTx(ctx, func(tx *sql.Tx) error {
		selectQuery := `
			SELECT task_id, workflow_id, command_index, task_kind, activity_type,
			       input_payload, input_hash, retry_policy, idempotency_key,
			       status, attempt, max_attempts, available_at, lease_generation
			FROM activity_tasks
			WHERE (status = 'PENDING' OR status = 'RETRY_WAIT')
			  AND available_at <= NOW()
			ORDER BY available_at ASC, task_id ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED;`

		scanErr := tx.QueryRowContext(ctx, selectQuery).Scan(
			&task.TaskID,
			&task.WorkflowID,
			&task.CommandIndex,
			&task.TaskKind,
			&task.ActivityType,
			&rawInput,
			&task.InputHash,
			&rawRetry,
			&task.IdempotencyKey,
			&task.Status,
			&task.Attempt,
			&task.MaxAttempts,
			&task.AvailableAt,
			&task.LeaseGeneration,
		)

		if scanErr != nil {
			if errors.Is(scanErr, sql.ErrNoRows) {
				return nil // No available task
			}
			return fmt.Errorf("failed querying available activity task: %w", scanErr)
		}

		newGen := task.LeaseGeneration + 1
		newAttempt := task.Attempt + 1
		leaseExpiresAt := time.Now().UTC().Add(leaseDuration)

		updateQuery := `
			UPDATE activity_tasks
			SET status = 'RUNNING',
			    lease_owner = $1,
			    lease_generation = $2,
			    lease_expires_at = $3,
			    attempt = $4,
			    updated_at = NOW()
			WHERE task_id = $5;`

		if _, err := tx.ExecContext(ctx, updateQuery, workerID, newGen, leaseExpiresAt, newAttempt, task.TaskID); err != nil {
			return fmt.Errorf("failed claiming activity task: %w", err)
		}

		// Atomically append ActivityAttemptStarted event
		attemptPayload := history.ActivityAttemptStartedPayload{
			TaskID:          task.TaskID,
			ActivityType:    task.ActivityType,
			Attempt:         newAttempt,
			LeaseOwner:      workerID,
			LeaseGeneration: newGen,
		}
		cmdIdx := task.CommandIndex
		if _, err := q.store.AppendEventInTx(ctx, tx, task.WorkflowID, history.EventActivityAttemptStarted, &cmdIdx, attemptPayload); err != nil {
			return fmt.Errorf("failed appending ActivityAttemptStarted: %w", err)
		}

		task.LeaseOwner = &workerID
		task.LeaseGeneration = newGen
		task.LeaseExpiresAt = &leaseExpiresAt
		task.Attempt = newAttempt
		task.Status = "RUNNING"
		task.InputPayload = json.RawMessage(rawInput)
		_ = json.Unmarshal(rawRetry, &task.RetryPolicy)

		claimed = true
		return nil
	})

	if err != nil {
		return nil, err
	}
	if !claimed {
		return nil, nil // No task available
	}

	return &task, nil
}

// CompleteTask verifies that the lease generation matches and atomically completes the task and appends ActivityCompleted.
func (q *TaskQueue) CompleteTask(ctx context.Context, taskID string, leaseGen int64, result json.RawMessage) error {
	return q.db.WithTx(ctx, func(tx *sql.Tx) error {
		updateQuery := `
			UPDATE activity_tasks
			SET status = 'COMPLETED',
			    result_payload = $1,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    updated_at = NOW()
			WHERE task_id = $2 
			  AND lease_generation = $3 
			  AND status = 'RUNNING'
			RETURNING workflow_id, command_index, activity_type;`

		var workflowID string
		var cmdIndex int
		var activityType string

		err := tx.QueryRowContext(ctx, updateQuery, result, taskID, leaseGen).Scan(&workflowID, &cmdIndex, &activityType)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleLease
			}
			return fmt.Errorf("failed updating completed task: %w", err)
		}

		// Append ActivityCompleted event
		completedPayload := history.ActivityCompletedPayload{
			TaskID:       taskID,
			ActivityType: activityType,
			Result:       result,
		}
		if _, err := q.store.AppendEventInTx(ctx, tx, workflowID, history.EventActivityCompleted, &cmdIndex, completedPayload); err != nil {
			return fmt.Errorf("failed appending ActivityCompleted: %w", err)
		}

		// Wake workflow run so it can be picked up for replay
		updateRunQuery := `
			UPDATE workflow_runs 
			SET status = $1, updated_at = NOW() 
			WHERE workflow_id = $2;`
		if _, err := tx.ExecContext(ctx, updateRunQuery, history.StatusRunning, workflowID); err != nil {
			return fmt.Errorf("failed waking workflow run: %w", err)
		}

		q.metrics.RecordActivityAttempt(activityType, "success")
		return nil
	})
}

// FailTask records a failed task attempt, computes exponential backoff retry if eligible, or records terminal failure.
func (q *TaskQueue) FailTask(ctx context.Context, taskID string, leaseGen int64, taskErr error) error {
	return q.db.WithTx(ctx, func(tx *sql.Tx) error {
		selectQuery := `
			SELECT workflow_id, command_index, activity_type, attempt, max_attempts, retry_policy
			FROM activity_tasks
			WHERE task_id = $1 AND lease_generation = $2 AND status = 'RUNNING'
			FOR UPDATE;`

		var workflowID, activityType string
		var cmdIndex, attempt, maxAttempts int
		var rawRetry []byte

		err := tx.QueryRowContext(ctx, selectQuery, taskID, leaseGen).Scan(
			&workflowID, &cmdIndex, &activityType, &attempt, &maxAttempts, &rawRetry,
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleLease
			}
			return fmt.Errorf("failed selecting failing task: %w", err)
		}

		var opts workflow.ActivityOptions
		_ = json.Unmarshal(rawRetry, &opts)

		var nonRetryable *NonRetryableError
		isNonRetryable := errors.As(taskErr, &nonRetryable)

		errorCode := "ACTIVITY_FAILED"
		errorMsg := taskErr.Error()
		if isNonRetryable {
			errorCode = nonRetryable.Code
		}

		// Check non-retryable codes list
		for _, code := range opts.NonRetryableErrorCodes {
			if code == errorCode {
				isNonRetryable = true
				break
			}
		}

		canRetry := (!isNonRetryable) && (attempt < maxAttempts)

		// 1. Append ActivityAttemptFailed event
		failPayload := history.ActivityAttemptFailedPayload{
			TaskID:       taskID,
			ActivityType: activityType,
			Attempt:      attempt,
			ErrorCode:    errorCode,
			ErrorMessage: errorMsg,
			Retryable:    canRetry,
		}
		if _, err := q.store.AppendEventInTx(ctx, tx, workflowID, history.EventActivityAttemptFailed, &cmdIndex, failPayload); err != nil {
			return fmt.Errorf("failed appending ActivityAttemptFailed: %w", err)
		}

		if canRetry {
			// Compute exponential backoff: min(initial * 2^(attempt-1), max)
			initBackoff := opts.InitialBackoff
			if initBackoff <= 0 {
				initBackoff = 1 * time.Second
			}
			maxBackoff := opts.MaxBackoff
			if maxBackoff <= 0 {
				maxBackoff = 10 * time.Second
			}

			multiplier := math.Pow(2, float64(attempt-1))
			backoff := time.Duration(float64(initBackoff) * multiplier)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}

			availableAt := time.Now().UTC().Add(backoff)

			updateQuery := `
				UPDATE activity_tasks
				SET status = 'RETRY_WAIT',
				    available_at = $1,
				    lease_owner = NULL,
				    lease_expires_at = NULL,
				    last_error_code = $2,
				    last_error_message = $3,
				    updated_at = NOW()
				WHERE task_id = $4;`

			if _, err := tx.ExecContext(ctx, updateQuery, availableAt, errorCode, errorMsg, taskID); err != nil {
				return fmt.Errorf("failed scheduling task retry: %w", err)
			}

			// Append ActivityRetryScheduled event
			retryPayload := history.ActivityRetryScheduledPayload{
				TaskID:       taskID,
				ActivityType: activityType,
				Attempt:      attempt,
				NextAttempt:  attempt + 1,
				AvailableAt:  availableAt,
				BackoffMs:    backoff.Milliseconds(),
			}
			if _, err := q.store.AppendEventInTx(ctx, tx, workflowID, history.EventActivityRetryScheduled, &cmdIndex, retryPayload); err != nil {
				return fmt.Errorf("failed appending ActivityRetryScheduled: %w", err)
			}
			q.metrics.RecordActivityAttempt(activityType, "retryable_failure")
		} else {
			// Final failure
			updateQuery := `
				UPDATE activity_tasks
				SET status = 'FAILED',
				    lease_owner = NULL,
				    lease_expires_at = NULL,
				    last_error_code = $1,
				    last_error_message = $2,
				    updated_at = NOW()
				WHERE task_id = $3;`

			if _, err := tx.ExecContext(ctx, updateQuery, errorCode, errorMsg, taskID); err != nil {
				return fmt.Errorf("failed marking task permanently failed: %w", err)
			}

			finalPayload := history.ActivityFailedFinalPayload{
				TaskID:        taskID,
				ActivityType:  activityType,
				TotalAttempts: attempt,
				ErrorCode:     errorCode,
				ErrorMessage:  errorMsg,
			}
			if _, err := q.store.AppendEventInTx(ctx, tx, workflowID, history.EventActivityFailedFinal, &cmdIndex, finalPayload); err != nil {
				return fmt.Errorf("failed appending ActivityFailedFinal: %w", err)
			}

			// Wake workflow run so replay can process terminal failure
			updateRunQuery := `
				UPDATE workflow_runs 
				SET status = $1, updated_at = NOW() 
				WHERE workflow_id = $2;`
			if _, err := tx.ExecContext(ctx, updateRunQuery, history.StatusRunning, workflowID); err != nil {
				return fmt.Errorf("failed waking workflow run on final failure: %w", err)
			}
			q.metrics.RecordActivityAttempt(activityType, "terminal_failure")
		}

		return nil
	})
}

// ReclaimExpiredLeases scans for tasks running past their lease expiry and makes them available again.
func (q *TaskQueue) ReclaimExpiredLeases(ctx context.Context) (int64, error) {
	query := `
		UPDATE activity_tasks
		SET status = 'RETRY_WAIT',
		    lease_owner = NULL,
		    lease_expires_at = NULL,
		    available_at = NOW(),
		    updated_at = NOW()
		WHERE status = 'RUNNING' 
		  AND lease_expires_at <= NOW();`

	res, err := q.db.ExecContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed reclaiming expired task leases: %w", err)
	}

	rows, _ := res.RowsAffected()
	if rows > 0 {
		for i := int64(0); i < rows; i++ {
			q.metrics.IncTaskLeaseReclaims()
		}
	}
	return rows, nil
}
