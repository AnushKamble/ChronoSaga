package saga

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
	"chronosaga/internal/workflow"
)

var (
	ErrWorkflowNotCompensating = errors.New("workflow is not in COMPENSATING or COMPENSATION_FAILED state")
	ErrNoFailedCompensation    = errors.New("no failed compensation found to retry")
)

// CompensationRecord represents a compensation task row.
type CompensationRecord struct {
	CompensationID      string
	WorkflowID          string
	OrderIndex          int
	ForwardCommandIndex int
	CompensationType    string
	InputPayload        json.RawMessage
	InputHash           string
	RetryPolicy         workflow.ActivityOptions
	IdempotencyKey      string
	Status              string
	Attempt             int
	MaxAttempts         int
	LeaseGeneration     int64
}

// Coordinator drives the reverse (LIFO) compensation chain for failing workflows.
type Coordinator struct {
	db       *persistence.DB
	store    *history.Store
	registry *activities.Registry
	metrics  *observability.Metrics
	logger   *slog.Logger
}

// NewCoordinator creates a new Saga coordinator instance.
func NewCoordinator(
	db *persistence.DB,
	store *history.Store,
	registry *activities.Registry,
	metrics *observability.Metrics,
) *Coordinator {
	if registry == nil {
		registry = activities.DefaultRegistry
	}
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	return &Coordinator{
		db:       db,
		store:    store,
		registry: registry,
		metrics:  metrics,
		logger:   slog.Default(),
	}
}

// DriveCompensation inspects the registered compensations in reverse order (LIFO),
// executes the next pending compensation, and advances the workflow state.
// Returns (done, error).
func (c *Coordinator) DriveCompensation(ctx context.Context, workflowID string, workerID string) (bool, error) {
	// 1. Load all compensation records in LIFO order (order_index DESC)
	query := `
		SELECT compensation_id, workflow_id, order_index, forward_command_index,
		       compensation_type, input_payload, input_hash, retry_policy,
		       idempotency_key, status, attempt, max_attempts, lease_generation
		FROM compensation_tasks
		WHERE workflow_id = $1
		ORDER BY order_index DESC;`

	rows, err := c.db.QueryContext(ctx, query, workflowID)
	if err != nil {
		return false, fmt.Errorf("failed querying compensation tasks: %w", err)
	}
	defer rows.Close()

	var records []CompensationRecord
	for rows.Next() {
		var rec CompensationRecord
		var rawInput, rawRetry []byte
		if err := rows.Scan(
			&rec.CompensationID,
			&rec.WorkflowID,
			&rec.OrderIndex,
			&rec.ForwardCommandIndex,
			&rec.CompensationType,
			&rawInput,
			&rec.InputHash,
			&rawRetry,
			&rec.IdempotencyKey,
			&rec.Status,
			&rec.Attempt,
			&rec.MaxAttempts,
			&rec.LeaseGeneration,
		); err != nil {
			return false, fmt.Errorf("failed scanning compensation task: %w", err)
		}
		rec.InputPayload = json.RawMessage(rawInput)
		_ = json.Unmarshal(rawRetry, &rec.RetryPolicy)
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	if len(records) == 0 {
		// Nothing to compensate
		return true, nil
	}

	// 2. Scan in LIFO order:
	// - If any task is FAILED -> workflow is blocked in COMPENSATION_FAILED!
	// - If task is COMPLETED -> already done, skip!
	// - Find first non-completed task.
	var target *CompensationRecord
	allCompleted := true

	for i := range records {
		rec := &records[i]
		if rec.Status == "FAILED" {
			// A compensation failed terminally: halt the entire chain!
			if updateErr := c.db.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `
					UPDATE workflow_runs
					SET status = $1, updated_at = NOW()
					WHERE workflow_id = $2;`,
					history.StatusCompensationFailed, workflowID)
				return err
			}); updateErr != nil {
				return false, fmt.Errorf("failed updating workflow to COMPENSATION_FAILED: %w", updateErr)
			}
			return false, fmt.Errorf("compensation chain blocked: task %s (%s) failed terminally", rec.CompensationID, rec.CompensationType)
		}

		if rec.Status != "COMPLETED" {
			allCompleted = false
			if target == nil {
				target = rec
			}
		}
	}

	if allCompleted {
		// All registered compensations completed! Mark workflow COMPENSATED!
		err = c.db.WithTx(ctx, func(tx *sql.Tx) error {
			compPayload := history.WorkflowCompensatedPayload{
				CompletedCompensations: len(records),
			}
			if _, err := c.store.AppendEventInTx(ctx, tx, workflowID, history.EventWorkflowCompensated, nil, compPayload); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `
				UPDATE workflow_runs
				SET status = $1, updated_at = NOW(), completed_at = NOW()
				WHERE workflow_id = $2;`,
				history.StatusCompensated, workflowID)
			return err
		})
		if err != nil {
			return false, fmt.Errorf("failed finalizing COMPENSATED status: %w", err)
		}
		c.metrics.IncWorkflowsCompensated()
		return true, nil
	}

	if target == nil {
		return true, nil
	}

	// 3. Atomically claim target compensation task
	newGen := target.LeaseGeneration + 1
	newAttempt := target.Attempt + 1

	claimErr := c.db.WithTx(ctx, func(tx *sql.Tx) error {
		claimQuery := `
			UPDATE compensation_tasks
			SET status = 'RUNNING',
			    lease_owner = $1,
			    lease_generation = $2,
			    lease_expires_at = NOW() + interval '30 seconds',
			    attempt = $3,
			    updated_at = NOW()
			WHERE compensation_id = $4
			  AND (status = 'PENDING' OR status = 'RETRY_WAIT');`

		res, err := tx.ExecContext(ctx, claimQuery, workerID, newGen, newAttempt, target.CompensationID)
		if err != nil {
			return err
		}
		rowsAff, _ := res.RowsAffected()
		if rowsAff == 0 {
			return errors.New("concurrent compensation claim conflict")
		}

		// Append CompensationAttemptStarted event
		attPayload := history.CompensationAttemptStartedPayload{
			CompensationID:   target.CompensationID,
			CompensationType: target.CompensationType,
			Attempt:          newAttempt,
			LeaseOwner:       workerID,
			LeaseGeneration:  newGen,
		}
		cmdIdx := target.ForwardCommandIndex
		_, err = c.store.AppendEventInTx(ctx, tx, workflowID, history.EventCompensationAttemptStarted, &cmdIdx, attPayload)
		return err
	})

	if claimErr != nil {
		return false, fmt.Errorf("failed claiming compensation task: %w", claimErr)
	}

	// 4. Lookup activity handler
	handler, err := c.registry.Get(target.CompensationType)
	if err != nil {
		// Handler not found is a terminal failure
		if recErr := c.recordCompensationFailure(ctx, target, newGen, newAttempt, "HANDLER_NOT_FOUND", err.Error(), false); recErr != nil {
			c.logger.Error("Failed recording compensation failure", "error", recErr)
		}
		return false, err
	}

	// 5. Execute compensating activity outside database transaction!
	res, execErr := handler(ctx, target.InputPayload, target.IdempotencyKey)
	if execErr != nil {
		var nonRet *activities.NonRetryableError
		isNonRet := errors.As(execErr, &nonRet)
		canRetry := (!isNonRet) && (newAttempt < target.MaxAttempts)

		code := "COMPENSATION_ERROR"
		if isNonRet {
			code = nonRet.Code
		}
		if recErr := c.recordCompensationFailure(ctx, target, newGen, newAttempt, code, execErr.Error(), canRetry); recErr != nil {
			c.logger.Error("Failed recording compensation failure", "error", recErr)
		}
		return false, execErr
	}

	// 6. Record successful compensation completion
	completeErr := c.db.WithTx(ctx, func(tx *sql.Tx) error {
		updateQuery := `
			UPDATE compensation_tasks
			SET status = 'COMPLETED',
			    result_payload = $1,
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    updated_at = NOW()
			WHERE compensation_id = $2 AND lease_generation = $3;`

		if _, err := tx.ExecContext(ctx, updateQuery, res, target.CompensationID, newGen); err != nil {
			return err
		}

		compPayload := history.CompensationCompletedPayload{
			CompensationID:   target.CompensationID,
			CompensationType: target.CompensationType,
			Result:           res,
		}
		cmdIdx := target.ForwardCommandIndex
		_, err = c.store.AppendEventInTx(ctx, tx, workflowID, history.EventCompensationCompleted, &cmdIdx, compPayload)
		return err
	})

	if completeErr != nil {
		return false, fmt.Errorf("failed completing compensation task: %w", completeErr)
	}

	// Successfully completed one step of the reverse chain!
	return false, nil
}

func (c *Coordinator) recordCompensationFailure(
	ctx context.Context,
	rec *CompensationRecord,
	leaseGen int64,
	attempt int,
	code, msg string,
	canRetry bool,
) error {
	return c.db.WithTx(ctx, func(tx *sql.Tx) error {
		cmdIdx := rec.ForwardCommandIndex

		if canRetry {
			backoff := time.Duration(attempt) * 500 * time.Millisecond
			updateQuery := `
				UPDATE compensation_tasks
				SET status = 'RETRY_WAIT',
				    available_at = NOW() + $1::interval,
				    lease_owner = NULL,
				    lease_expires_at = NULL,
				    last_error_code = $2,
				    last_error_message = $3,
				    updated_at = NOW()
				WHERE compensation_id = $4 AND lease_generation = $5;`

			intervalStr := fmt.Sprintf("%d milliseconds", backoff.Milliseconds())
			_, err := tx.ExecContext(ctx, updateQuery, intervalStr, code, msg, rec.CompensationID, leaseGen)
			return err
		}

		// Permanent failure -> mark COMPENSATION_FAILED
		c.metrics.IncCompensationFailures()
		updateTaskQuery := `
			UPDATE compensation_tasks
			SET status = 'FAILED',
			    lease_owner = NULL,
			    lease_expires_at = NULL,
			    last_error_code = $1,
			    last_error_message = $2,
			    updated_at = NOW()
			WHERE compensation_id = $3 AND lease_generation = $4;`

		if _, err := tx.ExecContext(ctx, updateTaskQuery, code, msg, rec.CompensationID, leaseGen); err != nil {
			return err
		}

		failPayload := history.CompensationFailedPayload{
			CompensationID:   rec.CompensationID,
			CompensationType: rec.CompensationType,
			Attempts:         attempt,
			ErrorCode:        code,
			ErrorMessage:     msg,
		}
		if _, err := c.store.AppendEventInTx(ctx, tx, rec.WorkflowID, history.EventCompensationFailed, &cmdIdx, failPayload); err != nil {
			return err
		}

		_, err := tx.ExecContext(ctx, `
			UPDATE workflow_runs
			SET status = $1, updated_at = NOW()
			WHERE workflow_id = $2;`,
			history.StatusCompensationFailed, rec.WorkflowID)
		return err
	})
}

// RetryFailedCompensation allows an operator to retry a failed compensation item.
// It resets the failed compensation task to RETRY_WAIT and restores workflow status to COMPENSATING.
func (c *Coordinator) RetryFailedCompensation(ctx context.Context, workflowID string, operator string) error {
	return c.db.WithTx(ctx, func(tx *sql.Tx) error {
		// 1. Verify workflow is in COMPENSATION_FAILED status
		var status string
		err := tx.QueryRowContext(ctx, "SELECT status FROM workflow_runs WHERE workflow_id = $1 FOR UPDATE;", workflowID).Scan(&status)
		if err != nil {
			return err
		}
		if status != history.StatusCompensationFailed {
			return fmt.Errorf("%w: current status is %s", ErrWorkflowNotCompensating, status)
		}

		// 2. Find the failed compensation task
		var compID string
		var cmdIdx int
		err = tx.QueryRowContext(ctx, `
			SELECT compensation_id, forward_command_index
			FROM compensation_tasks
			WHERE workflow_id = $1 AND status = 'FAILED'
			LIMIT 1
			FOR UPDATE;`, workflowID).Scan(&compID, &cmdIdx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNoFailedCompensation
			}
			return err
		}

		// 3. Reset task to RETRY_WAIT and clear errors
		resetQuery := `
			UPDATE compensation_tasks
			SET status = 'RETRY_WAIT',
			    available_at = NOW(),
			    attempt = 0,
			    last_error_code = NULL,
			    last_error_message = NULL,
			    updated_at = NOW()
			WHERE compensation_id = $1;`
		if _, err := tx.ExecContext(ctx, resetQuery, compID); err != nil {
			return err
		}

		// 4. Append CompensationRetryRequested event
		reqPayload := history.CompensationRetryRequestedPayload{
			CompensationID: compID,
			RequestedBy:    operator,
		}
		if _, err := c.store.AppendEventInTx(ctx, tx, workflowID, history.EventCompensationRetryRequested, &cmdIdx, reqPayload); err != nil {
			return err
		}

		// 5. Update workflow status back to COMPENSATING
		_, err = tx.ExecContext(ctx, `
			UPDATE workflow_runs
			SET status = $1, updated_at = NOW()
			WHERE workflow_id = $2;`,
			history.StatusCompensating, workflowID)
		return err
	})
}
