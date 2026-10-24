package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
)

// ReplayResult encapsulates the outcome of a replay execution.
type ReplayResult struct {
	WorkflowID string
	Status     string
	Suspended  bool
	Output     json.RawMessage
	Error      error
}

// Replayer coordinates workflow deterministic execution against recorded history.
type Replayer struct {
	db       *persistence.DB
	store    *history.Store
	registry *Registry
	metrics  *observability.Metrics
}

// NewReplayer creates a new Replayer engine instance.
func NewReplayer(db *persistence.DB, store *history.Store, registry *Registry, metrics *observability.Metrics) *Replayer {
	if registry == nil {
		registry = DefaultRegistry
	}
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	return &Replayer{
		db:       db,
		store:    store,
		registry: registry,
		metrics:  metrics,
	}
}

// ReplayWorkflow replays the given workflow run from start using recorded history.
func (r *Replayer) ReplayWorkflow(ctx context.Context, run *history.WorkflowRun) (*ReplayResult, error) {
	startReplay := time.Now()
	defer func() {
		r.metrics.RecordWorkflowReplayDuration(time.Since(startReplay).Milliseconds())
	}()

	// 1. Load all history events
	events, err := r.store.GetAllHistory(ctx, run.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed loading workflow history: %w", err)
	}

	// 2. Index historical commands
	recordedCommands, compensationsCount, isAlreadyTerminal, err := buildCommandIndex(events)
	if err != nil {
		return nil, fmt.Errorf("failed indexing historical events: %w", err)
	}

	if isAlreadyTerminal {
		return &ReplayResult{
			WorkflowID: run.WorkflowID,
			Status:     run.Status,
			Output:     run.OutputJSON,
		}, nil
	}

	// 3. Lookup registered workflow definition
	wfFunc, err := r.registry.Get(run.WorkflowType, run.WorkflowVersion)
	if err != nil {
		return nil, fmt.Errorf("workflow definition missing: %w", err)
	}

	// 4. Create Replay Context
	wfCtx := NewWorkflowContext(ctx, run.WorkflowID, run.WorkflowType, run.WorkflowVersion, recordedCommands, r.db, r.store)
	wfCtx.registeredComps = compensationsCount

	// 5. Execute workflow function with panic recovery
	var output json.RawMessage
	var execErr error

	func() {
		defer func() {
			if p := recover(); p != nil {
				execErr = &UserPanicError{Value: p}
			}
		}()
		output, execErr = wfFunc(wfCtx, run.InputJSON)
	}()

	// 6. Handle execution outcome
	if execErr != nil {
		if IsSuspended(execErr) {
			// Replay paused at first unresolved command (activity, timer, etc.)
			// Load latest run status to reflect WAITING_ACTIVITY or WAITING_TIMER
			latestRun, getErr := r.store.GetWorkflowRun(ctx, run.WorkflowID)
			status := run.Status
			if getErr == nil {
				status = latestRun.Status
			}
			return &ReplayResult{
				WorkflowID: run.WorkflowID,
				Status:     status,
				Suspended:  true,
			}, nil
		}

		var nondetErr *NondeterminismError
		if errors.As(execErr, &nondetErr) {
			// Persistent nondeterminism failure
			r.metrics.IncWorkflowsFailed()
			if txErr := r.db.WithTx(ctx, func(tx *sql.Tx) error {
				payload := history.WorkflowNondeterminismDetectedPayload{
					CommandIndex:        nondetErr.CommandIndex,
					ExpectedCommandType: nondetErr.ExpectedCommandType,
					ExpectedName:        nondetErr.ExpectedName,
					ExpectedHash:        nondetErr.ExpectedHash,
					ActualCommandType:   nondetErr.ActualCommandType,
					ActualName:          nondetErr.ActualName,
					ActualHash:          nondetErr.ActualHash,
				}
				cmdIdx := nondetErr.CommandIndex
				if _, err := r.store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventWorkflowNondeterminismDetected, &cmdIdx, payload); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `
					UPDATE workflow_runs 
					SET status = $1, error_code = 'NONDETERMINISM', error_message = $2, updated_at = NOW(), completed_at = NOW() 
					WHERE workflow_id = $3;`,
					history.StatusNondeterministic, nondetErr.Error(), run.WorkflowID)
				return err
			}); txErr != nil {
				return nil, fmt.Errorf("failed recording nondeterminism failure: %w", txErr)
			}

			return &ReplayResult{
				WorkflowID: run.WorkflowID,
				Status:     history.StatusNondeterministic,
				Error:      nondetErr,
			}, nil
		}

		// Workflow returned a terminal failure
		r.metrics.IncWorkflowsFailed()

		// If compensations are registered, trigger Saga compensation!
		if compensationsCount > 0 {
			if txErr := r.db.WithTx(ctx, func(tx *sql.Tx) error {
				compStartPayload := history.WorkflowCompensationStartedPayload{
					TriggerErrorCode:   "WORKFLOW_FAILED",
					TotalCompensations: compensationsCount,
				}
				if _, err := r.store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventWorkflowCompensationStarted, nil, compStartPayload); err != nil {
					return err
				}
				_, err := tx.ExecContext(ctx, `
					UPDATE workflow_runs 
					SET status = $1, error_code = 'WORKFLOW_FAILED', error_message = $2, updated_at = NOW() 
					WHERE workflow_id = $3;`,
					history.StatusCompensating, execErr.Error(), run.WorkflowID)
				return err
			}); txErr != nil {
				return nil, fmt.Errorf("failed starting workflow compensation: %w", txErr)
			}

			return &ReplayResult{
				WorkflowID: run.WorkflowID,
				Status:     history.StatusCompensating,
				Error:      execErr,
			}, nil
		}

		// No compensations -> mark FAILED
		if txErr := r.db.WithTx(ctx, func(tx *sql.Tx) error {
			failPayload := history.WorkflowFailedPayload{
				ErrorCode:    "WORKFLOW_FAILED",
				ErrorMessage: execErr.Error(),
				Terminal:     true,
			}
			if _, err := r.store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventWorkflowFailed, nil, failPayload); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `
				UPDATE workflow_runs 
				SET status = $1, error_code = 'WORKFLOW_FAILED', error_message = $2, updated_at = NOW(), completed_at = NOW() 
				WHERE workflow_id = $3;`,
				history.StatusFailed, execErr.Error(), run.WorkflowID)
			return err
		}); txErr != nil {
			return nil, fmt.Errorf("failed recording workflow failure: %w", txErr)
		}

		return &ReplayResult{
			WorkflowID: run.WorkflowID,
			Status:     history.StatusFailed,
			Error:      execErr,
		}, nil
	}

	// 7. Workflow succeeded normally
	r.metrics.IncWorkflowsCompleted()
	err = r.db.WithTx(ctx, func(tx *sql.Tx) error {
		compPayload := history.WorkflowCompletedPayload{
			Output: output,
		}
		if _, err := r.store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventWorkflowCompleted, nil, compPayload); err != nil {
			return fmt.Errorf("failed appending WorkflowCompleted: %w", err)
		}

		updateQuery := `
			UPDATE workflow_runs 
			SET status = $1, output_json = $2, updated_at = NOW(), completed_at = NOW() 
			WHERE workflow_id = $3;`
		if _, err := tx.ExecContext(ctx, updateQuery, history.StatusCompleted, output, run.WorkflowID); err != nil {
			return fmt.Errorf("failed updating workflow run to COMPLETED: %w", err)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed completing workflow: %w", err)
	}

	return &ReplayResult{
		WorkflowID: run.WorkflowID,
		Status:     history.StatusCompleted,
		Output:     output,
	}, nil
}

// buildCommandIndex groups historical events by command_index.
func buildCommandIndex(events []history.Event) (map[int]*RecordedCommand, int, bool, error) {
	commands := make(map[int]*RecordedCommand)
	compensationsCount := 0
	isTerminal := false

	for _, ev := range events {
		switch ev.EventType {
		case history.EventWorkflowCompleted, history.EventWorkflowFailed, history.EventWorkflowCompensated, history.EventWorkflowNondeterminismDetected:
			isTerminal = true

		case history.EventActivityScheduled:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			var p history.ActivityScheduledPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return nil, 0, false, err
			}
			commands[idx] = &RecordedCommand{
				CommandIndex: idx,
				Kind:         CommandKindActivity,
				Name:         p.ActivityType,
				Hash:         p.InputHash,
				Scheduled:    true,
			}

		case history.EventActivityCompleted:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			var p history.ActivityCompletedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return nil, 0, false, err
			}
			if cmd, ok := commands[idx]; ok {
				cmd.Completed = true
				cmd.Result = p.Result
			}

		case history.EventActivityFailedFinal:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			var p history.ActivityFailedFinalPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return nil, 0, false, err
			}
			if cmd, ok := commands[idx]; ok {
				cmd.Failed = true
				cmd.ErrorCode = p.ErrorCode
				cmd.ErrorMessage = p.ErrorMessage
			}

		case history.EventCompensationRegistered:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			var p history.CompensationRegisteredPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return nil, 0, false, err
			}
			commands[idx] = &RecordedCommand{
				CommandIndex: idx,
				Kind:         CommandKindCompensation,
				Name:         p.CompensationType,
				Hash:         p.InputHash,
				Scheduled:    true,
			}
			compensationsCount++

		case history.EventTimerStarted:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			var p history.TimerStartedPayload
			if err := json.Unmarshal(ev.Payload, &p); err != nil {
				return nil, 0, false, err
			}
			hash, err := CanonicalHash(CommandKindTimer, "sleep", p.DurationMs, nil)
			if err != nil {
				return nil, 0, false, err
			}
			commands[idx] = &RecordedCommand{
				CommandIndex: idx,
				Kind:         CommandKindTimer,
				Name:         "sleep",
				Hash:         hash,
				Scheduled:    true,
			}

		case history.EventTimerFired:
			if ev.CommandIndex == nil {
				continue
			}
			idx := *ev.CommandIndex
			if cmd, ok := commands[idx]; ok {
				cmd.Completed = true
			}
		}
	}

	return commands, compensationsCount, isTerminal, nil
}
