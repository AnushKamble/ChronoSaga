package timers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
)

// Scheduler scans and atomically fires due timers in PostgreSQL.
type Scheduler struct {
	db           *persistence.DB
	store        *history.Store
	metrics      *observability.Metrics
	logger       *slog.Logger
	pollInterval time.Duration
	nowFunc      func() time.Time
}

// NewScheduler creates a new durable timer scheduler instance.
func NewScheduler(
	db *persistence.DB,
	store *history.Store,
	metrics *observability.Metrics,
	pollInterval time.Duration,
) *Scheduler {
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	return &Scheduler{
		db:           db,
		store:        store,
		metrics:      metrics,
		logger:       slog.Default(),
		pollInterval: pollInterval,
		nowFunc:      func() time.Time { return time.Now().UTC() },
	}
}

// SetClock sets an injected clock function (for deterministic testing).
func (s *Scheduler) SetClock(nowFunc func() time.Time) {
	s.nowFunc = nowFunc
}

// FireDueTimers scans for pending timers whose due_at <= now and fires them atomically.
// Guarantees single-fire semantics: each timer fires at most once even under concurrent schedulers.
func (s *Scheduler) FireDueTimers(ctx context.Context) (int, error) {
	currentTime := s.nowFunc()
	firedCount := 0

	for {
		var timerID, workflowID string
		var cmdIndex int
		var dueAt time.Time
		var claimed bool

		err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
			query := `
				SELECT timer_id, workflow_id, command_index, due_at
				FROM timer_tasks
				WHERE status = 'PENDING' AND due_at <= $1
				ORDER BY due_at ASC, timer_id ASC
				LIMIT 1
				FOR UPDATE SKIP LOCKED;`

			err := tx.QueryRowContext(ctx, query, currentTime).Scan(&timerID, &workflowID, &cmdIndex, &dueAt)
			if err != nil {
				if err == sql.ErrNoRows {
					return nil
				}
				return fmt.Errorf("failed selecting due timer: %w", err)
			}

			// 1. Mark timer FIRED
			now := time.Now().UTC()
			updateQuery := `
				UPDATE timer_tasks
				SET status = 'FIRED', fired_at = $1
				WHERE timer_id = $2 AND status = 'PENDING';`

			res, err := tx.ExecContext(ctx, updateQuery, now, timerID)
			if err != nil {
				return fmt.Errorf("failed updating timer status: %w", err)
			}
			rows, _ := res.RowsAffected()
			if rows == 0 {
				return nil // Concurrently claimed
			}

			// 2. Atomically append TimerFired event
			firedPayload := history.TimerFiredPayload{
				TimerID: timerID,
				FiredAt: now,
			}
			if _, err := s.store.AppendEventInTx(ctx, tx, workflowID, history.EventTimerFired, &cmdIndex, firedPayload); err != nil {
				return fmt.Errorf("failed appending TimerFired event: %w", err)
			}

			// 3. Wake workflow run projection to RUNNING
			updateRunQuery := `
				UPDATE workflow_runs
				SET status = $1, updated_at = NOW()
				WHERE workflow_id = $2;`
			if _, err := tx.ExecContext(ctx, updateRunQuery, history.StatusRunning, workflowID); err != nil {
				return fmt.Errorf("failed waking workflow run after timer fire: %w", err)
			}

			claimed = true
			lateness := now.Sub(dueAt)
			s.metrics.RecordTimerLateness(lateness.Milliseconds())
			return nil
		})

		if err != nil {
			return firedCount, err
		}
		if !claimed {
			break
		}
		firedCount++
	}

	return firedCount, nil
}

// Start runs the periodic timer scheduler loop.
func (s *Scheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := s.FireDueTimers(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("Failed polling due timers", "error", err)
			}
		}
	}
}
