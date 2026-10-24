package workflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
)

// SagaDriver interface to decouple coordinator from replayer package.
type SagaDriver interface {
	DriveCompensation(ctx context.Context, workflowID string, workerID string) (bool, error)
}

// OrchestratorScheduler claims runnable workflow runs and executes replay and saga compensation.
type OrchestratorScheduler struct {
	schedulerID  string
	db           *persistence.DB
	store        *history.Store
	replayer     *Replayer
	sagaDriver   SagaDriver
	metrics      *observability.Metrics
	pollInterval time.Duration
	logger       *slog.Logger
}

// NewOrchestratorScheduler initializes a new orchestrator scheduler.
func NewOrchestratorScheduler(
	schedulerID string,
	db *persistence.DB,
	store *history.Store,
	replayer *Replayer,
	sagaDriver SagaDriver,
	metrics *observability.Metrics,
	pollInterval time.Duration,
) *OrchestratorScheduler {
	if schedulerID == "" {
		schedulerID = "orchestrator-" + uuid.NewString()[:8]
	}
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}

	return &OrchestratorScheduler{
		schedulerID:  schedulerID,
		db:           db,
		store:        store,
		replayer:     replayer,
		sagaDriver:   sagaDriver,
		metrics:      metrics,
		pollInterval: pollInterval,
		logger:       slog.Default().With("scheduler_id", schedulerID),
	}
}

// Tick claims and processes eligible workflows (RUNNING or COMPENSATING).
func (s *OrchestratorScheduler) Tick(ctx context.Context) (int, error) {
	// Find runnable workflow runs
	query := `
		SELECT workflow_id
		FROM workflow_runs
		WHERE status IN ('RUNNING', 'COMPENSATING')
		LIMIT 10;`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed querying runnable workflows: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, fmt.Errorf("failed scanning runnable workflow id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	processed := 0
	for _, id := range ids {
		run, err := s.store.GetWorkflowRun(ctx, id)
		if err != nil {
			s.logger.Warn("Failed retrieving workflow run for scheduler tick", "workflow_id", id, "error", err)
			continue
		}

		if run.Status == history.StatusRunning {
			_, repErr := s.replayer.ReplayWorkflow(ctx, run)
			if repErr != nil {
				s.logger.Warn("Replay returned error", "workflow_id", id, "error", repErr)
			}
			processed++
		} else if run.Status == history.StatusCompensating && s.sagaDriver != nil {
			_, sagaErr := s.sagaDriver.DriveCompensation(ctx, id, s.schedulerID)
			if sagaErr != nil {
				s.logger.Warn("Saga drive returned error", "workflow_id", id, "error", sagaErr)
			}
			processed++
		}
	}

	return processed, nil
}

// Start begins the orchestrator scheduler loop until context cancellation.
func (s *OrchestratorScheduler) Start(ctx context.Context) error {
	s.logger.Info("Starting orchestrator scheduler loop", "poll_interval", s.pollInterval)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Orchestrator scheduler stopped")
			return nil
		case <-ticker.C:
			if _, err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("Orchestrator scheduler tick failed", "error", err)
			}
		}
	}
}
