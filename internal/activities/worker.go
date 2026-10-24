package activities

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"chronosaga/internal/observability"
	"github.com/google/uuid"
)

// Worker runs task claim loops and executes registered activity handlers.
type Worker struct {
	workerID      string
	queue         *TaskQueue
	registry      *Registry
	metrics       *observability.Metrics
	concurrency   int
	pollInterval  time.Duration
	leaseDuration time.Duration
	logger        *slog.Logger
}

// NewWorker initializes a new activity worker daemon.
func NewWorker(
	workerID string,
	queue *TaskQueue,
	registry *Registry,
	metrics *observability.Metrics,
	concurrency int,
	pollInterval, leaseDuration time.Duration,
) *Worker {
	if workerID == "" {
		workerID = "worker-" + uuid.NewString()[:8]
	}
	if concurrency <= 0 {
		concurrency = 5
	}
	if pollInterval <= 0 {
		pollInterval = 300 * time.Millisecond
	}
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}
	if registry == nil {
		registry = DefaultRegistry
	}
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}

	return &Worker{
		workerID:      workerID,
		queue:         queue,
		registry:      registry,
		metrics:       metrics,
		concurrency:   concurrency,
		pollInterval:  pollInterval,
		leaseDuration: leaseDuration,
		logger:        slog.Default().With("worker_id", workerID),
	}
}

// Start begins processing tasks until context is canceled.
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Starting activity worker", "concurrency", w.concurrency, "lease_duration", w.leaseDuration)

	var wg sync.WaitGroup

	// Background lease reaper loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(w.leaseDuration / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reclaimed, err := w.queue.ReclaimExpiredLeases(ctx)
				if err != nil {
					w.logger.Error("Failed reclaiming expired task leases", "error", err)
				} else if reclaimed > 0 {
					w.logger.Warn("Reclaimed expired task leases", "count", reclaimed)
				}
			}
		}
	}()

	// Worker concurrency pool
	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func(workerSlot int) {
			defer wg.Done()
			w.runWorkerLoop(ctx, workerSlot)
		}(i)
	}

	wg.Wait()
	w.logger.Info("Activity worker stopped")
	return nil
}

func (w *Worker) runWorkerLoop(ctx context.Context, slot int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			task, err := w.queue.ClaimTask(ctx, w.workerID, w.leaseDuration)
			if err != nil {
				w.logger.Error("Error claiming task", "slot", slot, "error", err)
				time.Sleep(w.pollInterval)
				continue
			}

			if task == nil {
				// No task available, back off
				select {
				case <-ctx.Done():
					return
				case <-time.After(w.pollInterval):
					continue
				}
			}

			w.ExecuteTask(ctx, task)
		}
	}
}

// ExecuteTask invokes the handler and persists completion or failure with fencing token.
func (w *Worker) ExecuteTask(ctx context.Context, task *Task) {
	handler, err := w.registry.Get(task.ActivityType)
	if err != nil {
		w.logger.Error("No registered handler for activity", "activity", task.ActivityType, "task_id", task.TaskID)
		if failErr := w.queue.FailTask(ctx, task.TaskID, task.LeaseGeneration, NewNonRetryableError("HANDLER_NOT_FOUND", err.Error())); failErr != nil {
			w.logger.Error("Failed failing unhandled activity task", "task_id", task.TaskID, "error", failErr)
		}
		return
	}

	startTime := time.Now()
	res, execErr := handler(ctx, task.InputPayload, task.IdempotencyKey)
	durationMs := time.Since(startTime).Milliseconds()
	w.metrics.RecordActivityDuration(task.ActivityType, durationMs)

	if execErr != nil {
		w.logger.Warn("Activity execution returned error",
			"task_id", task.TaskID,
			"activity", task.ActivityType,
			"attempt", task.Attempt,
			"error", execErr,
		)
		if failErr := w.queue.FailTask(ctx, task.TaskID, task.LeaseGeneration, execErr); failErr != nil {
			w.logger.Error("Failed persisting task failure", "task_id", task.TaskID, "error", failErr)
		}
		return
	}

	if completeErr := w.queue.CompleteTask(ctx, task.TaskID, task.LeaseGeneration, res); completeErr != nil {
		if completeErr == ErrStaleLease {
			w.logger.Warn("Task completion rejected due to stale lease",
				"task_id", task.TaskID,
				"lease_generation", task.LeaseGeneration,
			)
		} else {
			w.logger.Error("Failed completing task", "task_id", task.TaskID, "error", completeErr)
		}
	}
}
