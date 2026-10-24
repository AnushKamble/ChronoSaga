package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/demo"
	"chronosaga/internal/history"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func TestIntegration_EventTaskAtomicity(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "atomicity_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "atom-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	taskID := uuid.NewString()
	cmdIdx := 0
	simulatedErr := errors.New("simulated fatal crash before commit")

	// Injected transaction failure midway
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		// Append event
		schedPayload := history.ActivityScheduledPayload{
			TaskID:         taskID,
			ActivityType:   "reserve_inventory",
			Input:          json.RawMessage(`{}`),
			InputHash:      "hash",
			RetryPolicy:    json.RawMessage(`{}`),
			IdempotencyKey: "k1",
		}
		_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityScheduled, &cmdIdx, schedPayload)
		if err != nil {
			return err
		}

		// Insert task
		insertQuery := `
			INSERT INTO activity_tasks (
				task_id, workflow_id, command_index, task_kind, activity_type,
				input_payload, input_hash, retry_policy, idempotency_key,
				status, attempt, max_attempts, available_at, lease_generation,
				created_at, updated_at
			) VALUES ($1, $2, $3, 'FORWARD', 'reserve_inventory', '{}', 'hash', '{}', 'k1', 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`
		_, err = tx.ExecContext(ctx, insertQuery, taskID, run.WorkflowID, cmdIdx)
		if err != nil {
			return err
		}

		// Inject failure right before commit
		return simulatedErr
	})

	// Invariant Check: Neither the event nor the task row can exist!
	var eventCount int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = 'ActivityScheduled'", run.WorkflowID).Scan(&eventCount)
	if eventCount != 0 {
		t.Fatalf("atomicity violation: ActivityScheduled was committed despite transaction error!")
	}

	var taskCount int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_tasks WHERE task_id = $1", taskID).Scan(&taskCount)
	if taskCount != 0 {
		t.Fatalf("atomicity violation: activity_tasks row was committed despite transaction error!")
	}
}

func TestIntegration_DuplicateActivityCompletion(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	queue := activities.NewTaskQueue(db, store, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "dup_comp_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "dup-act-" + uuid.NewString(),
	})

	taskID := uuid.NewString()
	_, _ = db.ExecContext(ctx, `
		INSERT INTO activity_tasks (
			task_id, workflow_id, command_index, task_kind, activity_type,
			input_payload, input_hash, retry_policy, idempotency_key,
			status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, 0, 'FORWARD', 'step', '{}', 'h', '{}', 'k', 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`,
		taskID, run.WorkflowID)

	// Claim task
	task, _ := queue.ClaimTask(ctx, "worker-1", 10*time.Second)

	// First completion -> succeeds
	err1 := queue.CompleteTask(ctx, task.TaskID, task.LeaseGeneration, json.RawMessage(`{"res":1}`))
	if err1 != nil {
		t.Fatalf("first completion failed: %v", err1)
	}

	// Duplicate completion (network retry / duplicate ACK) -> must be rejected as stale lease
	err2 := queue.CompleteTask(ctx, task.TaskID, task.LeaseGeneration, json.RawMessage(`{"res":1}`))
	if !errors.Is(err2, activities.ErrStaleLease) {
		t.Fatalf("expected ErrStaleLease on duplicate completion, got %v", err2)
	}

	// Verify only 1 ActivityCompleted event is recorded in history
	var completedCount int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $1 AND event_type = 'ActivityCompleted'", run.WorkflowID).Scan(&completedCount)
	if completedCount != 1 {
		t.Fatalf("expected exactly 1 ActivityCompleted event, got %d", completedCount)
	}
}

func TestIntegration_ConcurrentWorkflows_NoDeadlocks(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	effectStore := demo.NewEffectStore(db)

	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)

	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sku := "SKU-PARALLEL"
	_ = effectStore.SeedInventory(ctx, sku, 100)

	// Start 5 concurrent workflows
	workflowCount := 5
	var runIDs []string
	for i := 0; i < workflowCount; i++ {
		in := demo.OrderWorkflowInput{
			OrderID:    uuid.NewString(),
			SKU:        sku,
			Quantity:   1,
			TotalCents: 1000,
		}
		raw, _ := json.Marshal(in)
		run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    demo.OrderWorkflowType,
			WorkflowVersion: demo.OrderWorkflowVersion,
			Input:           raw,
			IdempotencyKey:  "parallel-" + uuid.NewString(),
		})
		if err != nil {
			t.Fatalf("failed starting workflow %d: %v", i, err)
		}
		runIDs = append(runIDs, run.WorkflowID)
	}

	// Run 2 workers and 1 scheduler concurrently
	queue := activities.NewTaskQueue(db, store, nil)
	replayer := workflow.NewReplayer(db, store, wfReg, nil)
	scheduler := workflow.NewOrchestratorScheduler("sched-1", db, store, replayer, nil, nil, 30*time.Millisecond)

	worker1 := activities.NewWorker("w1", queue, actReg, nil, 2, 30*time.Millisecond, 10*time.Second)
	worker2 := activities.NewWorker("w2", queue, actReg, nil, 2, 30*time.Millisecond, 10*time.Second)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	go worker1.Start(workerCtx)
	go worker2.Start(workerCtx)

	// Poll scheduler until all workflows are completed
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = scheduler.Tick(ctx)

		allDone := true
		for _, id := range runIDs {
			r, err := store.GetWorkflowRun(ctx, id)
			if err != nil || r.Status != history.StatusCompleted {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify all workflows completed cleanly
	for _, id := range runIDs {
		r, err := store.GetWorkflowRun(ctx, id)
		if err != nil || r.Status != history.StatusCompleted {
			t.Fatalf("workflow %s did not complete cleanly, status=%s", id, r.Status)
		}
	}
}
