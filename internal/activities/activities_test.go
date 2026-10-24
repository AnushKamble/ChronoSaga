package activities_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/history"
	"chronosaga/internal/persistence"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func insertTestTask(
	t *testing.T,
	db *persistence.DB,
	workflowID string,
	cmdIndex int,
	activityType string,
	opts workflow.ActivityOptions,
) string {
	t.Helper()
	taskID := uuid.NewString()
	idempKey := "idemp-" + taskID[:8]
	optsJSON, _ := json.Marshal(opts)

	query := `
		INSERT INTO activity_tasks (
			task_id, workflow_id, command_index, task_kind, activity_type,
			input_payload, input_hash, retry_policy, idempotency_key,
			status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, $3, 'FORWARD', $4, '{}', 'hash', $5, $6, 'PENDING', 0, $7, NOW(), 0, NOW(), NOW());`

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	_, err := db.ExecContext(context.Background(), query,
		taskID, workflowID, cmdIndex, activityType, optsJSON, idempKey, maxAttempts,
	)
	if err != nil {
		t.Fatalf("failed inserting test task: %v", err)
	}
	return taskID
}

func TestActivities_TwoWorkers_ClaimDistinctTasks(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	queue := activities.NewTaskQueue(db, store, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Create a workflow run
	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "two_worker_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "two-w-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	// 2. Insert two tasks
	task1ID := insertTestTask(t, db, run.WorkflowID, 0, "act_1", workflow.DefaultActivityOptions())
	task2ID := insertTestTask(t, db, run.WorkflowID, 1, "act_2", workflow.DefaultActivityOptions())

	// 3. Concurrently claim with Worker A and Worker B
	var claimed1, claimed2 *activities.Task
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		t1, _ := queue.ClaimTask(ctx, "worker-A", 10*time.Second)
		claimed1 = t1
	}()

	go func() {
		defer wg.Done()
		t2, _ := queue.ClaimTask(ctx, "worker-B", 10*time.Second)
		claimed2 = t2
	}()

	wg.Wait()

	if claimed1 == nil || claimed2 == nil {
		t.Fatalf("both workers should have claimed a task, got t1=%v, t2=%v", claimed1, claimed2)
	}

	if claimed1.TaskID == claimed2.TaskID {
		t.Fatalf("both workers claimed the same task! task_id=%s", claimed1.TaskID)
	}

	claimedIDs := map[string]bool{claimed1.TaskID: true, claimed2.TaskID: true}
	if !claimedIDs[task1ID] || !claimedIDs[task2ID] {
		t.Errorf("expected tasks %s and %s to be claimed, got %s and %s", task1ID, task2ID, claimed1.TaskID, claimed2.TaskID)
	}
}

func TestActivities_StaleWorker_Fencing(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	queue := activities.NewTaskQueue(db, store, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "fencing_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "fence-" + uuid.NewString(),
	})

	taskID := insertTestTask(t, db, run.WorkflowID, 0, "fenced_act", workflow.DefaultActivityOptions())

	// 1. Worker 1 claims task
	claim1, err := queue.ClaimTask(ctx, "worker-1", 1*time.Second)
	if err != nil || claim1 == nil {
		t.Fatalf("worker 1 claim failed: %v", err)
	}
	if claim1.LeaseGeneration != 1 {
		t.Errorf("expected lease generation 1, got %d", claim1.LeaseGeneration)
	}

	// 2. Simulate lease expiration by updating lease_expires_at into the past
	_, err = db.ExecContext(ctx, "UPDATE activity_tasks SET lease_expires_at = NOW() - interval '5 seconds' WHERE task_id = $1", taskID)
	if err != nil {
		t.Fatalf("failed expiring lease: %v", err)
	}

	// 3. Run reaper to reclaim lease
	reclaimed, err := queue.ReclaimExpiredLeases(ctx)
	if err != nil || reclaimed != 1 {
		t.Fatalf("expected 1 lease reclaimed, got %d (err: %v)", reclaimed, err)
	}

	// 4. Worker 2 claims the task (increments lease_generation to 2)
	claim2, err := queue.ClaimTask(ctx, "worker-2", 10*time.Second)
	if err != nil || claim2 == nil {
		t.Fatalf("worker 2 claim failed: %v", err)
	}
	if claim2.LeaseGeneration != 2 {
		t.Errorf("expected lease generation 2, got %d", claim2.LeaseGeneration)
	}

	// 5. Stale Worker 1 attempts to complete task with its old generation 1
	staleErr := queue.CompleteTask(ctx, taskID, claim1.LeaseGeneration, json.RawMessage(`{"stale":"data"}`))
	if !errors.Is(staleErr, activities.ErrStaleLease) {
		t.Fatalf("expected ErrStaleLease, got %v", staleErr)
	}

	// 6. Valid Worker 2 completes task with generation 2 -> must succeed
	validErr := queue.CompleteTask(ctx, taskID, claim2.LeaseGeneration, json.RawMessage(`{"valid":"data"}`))
	if validErr != nil {
		t.Fatalf("worker 2 completion failed: %v", validErr)
	}

	// 7. Verify result in DB is Worker 2's data
	var status string
	var resultPayload []byte
	err = db.QueryRowContext(ctx, "SELECT status, result_payload FROM activity_tasks WHERE task_id = $1", taskID).Scan(&status, &resultPayload)
	if err != nil {
		t.Fatalf("failed querying task: %v", err)
	}
	if status != "COMPLETED" {
		t.Errorf("expected status COMPLETED, got %s", status)
	}
	var resultMap map[string]string
	_ = json.Unmarshal(resultPayload, &resultMap)
	if resultMap["valid"] != "data" {
		t.Errorf("expected Worker 2 data, got %v", resultMap)
	}
}

func TestActivities_RetryPolicy_And_Persistence(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	queue := activities.NewTaskQueue(db, store, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "retry_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "retry-" + uuid.NewString(),
	})

	opts := workflow.ActivityOptions{
		MaxAttempts:    3,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}
	taskID := insertTestTask(t, db, run.WorkflowID, 0, "flaky_act", opts)

	// Attempt 1: Claim and Fail
	claim1, _ := queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if claim1.Attempt != 1 {
		t.Errorf("expected attempt 1, got %d", claim1.Attempt)
	}
	err := queue.FailTask(ctx, taskID, claim1.LeaseGeneration, errors.New("network blip"))
	if err != nil {
		t.Fatalf("FailTask error: %v", err)
	}

	// Verify task is RETRY_WAIT and attempt is 1 in DB
	var status string
	var attempt int
	_ = db.QueryRowContext(ctx, "SELECT status, attempt FROM activity_tasks WHERE task_id = $1", taskID).Scan(&status, &attempt)
	if status != "RETRY_WAIT" || attempt != 1 {
		t.Errorf("expected RETRY_WAIT and attempt 1, got status=%s, attempt=%d", status, attempt)
	}

	// Fast-forward available_at so it can be claimed again
	_, _ = db.ExecContext(ctx, "UPDATE activity_tasks SET available_at = NOW() - interval '1 second' WHERE task_id = $1", taskID)

	// Attempt 2: Claim and Fail
	claim2, _ := queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if claim2.Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", claim2.Attempt)
	}
	_ = queue.FailTask(ctx, taskID, claim2.LeaseGeneration, errors.New("network blip 2"))

	// Fast-forward available_at
	_, _ = db.ExecContext(ctx, "UPDATE activity_tasks SET available_at = NOW() - interval '1 second' WHERE task_id = $1", taskID)

	// Attempt 3: Final attempt (maxAttempts = 3) -> should enter FAILED
	claim3, _ := queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if claim3.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", claim3.Attempt)
	}
	_ = queue.FailTask(ctx, taskID, claim3.LeaseGeneration, errors.New("fatal terminal network failure"))

	// Verify status is FAILED
	_ = db.QueryRowContext(ctx, "SELECT status, attempt FROM activity_tasks WHERE task_id = $1", taskID).Scan(&status, &attempt)
	if status != "FAILED" || attempt != 3 {
		t.Errorf("expected FAILED after 3 attempts, got status=%s, attempt=%d", status, attempt)
	}
}

func TestActivities_NonRetryableError_ImmediatelyFails(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	queue := activities.NewTaskQueue(db, store, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "non_retry_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "nonret-" + uuid.NewString(),
	})

	taskID := insertTestTask(t, db, run.WorkflowID, 0, "auth_act", workflow.ActivityOptions{MaxAttempts: 5})

	claim, _ := queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	nonRetErr := activities.NewNonRetryableError("INVALID_CREDENTIALS", "bad secret key")
	err := queue.FailTask(ctx, taskID, claim.LeaseGeneration, nonRetErr)
	if err != nil {
		t.Fatalf("FailTask error: %v", err)
	}

	// Verify task immediately entered FAILED on attempt 1 despite maxAttempts=5
	var status string
	var attempt int
	_ = db.QueryRowContext(ctx, "SELECT status, attempt FROM activity_tasks WHERE task_id = $1", taskID).Scan(&status, &attempt)
	if status != "FAILED" || attempt != 1 {
		t.Errorf("expected FAILED on attempt 1, got status=%s, attempt=%d", status, attempt)
	}
}
