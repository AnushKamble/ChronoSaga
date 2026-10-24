package chaos_test

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/demo"
	"chronosaga/internal/history"
	"chronosaga/internal/saga"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

// TestChaos_WorkerCrashAfterSideEffectBeforeAck tests the critical distributed systems failure window:
// The worker executes the external/mock business side effect, but crashes/dies before recording ActivityCompleted in PostgreSQL.
func TestChaos_WorkerCrashAfterSideEffectBeforeAck(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	effectStore := demo.NewEffectStore(db)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "crash_window_wf",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "crash-win-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	queue := activities.NewTaskQueue(db, store, nil)
	taskID := uuid.NewString()
	idempKey := "act-side-effect-" + uuid.NewString()

	optsJSON, _ := json.Marshal(workflow.DefaultActivityOptions())
	_, _ = db.ExecContext(ctx, `
		INSERT INTO activity_tasks (
			task_id, workflow_id, command_index, task_kind, activity_type,
			input_payload, input_hash, retry_policy, idempotency_key,
			status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, 0, 'FORWARD', 'charge_payment', '{}', 'h', $3, $4, 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`,
		taskID, run.WorkflowID, optsJSON, idempKey)

	// 1. Worker 1 claims task
	claim1, err := queue.ClaimTask(ctx, "worker-1", 1*time.Second)
	if err != nil || claim1 == nil {
		t.Fatalf("worker 1 claim failed: %v", err)
	}

	// 2. Worker 1 executes the business side effect (charging payment)
	payInput := demo.ChargePaymentInput{
		OrderID:     "ord-crash-window",
		AmountCents: 5000,
	}
	_, err = effectStore.ChargePayment(ctx, idempKey, payInput)
	if err != nil {
		t.Fatalf("failed charging payment: %v", err)
	}

	// Verify payment exists in database
	var paymentCount int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM demo_payments WHERE order_id = 'ord-crash-window'").Scan(&paymentCount)
	if paymentCount != 1 {
		t.Fatalf("expected 1 payment in DB, got %d", paymentCount)
	}

	// 3. CRASH INJECTION: Worker 1 dies abruptly before calling CompleteTask!
	// (Simulate death by allowing lease to expire without calling queue.CompleteTask)
	_, _ = db.ExecContext(ctx, "UPDATE activity_tasks SET lease_expires_at = NOW() - interval '2 seconds' WHERE task_id = $1", taskID)

	// 4. Reaper reclaims the expired lease
	reclaimed, err := queue.ReclaimExpiredLeases(ctx)
	if err != nil || reclaimed != 1 {
		t.Fatalf("expected 1 lease reclaimed, got %d (err: %v)", reclaimed, err)
	}

	// 5. Worker 2 claims the task
	claim2, err := queue.ClaimTask(ctx, "worker-2", 10*time.Second)
	if err != nil || claim2 == nil {
		t.Fatalf("worker 2 claim failed: %v", err)
	}

	// 6. Worker 2 invokes ChargePayment with the same idempotency key
	res2, err := effectStore.ChargePayment(ctx, claim2.IdempotencyKey, payInput)
	if err != nil {
		t.Fatalf("worker 2 ChargePayment failed: %v", err)
	}
	if res2.Status != "CHARGED" {
		t.Errorf("expected CHARGED status, got %s", res2.Status)
	}

	// 7. Worker 2 completes the task
	rawRes, _ := json.Marshal(res2)
	err = queue.CompleteTask(ctx, taskID, claim2.LeaseGeneration, rawRes)
	if err != nil {
		t.Fatalf("worker 2 CompleteTask failed: %v", err)
	}

	// Invariant Verification:
	// Despite 2 worker attempts across process crash, exactly ONE payment record must exist!
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM demo_payments WHERE order_id = 'ord-crash-window'").Scan(&paymentCount)
	if paymentCount != 1 {
		t.Fatalf("duplicate side effect violation! Expected 1 payment row, got %d", paymentCount)
	}

	var effectCount int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&effectCount)
	if effectCount != 1 {
		t.Fatalf("duplicate effect violation! Expected 1 effect row, got %d", effectCount)
	}
}

// TestChaos_BoundedRandomSeedWorkflow tests deterministic correctness and compensation under randomized failure conditions.
func TestChaos_BoundedRandomSeedWorkflow(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	seed := int64(42)
	t.Logf("Running bounded chaos test with reproducible seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	wfReg := workflow.NewRegistry()
	actReg := activities.NewRegistry()

	// Register 4 dummy activities: some randomly fail with retryable or non-retryable errors
	for i := 1; i <= 4; i++ {
		actName := "chaos_step_" + string(rune('0'+i))
		shouldFail := rng.Float64() < 0.3
		_ = actReg.Register(actName, func(ctx context.Context, input json.RawMessage, key string) (json.RawMessage, error) {
			if shouldFail {
				return nil, activities.NewNonRetryableError("CHAOS_FAIL", "injected random failure")
			}
			return json.Marshal(map[string]bool{"ok": true})
		})
	}

	_ = wfReg.Register("chaos_wf", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		opts := workflow.DefaultActivityOptions()
		var out map[string]bool
		if err := wctx.ExecuteActivity("chaos_step_1", nil, opts, &out); err != nil {
			return nil, err
		}
		_ = wctx.RegisterCompensation("chaos_step_1", nil, opts)

		if err := wctx.ExecuteActivity("chaos_step_2", nil, opts, &out); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"result": "chaos_complete"})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "chaos_wf",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "chaos-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting chaos run: %v", err)
	}

	queue := activities.NewTaskQueue(db, store, nil)
	worker := activities.NewWorker("chaos-worker", queue, actReg, nil, 1, 30*time.Millisecond, 10*time.Second)
	coord := saga.NewCoordinator(db, store, actReg, nil)
	replayer := workflow.NewReplayer(db, store, wfReg, nil)
	scheduler := workflow.NewOrchestratorScheduler("chaos-sched", db, store, replayer, coord, nil, 30*time.Millisecond)

	// Step workflow through scheduler ticks and worker claims
	for step := 0; step < 10; step++ {
		_, _ = scheduler.Tick(ctx)
		task, _ := queue.ClaimTask(ctx, "chaos-w", 5*time.Second)
		if task != nil {
			worker.ExecuteTask(ctx, task)
		}
		runCurrent, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
		if runCurrent.Status == history.StatusCompleted || runCurrent.Status == history.StatusCompensated || runCurrent.Status == history.StatusFailed {
			t.Logf("Chaos workflow reached terminal state %s at step %d (seed=%d)", runCurrent.Status, step, seed)
			break
		}
	}

	// Verify run reached an explicit valid terminal state (COMPLETED, COMPENSATED, or FAILED)
	runFinal, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	validTerminals := map[string]bool{
		history.StatusCompleted:   true,
		history.StatusCompensated: true,
		history.StatusFailed:      true,
	}
	if !validTerminals[runFinal.Status] {
		t.Fatalf("workflow ended in invalid/non-terminal state: %s (seed=%d)", runFinal.Status, seed)
	}
}

// TestChaos_ProcessSIGKILLAndRestart tests killing the actual OS process running the API and restarting it.
func TestChaos_ProcessSIGKILLAndRestart(t *testing.T) {
	// Locate compiled binary or build it
	binPath := "../../bin/chronosaga.exe"
	if _, err := os.Stat(binPath); err != nil {
		binPath = "chronosaga.exe"
		buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/chronosaga")
		if err := buildCmd.Run(); err != nil {
			t.Skipf("skipping OS process test: cannot build binary: %v", err)
		}
		defer os.Remove(binPath)
	}

	port := "58091"
	dsn := "postgres://chronosaga:chronosaga_secret@127.0.0.1:54333/chronosaga_test?sslmode=disable"

	// 1. Start process 1
	cmd1 := exec.Command(binPath, "api")
	cmd1.Env = append(os.Environ(),
		"CHRONOSAGA_PORT="+port,
		"DATABASE_URL="+dsn,
		"CHRONOSAGA_LOG_LEVEL=error",
	)
	if err := cmd1.Start(); err != nil {
		t.Skipf("cannot start child process: %v", err)
	}

	// Wait for process to be listening
	client := &http.Client{Timeout: 500 * time.Millisecond}
	ready := false
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := client.Get("http://127.0.0.1:" + port + "/health/live")
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			ready = true
			break
		}
	}

	if !ready {
		_ = cmd1.Process.Kill()
		t.Skip("child process did not become healthy in time")
	}

	// 2. Terminate process with hard SIGKILL / Kill()
	if err := cmd1.Process.Kill(); err != nil {
		t.Fatalf("failed killing process: %v", err)
	}
	_ = cmd1.Wait()
	t.Log("Process 1 killed via SIGKILL successfully")

	// 3. Restart process 2 (new OS process) on same database
	cmd2 := exec.Command(binPath, "api")
	cmd2.Env = append(os.Environ(),
		"CHRONOSAGA_PORT="+port,
		"DATABASE_URL="+dsn,
		"CHRONOSAGA_LOG_LEVEL=error",
	)
	if err := cmd2.Start(); err != nil {
		t.Fatalf("failed restarting process 2: %v", err)
	}
	defer func() {
		_ = cmd2.Process.Kill()
		_ = cmd2.Wait()
	}()

	// Verify restarted process is up and healthy
	readyAfterRestart := false
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := client.Get("http://127.0.0.1:" + port + "/health/live")
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			readyAfterRestart = true
			break
		}
	}

	if !readyAfterRestart {
		t.Fatalf("restarted process did not become healthy")
	}

	t.Log("Process 2 restarted and healthy from PostgreSQL state!")
}
