package saga_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/history"
	"chronosaga/internal/saga"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func TestSaga_LIFO_Order_And_Durability(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	actReg := activities.NewRegistry()
	coordinator := saga.NewCoordinator(db, store, actReg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var executedOrder []string

	_ = actReg.Register("refund_payment", func(ctx context.Context, input json.RawMessage, key string) (json.RawMessage, error) {
		executedOrder = append(executedOrder, "refund_payment")
		return json.Marshal(map[string]bool{"refunded": true})
	})

	_ = actReg.Register("release_inventory", func(ctx context.Context, input json.RawMessage, key string) (json.RawMessage, error) {
		executedOrder = append(executedOrder, "release_inventory")
		return json.Marshal(map[string]bool{"released": true})
	})

	// 1. Create a workflow run in COMPENSATING status
	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "order_saga",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "saga-lifo-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	if _, err := db.ExecContext(ctx, "UPDATE workflow_runs SET status = 'COMPENSATING' WHERE workflow_id = $1", run.WorkflowID); err != nil {
		t.Fatalf("failed updating run to COMPENSATING: %v", err)
	}

	// 2. Insert two compensations:
	// Order 1: release_inventory (registered after step 1)
	// Order 2: refund_payment (registered after step 2)
	comp1ID := uuid.NewString()
	_, err = db.ExecContext(ctx, `
		INSERT INTO compensation_tasks (
			compensation_id, workflow_id, order_index, forward_command_index,
			compensation_type, input_payload, input_hash, retry_policy,
			idempotency_key, status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, 1, 1, 'release_inventory', '{}', 'h1', '{}', 'k1', 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`,
		comp1ID, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed inserting comp 1: %v", err)
	}

	comp2ID := uuid.NewString()
	_, err = db.ExecContext(ctx, `
		INSERT INTO compensation_tasks (
			compensation_id, workflow_id, order_index, forward_command_index,
			compensation_type, input_payload, input_hash, retry_policy,
			idempotency_key, status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, 2, 2, 'refund_payment', '{}', 'h2', '{}', 'k2', 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`,
		comp2ID, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed inserting comp 2: %v", err)
	}

	// 3. Step 1 of DriveCompensation: should execute order 2 (refund_payment)
	done, err := coordinator.DriveCompensation(ctx, run.WorkflowID, "test-worker")
	if err != nil {
		t.Fatalf("step 1 failed: %v", err)
	}
	if done {
		t.Errorf("expected done=false, still has comp 1")
	}

	if len(executedOrder) != 1 || executedOrder[0] != "refund_payment" {
		t.Fatalf("expected refund_payment executed first (LIFO), got %v", executedOrder)
	}

	// 4. Step 2 of DriveCompensation: should execute order 1 (release_inventory) and mark COMPENSATED
	done, err = coordinator.DriveCompensation(ctx, run.WorkflowID, "test-worker")
	if err != nil {
		t.Fatalf("step 2 failed: %v", err)
	}

	// Final check: should be done
	done, err = coordinator.DriveCompensation(ctx, run.WorkflowID, "test-worker")
	if err != nil || !done {
		t.Fatalf("expected done=true, got done=%v, err=%v", done, err)
	}

	// Verify LIFO order: [refund_payment, release_inventory]
	if len(executedOrder) != 2 || executedOrder[0] != "refund_payment" || executedOrder[1] != "release_inventory" {
		t.Fatalf("LIFO order violation! Got execution sequence: %v", executedOrder)
	}

	// Verify workflow status is COMPENSATED
	runAfter, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runAfter.Status != history.StatusCompensated {
		t.Errorf("expected workflow status COMPENSATED, got %s", runAfter.Status)
	}
}

func TestSaga_Failure_And_OperatorRetry(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	actReg := activities.NewRegistry()
	coordinator := saga.NewCoordinator(db, store, actReg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	shouldFail := true
	_ = actReg.Register("failing_comp", func(ctx context.Context, input json.RawMessage, key string) (json.RawMessage, error) {
		if shouldFail {
			return nil, activities.NewNonRetryableError("BANK_ERROR", "gateway down")
		}
		return json.Marshal(map[string]bool{"ok": true})
	})

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "fail_saga",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "saga-fail-" + uuid.NewString(),
	})
	if _, err := db.ExecContext(ctx, "UPDATE workflow_runs SET status = 'COMPENSATING' WHERE workflow_id = $1", run.WorkflowID); err != nil {
		t.Fatalf("failed updating run to COMPENSATING: %v", err)
	}

	compID := uuid.NewString()
	_, _ = db.ExecContext(ctx, `
		INSERT INTO compensation_tasks (
			compensation_id, workflow_id, order_index, forward_command_index,
			compensation_type, input_payload, input_hash, retry_policy,
			idempotency_key, status, attempt, max_attempts, available_at, lease_generation,
			created_at, updated_at
		) VALUES ($1, $2, 1, 1, 'failing_comp', '{}', 'h1', '{}', 'k1', 'PENDING', 0, 1, NOW(), 0, NOW(), NOW());`,
		compID, run.WorkflowID)

	// 1. Drive compensation -> should fail
	_, err := coordinator.DriveCompensation(ctx, run.WorkflowID, "w1")
	if err == nil {
		t.Fatalf("expected error from failing compensation")
	}

	// 2. Verify workflow status is COMPENSATION_FAILED
	runAfter, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runAfter.Status != history.StatusCompensationFailed {
		t.Fatalf("expected COMPENSATION_FAILED, got %s", runAfter.Status)
	}

	// 3. Operator triggers retry
	err = coordinator.RetryFailedCompensation(ctx, run.WorkflowID, "admin-alice")
	if err != nil {
		t.Fatalf("RetryFailedCompensation failed: %v", err)
	}

	// Verify status restored to COMPENSATING
	runRetried, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runRetried.Status != history.StatusCompensating {
		t.Errorf("expected COMPENSATING after retry request, got %s", runRetried.Status)
	}

	// 4. Now fix the condition and drive compensation again
	shouldFail = false
	done, err := coordinator.DriveCompensation(ctx, run.WorkflowID, "w1")
	if err != nil {
		t.Fatalf("retry execution failed: %v", err)
	}

	done, _ = coordinator.DriveCompensation(ctx, run.WorkflowID, "w1")
	if !done {
		t.Errorf("expected done=true after successful compensation retry")
	}

	runFinal, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runFinal.Status != history.StatusCompensated {
		t.Errorf("expected COMPENSATED, got %s", runFinal.Status)
	}
}
