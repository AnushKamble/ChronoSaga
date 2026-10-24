package demo_test

import (
	"context"
	"encoding/json"
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

func TestDemo_Idempotency_NoDuplicateSideEffects(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	effectStore := demo.NewEffectStore(db)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Seed inventory
	sku := "SKU-IDEMP"
	if err := effectStore.SeedInventory(ctx, sku, 10); err != nil {
		t.Fatalf("failed seeding inventory: %v", err)
	}

	idempKey := "act-idemp-" + uuid.NewString()

	// 1. First ReserveInventory
	res1, err := effectStore.ReserveInventory(ctx, idempKey, demo.ReserveInventoryInput{
		OrderID:  "ord-1",
		SKU:      sku,
		Quantity: 3,
	})
	if err != nil {
		t.Fatalf("first reserve failed: %v", err)
	}

	// 2. Second ReserveInventory with identical idempotency key (simulating retry after worker crash)
	res2, err := effectStore.ReserveInventory(ctx, idempKey, demo.ReserveInventoryInput{
		OrderID:  "ord-1",
		SKU:      sku,
		Quantity: 3,
	})
	if err != nil {
		t.Fatalf("second reserve failed: %v", err)
	}

	if res1.ReservationID != res2.ReservationID {
		t.Errorf("expected same reservation ID %s, got %s", res1.ReservationID, res2.ReservationID)
	}

	// 3. Verify stock: must be 7 available and 3 reserved (NOT 4 available / 6 reserved!)
	var avail, resv int
	err = db.QueryRowContext(ctx, "SELECT available_quantity, reserved_quantity FROM demo_inventory WHERE sku = $1", sku).Scan(&avail, &resv)
	if err != nil {
		t.Fatalf("failed querying stock: %v", err)
	}
	if avail != 7 || resv != 3 {
		t.Fatalf("inventory double-decremented! expected avail=7, resv=3, got avail=%d, resv=%d", avail, resv)
	}

	// 4. Verify demo_effects table: exactly 1 effect row
	var effectCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&effectCount)
	if err != nil || effectCount != 1 {
		t.Fatalf("expected 1 effect row, got %d (err: %v)", effectCount, err)
	}
}

func TestDemo_EndToEnd_OrderWorkflow_HappyPath(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	effectStore := demo.NewEffectStore(db)

	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)

	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	queue := activities.NewTaskQueue(db, store, nil)
	worker := activities.NewWorker("test-worker", queue, actReg, nil, 1, 50*time.Millisecond, 10*time.Second)
	coord := saga.NewCoordinator(db, store, actReg, nil)
	replayer := workflow.NewReplayer(db, store, wfReg, nil)
	scheduler := workflow.NewOrchestratorScheduler("test-sched", db, store, replayer, coord, nil, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Seed inventory
	sku := "SKU-HAPPY"
	_ = effectStore.SeedInventory(ctx, sku, 10)

	// 1. Start workflow run
	orderInput := demo.OrderWorkflowInput{
		OrderID:    "ord-happy-1",
		SKU:        sku,
		Quantity:   2,
		TotalCents: 4500,
	}
	rawInput, _ := json.Marshal(orderInput)

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    demo.OrderWorkflowType,
		WorkflowVersion: demo.OrderWorkflowVersion,
		Input:           rawInput,
		IdempotencyKey:  "happy-order-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting workflow: %v", err)
	}

	// Step-by-step engine execution:
	// Step 1: Replay schedules reserve_inventory -> Worker claims & completes
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick 1 failed: %v", err)
	}
	task, _ := queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if task == nil || task.ActivityType != "reserve_inventory" {
		t.Fatalf("expected reserve_inventory task, got %v", task)
	}
	worker.ExecuteTask(ctx, task)

	// Step 2: Replay registers compensation, schedules charge_payment -> Worker claims & completes
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick 2 failed: %v", err)
	}
	task, _ = queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if task == nil || task.ActivityType != "charge_payment" {
		t.Fatalf("expected charge_payment task, got %v", task)
	}
	worker.ExecuteTask(ctx, task)

	// Step 3: Replay registers compensation, schedules dispatch_shipment -> Worker claims & completes
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick 3 failed: %v", err)
	}
	task, _ = queue.ClaimTask(ctx, "worker-1", 5*time.Second)
	if task == nil || task.ActivityType != "dispatch_shipment" {
		t.Fatalf("expected dispatch_shipment task, got %v", task)
	}
	worker.ExecuteTask(ctx, task)

	// Step 4: Final replay completes workflow!
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick 4 failed: %v", err)
	}

	// Verify workflow status
	runFinal, err := store.GetWorkflowRun(ctx, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed getting run: %v", err)
	}
	if runFinal.Status != history.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", runFinal.Status)
	}

	var res demo.OrderWorkflowResult
	_ = json.Unmarshal(runFinal.OutputJSON, &res)
	if res.Status != "FULFILLED" || res.ShipmentID == "" {
		t.Errorf("expected FULFILLED with shipmentID, got %+v", res)
	}
}

func TestDemo_EndToEnd_OrderWorkflow_SagaCompensation(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	effectStore := demo.NewEffectStore(db)

	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)

	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	queue := activities.NewTaskQueue(db, store, nil)
	worker := activities.NewWorker("test-worker", queue, actReg, nil, 1, 50*time.Millisecond, 10*time.Second)
	coord := saga.NewCoordinator(db, store, actReg, nil)
	replayer := workflow.NewReplayer(db, store, wfReg, nil)
	scheduler := workflow.NewOrchestratorScheduler("test-sched", db, store, replayer, coord, nil, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sku := "SKU-SAGA"
	_ = effectStore.SeedInventory(ctx, sku, 10)

	// Start workflow with injected dispatch failure!
	orderInput := demo.OrderWorkflowInput{
		OrderID:              "ord-saga-fail",
		SKU:                  sku,
		Quantity:             4,
		TotalCents:           8900,
		ForceDispatchFailure: true,
	}
	rawInput, _ := json.Marshal(orderInput)

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    demo.OrderWorkflowType,
		WorkflowVersion: demo.OrderWorkflowVersion,
		Input:           rawInput,
		IdempotencyKey:  "saga-fail-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	// 1. reserve_inventory
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick reserve failed: %v", err)
	}
	t1, err := queue.ClaimTask(ctx, "w1", 5*time.Second)
	if err != nil || t1 == nil {
		t.Fatalf("failed claiming t1: %v", err)
	}
	worker.ExecuteTask(ctx, t1)

	// 2. charge_payment
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick charge failed: %v", err)
	}
	t2, err := queue.ClaimTask(ctx, "w1", 5*time.Second)
	if err != nil || t2 == nil {
		t.Fatalf("failed claiming t2: %v", err)
	}
	worker.ExecuteTask(ctx, t2)

	// 3. dispatch_shipment (FAILS!)
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick dispatch failed: %v", err)
	}
	t3, err := queue.ClaimTask(ctx, "w1", 5*time.Second)
	if err != nil || t3 == nil {
		t.Fatalf("failed claiming t3: %v", err)
	}
	worker.ExecuteTask(ctx, t3)

	// 4. Scheduler ticks: Replay sees terminal failure on dispatch_shipment, transitions workflow to COMPENSATING!
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick fail transition failed: %v", err)
	}

	runAfterFail, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runAfterFail.Status != history.StatusCompensating {
		t.Fatalf("expected status COMPENSATING after dispatch failure, got %s", runAfterFail.Status)
	}

	// 5. Drive compensation step 1: refund_payment (LIFO!)
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick comp 1 failed: %v", err)
	}

	// 6. Drive compensation step 2: release_inventory (LIFO!)
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick comp 2 failed: %v", err)
	}

	// 7. Drive compensation final check
	if _, err := scheduler.Tick(ctx); err != nil {
		t.Fatalf("tick comp complete failed: %v", err)
	}

	// Verify workflow ended in COMPENSATED
	runFinal, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
	if runFinal.Status != history.StatusCompensated {
		t.Fatalf("expected status COMPENSATED, got %s", runFinal.Status)
	}

	// Verify in database: payment status was updated to REFUNDED
	var payStatus string
	if err := db.QueryRowContext(ctx, "SELECT status FROM demo_payments WHERE order_id = $1", orderInput.OrderID).Scan(&payStatus); err != nil {
		t.Fatalf("failed scanning payment status: %v", err)
	}
	if payStatus != "REFUNDED" {
		t.Errorf("expected payment status REFUNDED, got %s", payStatus)
	}

	// Verify events in history show exact LIFO order: refund_payment then release_inventory
	events, _ := store.GetAllHistory(ctx, run.WorkflowID)
	var compCompletedEvents []string
	for _, ev := range events {
		if ev.EventType == history.EventCompensationCompleted {
			var p history.CompensationCompletedPayload
			_ = json.Unmarshal(ev.Payload, &p)
			compCompletedEvents = append(compCompletedEvents, p.CompensationType)
		}
	}

	if len(compCompletedEvents) != 2 || compCompletedEvents[0] != "refund_payment" || compCompletedEvents[1] != "release_inventory" {
		t.Fatalf("expected reverse LIFO compensation order [refund_payment, release_inventory], got %v", compCompletedEvents)
	}
}
