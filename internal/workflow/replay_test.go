package workflow_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

type StepInput struct {
	Value string `json:"value"`
}

type StepOutput struct {
	Echo string `json:"echo"`
}

func TestReplay_StepByStep_And_CachedResults(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	reg := workflow.NewRegistry()
	replayer := workflow.NewReplayer(db, store, reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Track how many times each line in workflow executes
	step1Calls := 0
	step2Calls := 0

	err := reg.Register("multi_step", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		step1Calls++
		var out1 StepOutput
		if err := wctx.ExecuteActivity("step_one", StepInput{Value: "hello"}, workflow.DefaultActivityOptions(), &out1); err != nil {
			return nil, err
		}

		step2Calls++
		var out2 StepOutput
		if err := wctx.ExecuteActivity("step_two", StepInput{Value: out1.Echo + "_world"}, workflow.DefaultActivityOptions(), &out2); err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{"final": out2.Echo})
	})
	if err != nil {
		t.Fatalf("failed registering workflow: %v", err)
	}

	// 1. Start workflow
	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "multi_step",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "wf-step-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed creating run: %v", err)
	}

	// 2. First replay: should schedule step_one and suspend
	res1, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("first replay failed: %v", err)
	}
	if !res1.Suspended {
		t.Errorf("expected res1 to be suspended")
	}
	if step1Calls != 1 || step2Calls != 0 {
		t.Errorf("expected step1=1, step2=0, got %d, %d", step1Calls, step2Calls)
	}

	// Verify activity_tasks has 1 task for step_one
	var taskCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_tasks WHERE workflow_id = $1", run.WorkflowID).Scan(&taskCount)
	if err != nil || taskCount != 1 {
		t.Fatalf("expected 1 activity task, got %d (err: %v)", taskCount, err)
	}

	// 3. Second replay while step_one is still pending: must NOT duplicate task!
	res2, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("second replay failed: %v", err)
	}
	if !res2.Suspended {
		t.Errorf("expected res2 to be suspended")
	}
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_tasks WHERE workflow_id = $1", run.WorkflowID).Scan(&taskCount)
	if err != nil || taskCount != 1 {
		t.Fatalf("duplicate task created! expected 1, got %d", taskCount)
	}

	// 4. Simulate worker completing step_one in DB
	cmd0 := 0
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, appendErr := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityCompleted, &cmd0, history.ActivityCompletedPayload{
			TaskID:       "dummy-task-1",
			ActivityType: "step_one",
			Result:       json.RawMessage(`{"echo":"HELLO"}`),
		})
		if appendErr != nil {
			return appendErr
		}
		_, updateErr := tx.ExecContext(ctx, "UPDATE workflow_runs SET status = 'RUNNING' WHERE workflow_id = $1", run.WorkflowID)
		return updateErr
	})

	// 5. Third replay: step_one completes from history, step_two schedules and suspends!
	res3, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("third replay failed: %v", err)
	}
	if !res3.Suspended {
		t.Errorf("expected res3 to be suspended on step_two")
	}
	// step 1 re-entered (step1Calls = 3), step 2 reached for first time (step2Calls = 1)
	if step2Calls != 1 {
		t.Errorf("expected step2Calls=1, got %d", step2Calls)
	}

	// Verify activity_tasks now has 2 tasks (step_one, step_two)
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM activity_tasks WHERE workflow_id = $1", run.WorkflowID).Scan(&taskCount)
	if err != nil || taskCount != 2 {
		t.Fatalf("expected 2 activity tasks, got %d", taskCount)
	}

	// 6. Simulate worker completing step_two
	cmd1 := 1
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, appendErr := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityCompleted, &cmd1, history.ActivityCompletedPayload{
			TaskID:       "dummy-task-2",
			ActivityType: "step_two",
			Result:       json.RawMessage(`{"echo":"HELLO_WORLD"}`),
		})
		return appendErr
	})

	// 7. Fourth replay: both complete from history, workflow finishes!
	res4, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("fourth replay failed: %v", err)
	}
	if res4.Suspended {
		t.Errorf("expected workflow to complete, got suspended")
	}
	if res4.Status != history.StatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", res4.Status)
	}

	var finalOutput map[string]string
	_ = json.Unmarshal(res4.Output, &finalOutput)
	if finalOutput["final"] != "HELLO_WORLD" {
		t.Errorf("expected final output HELLO_WORLD, got %v", finalOutput)
	}
}

func TestReplay_NondeterminismDetection(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	reg := workflow.NewRegistry()
	replayer := workflow.NewReplayer(db, store, reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register version 1
	_ = reg.Register("nondet_test", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		var out StepOutput
		return nil, wctx.ExecuteActivity("original_activity", StepInput{Value: "v1"}, workflow.DefaultActivityOptions(), &out)
	})

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "nondet_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "nondet-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	// Schedule original_activity
	_, _ = replayer.ReplayWorkflow(ctx, run)

	// Now overwrite registered function with a different activity name to simulate nondeterministic code change!
	regChanged := workflow.NewRegistry()
	_ = regChanged.Register("nondet_test", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		var out StepOutput
		// Calling DIFFERENT activity at command index 0
		return nil, wctx.ExecuteActivity("MODIFIED_ACTIVITY", StepInput{Value: "v1"}, workflow.DefaultActivityOptions(), &out)
	})

	replayerChanged := workflow.NewReplayer(db, store, regChanged, nil)
	res, err := replayerChanged.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("replay call error: %v", err)
	}

	if res.Status != history.StatusNondeterministic {
		t.Fatalf("expected status NONDETERMINISTIC, got %s", res.Status)
	}

	var nondetErr *workflow.NondeterminismError
	if !errors.As(res.Error, &nondetErr) {
		t.Fatalf("expected NondeterminismError, got %T: %v", res.Error, res.Error)
	}

	// Verify WorkflowNondeterminismDetected event is stored in database
	events, _ := store.GetAllHistory(ctx, run.WorkflowID)
	foundEvent := false
	for _, ev := range events {
		if ev.EventType == history.EventWorkflowNondeterminismDetected {
			foundEvent = true
			break
		}
	}
	if !foundEvent {
		t.Errorf("expected WorkflowNondeterminismDetected event in history")
	}
}

func TestReplay_UserPanicCaught(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	reg := workflow.NewRegistry()
	replayer := workflow.NewReplayer(db, store, reg, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = reg.Register("panic_wf", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		panic("user code nil pointer exception")
	})

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "panic_wf",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "panic-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	res, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil {
		t.Fatalf("unexpected error from ReplayWorkflow: %v", err)
	}

	if res.Status != history.StatusFailed {
		t.Errorf("expected status FAILED for panic, got %s", res.Status)
	}
	if res.Suspended {
		t.Errorf("panic was incorrectly classified as replay suspension")
	}
}
