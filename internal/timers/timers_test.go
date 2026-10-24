package timers_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"chronosaga/internal/history"
	"chronosaga/internal/timers"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func TestTimers_SingleFire_NoDoubleFire(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	_ = timers.NewScheduler(db, store, nil, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "timer_doublefire_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "time-df-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	timerID := uuid.NewString()
	pastDue := time.Now().UTC().Add(-5 * time.Second)
	_, err = db.ExecContext(ctx, `
		INSERT INTO timer_tasks (timer_id, workflow_id, command_index, duration_ms, due_at, status, created_at)
		VALUES ($1, $2, 0, 5000, $3, 'PENDING', NOW());`,
		timerID, run.WorkflowID, pastDue)
	if err != nil {
		t.Fatalf("failed inserting timer: %v", err)
	}

	// Concurrently attempt to fire the timer from 5 schedulers
	var wg sync.WaitGroup
	firedCounts := make([]int, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			s := timers.NewScheduler(db, store, nil, 100*time.Millisecond)
			n, _ := s.FireDueTimers(ctx)
			firedCounts[workerIdx] = n
		}(i)
	}

	wg.Wait()

	totalFired := 0
	for _, count := range firedCounts {
		totalFired += count
	}

	if totalFired != 1 {
		t.Fatalf("double-fire violation! Timer fired %d times across concurrent schedulers (expected 1)", totalFired)
	}

	// Verify only 1 TimerFired event exists in history
	events, err := store.GetAllHistory(ctx, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed fetching history: %v", err)
	}

	timerFiredEvents := 0
	for _, ev := range events {
		if ev.EventType == history.EventTimerFired {
			timerFiredEvents++
		}
	}

	if timerFiredEvents != 1 {
		t.Fatalf("expected exactly 1 TimerFired event, got %d", timerFiredEvents)
	}
}

func TestTimers_InjectedClock(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "clock_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "clk-" + uuid.NewString(),
	})
	scheduler := timers.NewScheduler(db, store, nil, 100*time.Millisecond)

	baseTime := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	dueTime := baseTime.Add(1 * time.Hour)

	timerID := uuid.NewString()
	_, _ = db.ExecContext(ctx, `
		INSERT INTO timer_tasks (timer_id, workflow_id, command_index, duration_ms, due_at, status, created_at)
		VALUES ($1, $2, 0, 3600000, $3, 'PENDING', $4);`,
		timerID, run.WorkflowID, dueTime, baseTime)

	// Injected clock set to baseTime (not due yet)
	scheduler.SetClock(func() time.Time { return baseTime })
	fired, err := scheduler.FireDueTimers(ctx)
	if err != nil || fired != 0 {
		t.Fatalf("timer should not fire before due time: fired=%d, err=%v", fired, err)
	}

	// Injected clock fast-forwarded to dueTime + 1 minute
	scheduler.SetClock(func() time.Time { return dueTime.Add(1 * time.Minute) })
	fired, err = scheduler.FireDueTimers(ctx)
	if err != nil || fired != 1 {
		t.Fatalf("timer should fire when clock advanced: fired=%d, err=%v", fired, err)
	}
}

func TestTimers_RestartRecovery_EndToEnd(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	_ = timers.NewScheduler(db, store, nil, 100*time.Millisecond)
	reg := workflow.NewRegistry()
	replayer := workflow.NewReplayer(db, store, reg, nil)
	scheduler := timers.NewScheduler(db, store, nil, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Register workflow with durable Sleep
	_ = reg.Register("timer_wf", "v1", func(wctx *workflow.WorkflowContext, input json.RawMessage) (json.RawMessage, error) {
		if err := wctx.Sleep(200 * time.Millisecond); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"resumed": "true"})
	})

	// 1. Start workflow
	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "timer_wf",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "recov-timer-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed starting run: %v", err)
	}

	// 2. First replay: reaches Sleep, persists TimerStarted and timer row, suspends!
	res1, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil || !res1.Suspended {
		t.Fatalf("expected res1 to suspend on Sleep: %v (suspended=%v)", err, res1.Suspended)
	}

	// 3. Process is "dead" / offline. Wait 250ms so timer matures while offline.
	time.Sleep(250 * time.Millisecond)

	// 4. "Restart" scheduler. Discover and fire overdue timer.
	fired, err := scheduler.FireDueTimers(ctx)
	if err != nil || fired != 1 {
		t.Fatalf("expected restarted scheduler to fire 1 overdue timer, got %d (err: %v)", fired, err)
	}

	// 5. Replay workflow again: Sleep discovers TimerFired and completes!
	res2, err := replayer.ReplayWorkflow(ctx, run)
	if err != nil || res2.Suspended {
		t.Fatalf("expected workflow to finish, got suspended=%v, err=%v", res2.Suspended, err)
	}

	if res2.Status != history.StatusCompleted {
		t.Errorf("expected COMPLETED, got %s", res2.Status)
	}

	var out map[string]string
	_ = json.Unmarshal(res2.Output, &out)
	if out["resumed"] != "true" {
		t.Errorf("expected resumed=true, got %v", out)
	}
}
