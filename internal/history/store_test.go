package history_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"chronosaga/internal/history"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func TestStore_StartWorkflow_Idempotency(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	params := history.StartWorkflowParams{
		WorkflowType:     "order_fulfillment",
		WorkflowVersion:  "v1",
		Input:            json.RawMessage(`{"order_id":"ord-101","total":100}`),
		IdempotencyScope: "test-scope",
		IdempotencyKey:   "key-start-101",
	}

	// 1. First start
	run1, isExisting1, err := store.CreateWorkflowRun(ctx, params)
	if err != nil {
		t.Fatalf("first CreateWorkflowRun failed: %v", err)
	}
	if isExisting1 {
		t.Errorf("expected isExisting=false for first call")
	}
	if run1.LastEventSequence != 1 {
		t.Errorf("expected LastEventSequence=1, got %d", run1.LastEventSequence)
	}

	// 2. Second start with identical key
	run2, isExisting2, err := store.CreateWorkflowRun(ctx, params)
	if err != nil {
		t.Fatalf("second CreateWorkflowRun failed: %v", err)
	}
	if !isExisting2 {
		t.Errorf("expected isExisting=true for duplicate key")
	}
	if run1.WorkflowID != run2.WorkflowID {
		t.Errorf("expected same workflow ID, got %s and %s", run1.WorkflowID, run2.WorkflowID)
	}

	// 3. Verify exactly 1 WorkflowStarted event in history
	events, err := store.GetAllHistory(ctx, run1.WorkflowID)
	if err != nil {
		t.Fatalf("failed loading history: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 event, got %d", len(events))
	}
	if events[0].EventType != history.EventWorkflowStarted {
		t.Errorf("expected event WorkflowStarted, got %s", events[0].EventType)
	}
}

func TestStore_ConcurrentAppends_StrictContiguity(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	params := history.StartWorkflowParams{
		WorkflowType:    "concurrency_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "conc-key-" + uuid.NewString(),
	}

	run, _, err := store.CreateWorkflowRun(ctx, params)
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	// Concurrently append 20 events across 5 goroutines
	concurrency := 5
	appendsPerWorker := 4
	totalAppends := concurrency * appendsPerWorker

	var wg sync.WaitGroup
	errCh := make(chan error, totalAppends)

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < appendsPerWorker; i++ {
				appendErr := db.WithTx(ctx, func(tx *sql.Tx) error {
					cmdIdx := workerID*100 + i
					payload := map[string]any{"worker": workerID, "step": i}
					_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityScheduled, &cmdIdx, payload)
					return err
				})
				if appendErr != nil {
					errCh <- appendErr
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent append error: %v", err)
	}

	// Verify all events are present and sequences are contiguous: 1, 2, ..., totalAppends+1
	events, err := store.GetAllHistory(ctx, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed reading history: %v", err)
	}

	expectedCount := totalAppends + 1 // including WorkflowStarted (seq 1)
	if len(events) != expectedCount {
		t.Fatalf("expected %d events, got %d", expectedCount, len(events))
	}

	for idx, ev := range events {
		expectedSeq := int64(idx + 1)
		if ev.SequenceNo != expectedSeq {
			t.Fatalf("sequence mismatch at index %d: expected %d, got %d", idx, expectedSeq, ev.SequenceNo)
		}
	}
}

func TestStore_TransactionRollback(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "rollback_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "rb-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed creating run: %v", err)
	}

	simulatedErr := errors.New("simulated error forcing rollback")
	err = db.WithTx(ctx, func(tx *sql.Tx) error {
		cmd := 0
		_, appendErr := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityScheduled, &cmd, map[string]string{"foo": "bar"})
		if appendErr != nil {
			return appendErr
		}
		return simulatedErr
	})

	if !errors.Is(err, simulatedErr) {
		t.Fatalf("expected simulatedErr, got %v", err)
	}

	// Verify only sequence 1 exists
	events, err := store.GetAllHistory(ctx, run.WorkflowID)
	if err != nil {
		t.Fatalf("failed fetching history: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event after rollback, got %d", len(events))
	}
	if events[0].SequenceNo != 1 {
		t.Errorf("expected sequence 1, got %d", events[0].SequenceNo)
	}
}

func TestStore_PaginationAndOrdering(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "page_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "page-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed creating run: %v", err)
	}

	// Append 5 additional events (total 6 events: seq 1..6)
	for i := 1; i <= 5; i++ {
		cmd := i
		err = db.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityScheduled, &cmd, map[string]int{"i": i})
			return err
		})
		if err != nil {
			t.Fatalf("failed appending event %d: %v", i, err)
		}
	}

	// Page 1: limit 2, after 0 -> seq 1, 2, hasMore = true
	p1, hasMore1, err := store.GetHistory(ctx, run.WorkflowID, 0, 2)
	if err != nil || len(p1) != 2 || !hasMore1 {
		t.Fatalf("page 1 failed: len=%d, hasMore=%v, err=%v", len(p1), hasMore1, err)
	}
	if p1[0].SequenceNo != 1 || p1[1].SequenceNo != 2 {
		t.Errorf("expected seq 1,2, got %d,%d", p1[0].SequenceNo, p1[1].SequenceNo)
	}

	// Page 2: limit 2, after 2 -> seq 3, 4, hasMore = true
	p2, hasMore2, err := store.GetHistory(ctx, run.WorkflowID, 2, 2)
	if err != nil || len(p2) != 2 || !hasMore2 {
		t.Fatalf("page 2 failed: len=%d, hasMore=%v, err=%v", len(p2), hasMore2, err)
	}
	if p2[0].SequenceNo != 3 || p2[1].SequenceNo != 4 {
		t.Errorf("expected seq 3,4, got %d,%d", p2[0].SequenceNo, p2[1].SequenceNo)
	}

	// Page 3: limit 2, after 4 -> seq 5, 6, hasMore = false
	p3, hasMore3, err := store.GetHistory(ctx, run.WorkflowID, 4, 2)
	if err != nil || len(p3) != 2 || hasMore3 {
		t.Fatalf("page 3 failed: len=%d, hasMore=%v, err=%v", len(p3), hasMore3, err)
	}
	if p3[0].SequenceNo != 5 || p3[1].SequenceNo != 6 {
		t.Errorf("expected seq 5,6, got %d,%d", p3[0].SequenceNo, p3[1].SequenceNo)
	}
}

func TestStore_RebuildProjection(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "rebuild_test",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{"item":"book"}`),
		IdempotencyKey:  "rb-proj-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("failed creating run: %v", err)
	}

	// Simulate workflow progress events
	cmd0 := 0
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityScheduled, &cmd0, map[string]string{"type": "reserve"})
		return err
	})
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventActivityCompleted, &cmd0, map[string]string{"res_id": "r1"})
		return err
	})
	_ = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := store.AppendEventInTx(ctx, tx, run.WorkflowID, history.EventWorkflowCompleted, nil, history.WorkflowCompletedPayload{
			Output: json.RawMessage(`{"status":"success"}`),
		})
		return err
	})

	// Rebuild projection from event log
	rebuilt, err := store.RebuildProjectionFromHistory(ctx, run.WorkflowID)
	if err != nil {
		t.Fatalf("RebuildProjectionFromHistory failed: %v", err)
	}

	if rebuilt.Status != history.StatusCompleted {
		t.Errorf("expected status %s, got %s", history.StatusCompleted, rebuilt.Status)
	}
	if rebuilt.LastEventSequence != 4 {
		t.Errorf("expected last sequence 4, got %d", rebuilt.LastEventSequence)
	}
	var outputMap map[string]string
	if err := json.Unmarshal(rebuilt.OutputJSON, &outputMap); err != nil {
		t.Fatalf("failed unmarshaling rebuilt output: %v", err)
	}
	if outputMap["status"] != "success" {
		t.Errorf("expected status success in output, got %v", outputMap)
	}
}
