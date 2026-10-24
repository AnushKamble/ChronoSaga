package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"chronosaga/internal/api"
	"chronosaga/internal/config"
	"chronosaga/internal/demo"
	"chronosaga/internal/history"
	"chronosaga/internal/saga"
	"chronosaga/internal/workflow"
	"chronosaga/tests/testutil"
	"github.com/google/uuid"
)

func setupTestServer(t *testing.T) (*api.Server, *history.Store, *saga.Coordinator) {
	t.Helper()
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)
	store := history.NewStore(db, nil)
	reg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(reg)
	coord := saga.NewCoordinator(db, store, nil, nil)
	cfg := &config.Config{
		Host: "127.0.0.1",
		Port: "8080",
	}

	server := api.NewServer(cfg, db, store, reg, coord)
	return server, store, coord
}

func TestAPI_StartWorkflow_And_Status(t *testing.T) {
	server, _, _ := setupTestServer(t)

	// 1. Start a new workflow
	idempKey := "order-api-" + uuid.NewString()
	body := map[string]any{
		"workflow_type":    "order_fulfillment",
		"workflow_version": "v1",
		"input": map[string]any{
			"order_id":    "ord-500",
			"sku":         "SKU-A",
			"quantity":    2,
			"total_cents": 2999,
		},
		"idempotency_key": idempKey,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	server.Router().ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201 Created, got %d (body: %s)", rr.Code, rr.Body.String())
	}

	var startResp api.StartWorkflowResponse
	if err := json.NewDecoder(rr.Body).Decode(&startResp); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}

	if startResp.WorkflowID == "" {
		t.Errorf("expected non-empty workflow_id")
	}
	if startResp.Status != "RUNNING" {
		t.Errorf("expected status RUNNING, got %s", startResp.Status)
	}

	// 2. Duplicate start with same idempotency key -> must return 200 OK and same workflow_id
	reqDup := httptest.NewRequest("POST", "/api/v1/workflows", bytes.NewReader(bodyBytes))
	reqDup.Header.Set("Content-Type", "application/json")
	rrDup := httptest.NewRecorder()

	server.Router().ServeHTTP(rrDup, reqDup)

	if rrDup.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK for duplicate start, got %d", rrDup.Code)
	}

	var dupResp api.StartWorkflowResponse
	_ = json.NewDecoder(rrDup.Body).Decode(&dupResp)
	if dupResp.WorkflowID != startResp.WorkflowID {
		t.Errorf("expected same workflow ID %s, got %s", startResp.WorkflowID, dupResp.WorkflowID)
	}
	if !dupResp.IsDuplicate {
		t.Errorf("expected IsDuplicate=true")
	}

	// 3. Query workflow status
	reqStatus := httptest.NewRequest("GET", "/api/v1/workflows/"+startResp.WorkflowID, nil)
	rrStatus := httptest.NewRecorder()

	server.Router().ServeHTTP(rrStatus, reqStatus)

	if rrStatus.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for status, got %d", rrStatus.Code)
	}

	var runResp history.WorkflowRun
	_ = json.NewDecoder(rrStatus.Body).Decode(&runResp)
	if runResp.WorkflowID != startResp.WorkflowID {
		t.Errorf("expected workflow ID %s, got %s", startResp.WorkflowID, runResp.WorkflowID)
	}

	// 4. Query events
	reqEvents := httptest.NewRequest("GET", "/api/v1/workflows/"+startResp.WorkflowID+"/events", nil)
	rrEvents := httptest.NewRecorder()

	server.Router().ServeHTTP(rrEvents, reqEvents)

	if rrEvents.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for events, got %d", rrEvents.Code)
	}

	var eventsResp api.GetWorkflowEventsResponse
	_ = json.NewDecoder(rrEvents.Body).Decode(&eventsResp)
	if len(eventsResp.Events) < 1 {
		t.Fatalf("expected at least 1 event (WorkflowStarted), got %d", len(eventsResp.Events))
	}
	if eventsResp.Events[0].EventType != history.EventWorkflowStarted {
		t.Errorf("expected first event WorkflowStarted, got %s", eventsResp.Events[0].EventType)
	}
}

func TestAPI_RetryCompensation_InvalidState(t *testing.T) {
	server, store, _ := setupTestServer(t)

	// Create running workflow
	run, _, _ := store.CreateWorkflowRun(context.Background(), history.StartWorkflowParams{
		WorkflowType:    "order_fulfillment",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "comp-test-" + uuid.NewString(),
	})

	// Try to retry compensation on a RUNNING workflow (not COMPENSATION_FAILED)
	req := httptest.NewRequest("POST", "/api/v1/workflows/"+run.WorkflowID+"/retry-compensation", nil)
	rr := httptest.NewRecorder()

	server.Router().ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when not in COMPENSATION_FAILED status, got %d (body: %s)", rr.Code, rr.Body.String())
	}

	var errResp api.ErrorResponse
	_ = json.NewDecoder(rr.Body).Decode(&errResp)
	if errResp.ErrorCode != "INVALID_STATE" {
		t.Errorf("expected error code INVALID_STATE, got %s", errResp.ErrorCode)
	}
}
