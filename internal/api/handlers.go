package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"chronosaga/internal/history"
	"chronosaga/internal/saga"
	"chronosaga/internal/workflow"
)

// Standard API Error response envelope
type ErrorResponse struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, val any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(val)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{
		ErrorCode: code,
		Message:   msg,
	})
}

// StartWorkflowRequest defines the incoming payload for POST /api/v1/workflows.
type StartWorkflowRequest struct {
	WorkflowType    string          `json:"workflow_type"`
	WorkflowVersion string          `json:"workflow_version"`
	Input           json.RawMessage `json:"input"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
}

// StartWorkflowResponse defines the response for POST /api/v1/workflows.
type StartWorkflowResponse struct {
	WorkflowID      string `json:"workflow_id"`
	Status          string `json:"status"`
	WorkflowType    string `json:"workflow_type"`
	WorkflowVersion string `json:"workflow_version"`
	IsDuplicate     bool   `json:"is_duplicate,omitempty"`
}

// StartWorkflowHandler handles POST /api/v1/workflows.
func StartWorkflowHandler(store *history.Store, reg *workflow.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Limit request body size to 1MB
		r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)

		var req StartWorkflowRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST_BODY", "failed to parse JSON body: "+err.Error())
			return
		}

		if strings.TrimSpace(req.WorkflowType) == "" {
			writeError(w, http.StatusBadRequest, "MISSING_WORKFLOW_TYPE", "workflow_type is required")
			return
		}
		if len(req.WorkflowType) > 128 {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_TYPE", "workflow_type cannot exceed 128 characters")
			return
		}
		if strings.TrimSpace(req.WorkflowVersion) == "" {
			req.WorkflowVersion = "v1"
		}
		if len(req.WorkflowVersion) > 64 {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_VERSION", "workflow_version cannot exceed 64 characters")
			return
		}

		// Idempotency key from header or body
		idempKey := r.Header.Get("Idempotency-Key")
		if idempKey == "" {
			idempKey = req.IdempotencyKey
		}
		if strings.TrimSpace(idempKey) == "" {
			writeError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key header or idempotency_key field is required")
			return
		}
		if len(idempKey) > 255 {
			writeError(w, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "idempotency_key cannot exceed 255 characters")
			return
		}

		// Verify workflow definition is registered
		if _, err := reg.Get(req.WorkflowType, req.WorkflowVersion); err != nil {
			writeError(w, http.StatusBadRequest, "UNKNOWN_WORKFLOW_DEFINITION", err.Error())
			return
		}

		if len(req.Input) == 0 {
			req.Input = json.RawMessage(`{}`)
		}

		run, isExisting, err := store.CreateWorkflowRun(r.Context(), history.StartWorkflowParams{
			WorkflowType:     req.WorkflowType,
			WorkflowVersion:  req.WorkflowVersion,
			Input:            req.Input,
			IdempotencyScope: "default",
			IdempotencyKey:   idempKey,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed creating workflow run: "+err.Error())
			return
		}

		statusCode := http.StatusCreated
		if isExisting {
			statusCode = http.StatusOK
		}

		writeJSON(w, statusCode, StartWorkflowResponse{
			WorkflowID:      run.WorkflowID,
			Status:          run.Status,
			WorkflowType:    run.WorkflowType,
			WorkflowVersion: run.WorkflowVersion,
			IsDuplicate:     isExisting,
		})
	}
}

// GetWorkflowStatusHandler handles GET /api/v1/workflows/{id}.
func GetWorkflowStatusHandler(store *history.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID is required")
			return
		}
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID must be a valid UUID")
			return
		}

		run, err := store.GetWorkflowRun(r.Context(), id)
		if err != nil {
			if errors.Is(err, history.ErrWorkflowNotFound) {
				writeError(w, http.StatusNotFound, "WORKFLOW_NOT_FOUND", "workflow run not found")
				return
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}

		writeJSON(w, http.StatusOK, run)
	}
}

// GetWorkflowEventsResponse defines the envelope for GET /api/v1/workflows/{id}/events.
type GetWorkflowEventsResponse struct {
	WorkflowID   string          `json:"workflow_id"`
	Events       []history.Event `json:"events"`
	HasMore      bool            `json:"has_more"`
	LastSequence int64           `json:"last_sequence"`
}

// GetWorkflowEventsHandler handles GET /api/v1/workflows/{id}/events.
func GetWorkflowEventsHandler(store *history.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID is required")
			return
		}
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID must be a valid UUID")
			return
		}

		afterSeq := int64(0)
		if s := r.URL.Query().Get("after_sequence"); s != "" {
			if parsed, err := strconv.ParseInt(s, 10, 64); err == nil {
				afterSeq = parsed
			}
		}

		limit := 100
		if l := r.URL.Query().Get("limit"); l != "" {
			if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 500 {
				limit = parsed
			}
		}

		events, hasMore, err := store.GetHistory(r.Context(), id, afterSeq, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed querying events: "+err.Error())
			return
		}

		var lastSeq int64 = afterSeq
		if len(events) > 0 {
			lastSeq = events[len(events)-1].SequenceNo
		}

		writeJSON(w, http.StatusOK, GetWorkflowEventsResponse{
			WorkflowID:   id,
			Events:       events,
			HasMore:      hasMore,
			LastSequence: lastSeq,
		})
	}
}

// RetryCompensationHandler handles POST /api/v1/workflows/{id}/retry-compensation.
func RetryCompensationHandler(coordinator *saga.Coordinator, adminKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Optional admin key verification
		if adminKey != "" {
			key := r.Header.Get("X-API-Key")
			if key != adminKey {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "valid X-API-Key required")
				return
			}
		}

		id := r.PathValue("id")
		if id == "" {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID is required")
			return
		}
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_WORKFLOW_ID", "workflow ID must be a valid UUID")
			return
		}

		operator := r.Header.Get("X-Operator-Name")
		if operator == "" {
			operator = "api-operator"
		}

		err := coordinator.RetryFailedCompensation(r.Context(), id, operator)
		if err != nil {
			if errors.Is(err, saga.ErrWorkflowNotCompensating) {
				writeError(w, http.StatusConflict, "INVALID_STATE", err.Error())
				return
			}
			if errors.Is(err, saga.ErrNoFailedCompensation) {
				writeError(w, http.StatusBadRequest, "NO_FAILED_COMPENSATION", err.Error())
				return
			}
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{
			"workflow_id": id,
			"status":      history.StatusCompensating,
			"message":     "compensation retry scheduled",
		})
	}
}

// DiscardBodyHelper safely reads and closes leftover body.
func DiscardBodyHelper(r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
	}
}
