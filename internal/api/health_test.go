package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLivenessHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/health/live", nil)
	rr := httptest.NewRecorder()

	handler := LivenessHandler()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode json response: %v", err)
	}

	if resp.Status != "UP" {
		t.Errorf("expected status UP, got %s", resp.Status)
	}
}

func TestReadinessHandler_UninitializedDB(t *testing.T) {
	req := httptest.NewRequest("GET", "/health/ready", nil)
	rr := httptest.NewRecorder()

	handler := ReadinessHandler(nil)
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", rr.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode json response: %v", err)
	}

	if resp.Status != "DOWN" {
		t.Errorf("expected status DOWN, got %s", resp.Status)
	}
}
