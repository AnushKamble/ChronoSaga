package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"chronosaga/internal/persistence"
)

// HealthResponse represents standard health check response.
type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

// LivenessHandler handles /health/live.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:    "UP",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}
}

// ReadinessHandler handles /health/ready by actively querying PostgreSQL.
func ReadinessHandler(db *persistence.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		checks := make(map[string]string)

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if db == nil {
			checks["postgres"] = "DOWN: uninitialized"
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(HealthResponse{
				Status:    "DOWN",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Checks:    checks,
			})
			return
		}

		if err := db.PingCheck(ctx); err != nil {
			checks["postgres"] = "DOWN: " + err.Error()
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(HealthResponse{
				Status:    "DOWN",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Checks:    checks,
			})
			return
		}

		checks["postgres"] = "UP"
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthResponse{
			Status:    "UP",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Checks:    checks,
		})
	}
}
