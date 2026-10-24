package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"chronosaga/internal/config"
	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
	"chronosaga/internal/saga"
	"chronosaga/internal/workflow"
)

// Server encapsulates the HTTP API server.
type Server struct {
	httpServer *http.Server
	db         *persistence.DB
	cfg        *config.Config
	mux        *http.ServeMux
}

// NewServer initializes a new Server instance with all routes registered.
func NewServer(
	cfg *config.Config,
	db *persistence.DB,
	store *history.Store,
	reg *workflow.Registry,
	coordinator *saga.Coordinator,
) *Server {
	mux := http.NewServeMux()

	// Operational endpoints
	mux.HandleFunc("GET /health/live", LivenessHandler())
	mux.HandleFunc("GET /health/ready", ReadinessHandler(db))
	mux.HandleFunc("GET /metrics", observability.DefaultMetrics.Handler())

	// Workflow endpoints
	mux.HandleFunc("POST /api/v1/workflows", StartWorkflowHandler(store, reg))
	mux.HandleFunc("GET /api/v1/workflows/{id}", GetWorkflowStatusHandler(store))
	mux.HandleFunc("GET /api/v1/workflows/{id}/events", GetWorkflowEventsHandler(store))
	mux.HandleFunc("POST /api/v1/workflows/{id}/retry-compensation", RetryCompensationHandler(coordinator, cfg.AdminAPIKey))

	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &Server{
		httpServer: srv,
		db:         db,
		cfg:        cfg,
		mux:        mux,
	}
}

// Router returns the underlying ServeMux.
func (s *Server) Router() *http.ServeMux {
	return s.mux
}

// Start begins listening and serving HTTP requests.
func (s *Server) Start() error {
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

// Shutdown gracefully terminates the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
