package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrWorkflowAlreadyRegistered = errors.New("workflow already registered")
	ErrWorkflowNotFound          = errors.New("workflow definition not found")
)

// WorkflowFunc represents the deterministic workflow entrypoint function.
type WorkflowFunc func(ctx *WorkflowContext, input json.RawMessage) (json.RawMessage, error)

// Registry manages registered workflow functions pinned by type and version.
type Registry struct {
	mu          sync.RWMutex
	definitions map[string]WorkflowFunc
}

// Global registry
var DefaultRegistry = NewRegistry()

// NewRegistry instantiates a new workflow registry.
func NewRegistry() *Registry {
	return &Registry{
		definitions: make(map[string]WorkflowFunc),
	}
}

func key(wfType, wfVersion string) string {
	return fmt.Sprintf("%s:%s", wfType, wfVersion)
}

// Register adds a workflow function to the registry.
func (r *Registry) Register(wfType, wfVersion string, fn WorkflowFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := key(wfType, wfVersion)
	if _, exists := r.definitions[k]; exists {
		return fmt.Errorf("%w: %s", ErrWorkflowAlreadyRegistered, k)
	}

	r.definitions[k] = fn
	return nil
}

// Get retrieves a workflow function by type and version.
func (r *Registry) Get(wfType, wfVersion string) (WorkflowFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	k := key(wfType, wfVersion)
	fn, exists := r.definitions[k]
	if !exists {
		return nil, fmt.Errorf("%w: type=%s, version=%s", ErrWorkflowNotFound, wfType, wfVersion)
	}
	return fn, nil
}
