package activities

import (
	"fmt"
	"sync"
)

// Registry maps activity names to executable handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]ActivityHandler
}

// Global default registry
var DefaultRegistry = NewRegistry()

// NewRegistry creates a new activity registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]ActivityHandler),
	}
}

// Register adds an activity handler to the registry.
func (r *Registry) Register(name string, handler ActivityHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("activity handler '%s' is already registered", name)
	}
	r.handlers[name] = handler
	return nil
}

// Get retrieves an activity handler by name.
func (r *Registry) Get(name string) (ActivityHandler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[name]
	if !exists {
		return nil, fmt.Errorf("%w: '%s'", ErrHandlerNotFound, name)
	}
	return handler, nil
}
