package workflow

import (
	"errors"
	"fmt"
)

// Private sentinel error used solely inside replay context to suspend execution cleanly.
var errSuspended = errors.New("CHRONOSAGA_PRIVATE_REPLAY_SUSPENDED")

// IsSuspended checks whether the error is the private replay suspension sentinel.
func IsSuspended(err error) bool {
	return errors.Is(err, errSuspended)
}

// ActivityError represents an activity execution failure returned to workflow code.
type ActivityError struct {
	ErrorCode    string
	ErrorMessage string
	Terminal     bool
}

func (e *ActivityError) Error() string {
	return fmt.Sprintf("activity error [%s]: %s (terminal=%v)", e.ErrorCode, e.ErrorMessage, e.Terminal)
}

// NewActivityError creates a typed activity error.
func NewActivityError(code, message string, terminal bool) *ActivityError {
	return &ActivityError{
		ErrorCode:    code,
		ErrorMessage: message,
		Terminal:     terminal,
	}
}

// NondeterminismError is returned when a replayed command deviates from recorded history.
type NondeterminismError struct {
	CommandIndex        int
	ExpectedCommandType string
	ExpectedName        string
	ExpectedHash        string
	ActualCommandType   string
	ActualName          string
	ActualHash          string
	Message             string
}

func (e *NondeterminismError) Error() string {
	return fmt.Sprintf("nondeterminism detected at command index %d: expected %s (%s, hash=%s) but got %s (%s, hash=%s): %s",
		e.CommandIndex, e.ExpectedCommandType, e.ExpectedName, e.ExpectedHash, e.ActualCommandType, e.ActualName, e.ActualHash, e.Message)
}

// UserPanicError represents an uncaught panic inside user workflow code.
type UserPanicError struct {
	Value any
}

func (e *UserPanicError) Error() string {
	return fmt.Sprintf("workflow panicked: %v", e.Value)
}
