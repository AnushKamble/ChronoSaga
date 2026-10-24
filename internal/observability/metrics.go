package observability

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

// Metrics holds the counters and gauges for ChronoSaga.
type Metrics struct {
	mu                          sync.RWMutex
	WorkflowsStarted            uint64
	WorkflowsCompleted          uint64
	WorkflowsFailed             uint64
	WorkflowsCompensated        uint64
	CompensationFailures        uint64
	TaskLeaseReclaims           uint64
	TaskQueuePending            int64
	WorkflowHistoryEvents       uint64
	ActivityAttempts            map[string]map[string]*uint64 // [activity_type][outcome] -> count
	ActivityDurationSumMs       map[string]*uint64
	ActivityDurationCount       map[string]*uint64
	WorkflowReplayDurationSumMs uint64
	WorkflowReplayCount         uint64
	TimerLatenessSumMs          uint64
	TimerLatenessCount          uint64
}

// Global registry
var DefaultMetrics = NewMetrics()

// NewMetrics instantiates a metrics container.
func NewMetrics() *Metrics {
	return &Metrics{
		ActivityAttempts:      make(map[string]map[string]*uint64),
		ActivityDurationSumMs: make(map[string]*uint64),
		ActivityDurationCount: make(map[string]*uint64),
	}
}

// IncWorkflowsStarted increments started workflows counter.
func (m *Metrics) IncWorkflowsStarted() {
	atomic.AddUint64(&m.WorkflowsStarted, 1)
}

// IncWorkflowsCompleted increments completed workflows counter.
func (m *Metrics) IncWorkflowsCompleted() {
	atomic.AddUint64(&m.WorkflowsCompleted, 1)
}

// IncWorkflowsFailed increments failed workflows counter.
func (m *Metrics) IncWorkflowsFailed() {
	atomic.AddUint64(&m.WorkflowsFailed, 1)
}

// IncWorkflowsCompensated increments compensated workflows counter.
func (m *Metrics) IncWorkflowsCompensated() {
	atomic.AddUint64(&m.WorkflowsCompensated, 1)
}

// IncCompensationFailures increments terminal compensation failures.
func (m *Metrics) IncCompensationFailures() {
	atomic.AddUint64(&m.CompensationFailures, 1)
}

// IncTaskLeaseReclaims increments lease reclaims counter.
func (m *Metrics) IncTaskLeaseReclaims() {
	atomic.AddUint64(&m.TaskLeaseReclaims, 1)
}

// SetTaskQueuePending sets the gauge for pending tasks.
func (m *Metrics) SetTaskQueuePending(val int64) {
	atomic.StoreInt64(&m.TaskQueuePending, val)
}

// IncWorkflowHistoryEvents records appended events.
func (m *Metrics) IncWorkflowHistoryEvents(count uint64) {
	atomic.AddUint64(&m.WorkflowHistoryEvents, count)
}

// RecordActivityAttempt records an activity attempt with type and outcome (e.g. success, failure).
func (m *Metrics) RecordActivityAttempt(activityType, outcome string) {
	m.mu.Lock()
	if _, ok := m.ActivityAttempts[activityType]; !ok {
		m.ActivityAttempts[activityType] = make(map[string]*uint64)
	}
	if _, ok := m.ActivityAttempts[activityType][outcome]; !ok {
		var zero uint64
		m.ActivityAttempts[activityType][outcome] = &zero
	}
	ptr := m.ActivityAttempts[activityType][outcome]
	m.mu.Unlock()

	atomic.AddUint64(ptr, 1)
}

// RecordActivityDuration records activity latency in milliseconds.
func (m *Metrics) RecordActivityDuration(activityType string, durationMs int64) {
	if durationMs < 0 {
		durationMs = 0
	}
	m.mu.Lock()
	if _, ok := m.ActivityDurationSumMs[activityType]; !ok {
		var zeroSum uint64
		var zeroCount uint64
		m.ActivityDurationSumMs[activityType] = &zeroSum
		m.ActivityDurationCount[activityType] = &zeroCount
	}
	sumPtr := m.ActivityDurationSumMs[activityType]
	countPtr := m.ActivityDurationCount[activityType]
	m.mu.Unlock()

	atomic.AddUint64(sumPtr, uint64(durationMs))
	atomic.AddUint64(countPtr, 1)
}

// RecordWorkflowReplayDuration records replay time in milliseconds.
func (m *Metrics) RecordWorkflowReplayDuration(durationMs int64) {
	if durationMs < 0 {
		durationMs = 0
	}
	atomic.AddUint64(&m.WorkflowReplayDurationSumMs, uint64(durationMs))
	atomic.AddUint64(&m.WorkflowReplayCount, 1)
}

// RecordTimerLateness records the delay between timer due time and actual fire time in milliseconds.
func (m *Metrics) RecordTimerLateness(latenessMs int64) {
	if latenessMs < 0 {
		latenessMs = 0
	}
	atomic.AddUint64(&m.TimerLatenessSumMs, uint64(latenessMs))
	atomic.AddUint64(&m.TimerLatenessCount, 1)
}

// Handler returns an HTTP handler serving Prometheus-formatted metrics text.
func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		fmt.Fprintf(w, "# HELP chronosaga_workflows_started_total Total number of workflows started.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_workflows_started_total counter\n")
		fmt.Fprintf(w, "chronosaga_workflows_started_total %d\n\n", atomic.LoadUint64(&m.WorkflowsStarted))

		fmt.Fprintf(w, "# HELP chronosaga_workflows_completed_total Total number of workflows completed successfully.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_workflows_completed_total counter\n")
		fmt.Fprintf(w, "chronosaga_workflows_completed_total %d\n\n", atomic.LoadUint64(&m.WorkflowsCompleted))

		fmt.Fprintf(w, "# HELP chronosaga_workflows_failed_total Total number of workflows failed.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_workflows_failed_total counter\n")
		fmt.Fprintf(w, "chronosaga_workflows_failed_total %d\n\n", atomic.LoadUint64(&m.WorkflowsFailed))

		fmt.Fprintf(w, "# HELP chronosaga_workflows_compensated_total Total number of workflows compensated successfully.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_workflows_compensated_total counter\n")
		fmt.Fprintf(w, "chronosaga_workflows_compensated_total %d\n\n", atomic.LoadUint64(&m.WorkflowsCompensated))

		fmt.Fprintf(w, "# HELP chronosaga_compensation_failures_total Total number of permanent compensation failures.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_compensation_failures_total counter\n")
		fmt.Fprintf(w, "chronosaga_compensation_failures_total %d\n\n", atomic.LoadUint64(&m.CompensationFailures))

		fmt.Fprintf(w, "# HELP chronosaga_task_lease_reclaims_total Total number of expired task leases reclaimed.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_task_lease_reclaims_total counter\n")
		fmt.Fprintf(w, "chronosaga_task_lease_reclaims_total %d\n\n", atomic.LoadUint64(&m.TaskLeaseReclaims))

		fmt.Fprintf(w, "# HELP chronosaga_task_queue_pending Current number of pending activity tasks.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_task_queue_pending gauge\n")
		fmt.Fprintf(w, "chronosaga_task_queue_pending %d\n\n", atomic.LoadInt64(&m.TaskQueuePending))

		fmt.Fprintf(w, "# HELP chronosaga_workflow_history_events Total number of workflow history events persisted.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_workflow_history_events counter\n")
		fmt.Fprintf(w, "chronosaga_workflow_history_events %d\n\n", atomic.LoadUint64(&m.WorkflowHistoryEvents))

		m.mu.RLock()
		defer m.mu.RUnlock()

		fmt.Fprintf(w, "# HELP chronosaga_activity_attempts_total Total attempts by activity type and outcome.\n")
		fmt.Fprintf(w, "# TYPE chronosaga_activity_attempts_total counter\n")
		for actType, outcomes := range m.ActivityAttempts {
			for outcome, ptr := range outcomes {
				fmt.Fprintf(w, "chronosaga_activity_attempts_total{activity_type=\"%s\",outcome=\"%s\"} %d\n", actType, outcome, atomic.LoadUint64(ptr))
			}
		}
	}
}
