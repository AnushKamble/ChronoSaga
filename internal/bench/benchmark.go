package bench

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/demo"
	"chronosaga/internal/history"
	"chronosaga/internal/persistence"
	"chronosaga/internal/timers"
	"chronosaga/internal/workflow"
	"github.com/google/uuid"
)

// EnvironmentManifest records the machine and software environment details.
type EnvironmentManifest struct {
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	GoVersion   string `json:"go_version"`
	NumCPU      int    `json:"num_cpu"`
	Database    string `json:"database"`
	Timestamp   string `json:"timestamp"`
	Concurrency int    `json:"concurrency"`
}

// LatencyPercentiles stores computed p50, p95, p99.
type LatencyPercentiles struct {
	Count int64   `json:"count"`
	MinMs float64 `json:"min_ms"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
}

// BenchmarkReport encapsulates all measured benchmarks.
type BenchmarkReport struct {
	Manifest                 EnvironmentManifest `json:"manifest"`
	WorkflowStartThroughput  float64             `json:"workflow_start_throughput_ops_sec"`
	WorkflowStartLatency     LatencyPercentiles  `json:"workflow_start_latency"`
	ReplayLatencyByEvents    map[string]float64  `json:"replay_latency_by_events_ms"`
	TaskCompletionThroughput float64             `json:"task_completion_throughput_ops_sec"`
	AverageTimerLatenessMs   float64             `json:"average_timer_lateness_ms"`
	ProcessRecoveryTimeMs    float64             `json:"process_recovery_time_ms"`
}

func calcPercentiles(durations []time.Duration) LatencyPercentiles {
	if len(durations) == 0 {
		return LatencyPercentiles{}
	}
	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	toMs := func(d time.Duration) float64 {
		return float64(d.Microseconds()) / 1000.0
	}

	n := len(durations)
	p50Idx := int(float64(n) * 0.50)
	p95Idx := int(float64(n) * 0.95)
	p99Idx := int(float64(n) * 0.99)
	if p95Idx >= n {
		p95Idx = n - 1
	}
	if p99Idx >= n {
		p99Idx = n - 1
	}

	return LatencyPercentiles{
		Count: int64(n),
		MinMs: toMs(durations[0]),
		P50Ms: toMs(durations[p50Idx]),
		P95Ms: toMs(durations[p95Idx]),
		P99Ms: toMs(durations[p99Idx]),
		MaxMs: toMs(durations[n-1]),
	}
}

// RunBenchmarks runs the full benchmark protocol against the provided PostgreSQL database.
func RunBenchmarks(ctx context.Context, db *persistence.DB, resultsDir string) (*BenchmarkReport, error) {
	if resultsDir == "" {
		resultsDir = "results"
	}
	_ = os.MkdirAll(resultsDir, 0755)

	manifest := EnvironmentManifest{
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		GoVersion:   runtime.Version(),
		NumCPU:      runtime.NumCPU(),
		Database:    "PostgreSQL 18 (Local/Embedded)",
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Concurrency: runtime.NumCPU(),
	}

	store := history.NewStore(db, nil)
	effectStore := demo.NewEffectStore(db)
	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)
	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	replayer := workflow.NewReplayer(db, store, wfReg, nil)
	queue := activities.NewTaskQueue(db, store, nil)
	timerSched := timers.NewScheduler(db, store, nil, 20*time.Millisecond)

	report := &BenchmarkReport{
		Manifest:              manifest,
		ReplayLatencyByEvents: make(map[string]float64),
	}

	// 1. Workflow Start Throughput & Latency
	numStarts := 50
	var startDurations []time.Duration
	startBegin := time.Now()

	for i := 0; i < numStarts; i++ {
		t0 := time.Now()
		_, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    demo.OrderWorkflowType,
			WorkflowVersion: demo.OrderWorkflowVersion,
			Input:           json.RawMessage(`{}`),
			IdempotencyKey:  fmt.Sprintf("bench-start-%s-%d", uuid.NewString()[:6], i),
		})
		if err == nil {
			startDurations = append(startDurations, time.Since(t0))
		}
	}
	totalStartTime := time.Since(startBegin).Seconds()
	report.WorkflowStartThroughput = float64(len(startDurations)) / totalStartTime
	report.WorkflowStartLatency = calcPercentiles(startDurations)

	// 2. Replay Latency by History Length (10, 50, 100 events)
	historyLengths := []int{10, 50, 100}
	for _, targetEvents := range historyLengths {
		run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    demo.OrderWorkflowType,
			WorkflowVersion: demo.OrderWorkflowVersion,
			Input:           json.RawMessage(`{}`),
			IdempotencyKey:  fmt.Sprintf("bench-replay-%d-%s", targetEvents, uuid.NewString()[:6]),
		})

		// Seed history events
		for evIdx := 0; evIdx < targetEvents-1; evIdx++ {
			cmd := evIdx
			// Direct insert of dummy completed events
			_, _ = db.ExecContext(ctx, `
				INSERT INTO workflow_events (event_id, workflow_id, sequence_no, event_type, schema_version, command_index, payload, created_at)
				VALUES ($1, $2, $3, 'ActivityCompleted', 1, $4, '{"result":{}}', NOW());`,
				uuid.NewString(), run.WorkflowID, evIdx+2, cmd)
			_, _ = db.ExecContext(ctx, "UPDATE workflow_runs SET last_event_sequence = $1 WHERE workflow_id = $2", evIdx+2, run.WorkflowID)
		}

		t0 := time.Now()
		_, _ = replayer.ReplayWorkflow(ctx, run)
		replayDurMs := float64(time.Since(t0).Microseconds()) / 1000.0
		report.ReplayLatencyByEvents[fmt.Sprintf("%d_events", targetEvents)] = replayDurMs
	}

	// 3. Task Completion Throughput
	numTasks := 30
	var taskIDs []string
	benchRun, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
		WorkflowType:    "bench_tasks",
		WorkflowVersion: "v1",
		Input:           json.RawMessage(`{}`),
		IdempotencyKey:  "bench-tasks-" + uuid.NewString(),
	})

	for i := 0; i < numTasks; i++ {
		tid := uuid.NewString()
		_, _ = db.ExecContext(ctx, `
			INSERT INTO activity_tasks (
				task_id, workflow_id, command_index, task_kind, activity_type,
				input_payload, input_hash, retry_policy, idempotency_key,
				status, attempt, max_attempts, available_at, lease_generation,
				created_at, updated_at
			) VALUES ($1, $2, $3, 'FORWARD', 'reserve_inventory', '{}', 'h', '{}', $4, 'PENDING', 0, 3, NOW(), 0, NOW(), NOW());`,
			tid, benchRun.WorkflowID, i, "task-key-"+tid[:8])
		taskIDs = append(taskIDs, tid)
	}

	taskStart := time.Now()
	completedTasks := 0
	for range taskIDs {
		t, err := queue.ClaimTask(ctx, "bench-worker", 5*time.Second)
		if err == nil && t != nil {
			if completeErr := queue.CompleteTask(ctx, t.TaskID, t.LeaseGeneration, json.RawMessage(`{}`)); completeErr == nil {
				completedTasks++
			}
		}
	}
	taskDur := time.Since(taskStart).Seconds()
	if taskDur > 0 {
		report.TaskCompletionThroughput = float64(completedTasks) / taskDur
	}

	// 4. Timer Lateness Measurement
	dueTimerID := uuid.NewString()
	dueAt := time.Now().UTC().Add(-50 * time.Millisecond)
	_, _ = db.ExecContext(ctx, `
		INSERT INTO timer_tasks (timer_id, workflow_id, command_index, duration_ms, due_at, status, created_at)
		VALUES ($1, $2, 999, 100, $3, 'PENDING', NOW());`,
		dueTimerID, benchRun.WorkflowID, dueAt)

	tBeforeFire := time.Now().UTC()
	_, _ = timerSched.FireDueTimers(ctx)
	report.AverageTimerLatenessMs = float64(tBeforeFire.Sub(dueAt).Milliseconds())

	// 5. Process Recovery Time Measurement
	tRecov0 := time.Now()
	recovRun, _ := store.GetWorkflowRun(ctx, benchRun.WorkflowID)
	_, _ = replayer.ReplayWorkflow(ctx, recovRun)
	report.ProcessRecoveryTimeMs = float64(time.Since(tRecov0).Microseconds()) / 1000.0

	// Save Manifest & Report JSON
	manifestBytes, _ := json.MarshalIndent(manifest, "", "  ")
	_ = os.WriteFile(filepath.Join(resultsDir, "environment_manifest.json"), manifestBytes, 0644)

	reportBytes, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(filepath.Join(resultsDir, "benchmark_report.json"), reportBytes, 0644)

	// Save CSV output
	csvFile, err := os.Create(filepath.Join(resultsDir, "benchmark_metrics.csv"))
	if err == nil {
		writer := csv.NewWriter(csvFile)
		_ = writer.Write([]string{"metric_name", "value", "unit"})
		_ = writer.Write([]string{"workflow_start_throughput", fmt.Sprintf("%.2f", report.WorkflowStartThroughput), "ops/sec"})
		_ = writer.Write([]string{"workflow_start_latency_p50", fmt.Sprintf("%.2f", report.WorkflowStartLatency.P50Ms), "ms"})
		_ = writer.Write([]string{"workflow_start_latency_p95", fmt.Sprintf("%.2f", report.WorkflowStartLatency.P95Ms), "ms"})
		_ = writer.Write([]string{"workflow_start_latency_p99", fmt.Sprintf("%.2f", report.WorkflowStartLatency.P99Ms), "ms"})
		_ = writer.Write([]string{"task_completion_throughput", fmt.Sprintf("%.2f", report.TaskCompletionThroughput), "ops/sec"})
		_ = writer.Write([]string{"timer_lateness", fmt.Sprintf("%.2f", report.AverageTimerLatenessMs), "ms"})
		_ = writer.Write([]string{"recovery_latency", fmt.Sprintf("%.2f", report.ProcessRecoveryTimeMs), "ms"})
		writer.Flush()
		_ = csvFile.Close()
	}

	return report, nil
}
