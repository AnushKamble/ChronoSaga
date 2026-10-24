package bench_test

import (
	"context"
	"testing"
	"time"

	"chronosaga/internal/bench"
	"chronosaga/tests/testutil"
)

func TestBenchmark_Protocol_Execution(t *testing.T) {
	db := testutil.SetupTestDB(t)
	testutil.TruncateAllTables(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	report, err := bench.RunBenchmarks(ctx, db, "../../results")
	if err != nil {
		t.Fatalf("RunBenchmarks failed: %v", err)
	}

	if report.WorkflowStartThroughput <= 0 {
		t.Errorf("expected positive start throughput, got %f", report.WorkflowStartThroughput)
	}
	if report.WorkflowStartLatency.P50Ms <= 0 {
		t.Errorf("expected positive p50 latency, got %f", report.WorkflowStartLatency.P50Ms)
	}
	if len(report.ReplayLatencyByEvents) == 0 {
		t.Errorf("expected replay latency entries, got 0")
	}
}

func BenchmarkWorkflowStart(b *testing.B) {
	// Standard Go benchmark
	b.ReportAllocs()
}
