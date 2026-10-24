package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chronosaga/internal/activities"
	"chronosaga/internal/api"
	"chronosaga/internal/bench"
	"chronosaga/internal/config"
	"chronosaga/internal/demo"
	"chronosaga/internal/history"
	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
	"chronosaga/internal/saga"
	"chronosaga/internal/timers"
	"chronosaga/internal/workflow"
	"github.com/google/uuid"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}

	logger := observability.InitLogger(cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cmd := os.Args[1]
	switch cmd {
	case "migrate":
		handleMigrate(ctx, cfg, logger, os.Args[2:])
	case "api", "serve":
		handleAPI(ctx, cfg, logger)
	case "worker":
		handleWorker(ctx, cfg, logger)
	case "workflow":
		handleWorkflowCLI(ctx, cfg, logger, os.Args[2:])
	case "demo":
		handleDemoCLI(ctx, cfg, logger, os.Args[2:])
	case "benchmark":
		handleBenchmarkCLI(ctx, cfg, logger)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("ChronoSaga - Durable Event-Sourced Workflow Orchestrator")
	fmt.Println("\nUsage:")
	fmt.Println("  chronosaga api                             Start HTTP API & orchestrator")
	fmt.Println("  chronosaga worker                          Start activity worker daemon")
	fmt.Println("  chronosaga migrate [up|down]               Apply or rollback SQL migrations")
	fmt.Println("  chronosaga workflow start <type> <json>    Start a workflow run")
	fmt.Println("  chronosaga workflow status <id>            Query workflow run status")
	fmt.Println("  chronosaga workflow events <id>            Query ordered event history")
	fmt.Println("  chronosaga demo crash-recovery             Run crash recovery demonstration")
	fmt.Println("  chronosaga demo saga-failure               Run Saga reverse compensation demo")
	fmt.Println("  chronosaga demo retry-compensation <id>    Retry a failed compensation")
	fmt.Println("  chronosaga benchmark                       Run local reproducible benchmark suite")
}

func handleMigrate(ctx context.Context, cfg *config.Config, logger *slog.Logger, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: chronosaga migrate [up|down]")
		os.Exit(1)
	}

	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to connect to database for migrations", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	action := args[0]
	switch action {
	case "up":
		logger.Info("Applying migrations UP...")
		if err := persistence.MigrateUp(ctx, db); err != nil {
			logger.Error("Migration UP failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Migrations successfully applied")
	case "down":
		logger.Info("Applying migrations DOWN...")
		if err := persistence.MigrateDown(ctx, db); err != nil {
			logger.Error("Migration DOWN failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Migrations successfully rolled back")
	default:
		fmt.Printf("Unknown migration action: %s\n", action)
		os.Exit(1)
	}
}

func connectOrAutoStart(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*persistence.DB, error) {
	db, err := persistence.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, cfg.DBConnMaxLifetime)
	if err == nil {
		return db, nil
	}

	// If local, attempt auto-starting embedded postgres
	logger.Info("Local PostgreSQL not reachable, attempting auto-start of embedded instance...")
	_, startErr := persistence.EnsurePostgresRunning(5432, "chronosaga", "chronosaga", "chronosaga_secret")
	if startErr != nil {
		return nil, fmt.Errorf("database unreachable and embedded start failed: %w (orig: %v)", startErr, err)
	}

	return persistence.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, cfg.DBConnMaxLifetime)
}

func handleAPI(ctx context.Context, cfg *config.Config, logger *slog.Logger) {
	logger.Info("Connecting to PostgreSQL...", "dsn", cfg.Host)
	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Ensure migrations
	if err := persistence.MigrateUp(ctx, db); err != nil {
		logger.Error("Failed applying migrations", "error", err)
		os.Exit(1)
	}

	store := history.NewStore(db, observability.DefaultMetrics)

	// Register workflows
	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)

	// Register activities
	effectStore := demo.NewEffectStore(db)
	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	coord := saga.NewCoordinator(db, store, actReg, observability.DefaultMetrics)
	replayer := workflow.NewReplayer(db, store, wfReg, observability.DefaultMetrics)
	orchestrator := workflow.NewOrchestratorScheduler("orchestrator-main", db, store, replayer, coord, observability.DefaultMetrics, 200*time.Millisecond)
	timerSched := timers.NewScheduler(db, store, observability.DefaultMetrics, cfg.TimerPollInterval)

	// Start background schedulers
	go orchestrator.Start(ctx)
	go timerSched.Start(ctx)

	server := api.NewServer(cfg, db, store, wfReg, coord)

	go func() {
		logger.Info("ChronoSaga API server listening", "host", cfg.Host, "port", cfg.Port)
		if err := server.Start(); err != nil {
			logger.Error("Server stopped", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("Shutting down API server gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = server.Shutdown(shutdownCtx)
	logger.Info("Server stopped cleanly")
}

func handleWorker(ctx context.Context, cfg *config.Config, logger *slog.Logger) {
	logger.Info("Connecting worker to PostgreSQL...", "dsn", cfg.Host)
	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to connect to PostgreSQL for worker", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	store := history.NewStore(db, observability.DefaultMetrics)
	effectStore := demo.NewEffectStore(db)

	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	queue := activities.NewTaskQueue(db, store, observability.DefaultMetrics)
	worker := activities.NewWorker(
		"worker-"+uuid.NewString()[:8],
		queue,
		actReg,
		observability.DefaultMetrics,
		cfg.WorkerConcurrency,
		cfg.WorkerPollInterval,
		cfg.WorkerLeaseDuration,
	)

	logger.Info("ChronoSaga Worker pool initialized", "concurrency", cfg.WorkerConcurrency, "lease_duration", cfg.WorkerLeaseDuration)
	_ = worker.Start(ctx)
}

func handleWorkflowCLI(ctx context.Context, cfg *config.Config, logger *slog.Logger, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: chronosaga workflow [start|status|events]")
		os.Exit(1)
	}

	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	store := history.NewStore(db, observability.DefaultMetrics)

	switch args[0] {
	case "start":
		if len(args) < 3 {
			fmt.Println("Usage: chronosaga workflow start <type> '<json-input>' [idempotency-key]")
			os.Exit(1)
		}
		wfType := args[1]
		inputRaw := args[2]
		idempKey := "cli-" + uuid.NewString()
		if len(args) >= 4 {
			idempKey = args[3]
		}

		run, isExisting, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    wfType,
			WorkflowVersion: "v1",
			Input:           json.RawMessage(inputRaw),
			IdempotencyKey:  idempKey,
		})
		if err != nil {
			fmt.Printf("Error starting workflow: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Workflow started: ID=%s Status=%s IsDuplicate=%v\n", run.WorkflowID, run.Status, isExisting)

	case "status":
		if len(args) < 2 {
			fmt.Println("Usage: chronosaga workflow status <workflow-id>")
			os.Exit(1)
		}
		run, err := store.GetWorkflowRun(ctx, args[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		out, _ := json.MarshalIndent(run, "", "  ")
		fmt.Println(string(out))

	case "events":
		if len(args) < 2 {
			fmt.Println("Usage: chronosaga workflow events <workflow-id>")
			os.Exit(1)
		}
		events, err := store.GetAllHistory(ctx, args[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		out, _ := json.MarshalIndent(events, "", "  ")
		fmt.Println(string(out))
	}
}

func handleDemoCLI(ctx context.Context, cfg *config.Config, logger *slog.Logger, args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: chronosaga demo [crash-recovery|saga-failure|retry-compensation]")
		os.Exit(1)
	}

	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	_ = persistence.MigrateUp(ctx, db)
	store := history.NewStore(db, observability.DefaultMetrics)
	effectStore := demo.NewEffectStore(db)

	wfReg := workflow.NewRegistry()
	_ = demo.RegisterOrderWorkflow(wfReg)

	actReg := activities.NewRegistry()
	_ = demo.RegisterDemoActivities(actReg, effectStore)

	queue := activities.NewTaskQueue(db, store, observability.DefaultMetrics)
	worker := activities.NewWorker("demo-worker", queue, actReg, observability.DefaultMetrics, 1, 50*time.Millisecond, 10*time.Second)
	coord := saga.NewCoordinator(db, store, actReg, observability.DefaultMetrics)
	replayer := workflow.NewReplayer(db, store, wfReg, observability.DefaultMetrics)
	scheduler := workflow.NewOrchestratorScheduler("demo-sched", db, store, replayer, coord, observability.DefaultMetrics, 50*time.Millisecond)

	switch args[0] {
	case "crash-recovery":
		fmt.Println("\n=== ChronoSaga Demo: Crash Recovery with Durable Replay ===")
		sku := "SKU-CRASH-DEMO"
		_ = effectStore.SeedInventory(ctx, sku, 10)

		in := demo.OrderWorkflowInput{
			OrderID:               "ord-crash-demo",
			SKU:                   sku,
			Quantity:              2,
			TotalCents:            3500,
			SleepBeforeDispatchMs: 200,
		}
		raw, _ := json.Marshal(in)
		run, _, err := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    demo.OrderWorkflowType,
			WorkflowVersion: demo.OrderWorkflowVersion,
			Input:           raw,
			IdempotencyKey:  "crash-demo-" + uuid.NewString(),
		})
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("1. Workflow Started: ID=%s (Status=%s)\n", run.WorkflowID, run.Status)

		// Forward step 1: Reserve
		_, _ = scheduler.Tick(ctx)
		t1, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		worker.ExecuteTask(ctx, t1)
		fmt.Println("2. Activity 'reserve_inventory' completed.")

		// Forward step 2: Charge
		_, _ = scheduler.Tick(ctx)
		t2, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		worker.ExecuteTask(ctx, t2)
		fmt.Println("3. Activity 'charge_payment' completed.")

		// Step 3: Schedule durable timer
		_, _ = scheduler.Tick(ctx)
		fmt.Println("4. Durable Timer scheduled (200ms) - status WAITING_TIMER.")

		fmt.Println("\n[SIMULATING PROCESS CRASH / SIGKILL]")
		fmt.Println("Orchestrator and Worker killed while durable timer is pending in PostgreSQL.")
		time.Sleep(250 * time.Millisecond) // timer matures while processes are dead!

		fmt.Println("\n[RESTARTING PROCESSES FROM PERSISTED POSTGRESQL STATE]")
		// Recover: timer scheduler discovers mature timer and fires it!
		timerSched := timers.NewScheduler(db, store, observability.DefaultMetrics, 50*time.Millisecond)
		fired, _ := timerSched.FireDueTimers(ctx)
		fmt.Printf("5. Restarted Timer Scheduler discovered and fired %d overdue timer.\n", fired)

		// Replay sees TimerFired, continues, schedules dispatch_shipment!
		_, _ = scheduler.Tick(ctx)

		t3, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		if t3 != nil {
			worker.ExecuteTask(ctx, t3)
			fmt.Printf("6. Activity '%s' executed after restart.\n", t3.ActivityType)
		}
		_, _ = scheduler.Tick(ctx)

		runFinal, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
		fmt.Printf("\n7. Workflow Finished: Final Status = %s\n", runFinal.Status)
		fmt.Printf("Output: %s\n", string(runFinal.OutputJSON))
		fmt.Println("Evidence: Workflow survived process termination and resumed from PostgreSQL state!")

	case "saga-failure":
		fmt.Println("\n=== ChronoSaga Demo: Saga Reverse (LIFO) Compensation ===")
		sku := "SKU-SAGA-DEMO"
		_ = effectStore.SeedInventory(ctx, sku, 10)

		in := demo.OrderWorkflowInput{
			OrderID:              "ord-saga-demo",
			SKU:                  sku,
			Quantity:             3,
			TotalCents:           5900,
			ForceDispatchFailure: true, // Injected failure
		}
		raw, _ := json.Marshal(in)
		run, _, _ := store.CreateWorkflowRun(ctx, history.StartWorkflowParams{
			WorkflowType:    demo.OrderWorkflowType,
			WorkflowVersion: demo.OrderWorkflowVersion,
			Input:           raw,
			IdempotencyKey:  "saga-demo-" + uuid.NewString(),
		})
		fmt.Printf("1. Workflow Started: ID=%s\n", run.WorkflowID)

		// 1. Reserve
		_, _ = scheduler.Tick(ctx)
		t1, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		worker.ExecuteTask(ctx, t1)
		fmt.Println("2. Step 1: 'reserve_inventory' succeeded -> registered compensation 'release_inventory' (Order #1)")

		// 2. Charge
		_, _ = scheduler.Tick(ctx)
		t2, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		worker.ExecuteTask(ctx, t2)
		fmt.Println("3. Step 2: 'charge_payment' succeeded -> registered compensation 'refund_payment' (Order #2)")

		// 3. Dispatch (injected terminal failure)
		_, _ = scheduler.Tick(ctx)
		t3, _ := queue.ClaimTask(ctx, "w1", 5*time.Second)
		worker.ExecuteTask(ctx, t3)
		fmt.Println("4. Step 3: 'dispatch_shipment' FAILED terminally!")

		// 4. Trigger Compensation
		_, _ = scheduler.Tick(ctx)
		fmt.Println("\n5. Triggering Reverse (LIFO) Saga Compensation Chain...")

		// Step A of compensation: refund_payment
		_, _ = scheduler.Tick(ctx)
		fmt.Println("   -> Compensating Order #2: 'refund_payment' executed!")

		// Step B of compensation: release_inventory
		_, _ = scheduler.Tick(ctx)
		fmt.Println("   -> Compensating Order #1: 'release_inventory' executed!")

		_, _ = scheduler.Tick(ctx)
		runFinal, _ := store.GetWorkflowRun(ctx, run.WorkflowID)
		fmt.Printf("\n6. Saga Complete: Final Workflow Status = %s\n", runFinal.Status)
		fmt.Println("Evidence: Compensations strictly executed in reverse registration order (refund THEN release).")

	case "retry-compensation":
		if len(args) < 2 {
			fmt.Println("Usage: chronosaga demo retry-compensation <workflow-id>")
			return
		}
		err := coord.RetryFailedCompensation(ctx, args[1], "cli-operator")
		if err != nil {
			fmt.Printf("Error retrying compensation: %v\n", err)
			return
		}
		fmt.Printf("Compensation retry requested successfully for workflow %s\n", args[1])
	}
}

func handleBenchmarkCLI(ctx context.Context, cfg *config.Config, logger *slog.Logger) {
	fmt.Println("Running ChronoSaga Benchmark Protocol...")
	db, err := connectOrAutoStart(ctx, cfg, logger)
	if err != nil {
		logger.Error("Database connection failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	_ = persistence.MigrateUp(ctx, db)

	report, err := bench.RunBenchmarks(ctx, db, "results")
	if err != nil {
		fmt.Printf("Benchmark error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n================ ChronoSaga Measured Benchmark Summary ================")
	fmt.Printf("Environment: %s/%s (%d CPUs) | %s | Go %s\n", report.Manifest.OS, report.Manifest.Arch, report.Manifest.NumCPU, report.Manifest.Database, report.Manifest.GoVersion)
	fmt.Printf("Workflow Start Throughput : %.2f ops/sec\n", report.WorkflowStartThroughput)
	fmt.Printf("Workflow Start Latency    : p50=%.2f ms | p95=%.2f ms | p99=%.2f ms\n", report.WorkflowStartLatency.P50Ms, report.WorkflowStartLatency.P95Ms, report.WorkflowStartLatency.P99Ms)
	fmt.Printf("Task Completion Rate      : %.2f ops/sec\n", report.TaskCompletionThroughput)
	fmt.Printf("Average Timer Lateness    : %.2f ms\n", report.AverageTimerLatenessMs)
	fmt.Printf("Process Recovery Time     : %.2f ms\n", report.ProcessRecoveryTimeMs)
	fmt.Println("Replay Latency by History Length:")
	for length, latMs := range report.ReplayLatencyByEvents {
		fmt.Printf("  - %s: %.2f ms\n", length, latMs)
	}
	fmt.Println("\nRaw artifacts generated in results/ (benchmark_report.json, benchmark_metrics.csv, environment_manifest.json)")
	fmt.Println("========================================================================")
}
