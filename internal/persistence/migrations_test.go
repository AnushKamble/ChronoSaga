package persistence_test

import (
	"context"
	"testing"
	"time"

	"chronosaga/internal/persistence"
	"chronosaga/tests/testutil"
)

func TestMigrations_Lifecycle(t *testing.T) {
	db := testutil.SetupTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Rollback any existing schema to ensure clean slate
	_ = persistence.MigrateDown(ctx, db)

	// 2. Apply migrations UP
	if err := persistence.MigrateUp(ctx, db); err != nil {
		t.Fatalf("MigrateUp failed from empty database: %v", err)
	}

	// 3. Verify core tables exist
	tables := []string{
		"schema_migrations",
		"workflow_runs",
		"workflow_events",
		"activity_tasks",
		"timer_tasks",
		"compensation_tasks",
		"demo_effects",
		"demo_inventory",
		"demo_payments",
	}

	for _, table := range tables {
		var exists bool
		query := `SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = $1
		);`
		if err := db.QueryRowContext(ctx, query, table).Scan(&exists); err != nil {
			t.Fatalf("failed checking table %s existence: %v", table, err)
		}
		if !exists {
			t.Errorf("expected table %s to exist after MigrateUp", table)
		}
	}

	// 4. Test idempotency of MigrateUp
	if err := persistence.MigrateUp(ctx, db); err != nil {
		t.Fatalf("repeated MigrateUp failed: %v", err)
	}

	// 5. Test MigrateDown
	if err := persistence.MigrateDown(ctx, db); err != nil {
		t.Fatalf("MigrateDown failed: %v", err)
	}

	// 6. Verify table was removed
	var runsTableExists bool
	query := `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'workflow_runs'
	);`
	if err := db.QueryRowContext(ctx, query).Scan(&runsTableExists); err != nil {
		t.Fatalf("failed checking workflow_runs table: %v", err)
	}
	if runsTableExists {
		t.Errorf("expected workflow_runs to be dropped after MigrateDown")
	}

	// 7. Migrate back UP for subsequent tests
	if err := persistence.MigrateUp(ctx, db); err != nil {
		t.Fatalf("final MigrateUp failed: %v", err)
	}
}
