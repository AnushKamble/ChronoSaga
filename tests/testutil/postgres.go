package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"chronosaga/internal/persistence"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var (
	embeddedLock     sync.Mutex
	embeddedInstance *embeddedpostgres.EmbeddedPostgres
	embeddedPort     uint32 = 54333
	testDSN                 = fmt.Sprintf("postgres://chronosaga:chronosaga_secret@127.0.0.1:%d/chronosaga_test?sslmode=disable", embeddedPort)
)

// isPortOpen checks if a TCP port is currently listening.
func isPortOpen(port uint32) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return true
	}
	return false
}

// SetupTestDB returns a live PostgreSQL connection to a real database.
// It will use TEST_DATABASE_URL if present, reuse an active test instance, or start embedded PostgreSQL.
func SetupTestDB(t *testing.T) *persistence.DB {
	t.Helper()

	embeddedLock.Lock()
	defer embeddedLock.Unlock()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = testDSN

		// If port is not already open, start embedded postgres
		if !isPortOpen(embeddedPort) {
			cfg := embeddedpostgres.DefaultConfig().
				Port(embeddedPort).
				Database("chronosaga_test").
				Username("chronosaga").
				Password("chronosaga_secret")

			instance := embeddedpostgres.NewDatabase(cfg)
			if err := instance.Start(); err != nil {
				// Double check if it started despite returning an error (e.g. race)
				if !isPortOpen(embeddedPort) {
					t.Fatalf("failed to start embedded postgres: %v", err)
				}
			}
			embeddedInstance = instance
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := persistence.Connect(ctx, dsn, 10, 5, 1*time.Minute)
	if err != nil {
		t.Fatalf("failed to connect to test postgres (%s): %v", dsn, err)
	}

	// Ensure schema migrations are up to date
	if err := persistence.MigrateUp(ctx, db); err != nil {
		t.Fatalf("failed applying migrations to test DB: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

// TruncateAllTables wipes data from tables between test runs while preserving schema.
func TruncateAllTables(t *testing.T, db *persistence.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tables := []string{
		"demo_payments",
		"demo_inventory",
		"demo_effects",
		"compensation_tasks",
		"timer_tasks",
		"activity_tasks",
		"workflow_events",
		"workflow_runs",
	}

	for _, tbl := range tables {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", tbl))
	}
}
