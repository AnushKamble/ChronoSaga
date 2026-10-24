package persistence

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"
	"time"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migration represents a single database migration file.
type Migration struct {
	Version int
	Name    string
	UpSQL   string
	DownSQL string
}

// EnsureSchemaMigrationsTable creates the migration tracking table if missing.
func EnsureSchemaMigrationsTable(ctx context.Context, db *DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version INT PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);`
	_, err := db.ExecContext(ctx, query)
	return err
}

// GetAppliedMigrations returns a map of applied migration versions.
func GetAppliedMigrations(ctx context.Context, db *DB) (map[int]time.Time, error) {
	if err := EnsureSchemaMigrationsTable(ctx, db); err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, `SELECT version, applied_at FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]time.Time)
	for rows.Next() {
		var v int
		var t time.Time
		if err := rows.Scan(&v, &t); err != nil {
			return nil, err
		}
		applied[v] = t
	}
	return applied, rows.Err()
}

// LoadEmbeddedMigrations loads migrations from the embedded filesystem.
func LoadEmbeddedMigrations() ([]Migration, error) {
	entries, err := embeddedMigrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded migrations: %w", err)
	}

	mMap := make(map[int]*Migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var version int
		var baseName string
		var dir string

		if strings.HasSuffix(name, ".up.sql") {
			_, err := fmt.Sscanf(name, "%03d_%s", &version, &baseName)
			if err != nil {
				continue
			}
			dir = "up"
		} else if strings.HasSuffix(name, ".down.sql") {
			_, err := fmt.Sscanf(name, "%03d_%s", &version, &baseName)
			if err != nil {
				continue
			}
			dir = "down"
		} else {
			continue
		}

		content, err := embeddedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}

		m, exists := mMap[version]
		if !exists {
			m = &Migration{Version: version, Name: name}
			mMap[version] = m
		}

		if dir == "up" {
			m.UpSQL = string(content)
			m.Name = name
		} else {
			m.DownSQL = string(content)
		}
	}

	var migrations []Migration
	for _, m := range mMap {
		migrations = append(migrations, *m)
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// MigrateUp applies all unapplied migrations in ascending order.
func MigrateUp(ctx context.Context, db *DB) error {
	applied, err := GetAppliedMigrations(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	migrations, err := LoadEmbeddedMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	for _, m := range migrations {
		if _, already := applied[m.Version]; already {
			continue
		}

		err = db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, execErr := tx.ExecContext(ctx, m.UpSQL); execErr != nil {
				return fmt.Errorf("failed executing migration %d (%s): %w", m.Version, m.Name, execErr)
			}
			_, insErr := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, NOW())`, m.Version, m.Name)
			return insErr
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// MigrateDown rolls back all applied migrations in descending order.
func MigrateDown(ctx context.Context, db *DB) error {
	applied, err := GetAppliedMigrations(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	migrations, err := LoadEmbeddedMigrations()
	if err != nil {
		return fmt.Errorf("failed to load migrations: %w", err)
	}

	// Reverse order for rollback
	for i := len(migrations) - 1; i >= 0; i-- {
		m := migrations[i]
		if _, wasApplied := applied[m.Version]; !wasApplied {
			continue
		}

		if m.DownSQL == "" {
			return fmt.Errorf("migration %d (%s) has no down migration defined", m.Version, m.Name)
		}

		err = db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, execErr := tx.ExecContext(ctx, m.DownSQL); execErr != nil {
				return fmt.Errorf("failed rolling back migration %d (%s): %w", m.Version, m.Name, execErr)
			}
			_, delErr := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = $1`, m.Version)
			return delErr
		})
		if err != nil {
			return err
		}
	}
	return nil
}
