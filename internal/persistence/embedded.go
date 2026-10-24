package persistence

import (
	"fmt"
	"net"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var activeEmbedded *embeddedpostgres.EmbeddedPostgres

// EnsurePostgresRunning checks if PostgreSQL is reachable at the given host:port.
// If not reachable and host is localhost/127.0.0.1, it launches an embedded PostgreSQL instance.
func EnsurePostgresRunning(port uint32, dbName, user, password string) (*embeddedpostgres.EmbeddedPostgres, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return nil, nil // Already listening
	}

	cfg := embeddedpostgres.DefaultConfig().
		Port(port).
		Database(dbName).
		Username(user).
		Password(password)

	instance := embeddedpostgres.NewDatabase(cfg)
	if err := instance.Start(); err != nil {
		// Double check if it succeeded
		c, dialErr := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if dialErr == nil {
			_ = c.Close()
			activeEmbedded = instance
			return instance, nil
		}
		return nil, fmt.Errorf("failed starting embedded postgres: %w", err)
	}

	activeEmbedded = instance
	return instance, nil
}
