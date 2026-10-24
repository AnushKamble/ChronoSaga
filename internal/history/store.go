package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"chronosaga/internal/observability"
	"chronosaga/internal/persistence"
)

var (
	ErrWorkflowNotFound = errors.New("workflow run not found")
	ErrSequenceConflict = errors.New("concurrent event sequence conflict")
)

// Store provides append-only access to workflow event history and projections.
type Store struct {
	db      *persistence.DB
	metrics *observability.Metrics
}

// NewStore initializes a new history Store.
func NewStore(db *persistence.DB, metrics *observability.Metrics) *Store {
	if metrics == nil {
		metrics = observability.DefaultMetrics
	}
	return &Store{
		db:      db,
		metrics: metrics,
	}
}

// AppendEventInTx appends an event to workflow_events inside an existing transaction.
// It acquires a row-lock on workflow_runs (FOR UPDATE), advances last_event_sequence,
// and guarantees strictly contiguous sequence numbers (1, 2, 3...) per workflow.
func (s *Store) AppendEventInTx(
	ctx context.Context,
	tx *sql.Tx,
	workflowID string,
	eventType string,
	cmdIndex *int,
	payload any,
) (*Event, error) {
	var currentSeq int64
	queryLock := `
		SELECT last_event_sequence 
		FROM workflow_runs 
		WHERE workflow_id = $1 
		FOR UPDATE;`
	err := tx.QueryRowContext(ctx, queryLock, workflowID).Scan(&currentSeq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrWorkflowNotFound
		}
		return nil, fmt.Errorf("failed to lock workflow row for append: %w", err)
	}

	nextSeq := currentSeq + 1
	event, err := NewEvent(workflowID, eventType, nextSeq, cmdIndex, payload)
	if err != nil {
		return nil, err
	}

	insertEventQuery := `
		INSERT INTO workflow_events (
			event_id, workflow_id, sequence_no, event_type, schema_version, command_index, payload, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`

	_, err = tx.ExecContext(ctx, insertEventQuery,
		event.EventID,
		event.WorkflowID,
		event.SequenceNo,
		event.EventType,
		event.SchemaVersion,
		event.CommandIndex,
		event.Payload,
		event.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert event into workflow_events: %w", err)
	}

	updateRunQuery := `
		UPDATE workflow_runs 
		SET last_event_sequence = $1, updated_at = NOW() 
		WHERE workflow_id = $2;`
	_, err = tx.ExecContext(ctx, updateRunQuery, nextSeq, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to update last_event_sequence: %w", err)
	}

	s.metrics.IncWorkflowHistoryEvents(1)
	return event, nil
}

// GetHistory retrieves ordered events for a workflow starting after a given sequence number.
// Returns events, hasMore flag, and error.
func (s *Store) GetHistory(ctx context.Context, workflowID string, afterSeq int64, limit int) ([]Event, bool, error) {
	if limit <= 0 {
		limit = 100
	}
	// Fetch limit + 1 to detect hasMore
	fetchLimit := limit + 1

	query := `
		SELECT event_id, workflow_id, sequence_no, event_type, schema_version, command_index, payload, created_at
		FROM workflow_events
		WHERE workflow_id = $1 AND sequence_no > $2
		ORDER BY sequence_no ASC
		LIMIT $3;`

	rows, err := s.db.QueryContext(ctx, query, workflowID, afterSeq, fetchLimit)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query workflow events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var rawPayload []byte
		if err := rows.Scan(
			&e.EventID,
			&e.WorkflowID,
			&e.SequenceNo,
			&e.EventType,
			&e.SchemaVersion,
			&e.CommandIndex,
			&rawPayload,
			&e.CreatedAt,
		); err != nil {
			return nil, false, fmt.Errorf("failed to scan event row: %w", err)
		}
		e.Payload = json.RawMessage(rawPayload)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	hasMore := false
	if len(events) > limit {
		hasMore = true
		events = events[:limit]
	}

	return events, hasMore, nil
}

// GetAllHistory loads all recorded events for a workflow run in sequence order.
func (s *Store) GetAllHistory(ctx context.Context, workflowID string) ([]Event, error) {
	events, _, err := s.GetHistory(ctx, workflowID, 0, 10000)
	return events, err
}
