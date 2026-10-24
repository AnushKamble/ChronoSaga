package demo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"chronosaga/internal/activities"
	"chronosaga/internal/persistence"
	"github.com/google/uuid"
)

var (
	ErrInsufficientStock = activities.NewNonRetryableError("INSUFFICIENT_STOCK", "requested quantity exceeds available stock")
	ErrCarrierRejected   = activities.NewNonRetryableError("SHIPPING_REJECTED", "injected delivery carrier failure")
)

// EffectStore encapsulates PostgreSQL-backed mock business ledgers with atomic idempotency checks.
type EffectStore struct {
	db *persistence.DB
}

// NewEffectStore creates a new EffectStore.
func NewEffectStore(db *persistence.DB) *EffectStore {
	return &EffectStore{db: db}
}

// SeedInventory ensures a SKU has the specified available stock.
func (s *EffectStore) SeedInventory(ctx context.Context, sku string, quantity int) error {
	query := `
		INSERT INTO demo_inventory (sku, available_quantity, reserved_quantity)
		VALUES ($1, $2, 0)
		ON CONFLICT (sku) DO UPDATE 
		SET available_quantity = $2, reserved_quantity = 0;`
	_, err := s.db.ExecContext(ctx, query, sku, quantity)
	return err
}

// ReserveInventory idempotently decrements available stock and increments reserved stock.
func (s *EffectStore) ReserveInventory(ctx context.Context, idempKey string, input ReserveInventoryInput) (*ReservationResult, error) {
	var result ReservationResult

	err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
		// 1. Idempotency check: if key already recorded, return previous result
		var rawPayload []byte
		err := tx.QueryRowContext(ctx, "SELECT payload FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&rawPayload)
		if err == nil {
			return json.Unmarshal(rawPayload, &result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 2. Perform state update
		updateQuery := `
			UPDATE demo_inventory
			SET available_quantity = available_quantity - $1,
			    reserved_quantity = reserved_quantity + $1
			WHERE sku = $2 AND available_quantity >= $1;`

		res, err := tx.ExecContext(ctx, updateQuery, input.Quantity, input.SKU)
		if err != nil {
			return err
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return ErrInsufficientStock
		}

		resID := "res_" + uuid.NewString()[:8]
		result = ReservationResult{
			ReservationID: resID,
			SKU:           input.SKU,
			Quantity:      input.Quantity,
		}

		payloadBytes, _ := json.Marshal(result)
		insertEffectQuery := `
			INSERT INTO demo_effects (effect_id, idempotency_key, effect_type, entity_id, payload, created_at)
			VALUES ($1, $2, 'INVENTORY_RESERVED', $3, $4, NOW());`
		_, err = tx.ExecContext(ctx, insertEffectQuery, uuid.NewString(), idempKey, resID, payloadBytes)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ReleaseInventory idempotently counteracts a reservation (compensation).
func (s *EffectStore) ReleaseInventory(ctx context.Context, idempKey string, input ReleaseInventoryInput) (*ReleaseResult, error) {
	var result ReleaseResult

	err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
		var rawPayload []byte
		err := tx.QueryRowContext(ctx, "SELECT payload FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&rawPayload)
		if err == nil {
			return json.Unmarshal(rawPayload, &result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		result = ReleaseResult{
			ReservationID: input.ReservationID,
			Released:      true,
		}

		payloadBytes, _ := json.Marshal(result)
		insertEffectQuery := `
			INSERT INTO demo_effects (effect_id, idempotency_key, effect_type, entity_id, payload, created_at)
			VALUES ($1, $2, 'INVENTORY_RELEASED', $3, $4, NOW());`
		_, err = tx.ExecContext(ctx, insertEffectQuery, uuid.NewString(), idempKey, input.ReservationID, payloadBytes)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ChargePayment idempotently records a charged payment.
func (s *EffectStore) ChargePayment(ctx context.Context, idempKey string, input ChargePaymentInput) (*PaymentResult, error) {
	var result PaymentResult

	err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
		var rawPayload []byte
		err := tx.QueryRowContext(ctx, "SELECT payload FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&rawPayload)
		if err == nil {
			return json.Unmarshal(rawPayload, &result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		payID := "pay_" + uuid.NewString()[:8]
		insertPaymentQuery := `
			INSERT INTO demo_payments (payment_id, order_id, amount_cents, status, created_at, updated_at)
			VALUES ($1, $2, $3, 'CHARGED', NOW(), NOW());`
		if _, err := tx.ExecContext(ctx, insertPaymentQuery, payID, input.OrderID, input.AmountCents); err != nil {
			return err
		}

		result = PaymentResult{
			PaymentID:   payID,
			OrderID:     input.OrderID,
			AmountCents: input.AmountCents,
			Status:      "CHARGED",
		}

		payloadBytes, _ := json.Marshal(result)
		insertEffectQuery := `
			INSERT INTO demo_effects (effect_id, idempotency_key, effect_type, entity_id, payload, created_at)
			VALUES ($1, $2, 'PAYMENT_CHARGED', $3, $4, NOW());`
		_, err = tx.ExecContext(ctx, insertEffectQuery, uuid.NewString(), idempKey, payID, payloadBytes)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}

// RefundPayment idempotently marks a payment refunded (compensation).
func (s *EffectStore) RefundPayment(ctx context.Context, idempKey string, input RefundPaymentInput) (*RefundResult, error) {
	var result RefundResult

	err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
		var rawPayload []byte
		err := tx.QueryRowContext(ctx, "SELECT payload FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&rawPayload)
		if err == nil {
			return json.Unmarshal(rawPayload, &result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		updateQuery := `
			UPDATE demo_payments
			SET status = 'REFUNDED', updated_at = NOW()
			WHERE payment_id = $1;`
		if _, err := tx.ExecContext(ctx, updateQuery, input.PaymentID); err != nil {
			return err
		}

		result = RefundResult{
			PaymentID: input.PaymentID,
			Refunded:  true,
		}

		payloadBytes, _ := json.Marshal(result)
		insertEffectQuery := `
			INSERT INTO demo_effects (effect_id, idempotency_key, effect_type, entity_id, payload, created_at)
			VALUES ($1, $2, 'PAYMENT_REFUNDED', $3, $4, NOW());`
		_, err = tx.ExecContext(ctx, insertEffectQuery, uuid.NewString(), idempKey, input.PaymentID, payloadBytes)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}

// DispatchShipment idempotently records a shipment dispatch, or simulates failure if requested.
func (s *EffectStore) DispatchShipment(ctx context.Context, idempKey string, input DispatchShipmentInput) (*ShipmentResult, error) {
	if input.ForceDispatchFailure {
		return nil, ErrCarrierRejected
	}

	var result ShipmentResult

	err := s.db.WithTx(ctx, func(tx *sql.Tx) error {
		var rawPayload []byte
		err := tx.QueryRowContext(ctx, "SELECT payload FROM demo_effects WHERE idempotency_key = $1", idempKey).Scan(&rawPayload)
		if err == nil {
			return json.Unmarshal(rawPayload, &result)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		shipID := "ship_" + uuid.NewString()[:8]
		result = ShipmentResult{
			ShipmentID: shipID,
			OrderID:    input.OrderID,
			Status:     "DISPATCHED",
		}

		payloadBytes, _ := json.Marshal(result)
		insertEffectQuery := `
			INSERT INTO demo_effects (effect_id, idempotency_key, effect_type, entity_id, payload, created_at)
			VALUES ($1, $2, 'SHIPMENT_DISPATCHED', $3, $4, NOW());`
		_, err = tx.ExecContext(ctx, insertEffectQuery, uuid.NewString(), idempKey, shipID, payloadBytes)
		return err
	})

	if err != nil {
		return nil, err
	}
	return &result, nil
}
