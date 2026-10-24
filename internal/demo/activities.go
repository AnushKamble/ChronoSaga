package demo

import (
	"context"
	"encoding/json"
	"fmt"

	"chronosaga/internal/activities"
)

// RegisterDemoActivities registers all demo business activity handlers with the given registry.
func RegisterDemoActivities(reg *activities.Registry, effectStore *EffectStore) error {
	// 1. Reserve Inventory
	err := reg.Register("reserve_inventory", func(ctx context.Context, input json.RawMessage, idempKey string) (json.RawMessage, error) {
		var in ReserveInventoryInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, activities.NewNonRetryableError("INVALID_ACTIVITY_INPUT", err.Error())
		}
		res, err := effectStore.ReserveInventory(ctx, idempKey, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	if err != nil {
		return err
	}

	// 2. Release Inventory (Compensation)
	err = reg.Register("release_inventory", func(ctx context.Context, input json.RawMessage, idempKey string) (json.RawMessage, error) {
		var in ReleaseInventoryInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, activities.NewNonRetryableError("INVALID_ACTIVITY_INPUT", err.Error())
		}
		res, err := effectStore.ReleaseInventory(ctx, idempKey, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	if err != nil {
		return err
	}

	// 3. Charge Payment
	err = reg.Register("charge_payment", func(ctx context.Context, input json.RawMessage, idempKey string) (json.RawMessage, error) {
		var in ChargePaymentInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, activities.NewNonRetryableError("INVALID_ACTIVITY_INPUT", err.Error())
		}
		res, err := effectStore.ChargePayment(ctx, idempKey, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	if err != nil {
		return err
	}

	// 4. Refund Payment (Compensation)
	err = reg.Register("refund_payment", func(ctx context.Context, input json.RawMessage, idempKey string) (json.RawMessage, error) {
		var in RefundPaymentInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, activities.NewNonRetryableError("INVALID_ACTIVITY_INPUT", err.Error())
		}
		res, err := effectStore.RefundPayment(ctx, idempKey, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	if err != nil {
		return err
	}

	// 5. Dispatch Shipment
	err = reg.Register("dispatch_shipment", func(ctx context.Context, input json.RawMessage, idempKey string) (json.RawMessage, error) {
		var in DispatchShipmentInput
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, activities.NewNonRetryableError("INVALID_ACTIVITY_INPUT", err.Error())
		}
		res, err := effectStore.DispatchShipment(ctx, idempKey, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
	})
	if err != nil {
		return fmt.Errorf("failed registering dispatch_shipment: %w", err)
	}

	return nil
}
