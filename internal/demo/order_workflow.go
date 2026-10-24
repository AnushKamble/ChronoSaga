package demo

import (
	"encoding/json"
	"fmt"
	"time"

	"chronosaga/internal/workflow"
)

// WorkflowType and WorkflowVersion identifiers
const (
	OrderWorkflowType    = "order_fulfillment"
	OrderWorkflowVersion = "v1"
)

// RegisterOrderWorkflow registers the deterministic OrderWorkflow with the workflow registry.
func RegisterOrderWorkflow(reg *workflow.Registry) error {
	return reg.Register(OrderWorkflowType, OrderWorkflowVersion, OrderWorkflow)
}

// OrderWorkflow coordinates inventory reservation, payment, durable sleep, shipment, and reverse Saga compensation.
func OrderWorkflow(ctx *workflow.WorkflowContext, raw json.RawMessage) (json.RawMessage, error) {
	var in OrderWorkflowInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, fmt.Errorf("invalid workflow input: %w", err)
	}

	opts := workflow.DefaultActivityOptions()

	// 1. Reserve Inventory
	var reservation ReservationResult
	resvInput := ReserveInventoryInput{
		OrderID:  in.OrderID,
		SKU:      in.SKU,
		Quantity: in.Quantity,
	}
	if err := ctx.ExecuteActivity("reserve_inventory", resvInput, opts, &reservation); err != nil {
		return nil, err
	}

	// 2. Register Compensation: Release Inventory
	relInput := ReleaseInventoryInput{
		ReservationID: reservation.ReservationID,
	}
	if err := ctx.RegisterCompensation("release_inventory", relInput, opts); err != nil {
		return nil, err
	}

	// 3. Charge Payment
	var payment PaymentResult
	chargeInput := ChargePaymentInput{
		OrderID:     in.OrderID,
		AmountCents: in.TotalCents,
	}
	if err := ctx.ExecuteActivity("charge_payment", chargeInput, opts, &payment); err != nil {
		return nil, err
	}

	// 4. Register Compensation: Refund Payment
	refInput := RefundPaymentInput{
		PaymentID: payment.PaymentID,
	}
	if err := ctx.RegisterCompensation("refund_payment", refInput, opts); err != nil {
		return nil, err
	}

	// 5. Optional durable timer
	if in.SleepBeforeDispatchMs > 0 {
		if err := ctx.Sleep(time.Duration(in.SleepBeforeDispatchMs) * time.Millisecond); err != nil {
			return nil, err
		}
	}

	// 6. Dispatch Shipment (supports injected failure)
	var shipment ShipmentResult
	shipInput := DispatchShipmentInput{
		OrderID:              in.OrderID,
		PaymentID:            payment.PaymentID,
		ForceDispatchFailure: in.ForceDispatchFailure,
	}
	if err := ctx.ExecuteActivity("dispatch_shipment", shipInput, opts, &shipment); err != nil {
		return nil, err
	}

	// 7. Success output
	return json.Marshal(OrderWorkflowResult{
		OrderID:       in.OrderID,
		ReservationID: reservation.ReservationID,
		PaymentID:     payment.PaymentID,
		ShipmentID:    shipment.ShipmentID,
		Status:        "FULFILLED",
	})
}
