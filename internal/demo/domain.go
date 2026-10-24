package demo

// OrderWorkflowInput represents the incoming payload to start an order workflow.
type OrderWorkflowInput struct {
	OrderID               string `json:"order_id"`
	SKU                   string `json:"sku"`
	Quantity              int    `json:"quantity"`
	TotalCents            int64  `json:"total_cents"`
	ForceDispatchFailure  bool   `json:"force_dispatch_failure,omitempty"`
	SleepBeforeDispatchMs int    `json:"sleep_before_dispatch_ms,omitempty"`
}

// OrderWorkflowResult represents the completed order output.
type OrderWorkflowResult struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	PaymentID     string `json:"payment_id"`
	ShipmentID    string `json:"shipment_id"`
	Status        string `json:"status"`
}

// --- Activity Inputs & Results ---

type ReserveInventoryInput struct {
	OrderID  string `json:"order_id"`
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type ReservationResult struct {
	ReservationID string `json:"reservation_id"`
	SKU           string `json:"sku"`
	Quantity      int    `json:"quantity"`
}

type ReleaseInventoryInput struct {
	ReservationID string `json:"reservation_id"`
}

type ReleaseResult struct {
	ReservationID string `json:"reservation_id"`
	Released      bool   `json:"released"`
}

type ChargePaymentInput struct {
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
}

type PaymentResult struct {
	PaymentID   string `json:"payment_id"`
	OrderID     string `json:"order_id"`
	AmountCents int64  `json:"amount_cents"`
	Status      string `json:"status"`
}

type RefundPaymentInput struct {
	PaymentID string `json:"payment_id"`
}

type RefundResult struct {
	PaymentID string `json:"payment_id"`
	Refunded  bool   `json:"refunded"`
}

type DispatchShipmentInput struct {
	OrderID              string `json:"order_id"`
	PaymentID            string `json:"payment_id"`
	ForceDispatchFailure bool   `json:"force_dispatch_failure,omitempty"`
}

type ShipmentResult struct {
	ShipmentID string `json:"shipment_id"`
	OrderID    string `json:"order_id"`
	Status     string `json:"status"`
}
