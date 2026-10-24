# ChronoSaga API Reference

The ChronoSaga API exposes versioned RESTful endpoints over HTTP/JSON.

## 1. Base URL & Security
- Default loopback binding: `http://127.0.0.1:8080`
- Non-loopback binding: If `CHRONOSAGA_ADMIN_API_KEY` is configured in environment, administrative endpoints require header `X-API-Key: <key>`.
- Request body limit: 1 MB.

---

## 2. Endpoints

### 2.1 Start Workflow
`POST /api/v1/workflows`

Initiates a new durable workflow run or idempotently returns an existing run.

#### Headers
- `Content-Type: application/json`
- `Idempotency-Key: <unique-client-key>` (Required if not specified in body)

#### Request Body
```json
{
  "workflow_type": "order_fulfillment",
  "workflow_version": "v1",
  "input": {
    "order_id": "ord-2026-001",
    "sku": "SKU-PROD-1",
    "quantity": 2,
    "total_cents": 5999,
    "force_dispatch_failure": false
  },
  "idempotency_key": "user-unique-key-001"
}
```

#### Responses
- **201 Created**: Workflow started successfully.
  ```json
  {
    "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
    "status": "RUNNING",
    "workflow_type": "order_fulfillment",
    "workflow_version": "v1"
  }
  ```
- **200 OK**: Duplicate start request with matching idempotency key.
  ```json
  {
    "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
    "status": "RUNNING",
    "workflow_type": "order_fulfillment",
    "workflow_version": "v1",
    "is_duplicate": true
  }
  ```
- **400 Bad Request**: Invalid body, missing required fields, or unregistered workflow definition.
  ```json
  {
    "error_code": "MISSING_IDEMPOTENCY_KEY",
    "message": "Idempotency-Key header or idempotency_key field is required"
  }
  ```

---

### 2.2 Get Workflow Status
`GET /api/v1/workflows/{id}`

Retrieves the current execution status, input, output, errors, and timestamps for a workflow run.

#### Response
- **200 OK**:
  ```json
  {
    "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
    "workflow_type": "order_fulfillment",
    "workflow_version": "v1",
    "status": "COMPLETED",
    "input_json": { ... },
    "output_json": {
      "order_id": "ord-2026-001",
      "reservation_id": "res_a1b2c3d4",
      "payment_id": "pay_e5f6g7h8",
      "shipment_id": "ship_i9j0k1l2",
      "status": "FULFILLED"
    },
    "last_event_sequence": 7,
    "created_at": "2026-10-24T12:00:00Z",
    "updated_at": "2026-10-24T12:00:02Z",
    "completed_at": "2026-10-24T12:00:02Z"
  }
  ```
- **404 Not Found**:
  ```json
  {
    "error_code": "WORKFLOW_NOT_FOUND",
    "message": "workflow run not found"
  }
  ```

---

### 2.3 Get Ordered Event History
`GET /api/v1/workflows/{id}/events?after_sequence={seq}&limit={limit}`

Returns the paginated, immutable sequence of events for the workflow run, strictly sorted by contiguous `sequence_no ASC`.

#### Query Parameters
- `after_sequence` (int64, optional, default 0): Returns events strictly greater than this sequence number.
- `limit` (int, optional, default 100, max 500): Maximum events to return.

#### Response
- **200 OK**:
  ```json
  {
    "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
    "events": [
      {
        "event_id": "673f8a00-11b3-4f93-bc42-f2868194cf21",
        "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
        "sequence_no": 1,
        "event_type": "WorkflowStarted",
        "schema_version": 1,
        "payload": { ... },
        "created_at": "2026-10-24T12:00:00Z"
      }
    ],
    "has_more": false,
    "last_sequence": 7
  }
  ```

---

### 2.4 Retry Failed Compensation
`POST /api/v1/workflows/{id}/retry-compensation`

Allows an operator to trigger manual retry of a permanently failed compensating action.

#### Headers
- `X-API-Key: <key>` (Required if admin key is configured)
- `X-Operator-Name: <name>` (Optional, e.g. `admin-jane`)

#### Responses
- **200 OK**:
  ```json
  {
    "workflow_id": "9a3e2c34-89c0-4318-b2ef-37dc6a165b5f",
    "status": "COMPENSATING",
    "message": "compensation retry scheduled"
  }
  ```
- **409 Conflict**: Workflow is not in `COMPENSATION_FAILED` status.
  ```json
  {
    "error_code": "INVALID_STATE",
    "message": "workflow is not in COMPENSATING or COMPENSATION_FAILED state: current status is COMPLETED"
  }
  ```

---

### 2.5 Operational Health & Metrics
- `GET /health/live`: HTTP 200 `{"status":"UP"}` indicating the HTTP process is alive.
- `GET /health/ready`: HTTP 200 if PostgreSQL connection ping succeeds; HTTP 503 `{"status":"DOWN"}` if database is unreachable.
- `GET /metrics`: Standard Prometheus exposition text format containing counters, histograms, and latencies.
