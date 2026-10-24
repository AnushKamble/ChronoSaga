# ChronoSaga Event Catalog

This document specifies the authoritative catalog of immutable workflow events stored in `workflow_events`. Every event record contains an envelope and a typed JSON payload.

## 1. Event Envelope Specification

Each event row in `workflow_events` conforms to the following schema:

| Column | Type | Description |
|---|---|---|
| `event_id` | `UUID PRIMARY KEY` | Globally unique event identifier |
| `workflow_id` | `UUID NOT NULL REFERENCES workflow_runs(workflow_id)` | Scoped workflow instance ID |
| `sequence_no` | `BIGINT NOT NULL` | Contiguous 1-based sequence number per workflow |
| `event_type` | `VARCHAR(64) NOT NULL` | Name of the state transition event |
| `schema_version` | `INT NOT NULL DEFAULT 1` | Payload schema version |
| `command_index` | `INT NULL` | Associated deterministic workflow command index (0-based) |
| `payload` | `JSONB NOT NULL` | Structured, typed event payload |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Server timestamp of creation |

Constraint: `UNIQUE (workflow_id, sequence_no)`.

---

## 2. Event Types & Payloads

### 2.1 Workflow Lifecycle Events

#### `WorkflowStarted`
- **Schema Version:** 1
- **Producer:** HTTP API (`POST /api/v1/workflows`)
- **Consumer:** Replay Engine
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "workflow_type": "order_fulfillment",
    "workflow_version": "v1",
    "input": { ... },
    "start_idempotency_key": "user-supplied-key"
  }
  ```
- **Description:** Marks the creation and initial dispatch eligibility of a workflow run.

#### `WorkflowCompleted`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** API Readers, Clients
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "output": { ... }
  }
  ```
- **Description:** Emitted when the registered workflow function returns cleanly without errors.

#### `WorkflowFailed`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** API Readers, Clients
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "error_code": "INVALID_INPUT",
    "error_message": "SKU not found",
    "terminal": true
  }
  ```
- **Description:** Emitted when the workflow function returns an unrecoverable error and no compensations were registered.

#### `WorkflowNondeterminismDetected`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** Operator, API Readers
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "command_index": 2,
    "expected_command_type": "ACTIVITY",
    "expected_name": "charge_payment",
    "expected_hash": "a1b2c3...",
    "actual_command_type": "ACTIVITY",
    "actual_name": "dispatch_shipment",
    "actual_hash": "d4e5f6..."
  }
  ```
- **Description:** Emitted when workflow code replayed against history emits a command that does not match historical records at that command index.

---

### 2.2 Activity Task Events

#### `ActivityScheduled`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** Activity Workers, Replay Engine
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "input": { "order_id": "123", "sku": "A", "quantity": 1 },
    "input_hash": "sha256...",
    "retry_policy": {
      "max_attempts": 3,
      "initial_backoff_ms": 1000,
      "max_backoff_ms": 10000
    },
    "idempotency_key": "cs-act-uuid-idx-0"
  }
  ```
- **Description:** Records that an activity command was encountered for the first time and scheduled in `activity_tasks`.

#### `ActivityAttemptStarted`
- **Schema Version:** 1
- **Producer:** Activity Worker (Task Claim)
- **Consumer:** Observability, Auditing
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "attempt": 1,
    "lease_owner": "worker-pod-1",
    "lease_generation": 1
  }
  ```
- **Description:** Emitted atomically when a worker successfully claims an activity task via `SKIP LOCKED`.

#### `ActivityAttemptFailed`
- **Schema Version:** 1
- **Producer:** Activity Worker
- **Consumer:** Retry Scheduler, Observability
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "attempt": 1,
    "error_code": "NETWORK_TIMEOUT",
    "error_message": "connection reset by peer",
    "retryable": true
  }
  ```
- **Description:** Emitted when an activity execution attempt produces an error.

#### `ActivityRetryScheduled`
- **Schema Version:** 1
- **Producer:** Activity Worker / Retry Manager
- **Consumer:** Worker Claim Engine
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "attempt": 1,
    "next_attempt": 2,
    "available_at": "2026-10-24T12:00:02Z",
    "backoff_ms": 2000
  }
  ```
- **Description:** Records the calculated backoff deadline for the next retry attempt.

#### `ActivityCompleted`
- **Schema Version:** 1
- **Producer:** Activity Worker
- **Consumer:** Replay Engine
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "result": { "reservation_id": "res_987" }
  }
  ```
- **Description:** Emitted when an activity succeeds. Contains the recorded result payload that will be replayed in future replays without calling the handler again.

#### `ActivityFailedFinal`
- **Schema Version:** 1
- **Producer:** Activity Worker / Retry Manager
- **Consumer:** Replay Engine
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "task_id": "UUID",
    "activity_type": "reserve_inventory",
    "total_attempts": 3,
    "error_code": "INSUFFICIENT_STOCK",
    "error_message": "sku out of stock"
  }
  ```
- **Description:** Emitted when an activity has exhausted all retry attempts or encountered an explicitly non-retryable error.

---

### 2.3 Saga Compensation Events

#### `CompensationRegistered`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** Saga Orchestrator
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "compensation_id": "UUID",
    "order_index": 1,
    "forward_command_index": 0,
    "compensation_type": "release_inventory",
    "input": { "reservation_id": "res_987" },
    "input_hash": "sha256...",
    "idempotency_key": "cs-comp-uuid-order-1"
  }
  ```
- **Description:** Records a compensation action registered by workflow code following the success of a forward activity.

#### `WorkflowCompensationStarted`
- **Schema Version:** 1
- **Producer:** Orchestrator Engine
- **Consumer:** Saga Orchestrator, Observability
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "trigger_error_code": "PAYMENT_FAILED",
    "total_compensations": 2
  }
  ```
- **Description:** Emitted when a workflow begins executing its reverse compensation sequence.

#### `CompensationAttemptStarted`
- **Schema Version:** 1
- **Producer:** Activity Worker
- **Consumer:** Observability
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "compensation_id": "UUID",
    "compensation_type": "release_inventory",
    "attempt": 1,
    "lease_owner": "worker-1",
    "lease_generation": 1
  }
  ```
- **Description:** Worker claims a compensation task.

#### `CompensationCompleted`
- **Schema Version:** 1
- **Producer:** Activity Worker
- **Consumer:** Saga Orchestrator
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "compensation_id": "UUID",
    "compensation_type": "release_inventory",
    "result": { "released": true }
  }
  ```
- **Description:** Emitted when a compensating activity successfully completes.

#### `CompensationFailed`
- **Schema Version:** 1
- **Producer:** Activity Worker
- **Consumer:** Saga Orchestrator, Operator API
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "compensation_id": "UUID",
    "compensation_type": "release_inventory",
    "attempts": 3,
    "error_code": "DOWNSTREAM_UNAVAILABLE",
    "error_message": "failed to release lock"
  }
  ```
- **Description:** Emitted when a compensating activity fails permanently. Transitions the workflow to `COMPENSATION_FAILED`.

#### `CompensationRetryRequested`
- **Schema Version:** 1
- **Producer:** HTTP API (`POST /api/v1/workflows/{id}/retry-compensation`)
- **Consumer:** Saga Orchestrator
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "compensation_id": "UUID",
    "requested_by": "operator"
  }
  ```
- **Description:** Emitted when an operator requests retry of a failed compensation.

#### `WorkflowCompensated`
- **Schema Version:** 1
- **Producer:** Saga Orchestrator
- **Consumer:** API Readers, Clients
- **Command Index:** `NULL`
- **Payload Fields:**
  ```json
  {
    "completed_compensations": 2
  }
  ```
- **Description:** Emitted when all registered compensations have successfully executed in reverse order.

---

### 2.4 Durable Timer Events

#### `TimerStarted`
- **Schema Version:** 1
- **Producer:** Replay Engine
- **Consumer:** Timer Scheduler
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "timer_id": "UUID",
    "duration_ms": 5000,
    "due_at": "2026-10-24T12:00:05Z"
  }
  ```
- **Description:** Records a `ctx.Sleep` command and schedules a timer in `timer_tasks`.

#### `TimerFired`
- **Schema Version:** 1
- **Producer:** Timer Scheduler
- **Consumer:** Replay Engine
- **Command Index:** `<index>`
- **Payload Fields:**
  ```json
  {
    "timer_id": "UUID",
    "fired_at": "2026-10-24T12:00:05.012Z"
  }
  ```
- **Description:** Emitted atomically when a pending timer is claimed and fired, waking the workflow for re-execution.
