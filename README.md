# ChronoSaga

[![Go Report Card](https://goreportcard.com/badge/chronosaga)](https://goreportcard.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**ChronoSaga** is an educational, production-patterned, durable event-sourced workflow orchestrator written in Go and backed by PostgreSQL. It provides deterministic workflow replay, durable activity queues with lease fencing, exponential backoff retries, durable timers, and transactional reverse Saga compensation.

---

## Architecture Overview

```
                      +---------------------------------------+
                      |               Client                  |
                      |   (HTTP REST API / CLI / SDK)         |
                      +-------------------+-------------------+
                                          |
                                          | HTTP / JSON
                                          v
                      +---------------------------------------+
                      |          HTTP API Server              |
                      |   (/api/v1/workflows, health, metrics)|
                      +-------------------+-------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |         ChronoSaga Orchestrator       |
                      |                                       |
                      |  +---------------------------------+  |
                      |  |     Deterministic Replay Engine |  |
                      |  |  - Canonical command hashing    |  |
                      |  |  - Step-by-step history matching|  |
                      |  |  - Non-determinism detection    |  |
                      |  +----------------+----------------+  |
                      |                   |                   |
                      |  +----------------v----------------+  |
                      |  |    Saga Compensation Coordinator|  |
                      |  |  - Strict reverse LIFO order    |  |
                      |  |  - COMPENSATION_FAILED pause    |  |
                      |  |  - Operator retry support       |  |
                      |  +---------------------------------+  |
                      +-------------------+-------------------+
                                          |
                +-------------------------+-------------------------+
                |                                                   |
                v                                                   v
+-------------------------------+                   +-------------------------------+
|     Durable Activity Queue    |                   |      Durable Timer Engine     |
| - FOR UPDATE SKIP LOCKED      |                   | - UTC due_at persistence      |
| - Monotonic lease fencing     |                   | - Single-fire CAS             |
| - Exponential backoff retry   |                   | - Injected test clock         |
+---------------+---------------+                   +---------------+---------------+
                |                                                   |
                v                                                   v
+-------------------------------+                   +-------------------------------+
|       Activity Workers        |                   |       History Event Store     |
| - At-least-once dispatch      |                   | - Strictly append-only        |
| - Stable idempotency keys     |                   | - Contiguous sequence numbers |
| - Synthetic DB mock effects   |                   | - Atomic state projections    |
+---------------+---------------+                   +---------------+---------------+
                \                                                   /
                 \                                                 /
                  +-----------------------+-----------------------+
                                          |
                                          v
                      +---------------------------------------+
                      |          PostgreSQL Database          |
                      | (workflow_runs, workflow_events,      |
                      |  activity_tasks, timer_tasks,         |
                      |  compensation_tasks, demo_effects)    |
                      +---------------------------------------+
```

---

## Core Guarantees & Semantics

| Guarantee | Mechanism | Invariant |
|---|---|---|
| **Authoritative State** | PostgreSQL 14+ | PostgreSQL is the single source of truth; no in-memory queues or state machines are trusted across restarts. |
| **Strict Event Append** | `workflow_events` | Append-only ledger ordered by contiguous `sequence_no` (1, 2, 3...) per workflow; historical rows are never updated or deleted. |
| **Deterministic Replay** | `workflow.ReplayEngine` | Re-executes registered Go workflow definitions against recorded events; cached activity outputs are decoded from history without re-invoking side effects. |
| **At-Least-Once Activity Delivery** | `activity_tasks` | Activities are claimed using `FOR UPDATE SKIP LOCKED`. Fenced with monotonic `lease_generation`. Downstream systems deduplicate via stable `idempotency_key`. |
| **Reverse Saga Compensation** | `saga.Coordinator` | Upon terminal failure, completed forward steps are compensated in strict reverse (LIFO) order. Failure pauses workflow in `COMPENSATION_FAILED` for operator inspection. |
| **Durable Timers** | `timer_tasks` | Persists exact UTC `due_at`. Survives orchestrator restarts; fired exactly once via conditional lock and transactional event append. |

---

## Prerequisites

- **Go:** 1.22+ (tested on Go 1.27)
- **PostgreSQL:** 14+ (or Docker / Docker Compose). ChronoSaga also includes automated embedded PostgreSQL support for running tests and demos without requiring external dependencies.
- **Git**

---

## Quickstart & Setup

### 1. Build the Binary
```bash
go build -o bin/chronosaga ./cmd/chronosaga
# On Windows:
go build -o bin\chronosaga.exe .\cmd\chronosaga
```

### 2. Configure Environment
Copy the example environment configuration:
```bash
cp .env.example .env
```

Default variables:
```env
DATABASE_URL=postgres://chronosaga:chronosaga_secret@127.0.0.1:5432/chronosaga?sslmode=disable
CHRONOSAGA_HOST=127.0.0.1
CHRONOSAGA_PORT=8080
WORKER_CONCURRENCY=5
WORKER_LEASE_DURATION=30s
WORKER_POLL_INTERVAL=300ms
TIMER_POLL_INTERVAL=200ms
```

### 3. Apply Schema Migrations
```bash
./bin/chronosaga migrate up
```

### 4. Run Services
Start the HTTP API server:
```bash
./bin/chronosaga api
```

Start the activity worker in a separate shell:
```bash
./bin/chronosaga worker
```

---

## Interactive Demos

ChronoSaga includes end-to-end demonstrations featuring a simulated e-commerce order workflow: **Reserve Inventory** -> **Process Payment** -> **Ship Goods**.

### Demo 1: Process Interruption & Crash Recovery
Simulates an abrupt process termination (SIGKILL) while an order workflow is executing an offline durable timer. Shows how the restarted engine loads event history, detects the elapsed timer, and completes the workflow without duplicating earlier forward tasks.

```bash
# PowerShell
.\scripts\demo_crash_recovery.ps1

# Bash / Linux / macOS
./scripts/demo_crash_recovery.sh
```

### Demo 2: Reverse Saga Compensation
Simulates a downstream business failure (shipping out of delivery area). Shows the orchestrator initiating reverse LIFO compensation: refunding the payment first, then releasing inventory.

```bash
# PowerShell
.\scripts\demo_saga_failure.ps1

# Bash / Linux / macOS
./scripts/demo_saga_failure.sh
```

---

## HTTP REST API Reference

All requests accept and return JSON. The API binds to loopback (`127.0.0.1`) by default.

### 1. Start Workflow
Starts a new workflow run. If a run with the given `Idempotency-Key` already exists, returns the existing workflow without re-scheduling.

```bash
curl -X POST http://127.0.0.1:8080/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-req-1001" \
  -d '{
    "workflow_type": "OrderWorkflow",
    "workflow_version": "v1",
    "input": {
      "order_id": "ORD-1001",
      "customer_id": "CUST-42",
      "amount_cents": 4999,
      "items": ["ITEM-A", "ITEM-B"]
    }
  }'
```

**Response (HTTP 201 Created):**
```json
{
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "RUNNING",
  "workflow_type": "OrderWorkflow",
  "workflow_version": "v1",
  "is_duplicate": false
}
```

### 2. Query Workflow Status
Fetches the current projection status, sequence number, and failure details.

```bash
curl -X GET http://127.0.0.1:8080/api/v1/workflows/550e8400-e29b-41d4-a716-446655440000
```

**Response (HTTP 200 OK):**
```json
{
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "workflow_type": "OrderWorkflow",
  "workflow_version": "v1",
  "status": "COMPLETED",
  "current_sequence_no": 12,
  "created_at": "2026-10-24T09:50:00Z",
  "updated_at": "2026-10-24T09:50:05Z"
}
```

### 3. Query Event History (Cursor-Paginated)
Retrieves the ordered append-only event stream. Supports `after_sequence` and `limit`.

```bash
curl -X GET "http://127.0.0.1:8080/api/v1/workflows/550e8400-e29b-41d4-a716-446655440000/events?after_sequence=0&limit=50"
```

**Response (HTTP 200 OK):**
```json
{
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "events": [
    {
      "sequence_no": 1,
      "event_type": "WorkflowExecutionStarted",
      "timestamp": "2026-10-24T09:50:00.100Z",
      "payload": {"workflow_type": "OrderWorkflow"}
    },
    {
      "sequence_no": 2,
      "event_type": "ActivityTaskScheduled",
      "timestamp": "2026-10-24T09:50:00.120Z",
      "payload": {"activity_type": "reserve_inventory"}
    }
  ],
  "has_more": false,
  "last_sequence": 2
}
```

### 4. Retry Failed Compensation
Allows an authorized operator to resume a workflow halted in `COMPENSATION_FAILED`.

```bash
curl -X POST http://127.0.0.1:8080/api/v1/workflows/550e8400-e29b-41d4-a716-446655440000/retry-compensation \
  -H "X-API-Key: admin-secret-key" \
  -H "X-Operator-Name: alice"
```

**Response (HTTP 200 OK):**
```json
{
  "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "COMPENSATING",
  "message": "compensation retry scheduled"
}
```

### 5. Health & Observability
- **Liveness:** `GET /health/live` -> `{"status":"ok"}`
- **Readiness:** `GET /health/ready` -> verifies database connectivity and schema migrations
- **Prometheus Metrics:** `GET /metrics` -> low-cardinality counters and latency histograms

---

## Verified Benchmarks & Performance

Measured on a Windows 11 host with AMD64 architecture, 32 logical cores, and PostgreSQL 18.3:

| Metric | Result | Target / Baseline |
|---|---|---|
| **Workflow Start Throughput** | **595.48 ops/sec** | > 200 ops/sec |
| **Workflow Start Latency (p50)** | **1.64 ms** | < 10 ms |
| **Workflow Start Latency (p95)** | **2.30 ms** | < 50 ms |
| **Workflow Start Latency (p99)** | **7.83 ms** | < 100 ms |
| **Task Completion Rate** | **314.59 ops/sec** | > 100 ops/sec |
| **Replay Latency (10 events)** | **6.02 ms** | < 10 ms |
| **Replay Latency (50 events)** | **3.77 ms** | < 15 ms |
| **Replay Latency (100 events)** | **4.58 ms** | < 25 ms |
| **Average Timer Lateness** | **52.00 ms** | Bounded by 200ms poll interval |
| **Process Recovery Time** | **1.59 ms** | Instant replay on startup |

*Data sourced directly from machine-readable benchmark outputs in [`results/benchmark_report.json`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/results/benchmark_report.json).*

To execute a fresh benchmark run:
```bash
./scripts/run_benchmarks.sh
# Or on Windows:
.\scripts\run_benchmarks.ps1
```

---

## Explicit Guarantees, Non-Guarantees & Failure Windows

### 1. At-Least-Once Activity Execution
ChronoSaga provides **at-least-once activity execution**, not generic exactly-once execution.
- If a worker executes a side effect against an external service and crashes before PostgreSQL records the `ActivityTaskCompleted` event, a replacement worker will re-attempt the task.
- To prevent duplicate real-world effects, external side effects **must** be idempotent using the stable `idempotency_key` supplied in the task payload.

### 2. Leases & Fencing Tokens
- Every worker claim increments a monotonic `lease_generation`.
- When completing a task, PostgreSQL verifies that `current_generation == lease_generation`. Stale worker acknowledgments from network-partitioned or paused workers are rejected with `ErrStaleLease`.

### 3. Saga Compensation Semantics
- Compensation is an application-level compensating action, not a distributed two-phase commit (2PC) or database rollback.
- Compensations run strictly in reverse LIFO order. If a compensation fails after all retries, the workflow stops in `COMPENSATION_FAILED` to avoid inconsistent cascading failures.

---

## Documentation Index

- [`docs/architecture.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/architecture.md) — Replay algorithms, transaction matrix, and component architecture.
- [`docs/event-catalog.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/event-catalog.md) — Comprehensive schema for all 18 typed event envelopes.
- [`docs/guarantees-and-limitations.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/guarantees-and-limitations.md) — Failure windows and consistency semantics.
- [`docs/decisions.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/decisions.md) — Architectural Decision Records (ADRs 001 - 007).
- [`docs/api.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/api.md) — Full HTTP REST specification and schemas.
- [`docs/limitations.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/limitations.md) — Operational boundaries and design constraints.
- [`docs/testing-and-benchmarks.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/docs/testing-and-benchmarks.md) — Test methodology and benchmark protocols.
- [`PROJECT_STATE.md`](file:///C:/Users/anush/OneDrive/Desktop/Project/Projects/SWE/ChronoSaga/PROJECT_STATE.md) — Persistent project execution ledger and audit trail.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
