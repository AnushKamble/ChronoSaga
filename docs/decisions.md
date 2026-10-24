# ChronoSaga Architecture Decision Records (ADRs)

## ADR 001: Language & Architecture Baseline
- **Status:** Accepted
- **Context:** ChronoSaga is designed as an educational, high-integrity durable workflow orchestrator.
- **Decision:** Implement ChronoSaga in Go with standard library idioms, explicit error wrapping, `context.Context` propagation, and unified binary entrypoints (`cmd/chronosaga/main.go` supporting `api` and `worker` modes).
- **Consequences:** Clean multi-process execution, zero opaque reflection frameworks, and verifiable memory safety under `-race`.

## ADR 002: PostgreSQL as Authoritative Store & Durable Queue
- **Status:** Accepted
- **Context:** Distributed systems need reliable queuing, append-only history, and atomic transitions. Some designs propose Redis or SQLite alongside Postgres.
- **Decision:** Use PostgreSQL as the sole authoritative storage engine for workflow runs, append-only events, activity queues, timer states, and compensation chains. Use `SELECT ... FOR UPDATE SKIP LOCKED` for worker task claims. Reject SQLite because it lacks row-level `SKIP LOCKED` and concurrent multi-process writer throughput.
- **Consequences:** Eliminates dual-write anomalies. Atomically commits event appends alongside task insertions in a single transaction.

## ADR 003: Deterministic Replay Model & Suspension Sentinel
- **Status:** Accepted
- **Context:** Workflows need to re-execute code to reconstruct state without re-running external activities.
- **Decision:** Workflow definitions are registered Go functions taking `*WorkflowContext`. The context assigns monotonically increasing command indices and canonical SHA-256 hashes of inputs and options. When an unresolved or first-time activity or timer is scheduled, replay halts via an internal private sentinel error `errSuspended`. If user code panics unexpectedly, the panic is caught and mapped to `WorkflowFailed`, never misclassified as suspension.
- **Consequences:** Eliminates manual step-state machines while guaranteeing safe, deterministic code replay.

## ADR 004: At-Least-Once Delivery & Worker Lease Fencing
- **Status:** Accepted
- **Context:** External side effects cannot be guaranteed "exactly once" across worker crash boundaries.
- **Decision:** Activity dispatch is explicitly modeled as **at-least-once**. Each task claim increments a monotonic `lease_generation` fencing token and assigns a `lease_expires_at` deadline. Completions and failures must match the active `lease_generation`. Tasks provide a stable, deterministic `idempotency_key` so downstream side-effect handlers can deduplicate invocations.
- **Consequences:** Eliminates stale worker write-back anomalies. Requires downstream services to enforce idempotency on side effects.

## ADR 005: Reverse (LIFO) Saga Compensation Engine
- **Status:** Accepted
- **Context:** When a distributed business process encounters an unrecoverable failure midway through execution, completed steps must be counteracted.
- **Decision:** Compensations are registered in code only after the corresponding forward activity succeeds. On terminal workflow failure, registered compensations are scheduled one by one in reverse order (LIFO). If a compensation fails permanently, the workflow transitions to `COMPENSATION_FAILED`, pausing the chain to prevent out-of-order cascading side effects and allowing an operator to inspect and retry.
- **Consequences:** Prevents partial inconsistent rollbacks. Provides explicit audit trail of every compensating action.

## ADR 006: Durable Timers via PostgreSQL Scheduling
- **Status:** Accepted
- **Context:** In-memory timers (`time.Sleep` or Go timers) are lost when a process restarts or crashes.
- **Decision:** `ctx.Sleep` creates a durable row in `timer_tasks` with an absolute UTC `due_at` timestamp and an immutable `TimerStarted` event. A dedicated timer poller fires due timers by atomically transitioning status to `FIRED` and appending `TimerFired`, which wakes the workflow for replay.
- **Consequences:** Timers survive process crashes and node reboots with minimal firing jitter.

## ADR 007: Integration & Process-Level Testing Harness
- **Status:** Accepted
- **Context:** Unit tests using in-memory mocks cannot validate database transaction boundaries, locking semantics, or real process crash-recovery.
- **Decision:** Integration tests run against a real PostgreSQL instance. Crash tests spawn actual OS subprocesses (API, Worker, Orchestrator) and terminate them abruptly with `SIGKILL` / process kill signals to prove recovery of pending tasks, timers, and compensation chains from PostgreSQL state.
- **Consequences:** Absolute confidence in distributed systems invariants and crash-recovery semantics.
