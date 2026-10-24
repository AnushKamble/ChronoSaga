# ChronoSaga: Guarantees & System Limitations

## 1. Non-Negotiable Contract & Approved Language

> **Authoritative Statement of Semantics:**  
> "ChronoSaga persists workflow decisions and supports replay-based recovery. Activity tasks are delivered and attempted **at least once**; stable idempotency keys let cooperating activity handlers deduplicate repeated side effects. The engine does not guarantee exactly-once behavior for arbitrary external systems."

### Explicitly Prohibited Claims
The following statements are false in distributed systems and are strictly prohibited in documentation and code comments:
- ❌ *"ChronoSaga guarantees exactly-once activity execution."*
- ❌ *"Zero state loss under all failure conditions"* (without specifying the PostgreSQL commit boundary).
- ❌ *"Transactions across microservices are rolled back automatically."*
- ❌ *"Completed side effects will never be re-attempted."*
- ❌ *"Production-grade drop-in replacement for Temporal."*

---

## 2. Invariants & Guarantees Provided

### 2.1 Durability & Atomicity
- **State Commitment Boundary:** Workflow transitions, event appends, durable task creations, and timer registrations are committed to PostgreSQL prior to being visible to workers or schedulers.
- **Append-Only History:** Historical events are never updated or deleted by normal engine operations. The per-workflow event sequence is contiguous and protected by `UNIQUE(workflow_id, sequence_no)`.
- **Atomic Scheduling:** A command cannot be scheduled without atomically creating both the event row (`ActivityScheduled`, `TimerStarted`, `CompensationRegistered`) and the operational task row.

### 2.2 Deterministic Replay
- **Cached Result Replay:** If an activity has completed and its outcome is committed to `workflow_events`, subsequent replays return that result directly to the workflow function. The activity handler is **not** re-invoked.
- **Nondeterminism Detection:** The engine hashes command parameters (kind, activity name, input payload, retry options, timer duration) using canonical JSON formatting. If workflow code produces a different command or argument at an existing command index, the engine aborts with `WorkflowNondeterminismDetected` and marks the workflow `NONDETERMINISTIC`.
- **Workflow Version Pinning:** Every workflow run pins an explicit `workflow_type` and `workflow_version`. Replaying against incompatible code stops cleanly.

### 2.3 Task Queue & Fencing
- **Concurrent Worker Safety:** Multiple worker processes can claim tasks concurrently from the same PostgreSQL database without double-claiming, enforced by `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Lease Fencing:** Each claim increments a monotonic `lease_generation`. Worker completion updates are conditioned on `lease_generation` and `lease_owner`. If a worker times out and its lease expires, a newer worker will acquire a higher generation; any late acknowledgement by the stale worker will be rejected by the database.

### 2.4 Durable Timers
- **Restart Survival:** `ctx.Sleep` schedules a durable row in `timer_tasks` with a UTC `due_at` timestamp. If the orchestrator process crashes or is killed with `SIGKILL`, the timer survives in PostgreSQL and will be discovered and fired upon process restart.
- **Single-Fire Guarantee:** Timers are transitioned from `PENDING` to `FIRED` using atomic row updates, guaranteeing that a timer fires at most once even across multiple schedulers.

### 2.5 Saga Compensation
- **Strict LIFO Ordering:** Completed forward operations with registered compensations are counteracted in exact reverse order (Last-In, First-Out).
- **Durable Progress:** Compensation state, attempts, and outcomes are tracked in `compensation_tasks` and `workflow_events`. A crash during compensation does not restart the compensation chain from the beginning.
- **Explicit Failure Stop:** If a compensation activity fails terminally, the workflow enters `COMPENSATION_FAILED` and halts. It does not blindly continue to earlier compensations, avoiding cascading state corruption. Operators can trigger an explicit retry.

---

## 3. Failure Windows & Honest Limitations

### 3.1 The "Side-Effect-Before-Ack" Window
In distributed systems, an external side effect (e.g., an HTTP API call to a third-party payment gateway or warehouse) and a database commit in ChronoSaga cannot be atomically combined without a distributed consensus/2PC protocol.

```text
Worker Thread                    Downstream Service              PostgreSQL
      |                                   |                           |
      |--- 1. Call External API --------->| (Charges credit card)     |
      |<-- 2. Success 200 OK -------------|                           |
      |                                                               |
  [CRASH / SIGKILL / NETWORK DISCONNECT]                              |
      x (Worker dies before step 3)                                   |
      |                                                               |
      +--- 3. Commit ActivityCompleted (NEVER REACHED) -------------->x
```

**Consequence:**
- PostgreSQL still views the task as running or expired.
- When the lease expires, the task is reclaimed and retried by another worker.
- The activity handler is invoked a second time.

**Resolution & Engine Contract:**
- ChronoSaga generates a stable, deterministic `idempotency_key` for every activity task and compensation task.
- The engine passes this key to the activity handler.
- **Cooperating downstream systems must deduplicate using this key.** In the demo system, synthetic SQLite/PostgreSQL ledger tables enforce uniqueness on this idempotency key.
- For non-idempotent third-party APIs that do not accept idempotency keys, duplicate side effects can occur if a crash happens in this window.

### 3.2 Time & Clock Skew
- ChronoSaga relies on PostgreSQL server time (`NOW()`) for task lease expiration and timer maturity to avoid clock divergence between worker nodes.
- If worker host clocks skew significantly from PostgreSQL server time, lease duration calculations may differ slightly, but lease ownership is always strictly fenced by PostgreSQL.

### 3.3 Explicit Architectural Non-Goals
1. **No Distributed Consensus (Raft/Paxos):** ChronoSaga achieves coordination via PostgreSQL ACID transactions. High availability depends on PostgreSQL HA configurations.
2. **No Arbitrary Goroutine Stack Serialization:** Workflows must be structured as deterministic top-to-bottom Go functions communicating solely via `WorkflowContext`.
3. **No Dynamic Child Workflows or Arbitrary Branch Fan-Out:** Workflow structures are sequential or explicitly bounded.
4. **No Cross-Service 2PC:** Sagas provide eventual consistency through semantic compensation, not isolation or atomic ACID rollbacks across multiple independent databases.
