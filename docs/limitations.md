# ChronoSaga: Invariants, Guarantees & Honest Boundaries

## 1. System Invariants

1. **PostgreSQL Single Source of Durability:**
   Workflow decisions, event history logs, task queues, lease generations, and timer maturity state are persisted exclusively in PostgreSQL transactions.
2. **Deterministic Replay Guarantee:**
   When replaying workflow code against recorded history, completed activity results are decoded directly from stored history. Handlers are never executed again for recorded steps.
3. **Command Mismatch Halting:**
   If a workflow definition produces a command index, activity type, or canonical hash that conflicts with historical records, the engine appends `WorkflowNondeterminismDetected` and marks the workflow `NONDETERMINISTIC`.
4. **At-Least-Once Delivery & Lease Fencing:**
   Tasks are claimed via `SELECT ... FOR UPDATE SKIP LOCKED` and fenced with monotonically increasing `lease_generation` tokens. Stale worker write-backs are rejected.
5. **Reverse (LIFO) Saga Compensation:**
   Compensations execute strictly in reverse registration order. Failed compensations pause the chain in `COMPENSATION_FAILED` to prevent cascading state corruption until an operator triggers retry.
6. **Durable Timers:**
   `ctx.Sleep` registers an absolute UTC `due_at` timestamp in PostgreSQL. Overdue timers survive crashes and fire exactly once.

---

## 2. Failure Windows & Limitations

1. **External Side-Effect Uncertainty:**
   If a worker completes an external API call (e.g. charging a payment) and crashes before committing `ActivityCompleted`, the task lease will expire and be retried by another worker. Downstream systems must deduplicate using ChronoSaga's stable `idempotency_key`.
2. **Clock Skew Assumptions:**
   Leases and timers evaluate maturity against PostgreSQL server time (`NOW()`). Host system clocks should be synchronized via NTP.
3. **Unsupported Scope:**
   - No arbitrary cross-goroutine thread serialization.
   - No distributed consensus (Raft/Paxos) clusters; durability relies on PostgreSQL ACID properties.
   - No dynamic child workflow hierarchies.
