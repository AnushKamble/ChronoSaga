-- ChronoSaga Initial Schema Migration (001_initial_schema.up.sql)

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow runs projection table
CREATE TABLE IF NOT EXISTS workflow_runs (
    workflow_id UUID PRIMARY KEY,
    workflow_type VARCHAR(128) NOT NULL,
    workflow_version VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    input_json JSONB NOT NULL,
    output_json JSONB NULL,
    error_code VARCHAR(64) NULL,
    error_message TEXT NULL,
    last_event_sequence BIGINT NOT NULL DEFAULT 0,
    start_idempotency_scope VARCHAR(64) NOT NULL DEFAULT 'default',
    start_idempotency_key VARCHAR(128) NOT NULL,
    lease_owner VARCHAR(128) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    lease_generation BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT uq_workflow_runs_idempotency UNIQUE(start_idempotency_scope, start_idempotency_key),
    CONSTRAINT chk_workflow_runs_status CHECK (
        status IN (
            'RUNNING',
            'WAITING_ACTIVITY',
            'WAITING_TIMER',
            'COMPENSATING',
            'COMPLETED',
            'FAILED',
            'COMPENSATED',
            'COMPENSATION_FAILED',
            'NONDETERMINISTIC'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_status ON workflow_runs(status);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_lease ON workflow_runs(status, lease_expires_at);

-- Workflow events append-only history table
CREATE TABLE IF NOT EXISTS workflow_events (
    event_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_runs(workflow_id) ON DELETE CASCADE,
    sequence_no BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    command_index INT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_workflow_events_sequence UNIQUE(workflow_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_workflow_events_wid_seq ON workflow_events(workflow_id, sequence_no ASC);

-- Activity tasks durable queue table
CREATE TABLE IF NOT EXISTS activity_tasks (
    task_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_runs(workflow_id) ON DELETE CASCADE,
    command_index INT NOT NULL,
    task_kind VARCHAR(32) NOT NULL DEFAULT 'FORWARD',
    activity_type VARCHAR(128) NOT NULL,
    input_payload JSONB NOT NULL,
    input_hash VARCHAR(64) NOT NULL,
    retry_policy JSONB NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(128) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    lease_generation BIGINT NOT NULL DEFAULT 0,
    last_error_code VARCHAR(64) NULL,
    last_error_message TEXT NULL,
    result_payload JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_activity_tasks_cmd UNIQUE(workflow_id, command_index, task_kind),
    CONSTRAINT chk_activity_tasks_status CHECK (
        status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT')
    )
);

CREATE INDEX IF NOT EXISTS idx_activity_tasks_claim 
    ON activity_tasks(status, available_at ASC) 
    WHERE status IN ('PENDING', 'RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_activity_tasks_lease 
    ON activity_tasks(status, lease_expires_at) 
    WHERE status = 'RUNNING';

-- Timer tasks durable table
CREATE TABLE IF NOT EXISTS timer_tasks (
    timer_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_runs(workflow_id) ON DELETE CASCADE,
    command_index INT NOT NULL,
    duration_ms BIGINT NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    fired_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_timer_tasks_cmd UNIQUE(workflow_id, command_index),
    CONSTRAINT chk_timer_tasks_status CHECK (status IN ('PENDING', 'FIRED'))
);

CREATE INDEX IF NOT EXISTS idx_timer_tasks_due 
    ON timer_tasks(status, due_at ASC) 
    WHERE status = 'PENDING';

-- Compensation tasks durable table
CREATE TABLE IF NOT EXISTS compensation_tasks (
    compensation_id UUID PRIMARY KEY,
    workflow_id UUID NOT NULL REFERENCES workflow_runs(workflow_id) ON DELETE CASCADE,
    order_index INT NOT NULL,
    forward_command_index INT NOT NULL,
    compensation_type VARCHAR(128) NOT NULL,
    input_payload JSONB NOT NULL,
    input_hash VARCHAR(64) NOT NULL,
    retry_policy JSONB NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempt INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner VARCHAR(128) NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    lease_generation BIGINT NOT NULL DEFAULT 0,
    last_error_code VARCHAR(64) NULL,
    last_error_message TEXT NULL,
    result_payload JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_compensation_tasks_order UNIQUE(workflow_id, order_index),
    CONSTRAINT chk_compensation_tasks_status CHECK (
        status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT')
    )
);

CREATE INDEX IF NOT EXISTS idx_comp_tasks_runnable 
    ON compensation_tasks(workflow_id, order_index DESC, status);

-- Demo domain synthetic business effects tables (for idempotency and compensation verification)
CREATE TABLE IF NOT EXISTS demo_effects (
    effect_id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    effect_type VARCHAR(64) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS demo_inventory (
    sku VARCHAR(64) PRIMARY KEY,
    available_quantity INT NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity INT NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0)
);

CREATE TABLE IF NOT EXISTS demo_payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    amount_cents BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
