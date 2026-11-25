"""Database schema.

Constraints act as a *second* line of defence. Even if a service had a bug,
the database itself would refuse:

* an agent bound to two calls (``ux_agents_current_call``, ``ux_calls_active_agent``),
* a borrower with two active calls (``ux_calls_active_borrower``),
* the same attempt being created twice by two workers (``UNIQUE(borrower_id, attempt_seq)``),
* a permit being over-spent (``CHECK(consumed <= approved)``),
* the same provider event being applied twice (``UNIQUE(provider, event_id)``),
* inconsistent agent/borrower rows (CHECK constraints tying status to the call id).
"""

ACTIVE_CALL_STATES_SQL = "('QUEUED','RESERVED','INITIATED','RINGING','ANSWERED','CONNECTED')"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS campaigns (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    mode                 TEXT NOT NULL CHECK (mode IN ('PROGRESSIVE','PREDICTIVE')),
    status               TEXT NOT NULL CHECK (status IN ('DRAFT','RUNNING','PAUSED','COMPLETED')),
    max_attempts         INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    retry_delay_s        REAL NOT NULL DEFAULT 300 CHECK (retry_delay_s >= 0),
    max_concurrent_calls INTEGER NOT NULL DEFAULT 1000 CHECK (max_concurrent_calls > 0),
    created_at           REAL NOT NULL,
    updated_at           REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL CHECK (status IN
                       ('OFFLINE','AVAILABLE','RESERVED','DIALING','CONNECTED','WRAP_UP','PAUSED')),
    current_call_id  TEXT,
    reserved_by      TEXT,
    reserved_at      REAL,
    reserved_until   REAL,
    wrap_up_until    REAL,
    pause_requested  INTEGER NOT NULL DEFAULT 0 CHECK (pause_requested IN (0,1)),
    last_activity_at REAL NOT NULL,
    version          INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    -- an agent is attached to a call if and only if it is RESERVED/DIALING/CONNECTED
    CHECK ((status IN ('RESERVED','DIALING','CONNECTED')) = (current_call_id IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_agents_current_call ON agents(current_call_id)
    WHERE current_call_id IS NOT NULL;
-- Matches reserve_any's ORDER BY exactly, so picking the longest-idle agent is an
-- index walk, not a sort of every idle agent.
CREATE INDEX IF NOT EXISTS ix_agents_status ON agents(status, pause_requested, last_activity_at, id);

CREATE TABLE IF NOT EXISTS borrowers (
    id               TEXT PRIMARY KEY,
    campaign_id      TEXT NOT NULL REFERENCES campaigns(id),
    phone            TEXT NOT NULL,
    name             TEXT,
    status           TEXT NOT NULL CHECK (status IN
                       ('PENDING','RESERVED','RETRY_WAIT','COMPLETED','EXHAUSTED','DO_NOT_CALL')),
    priority         INTEGER NOT NULL DEFAULT 0,
    attempt_count    INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    call_count       INTEGER NOT NULL DEFAULT 0 CHECK (call_count >= 0),
    last_attempt_at  REAL,
    next_eligible_at REAL NOT NULL DEFAULT 0,
    active_call_id   TEXT,
    reserved_by      TEXT,
    reserved_until   REAL,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    UNIQUE (campaign_id, phone),
    CHECK ((status = 'RESERVED') = (active_call_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS ix_borrowers_eligible
    ON borrowers(campaign_id, status, next_eligible_at, priority DESC);
-- Walk order for BorrowerRepository.reserve_next (priority first, no sort).
CREATE INDEX IF NOT EXISTS ix_borrowers_pick
    ON borrowers(campaign_id, status, priority DESC, next_eligible_at, id);

CREATE TABLE IF NOT EXISTS safety_decisions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       REAL NOT NULL,
    campaign_id      TEXT NOT NULL REFERENCES campaigns(id),
    engine           TEXT NOT NULL,
    safety_mode      TEXT NOT NULL,
    binding          TEXT NOT NULL,
    requested        INTEGER NOT NULL,
    approved         INTEGER NOT NULL,
    rejected         INTEGER NOT NULL,
    decision         TEXT NOT NULL CHECK (decision IN
                       ('APPROVED','REDUCED','REJECTED','FALLBACK_TO_PROGRESSIVE')),
    reason           TEXT NOT NULL,
    capacity_json    TEXT NOT NULL,
    explanation_json TEXT NOT NULL,
    CHECK (approved >= 0 AND approved <= requested AND rejected = requested - approved)
);
CREATE INDEX IF NOT EXISTS ix_decisions_campaign ON safety_decisions(campaign_id, id);

CREATE TABLE IF NOT EXISTS dial_permits (
    id           TEXT PRIMARY KEY,
    campaign_id  TEXT NOT NULL REFERENCES campaigns(id),
    decision_id  INTEGER NOT NULL REFERENCES safety_decisions(id),
    binding      TEXT NOT NULL CHECK (binding IN ('BOUND','UNBOUND')),
    approved     INTEGER NOT NULL CHECK (approved > 0),
    consumed     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL CHECK (status IN ('ACTIVE','EXHAUSTED','EXPIRED','REVOKED')),
    issued_at    REAL NOT NULL,
    expires_at   REAL NOT NULL,
    CHECK (consumed >= 0 AND consumed <= approved)
);
CREATE INDEX IF NOT EXISTS ix_permits_active ON dial_permits(status, expires_at);

CREATE TABLE IF NOT EXISTS calls (
    id               TEXT PRIMARY KEY,
    campaign_id      TEXT NOT NULL REFERENCES campaigns(id),
    borrower_id      TEXT NOT NULL REFERENCES borrowers(id),
    agent_id         TEXT REFERENCES agents(id),
    binding          TEXT NOT NULL CHECK (binding IN ('BOUND','UNBOUND')),
    permit_id        TEXT REFERENCES dial_permits(id),
    attempt_seq      INTEGER NOT NULL,
    provider         TEXT,
    provider_call_id TEXT,
    status           TEXT NOT NULL CHECK (status IN ('QUEUED','RESERVED','INITIATED','RINGING',
                       'ANSWERED','CONNECTED','COMPLETED','FAILED','CANCELLED')),
    outcome          TEXT,
    -- Cancellation is a *request* until the provider confirms the call ended
    -- unanswered. If the borrower answers first, the call is handled normally.
    cancel_reason       TEXT,
    cancel_requested_at REAL,
    last_event_seq   INTEGER NOT NULL DEFAULT 0,
    last_event_at    REAL,
    lease_owner      TEXT,
    lease_until      REAL,
    setup_latency_ms REAL,
    created_at       REAL NOT NULL,
    initiated_at     REAL,
    answered_at      REAL,
    connected_at     REAL,
    ended_at         REAL,
    talk_duration_s  REAL,
    updated_at       REAL NOT NULL,
    UNIQUE (borrower_id, attempt_seq),
    UNIQUE (provider, provider_call_id)
);
-- At most one active call per borrower and per agent, enforced by the database.
CREATE UNIQUE INDEX IF NOT EXISTS ux_calls_active_borrower ON calls(borrower_id)
    WHERE status IN {ACTIVE_CALL_STATES_SQL};
CREATE UNIQUE INDEX IF NOT EXISTS ux_calls_active_agent ON calls(agent_id)
    WHERE agent_id IS NOT NULL AND status IN {ACTIVE_CALL_STATES_SQL};
CREATE INDEX IF NOT EXISTS ix_calls_status ON calls(status, binding);
CREATE INDEX IF NOT EXISTS ix_calls_campaign_status ON calls(campaign_id, status);

-- Event inbox: the UNIQUE key is the idempotency guarantee.
CREATE TABLE IF NOT EXISTS provider_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    provider         TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    call_id          TEXT,
    provider_call_id TEXT,
    event_type       TEXT NOT NULL,
    sequence         INTEGER,
    occurred_at      REAL,
    received_at      REAL NOT NULL,
    result           TEXT NOT NULL,
    detail           TEXT,
    payload          TEXT,
    duplicate_count  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (provider, event_id)
);
CREATE INDEX IF NOT EXISTS ix_events_call ON provider_events(call_id, id);

CREATE TABLE IF NOT EXISTS provider_health (
    provider             TEXT PRIMARY KEY,
    state                TEXT NOT NULL CHECK (state IN ('CLOSED','OPEN','HALF_OPEN')),
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    open_count           INTEGER NOT NULL DEFAULT 0,   -- consecutive openings (drives backoff)
    total_openings       INTEGER NOT NULL DEFAULT 0,
    opened_at            REAL,
    retry_at             REAL,
    half_open_in_flight  INTEGER NOT NULL DEFAULT 0,
    total_requests       INTEGER NOT NULL DEFAULT 0,
    total_failures       INTEGER NOT NULL DEFAULT 0,
    total_timeouts       INTEGER NOT NULL DEFAULT 0,
    ewma_latency_ms      REAL NOT NULL DEFAULT 0,
    ewma_failure_rate    REAL NOT NULL DEFAULT 0,
    updated_at           REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS pacing_stats (
    campaign_id         TEXT PRIMARY KEY REFERENCES campaigns(id),
    answer_rate_ewma    REAL NOT NULL,
    answer_rate_fast    REAL NOT NULL,
    answer_samples      INTEGER NOT NULL DEFAULT 0,
    answered_count      INTEGER NOT NULL DEFAULT 0,
    talk_time_ewma      REAL NOT NULL,
    talk_samples        INTEGER NOT NULL DEFAULT 0,
    setup_time_ewma     REAL NOT NULL,
    abandoned_count     INTEGER NOT NULL DEFAULT 0,
    updated_at          REAL NOT NULL
);

-- Logged-in agent counts over time, so the controller can detect sudden drops.
CREATE TABLE IF NOT EXISTS agent_availability_samples (
    at        REAL NOT NULL,
    logged_in INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_avail_at ON agent_availability_samples(at);
"""
