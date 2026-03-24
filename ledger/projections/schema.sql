CREATE TABLE application_summary (
    application_id            TEXT PRIMARY KEY,
    state                     TEXT,
    applicant_id              TEXT,
    requested_amount_usd      DECIMAL,
    approved_amount_usd       DECIMAL,
    risk_tier                 TEXT,
    fraud_score               FLOAT,
    compliance_status         TEXT,
    decision                  TEXT,
    agent_sessions_completed  TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    last_event_type           TEXT,
    last_event_at             TIMESTAMPTZ,
    human_reviewer_id         TEXT,
    final_decision_at         TIMESTAMPTZ
);

CREATE TABLE agent_performance_ledger (
    agent_id              TEXT NOT NULL,
    model_version         TEXT NOT NULL,
    analyses_completed    INT NOT NULL DEFAULT 0,
    decisions_generated   INT NOT NULL DEFAULT 0,
    avg_confidence_score  FLOAT,
    avg_duration_ms       FLOAT,
    approve_rate          FLOAT,
    decline_rate          FLOAT,
    refer_rate            FLOAT,
    human_override_rate   FLOAT,
    first_seen_at         TIMESTAMPTZ,
    last_seen_at          TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);

CREATE TABLE compliance_audit_snapshots (
    snapshot_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id         TEXT NOT NULL,
    state                  JSONB NOT NULL,
    state_as_of_event_at   TIMESTAMPTZ NOT NULL,
    source_event_id        UUID NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_compliance_audit_snapshots_app_time
    ON compliance_audit_snapshots (application_id, state_as_of_event_at);
