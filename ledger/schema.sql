-- The foundational schema for The Ledger event store.
-- This defines the contract for how events and stream metadata are stored.

-- 1. The main event log. Append-only.
CREATE TABLE events (
	event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	stream_id       TEXT NOT NULL,
	stream_position BIGINT NOT NULL,
	global_position BIGINT GENERATED ALWAYS AS IDENTITY,
	event_type      TEXT NOT NULL,
	event_version   SMALLINT NOT NULL DEFAULT 1,
	payload         JSONB NOT NULL,
	metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
	recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    
	-- Ensures that a stream can't have two events at the same position.
	CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Indexes for efficient querying.
CREATE INDEX idx_events_stream_id ON events (stream_id);
CREATE INDEX idx_events_global_pos ON events (global_position);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_recorded ON events (recorded_at);


-- 2. A metadata table for streams.
CREATE TABLE event_streams (
	stream_id       TEXT PRIMARY KEY,
	aggregate_type  TEXT NOT NULL,
	current_version BIGINT NOT NULL DEFAULT 0,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	archived_at     TIMESTAMPTZ,
	metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);


-- 3. A table to track the progress of projections.
CREATE TABLE projection_checkpoints (
	projection_name TEXT PRIMARY KEY,
	last_position   BIGINT NOT NULL DEFAULT 0,
	updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- 4. An outbox table for reliable integration with external systems.
CREATE TABLE outbox (
	id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	event_id        UUID NOT NULL REFERENCES events(event_id),
	destination     TEXT NOT NULL,
	payload         JSONB NOT NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	published_at    TIMESTAMPTZ,
	attempts        SMALLINT NOT NULL DEFAULT 0
);

