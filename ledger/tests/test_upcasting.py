import uuid

import pytest

from ledger.event_store import EventStore
from ledger.events.upcasting import registry, upcast_credit_v1_to_v2
from ledger.schema.events import CreditAnalysisCompleted
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_upcasting_immutability(projection_env):
    event_store: EventStore = projection_env
    stream_id = f"credit-{uuid.uuid4()}"
    v1_payload = {
        "application_id": f"app-{uuid.uuid4()}",
        "session_id": f"session-{uuid.uuid4()}",
        "decision": {
            "risk_tier": "LOW",
            "recommended_limit_usd": "100000",
            "confidence": 0.91,
            "rationale": "Legacy event payload",
            "key_concerns": [],
            "data_quality_caveats": [],
        },
        "model_deployment_id": "legacy-deployment",
        "input_data_hash": "legacy-hash",
        "analysis_duration_ms": 1250,
        "completed_at": "2026-03-25T00:00:00",
    }

    example_upcast = upcast_credit_v1_to_v2(v1_payload)
    assert example_upcast["confidence_score"] is None
    assert registry._upcasters[(CreditAnalysisCompleted.__name__, 1)] is upcast_credit_v1_to_v2

    async with event_store._pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (
                stream_id,
                stream_position,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
            """,
            stream_id,
            1,
            CreditAnalysisCompleted.__name__,
            1,
            v1_payload,
            {},
        )

        events = await event_store.load_stream(stream_id)
        upcasted_event = events[0]

        assert upcasted_event.event_version == 2
        assert "model_version" in upcasted_event.payload
        assert "confidence_score" in upcasted_event.payload
        assert upcasted_event.payload["confidence_score"] is None

        raw_payload = await conn.fetchval(
            """
            SELECT payload
            FROM events
            WHERE stream_id = $1
            """,
            stream_id,
        )

    assert "model_version" not in raw_payload
    assert "confidence_score" not in raw_payload