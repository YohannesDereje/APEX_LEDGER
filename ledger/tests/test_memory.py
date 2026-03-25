import uuid
from datetime import datetime, timezone

import pytest

from ledger.agents.memory import reconstruct_agent_context
from ledger.schema.events import AgentNodeExecuted, AgentSessionStarted, AgentToolCalled
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_agent_context_reconstruction(projection_env):
    event_store = projection_env

    session_id = f"session-{uuid.uuid4()}"
    stream_id = f"agent-{session_id}"

    events = [
        AgentSessionStarted(
            session_id=session_id,
            agent_type="DOCUMENT_PROCESSING",
            agent_id="test-agent",
            application_id=f"app-{uuid.uuid4()}",
            model_version="v1",
            langgraph_graph_version="graph-v1",
            context_source="replay",
            context_token_count=250,
            started_at=datetime.now(timezone.utc),
        ),
        AgentNodeExecuted(
            session_id=session_id,
            agent_type="DOCUMENT_PROCESSING",
            node_name="parse_input",
            node_sequence=1,
            input_keys=["document"],
            output_keys=["parsed_document"],
            llm_called=False,
            llm_tokens_input=None,
            llm_tokens_output=None,
            llm_cost_usd=None,
            duration_ms=120,
            executed_at=datetime.now(timezone.utc),
        ),
        AgentNodeExecuted(
            session_id=session_id,
            agent_type="DOCUMENT_PROCESSING",
            node_name="extract_entities",
            node_sequence=2,
            input_keys=["parsed_document"],
            output_keys=["entities"],
            llm_called=True,
            llm_tokens_input=800,
            llm_tokens_output=220,
            llm_cost_usd=0.03,
            duration_ms=560,
            executed_at=datetime.now(timezone.utc),
        ),
        AgentNodeExecuted(
            session_id=session_id,
            agent_type="DOCUMENT_PROCESSING",
            node_name="validate_policy",
            node_sequence=3,
            input_keys=["entities"],
            output_keys=["validation_result"],
            llm_called=False,
            llm_tokens_input=None,
            llm_tokens_output=None,
            llm_cost_usd=None,
            duration_ms=180,
            executed_at=datetime.now(timezone.utc),
        ),
        AgentToolCalled(
            session_id=session_id,
            agent_type="DOCUMENT_PROCESSING",
            tool_name="sanctions_lookup",
            tool_input_summary="beneficial_owner=John Doe",
            tool_output_summary="pending",
            tool_duration_ms=45,
            called_at=datetime.now(timezone.utc),
        ),
    ]

    await event_store.append(
        stream_id=stream_id,
        events=events,
        expected_version=0,
        aggregate_type="AgentSession",
    )

    context = await reconstruct_agent_context(
        event_store,
        agent_id="test-agent",
        session_id=session_id,
    )

    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert context.last_event_position == len(events)
    assert context.pending_work
    assert any("Reconcile" in item or "reconcile" in item for item in context.pending_work)
    assert "validate_policy" in context.context_text
