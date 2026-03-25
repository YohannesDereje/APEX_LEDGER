from typing import List

from pydantic import BaseModel

from ledger.event_store import EventStore
from ledger.schema.events import AgentNodeExecuted, AgentToolCalled


class AgentContext(BaseModel):
    context_text: str
    last_event_position: int
    pending_work: List[str]
    session_health_status: str


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    stream_id = f"agent-{session_id}"
    events = await store.load_stream(stream_id)

    if not events:
        return AgentContext(
            context_text="No prior session activity.",
            last_event_position=0,
            pending_work=[],
            session_health_status="HEALTHY",
        )

    last_node_event = None
    last_tool_event = None
    for event in events:
        if event.event_type == AgentNodeExecuted.__name__:
            last_node_event = event
        elif event.event_type == AgentToolCalled.__name__:
            last_tool_event = event

    last_node_name = (
        last_node_event.payload.get("node_name", "unknown")
        if last_node_event
        else "none"
    )
    last_tool_name = (
        last_tool_event.payload.get("tool_name", "none")
        if last_tool_event
        else "none"
    )

    summary_text = (
        f"Session started. {len(events)} events have occurred. "
        f"The last node executed was '{last_node_name}'. "
        f"The last tool called was '{last_tool_name}'."
    )

    recent_events = events[-3:]
    recent_event_lines = [
        f"- pos={event.stream_position} type={event.event_type} payload={event.payload}"
        for event in recent_events
    ]
    recent_events_text = "Recent events:\n" + "\n".join(recent_event_lines)

    full_context_text = f"{summary_text}\n\n{recent_events_text}"
    if token_budget > 0 and len(full_context_text) > token_budget:
        full_context_text = full_context_text[:token_budget]

    last_event = events[-1]
    session_health_status = "HEALTHY"
    pending_work: List[str] = []

    if last_event.event_type == AgentToolCalled.__name__:
        session_health_status = "NEEDS_RECONCILIATION"
        pending_work.append(
            f"Reconcile unfinished tool call '{last_event.payload.get('tool_name', 'unknown')}'"
        )

    return AgentContext(
        context_text=full_context_text,
        last_event_position=int(last_event.stream_position),
        pending_work=pending_work,
        session_health_status=session_health_status,
    )