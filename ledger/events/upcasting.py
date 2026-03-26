import inspect
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple

from ledger.schema.events import BaseEvent, StoredEvent

if TYPE_CHECKING:
    from ledger.event_store import EventStore


class UpcasterRegistry:
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[..., dict]] = {}

    def register(self, event_type: str, from_version: int):
        def decorator(func: Callable[..., dict]) -> Callable[..., dict]:
            self._upcasters[(event_type, from_version)] = func
            return func

        return decorator

    async def upcast(
        self,
        event: StoredEvent,
        *,
        store: Optional["EventStore"] = None,
    ) -> StoredEvent:
        current_event = event

        while True:
            upcaster = self._upcasters.get((current_event.event_type, current_event.event_version))
            if upcaster is None:
                return current_event

            updated_payload = upcaster(dict(current_event.payload), store=store)
            if inspect.isawaitable(updated_payload):
                updated_payload = await updated_payload
            current_event = current_event.model_copy(
                update={
                    "payload": updated_payload,
                    "event_version": current_event.event_version + 1,
                }
            )


registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict, store: Optional["EventStore"] = None) -> dict:
    return {
        **payload,
        "model_version": payload.get("model_version", "unknown"),
        "confidence_score": payload.get("confidence_score", None),
    }


def _agent_type_key(agent_type: Any) -> str:
    raw = str(agent_type or "unknown")
    if raw.isupper() and "_" in raw:
        return "".join(part.title() for part in raw.split("_")) + "Agent"
    return raw


@registry.register("DecisionGenerated", from_version=1)
async def upcast_decision_v1_to_v2(
    payload: dict,
    store: Optional["EventStore"] = None,
) -> dict:
    if store is None:
        return {
            **payload,
            "contributing_sessions": payload.get(
                "contributing_sessions",
                payload.get("contributing_agent_sessions", []),
            ),
            "model_versions": payload.get("model_versions", {}),
        }

    contributing_sessions = payload.get(
        "contributing_agent_sessions",
        payload.get("contributing_sessions", []),
    )
    model_versions = dict(payload.get("model_versions", {}))

    for session_id in contributing_sessions:
        events = await store.load_stream(f"agent-{session_id}")
        started_event = next(
            (event for event in events if event.event_type == "AgentSessionStarted"),
            None,
        )
        if started_event is None:
            continue

        agent_type = _agent_type_key(started_event.payload.get("agent_type"))
        model_version = started_event.payload.get("model_version", "unknown")
        model_versions[agent_type] = model_version

    return {
        **payload,
        "contributing_sessions": list(contributing_sessions),
        "model_versions": model_versions,
    }