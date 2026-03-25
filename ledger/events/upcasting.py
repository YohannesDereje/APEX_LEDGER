from typing import Callable, Dict, Tuple

from ledger.schema.events import BaseEvent, StoredEvent


class UpcasterRegistry:
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int):
        def decorator(func: Callable[[dict], dict]) -> Callable[[dict], dict]:
            self._upcasters[(event_type, from_version)] = func
            return func

        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        current_event = event

        while True:
            upcaster = self._upcasters.get((current_event.event_type, current_event.event_version))
            if upcaster is None:
                return current_event

            updated_payload = upcaster(dict(current_event.payload))
            current_event = current_event.model_copy(
                update={
                    "payload": updated_payload,
                    "event_version": current_event.event_version + 1,
                }
            )


registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    return {
        **payload,
        "model_version": payload.get("model_version", "unknown"),
        "confidence_score": payload.get("confidence_score", None),
    }