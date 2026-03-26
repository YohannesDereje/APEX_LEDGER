from typing import Optional

from ledger.event_store import EventStore
from ledger.schema.events import AuditIntegrityCheckRun, DomainError, StoredEvent


class AuditLedgerAggregate:
    def __init__(self, entity_id: str):
        self.entity_id = entity_id
        self.version = 0
        self.last_known_hash: Optional[str] = None

    @classmethod
    async def load(
        cls,
        store: EventStore,
        entity_id: str,
    ) -> "AuditLedgerAggregate":
        aggregate = cls(entity_id=entity_id)
        events = await store.load_stream(f"audit-ledger-{entity_id}")

        for event in events:
            aggregate._apply(event)

        return aggregate

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler is not None:
            handler(event)

        self.version = event.stream_position

    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        self.last_known_hash = str(event.payload["new_hash"])

    def assert_chain_is_valid(self, previous_hash: str) -> None:
        expected_previous_hash = self.last_known_hash or ""
        if previous_hash != expected_previous_hash:
            raise DomainError(
                "Audit chain mismatch. "
                f"Expected previous hash '{expected_previous_hash}', got '{previous_hash}'."
            )