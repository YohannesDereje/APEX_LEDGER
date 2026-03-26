from typing import Optional

from ledger.event_store import EventStore
from ledger.schema.events import ComplianceRuleFailed, ComplianceRulePassed, DomainError, StoredEvent


class ComplianceRecordAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.version = 0
        self.passed_rules: set[str] = set()
        self.failed_rules: set[str] = set()

    @classmethod
    async def load(
        cls,
        store: EventStore,
        application_id: str,
    ) -> "ComplianceRecordAggregate":
        aggregate = cls(application_id=application_id)
        events = await store.load_stream(f"compliance-{application_id}")

        for event in events:
            aggregate._apply(event)

        return aggregate

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler is not None:
            handler(event)

        self.version = event.stream_position

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = str(event.payload["rule_id"])
        self.passed_rules.add(rule_id)
        self.failed_rules.discard(rule_id)

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        rule_id = str(event.payload["rule_id"])
        self.failed_rules.add(rule_id)
        self.passed_rules.discard(rule_id)

    def assert_all_required_rules_passed(self, required_rules: set[str]) -> None:
        missing_rules = required_rules - self.passed_rules
        if missing_rules:
            missing_rules_text = ", ".join(sorted(missing_rules))
            raise DomainError(
                f"Required compliance rules have not passed: {missing_rules_text}."
            )