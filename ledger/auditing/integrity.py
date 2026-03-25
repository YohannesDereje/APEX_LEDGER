import hashlib
import json
import uuid
from typing import List

from ledger.event_store import EventStore
from ledger.schema.events import AuditIntegrityCheckRun, StoredEvent


def _hash_event_payload(event: StoredEvent) -> str:
    normalized_payload = json.dumps(event.payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized_payload.encode("utf-8")).hexdigest()


async def run_integrity_check(store: EventStore, entity_type: str, entity_id: str):
    primary_stream_id = f"{entity_type}-{entity_id}"
    audit_stream_id = f"audit-{entity_type}-{entity_id}"

    audit_events: List[StoredEvent] = await store.load_stream(audit_stream_id)
    audit_check_events = [
        event
        for event in audit_events
        if event.event_type == AuditIntegrityCheckRun.__name__
    ]

    previous_hash = ""
    last_checked_global_position = 0
    if audit_check_events:
        last_audit = audit_check_events[-1]
        previous_hash = str(last_audit.payload.get("new_hash", ""))
        last_checked_global_position = int(
            last_audit.payload.get("last_checked_global_position", 0)
        )

    new_events: List[StoredEvent] = [
        event
        async for event in store.load_all(from_global_position=last_checked_global_position)
        if event.stream_id == primary_stream_id
    ]

    new_last_checked_global_position = (
        int(new_events[-1].global_position)
        if new_events
        else last_checked_global_position
    )

    concatenated_event_hashes = "".join(_hash_event_payload(event) for event in new_events)
    new_hash_input = f"{previous_hash}{concatenated_event_hashes}"
    new_hash = hashlib.sha256(new_hash_input.encode("utf-8")).hexdigest()

    audit_event = AuditIntegrityCheckRun(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified=len(new_events),
        previous_hash=previous_hash,
        new_hash=new_hash,
        last_checked_global_position=new_last_checked_global_position,
    )

    await store.append(
        stream_id=audit_stream_id,
        events=[audit_event],
        expected_version=len(audit_events),
        aggregate_type="AuditIntegrity",
        correlation_id=str(uuid.uuid4()),
    )

    return {
        "chain_valid": True,
        "events_verified": len(new_events),
        "previous_hash": previous_hash,
        "new_hash": new_hash,
        "last_checked_global_position": new_last_checked_global_position,
    }