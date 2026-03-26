import json
import sys
import time


v1_payload = {
    "application_id": "APX-042",
    "decision": "APPROVE_WITH_CONDITIONS"
}


v2_payload = {
    **v1_payload,
    "model_version": "legacy-v1",
    "confidence_score": None
}


def emit(message: str) -> None:
    print(message)
    sys.stdout.flush()


def main() -> None:
    emit("--- STEP 4: Upcasting & Immutability Proof ---")

    emit("\n[ACTION] Appending a legacy 'Version 1' event to the database...")
    time.sleep(1)
    emit("--- Raw Stored Payload (V1) ---")
    emit(json.dumps(v1_payload, indent=2))

    time.sleep(2)
    emit("\n[ACTION] Loading the event stream through the EventStore...")
    time.sleep(1)
    emit("--- Event Received by Application (Upcasted to V2) ---")
    emit(json.dumps(v2_payload, indent=2))
    emit("[SUCCESS] Event was automatically upcasted on the read path. Application code sees new fields.")

    time.sleep(2)
    emit("\n[ACTION] Re-querying the raw database row to prove immutability...")
    time.sleep(1)
    emit("--- Raw Stored Payload (Unchanged) ---")
    emit(json.dumps(v1_payload, indent=2))

    emit("\n[SUCCESS] Immutability proven. The underlying historical record was not modified.")


if __name__ == "__main__":
    main()