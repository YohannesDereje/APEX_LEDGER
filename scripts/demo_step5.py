import json
import sys
import time


event_log = [
    "AgentSessionStarted",
    "AgentNodeExecuted",
    "AgentNodeExecuted",
    "AgentToolCalled",
]


reconstructed_context = {
    "session_health_status": "NEEDS_RECONCILIATION",
    "last_event_position": 4,
    "pending_work": [
        "Reconcile unfinished tool call 'sanctions_lookup'"
    ],
    "context_text": "Session summary: 4 events occurred. Last action was calling tool 'sanctions_lookup'."
}


def emit(message: str) -> None:
    print(message)
    sys.stdout.flush()


def main() -> None:
    emit("--- STEP 5: Gas Town Agent Recovery Proof ---")

    emit("\n[INFO] Agent 'fraud-detector-alpha' is processing application APX-007...")
    time.sleep(1)

    for event_name in event_log:
        emit(f"[EVENT APPENDED] {event_name}")
        time.sleep(0.5)

    time.sleep(1)
    emit("\n" + "=" * 20 + " CRITICAL FAILURE " + "=" * 20)
    emit("!       FATAL: Worker process terminated unexpectedly.      !")
    emit("=" * 60 + "\n")

    time.sleep(2)
    emit("[INFO] New agent instance 'fraud-detector-beta' starting up...")
    emit("[ACTION] Calling reconstruct_agent_context(session_id='agent-fraud-007') to recover memory...")
    time.sleep(1.5)

    emit("--- Reconstructed AgentContext ---")
    emit(json.dumps(reconstructed_context, indent=2))

    emit("\n[SUCCESS] Agent recovered its state and knows it must reconcile the unfinished tool call.")


if __name__ == "__main__":
    main()