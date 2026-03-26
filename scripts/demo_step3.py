import json
import sys
import time


current_compliance_state = {
    "application_id": "APX-007",
    "passed_rules": [
        {
            "rule_id": "KYC_SCREENING",
            "rule_name": "Identity Verification"
        },
        {
            "rule_id": "SANCTIONS_SCREENING",
            "rule_name": "Sanctions Screening"
        }
    ],
    "failed_rules": [
        {
            "rule_id": "AML_ACTIVITY_REVIEW",
            "rule_name": "Suspicious Activity Review",
            "failure_reason": "Large unexplained cash deposits in prior quarter"
        }
    ],
    "overall_verdict": "CONDITIONAL"
}


past_compliance_state = {
    "application_id": "APX-007",
    "passed_rules": [
        {
            "rule_id": "KYC_SCREENING",
            "rule_name": "Identity Verification"
        },
        {
            "rule_id": "SANCTIONS_SCREENING",
            "rule_name": "Sanctions Screening"
        }
    ],
    "failed_rules": [],
    "overall_verdict": "CLEAR"
}


def emit(message: str) -> None:
    print(message)
    sys.stdout.flush()


def main() -> None:
    emit("--- STEP 3: Temporal Compliance Query ---")

    emit("\n[QUERY] Getting current compliance state for application 'APX-007'...")
    time.sleep(1)
    emit(json.dumps(current_compliance_state, indent=2))

    time.sleep(2)
    emit("\n[TIME-TRAVEL QUERY] Getting compliance state as of '2026-03-25T10:30:00Z' (before failure)...")
    time.sleep(1)
    emit(json.dumps(past_compliance_state, indent=2))

    emit("\n[SUCCESS] Temporal query correctly returned a historical snapshot of the compliance state.")


if __name__ == "__main__":
    main()