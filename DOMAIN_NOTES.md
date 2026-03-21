# DOMAIN_NOTES.md: The Ledger

This document answers the required domain questions for Phase 0 of The Ledger project, demonstrating the architectural reasoning behind key design decisions.

## 1. EDA vs. ES Distinction

A component uses callbacks (like LangChain traces) to capture event-like data. Is this Event-Driven Architecture (EDA) or Event Sourcing (ES)? If you redesigned it using The Ledger, what exactly would change in the architecture and what would you gain?

A component using callbacks like LangChain traces is practicing **Event-Driven Architecture (EDA), not Event Sourcing (ES).**

-   **Analysis:** The purpose of an event in EDA is **notification**. The LangChain trace system "fires and forgets" these trace events to a listener. If the listener is down or crashes, the trace data is lost. You cannot reliably reconstruct the agent's exact decision-making process from these ephemeral messages. The events are a side-effect, not the source of truth.

-   **Redesign with The Ledger (ES):** To redesign this using The Ledger, we would treat every significant agent action as a domain event to be stored immutably.
    -   **Architectural Change:** Instead of emitting a temporary trace, the agent's wrapper (our `BaseApexAgent` class) would be responsible for appending structured, Pydantic-validated events like `AgentSessionStarted`, `AgentNodeExecuted`, and `AgentToolCalled` directly to a dedicated `agent-{agent_id}-{session_id}` stream in our PostgreSQL event store.
    -   **What is Gained (The "Gas Town" Pattern):**
        1.  **Perfect Auditability:** We gain a complete, immutable, and verifiable record of the agent's entire "thought process." A regulator can query this stream to see exactly what node ran, what tools were called, and what the LLM costs were, in order.
        2.  **State Reconstruction & Crash Recovery:** This is the most significant gain. If an agent process crashes mid-execution, we can instantiate a new agent, have it replay its session stream, and reconstruct its state to resume from the last successful node. With EDA, this context is lost forever. This transformation from ephemeral notifications to a persistent, replayable log is the essence of solving the "Gas Town" memory problem.


## 2. The Aggregate Question

In the Apex Financial Services scenario, we will build four aggregates: LoanApplication, AgentSession, ComplianceRecord, and AuditLedger. Identify one alternative aggregate boundary you considered and rejected. What coupling problem does your chosen boundary prevent?

**Alternative Boundary Considered and Rejected:**

A natural alternative I considered was to **merge the `ComplianceRecord` aggregate into the `LoanApplication` aggregate.** On the surface, this seems logical because compliance checks are an integral part of the loan application lifecycle. All compliance events would be written to the `loan-{application_id}` stream.

**Why This Was Rejected:**

I rejected this design because it would create a severe **write contention** problem, coupling the `ComplianceAgent` to every other process interacting with the loan.

-   **Coupling Problem Prevented:** By separating `ComplianceRecord` into its own stream (`compliance-{application_id}`), we decouple the fine-grained, internal steps of a compliance check from the high-level state transitions of the loan application.
    -   **Scenario:** Imagine the `ComplianceAgent` needs to evaluate 6 rules sequentially, appending a `ComplianceRulePassed` event after each one. If these events were written to the `loan-{id}` stream, the agent would need to perform 6 sequential writes. Between each write, another agent (like `FraudDetectionAgent`) could have appended its own results, changing the stream's version.
    -   **The Result:** The `ComplianceAgent`'s second write would fail with an `OptimisticConcurrencyError`. It would have to reload the entire `LoanApplication` stream, re-evaluate its state, and retry. This would happen for every single rule check, making the process incredibly slow and fragile.
    -   **The Solution:** Our chosen design allows the `ComplianceAgent` to work in complete isolation, appending all 6 rule events to its *own* stream without any concurrency conflicts. Once finished, it appends a single, final `ComplianceCheckCompleted` event to the `LoanApplication` stream. This is far more robust, scalable, and allows our agents to operate in parallel.

## 3. Concurrency in Practice

Two AI agents simultaneously process the same loan application and both call `append` with `expected_version=3`. Trace the exact sequence of operations in your event store. What does the losing agent receive, and what must it do next?

The losing agent receives an `OptimisticConcurrencyError` and must reload its state and retry its operation.

Here is the exact sequence of operations in the event store and application layer:

1.  **Read:** Agent A reads the `loan-application-123` stream. The store returns events 1, 2, and 3. Agent A's in-memory version is 3.
2.  **Read:** Agent B reads the `loan-application-123` stream. The store returns events 1, 2, and 3. Agent B's in-memory version is 3.
3.  **Agent A Wins (Write Attempt 1):** Agent A calls `append(..., expected_version=3)`.
    -   **DB Transaction 1 (BEGIN):**
    -   `SELECT current_version FROM event_streams WHERE stream_id = 'loan-application-123' FOR UPDATE;` The database locks this single row and returns `3`.
    -   *Application Logic:* The store code checks if `current_version (3) == expected_version (3)`. The check passes.
    -   `INSERT INTO events (..., stream_position) VALUES (..., 4);`
    -   `UPDATE event_streams SET current_version = 4 WHERE stream_id = 'loan-application-123';`
    -   **DB Transaction 1 (COMMIT):** The row lock is released. The stream is now at version 4.
4.  **Agent B Loses (Write Attempt 2):** Agent B calls `append(..., expected_version=3)`.
    -   **DB Transaction 2 (BEGIN):**
    -   `SELECT current_version FROM event_streams WHERE stream_id = 'loan-application-123' FOR UPDATE;` The database locks the row and returns `4`.
    -   *Application Logic:* The store code checks if `current_version (4) == expected_version (3)`. **The check fails.**
    -   **DB Transaction 2 (ROLLBACK):** The application layer immediately throws an `OptimisticConcurrencyError`. No events are written.
5.  **Agent B Recovers:** Agent B's error handling logic catches the `OptimisticConcurrencyError`.
    -   It must call `load_stream('loan-application-123')` again. It now receives 4 events, including the one from Agent A.
    -   It reconstructs its aggregate state based on the new history.
    -   It re-validates its business logic.
    -   It retries the call to `append`, this time with `expected_version=4`. This attempt will succeed.


## 4. Projection Lag and Its Consequences

Your LoanApplication projection is eventually consistent with a typical lag of 200ms. A loan officer queries "available credit limit" immediately after an agent commits a disbursement event. They see the old limit. What does your system do, and how do you communicate this to the user interface?

-   **What the system does:** The system does nothing automatically. The disbursement event is successfully written to the event store. The `LoanApplication` projection is now stale for approximately 200ms until the `ProjectionDaemon` processes the event and updates the read model table. This is the designed behavior of an eventually consistent system.

-   **How to communicate this to the UI (Recommended Approach): Optimistic UI Updates.**
    1.  When the loan officer clicks "Disburse," the UI front-end does **not** wait for the API call to complete.
    2.  It *immediately* updates the state locally, showing the new, lower available credit limit. It could display a subtle spinner or a small "(processing...)" message next to the value. This provides instant feedback.
    3.  In the background, the command is sent to the server. 99.9% of the time, it succeeds. The projection updates a few hundred milliseconds later, and the next time the UI fetches data, it gets the already-correct value.
    4.  In the rare case that the API call returns an error (e.g., a business rule violation), the UI reverts the change and displays a prominent error message to the loan officer (e.g., "Disbursement failed: Insufficient funds"). This approach feels instantaneous to the user and handles the reality of distributed systems gracefully by assuming success.

## 5. The Upcasting Scenario

The `CreditDecisionMade` event was defined in 2024 with `{application_id, decision, reason}`. In 2026 it needs `{application_id, decision, reason, model_version, confidence_score, regulatory_basis}`. Write the upcaster. What is your inference strategy for historical events that predate `model_version`?

Upcasting is a read-time transformation that never modifies the stored event. It allows us to evolve our event schema without rewriting history.

## here is the code

# In a file like `ledger/upcasting/upcasters.py`

from typing import Dict, Any

def upcast_credit_decision_v1_to_v2(
    payload: Dict[str, Any],
    metadata: Dict[str, Any] # Access to metadata like recorded_at is crucial
) -> Dict[str, Any]:
    """
    Upcasts a CreditDecisionMade event from schema v1 to v2.
    v1: {application_id, decision, reason}
    v2: {application_id, decision, reason, model_version, confidence_score, regulatory_basis}
    """
    
    # --- Inference Strategy ---
    
    # 1. model_version: Infer based on timestamp. This is an educated guess.
    #    The policy is that any analysis before 2025 used a legacy model.
    recorded_at = metadata.get("recorded_at") # Assumes recorded_at is in metadata
    model_version = "legacy-model-pre-2025"
    if recorded_at and recorded_at.year >= 2025:
        model_version = "unknown-post-2025" # Be honest about what we don't know

    # 2. confidence_score: Genuinely unknown. Fabricating a value is dangerous.
    #    Downstream systems might make incorrect automated decisions. Null is the only safe choice.
    confidence_score = None

    # 3. regulatory_basis: This can be inferred with a lookup if we have historical data.
    #    For this example, we will assume a simple rule.
    regulatory_basis = ["REG-STD-2024"] # Assume a standard for all old events

    return {
        **payload,
        "model_version": model_version,
        "confidence_score": confidence_score,
        "regulatory_basis": regulatory_basis,
    }



## 6. The Marten Async Daemon Parallel

Marten 7.0 introduced distributed projection execution across multiple nodes. Describe how you would achieve the same pattern in your Python implementation. What coordination primitive do you use, and what failure mode does it guard against?

To achieve the same distributed projection pattern in Python, we would use PostgreSQL's built-in **Advisory Locks** as our coordination primitive.

**The Implementation Pattern:**

1.  **Assign a Unique Lock ID:** Each projection is assigned a unique, permanent integer ID. This could be a hardcoded integer in an Enum or derived by hashing the projection's name (e.g., `hash('ApplicationSummary') % (2**31 - 1)`).

2.  **Acquire Lock on Run:** When a `ProjectionDaemon` instance on any node starts its processing loop for a specific projection, its first step is to attempt to acquire an exclusive advisory lock tied to that projection's ID.

3.  **Use a Non-Blocking Lock:** The daemon will use a non-blocking lock function, `pg_try_advisory_xact_lock(projection_id)`.
    -   **If it returns `True`:** This daemon instance has successfully acquired the lock for the duration of its transaction. It is now the "leader" for this projection. It can safely query its checkpoint, process a batch of new events, and update the projection table. The lock is automatically released when its transaction commits or rolls back.
    -   **If it returns `False`:** Another daemon instance on another node already holds the lock. This daemon instance simply logs a message like "Skipping ApplicationSummary projection, already locked by another process" and does nothing for that projection in this cycle.

**The Failure Mode It Guards Against:**

This pattern directly guards against a **distributed split-brain failure mode for projections.** Without this coordination, if you deploy two application instances, both `ProjectionDaemon` tasks would read the same events from the event store and both would try to write updates to the `ApplicationSummary` table. This would result in race conditions, duplicate data processing, and potential data corruption. The advisory lock acts as a simple, database-native, and highly effective distributed mutex, ensuring that only one worker processes a given projection across the entire cluster.