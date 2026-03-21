# The Ledger: Agentic Event Store & Document-to-Decision Platform

The Ledger is an event-sourced system for agentic decision-making, designed to capture immutable business history and power reliable workflow orchestration from document ingestion through final decisions. It is built in Python using `asyncpg` for PostgreSQL persistence and Pydantic for strongly typed contracts, and it follows CQRS and Domain-Driven Design principles to keep write-side behavior, read-side reconstruction, and domain logic explicit and auditable.

## Core Domain Aggregates

| Aggregate Name | Stream ID Format | Purpose |
|---|---|---|
| LoanApplication | `loan-{application_id}` | Canonical lifecycle of a loan request from submission through final outcome. |
| AgentSession | `agent-{session_id}` | Records an agent execution session and enforces session-level audit guards. |
| DocumentPackage | `docpkg-{application_id}` | Tracks uploaded documents, extraction progress, and package readiness for analysis. |
| CreditAnalysis | `credit-{application_id}` | Stores credit risk analysis artifacts, decision recommendations, and model context. |
| FraudScreening | `fraud-{application_id}` | Captures fraud screening initiation, scoring outcomes, and recommendations. |
| ComplianceRecord | `compliance-{application_id}` | Persists compliance rule evaluations, verdicts, and hard-block decisions. |
| AuditLedger | `audit-{application_id}` | Consolidates cross-stream traceability for governance and end-to-end auditability. |

## Getting Started

Set up a local Python environment, install dependencies with `uv`, configure PostgreSQL connectivity, and run the test suite to validate the event store behavior.

### Installation

This project uses `uv` for package management and environment synchronization.

- macOS/Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Windows: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
- Install project dependencies: `uv sync`

### Running the Test Suite

The concurrency test requires a running PostgreSQL instance.

Create a `.env` file in the project root and configure `DATABASE_URL`, for example:

`DATABASE_URL="postgresql://user:password@host/apex_ledger_test"`

Run all tests with:

`uv run python -m pytest`

