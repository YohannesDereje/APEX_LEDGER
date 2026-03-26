import json
import os
import uuid

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ledger.commands.handlers import SubmitApplicationCommand, handle_submit_application
from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection


app = FastAPI()


class ApplicationSubmissionRequest(BaseModel):
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    loan_term_months: int
    submission_channel: str
    contact_email: str
    contact_name: str
    application_reference: str


@app.on_event("startup")
async def startup_event() -> None:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")

    async def init_connection(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
            format="text",
        )

    app.state.pool = await asyncpg.create_pool(database_url, init=init_connection)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    pool = getattr(app.state, "pool", None)
    if pool is not None:
        await pool.close()


@app.get("/")
async def read_root() -> dict[str, str]:
    return {"message": "Welcome to The Ledger MCP"}


@app.get("/applications/{application_id}/summary")
async def get_application_summary(application_id: str) -> dict:
    pool = app.state.pool
    projection = ApplicationSummaryProjection(pool)
    summary = await projection.get_summary(application_id)

    if summary is None:
        raise HTTPException(status_code=404, detail="Application not found")

    return summary


@app.post("/applications", status_code=202)
async def submit_application(request: ApplicationSubmissionRequest) -> dict[str, str]:
    pool = app.state.pool
    event_store = EventStore(pool)
    application_id = str(uuid.uuid4())

    command = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=request.applicant_id,
        requested_amount_usd=request.requested_amount_usd,
        loan_purpose=request.loan_purpose,
        loan_term_months=request.loan_term_months,
        submission_channel=request.submission_channel,
        contact_email=request.contact_email,
        contact_name=request.contact_name,
        application_reference=request.application_reference,
    )
    await handle_submit_application(command, event_store)

    return {
        "message": "Application submission accepted",
        "application_id": application_id,
    }