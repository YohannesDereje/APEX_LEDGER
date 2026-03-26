import asyncio

import httpx
import pytest

from ledger.mcp.main import app
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.tests.test_projections import projection_env


@pytest.mark.asyncio
async def test_submit_and_get_summary_api(projection_env):
    event_store = projection_env["event_store"]
    pool = projection_env["pool"]
    test_app = projection_env["app"]
    transport = httpx.ASGITransport(app=test_app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        app_data = {
            "applicant_id": "applicant-api-001",
            "requested_amount_usd": 125000,
            "loan_purpose": "WORKING_CAPITAL",
            "loan_term_months": 24,
            "submission_channel": "API_TEST",
            "contact_email": "api-test@example.com",
            "contact_name": "API Test Applicant",
            "application_reference": "api-test-reference-001",
        }

        post_response = await client.post("/applications", json=app_data)

        assert post_response.status_code == 202
        application_id = post_response.json()["application_id"]

        daemon = ProjectionDaemon(
            event_store,
            [ApplicationSummaryProjection(pool)],
        )
        await daemon._process_batch()
        await asyncio.sleep(0)

        get_response = await client.get(
            f"/applications/{application_id}/summary"
        )

    assert get_response.status_code == 200
    summary = get_response.json()
    assert summary["state"] == "SUBMITTED"
    assert summary["applicant_id"] == app_data["applicant_id"]