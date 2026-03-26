import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from ledger.schema.events import PackageReadyForAnalysis
from ledger.tests.test_projections import projection_env


class FakeDocumentProcessingAgent:
    def __init__(self, store):
        self.store = store

    async def run(self, application_id: str, documents: list[dict]) -> None:
        expected_version = await self.store.stream_version(f"docpkg-{application_id}")
        event = PackageReadyForAnalysis(
            package_id=f"pkg-{application_id}",
            application_id=application_id,
            documents_processed=len(documents),
            has_quality_flags=False,
            quality_flag_count=0,
            ready_at=datetime.now(timezone.utc),
        )
        await self.store.append(
            stream_id=f"docpkg-{application_id}",
            events=[event],
            expected_version=expected_version,
            aggregate_type="DocumentPackage",
        )


@pytest.mark.asyncio
async def test_full_lifecycle_via_mcp_tools(projection_env):
    test_app = projection_env["app"]
    test_app.state.document_processing_agent_factory = lambda store: FakeDocumentProcessingAgent(store)
    transport = httpx.ASGITransport(app=test_app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        create_response = await client.post(
            "/applications",
            json={
                "applicant_id": "mcp-lifecycle-applicant",
                "requested_amount_usd": 200000,
                "loan_purpose": "WORKING_CAPITAL",
                "loan_term_months": 24,
                "submission_channel": "MCP_TEST",
                "contact_email": "lifecycle@example.com",
                "contact_name": "Lifecycle Test",
                "application_reference": "mcp-lifecycle-ref",
            },
        )
        assert create_response.status_code == 202
        application_id = create_response.json()["application_id"]

        await test_app.state.daemon._process_batch()
        await asyncio.sleep(0)
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.status_code == 200
        assert summary_response.json()["state"] == "SUBMITTED"

        doc_response = await client.post(
            f"/applications/{application_id}/start-document-processing",
            json={
                "documents": [
                    {
                        "document_id": "doc-1",
                        "file_path": "placeholder.pdf",
                        "content": "placeholder content",
                    }
                ]
            },
        )
        assert doc_response.status_code == 202
        await test_app.state.daemon._process_batch()
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.json()["state"] == "AWAITING_ANALYSIS"

        credit_response = await client.post(f"/applications/{application_id}/start-credit-analysis")
        assert credit_response.status_code == 202
        await test_app.state.daemon._process_batch()
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.json()["state"] == "FRAUD_REVIEW"

        fraud_response = await client.post(f"/applications/{application_id}/start-fraud-screening")
        assert fraud_response.status_code == 202
        await test_app.state.daemon._process_batch()
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.json()["state"] == "COMPLIANCE_REVIEW"

        compliance_response = await client.post(f"/applications/{application_id}/start-compliance-check")
        assert compliance_response.status_code == 202
        await test_app.state.daemon._process_batch()
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.json()["state"] == "PENDING_DECISION"

        decision_response = await client.post(f"/applications/{application_id}/generate-decision")
        assert decision_response.status_code == 202
        await test_app.state.daemon._process_batch()
        summary_response = await client.get(f"/applications/{application_id}/summary")
        assert summary_response.json()["state"] in {"FINAL_APPROVED", "FINAL_DECLINED"}