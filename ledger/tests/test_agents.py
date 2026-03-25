import asyncio
import uuid
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from ledger.agents.document_processor import DocumentProcessingAgent
from ledger.event_store import EventStore
from ledger.schema.events import ExtractionCompleted, FinancialFacts, PackageReadyForAnalysis
from ledger.tests.test_projections import projection_env


class StartDocumentProcessingCommand(BaseModel):
    application_id: str
    documents: list


async def handle_start_document_processing(cmd: StartDocumentProcessingCommand, store: EventStore):
    agent = DocumentProcessingAgent(
        store=store,
        agent_id="doc-processor-test",
        model_version="test-model-v1",
    )
    await agent.run(application_id=cmd.application_id, documents=cmd.documents)
    await asyncio.sleep(0)


@pytest.mark.asyncio
@patch("ledger.agents.document_processor.extract_financial_facts")
async def test_document_processing_agent_end_to_end(mock_extract_facts, projection_env):
    event_store: EventStore = projection_env

    fake_financial_facts = FinancialFacts.model_construct(
        total_revenue=123456,
    )
    mock_extract_facts.return_value = fake_financial_facts

    application_id = f"app-{uuid.uuid4()}"
    cmd = StartDocumentProcessingCommand(
        application_id=application_id,
        documents=[
            {
                "document_id": "doc-income-001",
                "pdf_path": "income_statement_2025.pdf",
                "content": "Revenue 123456, Net Income 12000",
                "filename": "income_statement_2025.pdf",
                "document_type": "INCOME_STATEMENT",
            }
        ],
    )

    await handle_start_document_processing(cmd, event_store)

    stream = await event_store.load_stream(f"docpkg-{application_id}")

    assert stream
    assert stream[-1].event_type == PackageReadyForAnalysis.__name__
    assert stream[-2].event_type == ExtractionCompleted.__name__
    assert stream[-2].payload["facts"]["total_revenue"] == fake_financial_facts.total_revenue
