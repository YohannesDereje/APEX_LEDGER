from typing import Any, TypedDict
from datetime import datetime, timezone

from langgraph.graph import END, START, StateGraph

from ledger.agents.base_agent import BaseApexAgent
from ledger.domain.aggregates.agent_session import AgentSessionAggregate
from ledger.event_store import EventStore
from ledger.refinery.models import ApplicantProfile, Document
from ledger.refinery.pipeline import extract_financial_facts
from ledger.schema.events import (
    AgentType,
    DocumentType,
    ExtractionCompleted,
    FinancialFacts,
    PackageReadyForAnalysis,
)


class DocumentProcessingState(TypedDict):
    application_id: str
    documents: list[Any]
    extracted_facts: FinancialFacts | None
    quality_assessment: dict[str, Any] | None
    session: AgentSessionAggregate


class DocumentProcessingAgent(BaseApexAgent):
    def __init__(
        self,
        store: EventStore,
        agent_id: str,
        model_version: str,
        **shared_config: Any,
    ) -> None:
        super().__init__(
            store=store,
            agent_id=agent_id,
            model_version=model_version,
            agent_type=AgentType.DOCUMENT_PROCESSING,
            **shared_config,
        )
        self.graph = None
        self.build_graph()

    async def _node_validate_inputs(
        self,
        state: DocumentProcessingState,
    ) -> DocumentProcessingState:
        print("DocumentProcessingAgent.validate_inputs")
        return state

    async def _node_extract_income_statement(
        self,
        state: DocumentProcessingState,
    ) -> DocumentProcessingState:
        applicant = ApplicantProfile(applicant_id=state["application_id"])
        documents = [
            document
            if isinstance(document, Document)
            else Document(
                pdf_path=document.get("pdf_path", document.get("file_path", "placeholder.pdf")),
                document_id=document.get("document_id", f"doc-{index}"),
                metadata={
                    "content": document.get("content", ""),
                    "filename": document.get("filename"),
                    "document_type": document.get("document_type"),
                },
            )
            for index, document in enumerate(state["documents"], start=1)
        ]

        extracted_facts = await extract_financial_facts(applicant, documents)
        session = await self._record_node_execution(
            state["session"],
            "extract_income_statement",
            input_keys=["application_id", "documents"],
            output_keys=["extracted_facts"],
            llm_called=True,
        )

        updated_state = dict(state)
        updated_state["extracted_facts"] = extracted_facts
        updated_state["session"] = session
        return updated_state

    async def _node_assess_quality(
        self,
        state: DocumentProcessingState,
    ) -> DocumentProcessingState:
        print("DocumentProcessingAgent.assess_quality")
        return state

    async def _node_write_output(
        self,
        state: DocumentProcessingState,
    ) -> DocumentProcessingState:
        application_id = state["application_id"]
        session = state["session"]
        extracted_facts = state.get("extracted_facts")

        if extracted_facts is not None:
            docpkg_stream_id = f"docpkg-{application_id}"
            expected_version = await self.store.stream_version(docpkg_stream_id)

            package_id = f"pkg-{application_id}"
            first_document = state["documents"][0] if state["documents"] else None
            if isinstance(first_document, Document):
                document_id = first_document.document_id
            elif isinstance(first_document, dict):
                document_id = first_document.get("document_id", "unknown-doc")
            else:
                document_id = "unknown-doc"

            extraction_completed = ExtractionCompleted(
                package_id=package_id,
                document_id=document_id,
                document_type=DocumentType.INCOME_STATEMENT,
                facts=extracted_facts,
                raw_text_length=0,
                tables_extracted=0,
                processing_ms=0,
                completed_at=datetime.now(timezone.utc),
            )

            package_ready = PackageReadyForAnalysis(
                package_id=package_id,
                application_id=application_id,
                documents_processed=len(state["documents"]),
                has_quality_flags=False,
                quality_flag_count=0,
                ready_at=datetime.now(timezone.utc),
            )

            await self.store.append(
                stream_id=docpkg_stream_id,
                events=[extraction_completed, package_ready],
                expected_version=expected_version,
                aggregate_type="DocumentPackage",
            )
        else:
            print(
                f"DocumentProcessingAgent.write_output: extraction failed for application {application_id}; no events written"
            )

        session = await self._record_node_execution(
            session,
            "write_output",
            input_keys=["application_id", "extracted_facts"],
            output_keys=["docpkg_events_written"],
            llm_called=False,
        )

        updated_state = dict(state)
        updated_state["session"] = session
        return updated_state

    def build_graph(self) -> None:
        graph = StateGraph(DocumentProcessingState)

        graph.add_node("validate_inputs", self._node_validate_inputs)
        graph.add_node("extract_income_statement", self._node_extract_income_statement)
        graph.add_node("assess_quality", self._node_assess_quality)
        graph.add_node("write_output", self._node_write_output)

        graph.add_edge(START, "validate_inputs")
        graph.add_edge("validate_inputs", "extract_income_statement")
        graph.add_edge("extract_income_statement", "assess_quality")
        graph.add_edge("assess_quality", "write_output")
        graph.add_edge("write_output", END)

        self.graph = graph.compile()

    async def run(self, application_id: str, documents: list[Any]) -> None:
        session = await self._start_session(application_id)
        await self.graph.ainvoke(
            {
                "application_id": application_id,
                "documents": documents,
                "extracted_facts": None,
                "quality_assessment": None,
                "session": session,
            }
        )