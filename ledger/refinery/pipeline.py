from __future__ import annotations

import inspect
from typing import Optional

from ledger.refinery.models import ApplicantProfile, Document, FinancialFacts


async def extract_financial_facts(
    applicant: ApplicantProfile,
    documents: list[Document],
) -> Optional[FinancialFacts]:
    from ledger.refinery.agents.orchestrator import Orchestrator

    orchestrator = Orchestrator()
    pipeline_result = orchestrator.run_pipeline(applicant, documents)

    if inspect.isawaitable(pipeline_result):
        final_output = await pipeline_result
    else:
        final_output = pipeline_result

    quality = final_output.quality_assessment
    if quality.is_coherent and not quality.reextraction_recommended:
        return final_output.financial_facts

    return None
