"""
M11: Response Finalizer — Resolves citations, validates, packages AgentResponse.

No LLM calls.
"""

from __future__ import annotations
import logging

from models.response import (
    AgentResponse,
    RawLLMResponse,
    ContextPayload,
    QueryDecomposition,
    RerankResult,
)

logger = logging.getLogger(__name__)


def finalize_response(
    raw_response: RawLLMResponse,
    context_payload: ContextPayload,
    decomposition: QueryDecomposition,
    rerank_result: RerankResult,
    latency_ms: int,
) -> AgentResponse:
    """
    Resolves citation keys → full provenance metadata.
    Validates all keys in citations_used exist in registry.
    Packages final AgentResponse.
    """
    registry = context_payload.citation_registry

    # ── Resolve citations ────────────────────────────────────────────────────
    resolved_citations: list[dict] = []
    facts_used: list[str] = []
    orphaned_keys: list[str] = []

    for key in raw_response.citations_used:
        entry = registry.resolve(key)
        if entry:
            resolved_citations.append({
                "key": key,
                "company": entry.company,
                "fiscal_year": entry.fiscal_year,
                "section_path": entry.section_path,
                "page": entry.page,
                "chunk_type": entry.chunk_type,
                "confidence": entry.confidence,
                "content_preview": entry.content_preview[:200],
            })
            if entry.is_fact and entry.fact_id:
                facts_used.append(entry.fact_id)
        else:
            orphaned_keys.append(key)

    if orphaned_keys:
        logger.warning(f"Orphaned citation keys (not in registry): {orphaned_keys}")

    # ── Merge conflicts from reranker + LLM ──────────────────────────────────
    all_conflicts = list(set(rerank_result.conflicts + raw_response.conflicts_detected))

    # ── Build response ───────────────────────────────────────────────────────
    confidence_dict = context_payload.confidence.to_dict()

    response = AgentResponse(
        query=decomposition.sub_questions[0] if decomposition.sub_questions else "",
        answer=raw_response.answer,
        resolved_citations=resolved_citations,
        retrieval_confidence=confidence_dict,
        unanswerable_sub_questions=raw_response.unanswerable_sub_questions,
        conflicts_detected=all_conflicts,
        facts_used=facts_used,
        sub_questions=decomposition.sub_questions,
        chunks_retrieved=len(rerank_result.top_chunks) + len(rerank_result.facts),
        chunks_used=len(resolved_citations),
        latency_ms=latency_ms,
    )

    logger.info(
        f"Response finalized: confidence={confidence_dict['label']}, "
        f"{len(resolved_citations)} citations, {len(facts_used)} facts, "
        f"{latency_ms}ms"
    )

    return response
