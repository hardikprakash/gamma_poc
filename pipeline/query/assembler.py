"""
M9: Context Assembler — Builds the context string + citation registry for LLM generation.

No LLM calls. Deterministic.
Citation keys assigned HERE before LLM generation.
FACT blocks always first in context_str.
"""

from __future__ import annotations
import logging
import re

from models.chunk import Chunk
from models.fact import FinancialFact
from models.response import (
    RerankResult,
    QueryDecomposition,
    ContextPayload,
    CitationRegistry,
    CitationEntry,
    RetrievalConfidence,
    ScoredChunk,
)

logger = logging.getLogger(__name__)


def assemble_context(
    rerank_result: RerankResult,
    decomposition: QueryDecomposition,
) -> ContextPayload:
    """
    Returns ContextPayload with context_str, citation_registry, confidence, token_count.
    Citation keys assigned before LLM generation (C-03).
    """
    registry = CitationRegistry()
    fact_blocks: list[str] = []
    source_blocks: list[str] = []

    # ── Register FACT blocks (always first) ──────────────────────────────────
    for fact in rerank_result.facts:
        key = _make_fact_citation_key(fact, registry)
        entry = CitationEntry(
            key=key,
            chunk_id=fact.source_chunk_id,
            company=fact.company,
            fiscal_year=fact.fiscal_year,
            section_path="",
            page=_page_from_chunk_id(fact.source_chunk_id),
            chunk_type="fact",
            confidence=fact.confidence,
            content_preview=f"{fact.metric_name_canonical}: {fact.value} {fact.unit} ({fact.period})",
            is_fact=True,
            fact_id=fact.fact_id,
        )
        registry.add(entry)
        fact_blocks.append(_format_fact_block(fact, key))

    # ── Register SOURCE blocks ───────────────────────────────────────────────
    for sc in rerank_result.top_chunks:
        chunk = sc.chunk
        key = _make_citation_key(chunk, registry)
        entry = CitationEntry(
            key=key,
            chunk_id=chunk.chunk_id,
            company=chunk.company,
            fiscal_year=chunk.fiscal_year,
            section_path=chunk.section_path,
            page=chunk.page_start or _page_from_chunk_id(chunk.chunk_id),
            chunk_type=chunk.chunk_type,
            confidence="high" if chunk.chunk_type == "table" else "medium",
            content_preview=chunk.content[:200],
        )
        registry.add(entry)
        source_blocks.append(_format_source_block(chunk, key))

    # ── Build context string ─────────────────────────────────────────────────
    context_str = ""
    if fact_blocks:
        context_str += "=== FINANCIAL FACTS ===\n"
        context_str += "\n".join(fact_blocks) + "\n\n"
    if source_blocks:
        context_str += "=== SOURCE DOCUMENTS ===\n\n"
        context_str += "\n".join(source_blocks)

    # ── Compute confidence ───────────────────────────────────────────────────
    confidence = _compute_confidence(
        decomposition, rerank_result.facts, rerank_result.top_chunks
    )

    # ── Count tokens ─────────────────────────────────────────────────────────
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    token_count = len(enc.encode(context_str))

    logger.info(
        f"Context assembled: {len(fact_blocks)} facts, {len(source_blocks)} sources, "
        f"{token_count} tokens, confidence={confidence.label}"
    )

    return ContextPayload(
        context_str=context_str,
        citation_registry=registry,
        confidence=confidence,
        token_count=token_count,
    )


# ── Citation Key Generation (DOC3 §2.5) ─────────────────────────────────────

def _make_citation_key(chunk: Chunk, registry: CitationRegistry) -> str:
    company_abbr = chunk.ticker or chunk.company[:2].upper()
    cat_abbr = chunk.semantic_category[:4]
    page = chunk.page_start or _page_from_chunk_id(chunk.chunk_id)
    base_key = f"[{company_abbr}-{chunk.fiscal_year}-{cat_abbr}-p{page}]"

    key = base_key
    counter = 1
    while key in registry.citations and registry.citations[key].chunk_id != chunk.chunk_id:
        key = base_key[:-1] + f"-{counter}]"
        counter += 1
    return key


def _make_fact_citation_key(fact: FinancialFact, registry: CitationRegistry) -> str:
    company_abbr = fact.company[:4].upper().replace(" ", "")
    metric_abbr = fact.metric_name_canonical[:6]
    base_key = f"[FACT-{company_abbr}-{fact.fiscal_year}-{metric_abbr}]"

    key = base_key
    counter = 1
    while key in registry.citations and registry.citations[key].fact_id != fact.fact_id:
        key = base_key[:-1] + f"-{counter}]"
        counter += 1
    return key


# ── Context Block Formatting (DOC3 §2.6) ────────────────────────────────────

def _format_source_block(chunk: Chunk, citation_key: str) -> str:
    page = chunk.page_start or _page_from_chunk_id(chunk.chunk_id)
    return (
        f"[SOURCE {citation_key}]\n"
        f"Company: {chunk.company} | Year: {chunk.fiscal_year} | "
        f"Section: {chunk.section_path} | Page: {page} | Type: {chunk.chunk_type.upper()}\n"
        f"{'=' * 60}\n"
        f"{chunk.content}\n\n"
    )


def _format_fact_block(fact: FinancialFact, citation_key: str) -> str:
    return (
        f"[FACT {citation_key}] "
        f"{fact.metric_name_canonical}: {fact.value} {fact.unit} "
        f"({fact.period}, {fact.company}, confidence={fact.confidence})"
    )


# ── Page Extraction Utility ────────────────────────────────────────────────

def _page_from_chunk_id(chunk_id: str) -> int:
    """Extract page number from chunk_id (format: {TICKER}_{YEAR}_{cat}_p{PAGE}_{seq})."""
    m = re.search(r'_p(\d+)_', chunk_id)
    return int(m.group(1)) if m else 0


# ── Confidence Scoring ───────────────────────────────────────────────────────

def _compute_confidence(
    decomposition: QueryDecomposition,
    facts: list[FinancialFact],
    top_chunks: list[ScoredChunk],
) -> RetrievalConfidence:
    """
    Confidence per sub-question:
      fact answer = 1.0, chunk answer = 0.7, no answer = 0.0
    """
    sub_questions = decomposition.sub_questions or [decomposition.metrics[0] if decomposition.metrics else "query"]

    answered_by_facts = 0
    answered_by_chunks = 0
    unanswered = 0

    requested_metrics = set(decomposition.metrics)
    fact_metrics = {f.metric_name_canonical for f in facts}
    chunk_categories = {sc.chunk.semantic_category for sc in top_chunks}

    for sq in sub_questions:
        sq_lower = sq.lower()
        # Check if any fact metric matches
        if any(m in sq_lower or m.replace("_", " ") in sq_lower for m in fact_metrics):
            answered_by_facts += 1
        elif any(cat in sq_lower for cat in chunk_categories) or top_chunks:
            answered_by_chunks += 1
        else:
            unanswered += 1

    # If we have facts for requested metrics, count those
    if requested_metrics:
        matched_metrics = requested_metrics & fact_metrics
        answered_by_facts = max(answered_by_facts, len(matched_metrics))

    total = len(sub_questions) if sub_questions else 1
    score = (answered_by_facts * 1.0 + answered_by_chunks * 0.7) / max(total, 1)

    if score >= 0.8:
        label = "HIGH"
    elif score >= 0.4:
        label = "MEDIUM"
    else:
        label = "LOW"

    return RetrievalConfidence(
        score=round(score, 2),
        label=label,
        answered_by_facts=answered_by_facts,
        answered_by_chunks=answered_by_chunks,
        unanswered=unanswered,
    )
