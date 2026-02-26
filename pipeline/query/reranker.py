"""
M8: Re-ranker — Scores chunks with cross-encoder, applies bonuses, detects conflicts.

No LLM calls. Uses cross-encoder/ms-marco-MiniLM-L-6-v2.
FinancialFact nodes bypass re-ranking entirely.
"""

from __future__ import annotations
import logging
from collections import defaultdict

from models.response import ScoredChunk, RetrievalResult, RerankResult
from models.fact import FinancialFact
from config import RERANKER_MODEL, RERANK_TOP_K, CONTEXT_TOKEN_BUDGET

logger = logging.getLogger(__name__)

# Lazy-load cross-encoder to avoid import time at startup
_cross_encoder = None


def _get_cross_encoder():
    global _cross_encoder
    if _cross_encoder is None:
        from sentence_transformers import CrossEncoder
        _cross_encoder = CrossEncoder(RERANKER_MODEL)
        logger.info(f"Loaded re-ranker model: {RERANKER_MODEL}")
    return _cross_encoder


def rerank(
    query: str,
    retrieval_result: RetrievalResult,
    token_budget: int = CONTEXT_TOKEN_BUDGET,
) -> RerankResult:
    """
    Re-rank chunks using cross-encoder. Facts bypass re-ranking.
    Score = cross_encoder_score + source_type_bonus + section_relevance_bonus.
    """
    chunks = retrieval_result.chunks
    facts = retrieval_result.facts

    # ── Detect conflicts among facts ─────────────────────────────────────────
    conflicts = _detect_conflicts(facts)

    if not chunks:
        return RerankResult(top_chunks=[], facts=facts, conflicts=conflicts)

    # ── Cross-encoder scoring ────────────────────────────────────────────────
    encoder = _get_cross_encoder()
    pairs = [(query, sc.chunk.content) for sc in chunks]

    try:
        scores = encoder.predict(pairs)
    except Exception as e:
        logger.warning(f"Cross-encoder failed, using retrieval scores: {e}")
        scores = [sc.score for sc in chunks]

    # ── Apply bonuses ────────────────────────────────────────────────────────
    scored: list[ScoredChunk] = []
    for i, sc in enumerate(chunks):
        base_score = float(scores[i])

        # Table chunks: +0.2 bonus
        if sc.chunk.chunk_type == "table":
            base_score += 0.2

        # High-relevance semantic sections: +0.15 bonus
        high_relevance_sections = {
            "financial_results", "financial_statements",
            "segment_data", "risk_factors"
        }
        if sc.chunk.semantic_category in high_relevance_sections:
            base_score += 0.15

        scored.append(ScoredChunk(
            chunk=sc.chunk,
            score=base_score,
            source=sc.source,
        ))

    # ── Sort and apply token budget ──────────────────────────────────────────
    scored.sort(key=lambda x: x.score, reverse=True)

    top_chunks: list[ScoredChunk] = []
    total_tokens = 0
    for sc in scored:
        if len(top_chunks) >= RERANK_TOP_K:
            break
        if total_tokens + sc.chunk.token_count > token_budget:
            # Try to fit smaller chunks
            continue
        top_chunks.append(sc)
        total_tokens += sc.chunk.token_count

    logger.info(
        f"Re-ranked: {len(chunks)} → {len(top_chunks)} chunks "
        f"({total_tokens} tokens), {len(conflicts)} conflicts"
    )

    return RerankResult(
        top_chunks=top_chunks,
        facts=facts,
        conflicts=conflicts,
    )


# ── Conflict Detection (DOC3 §2.4) ──────────────────────────────────────────

def _detect_conflicts(facts: list[FinancialFact]) -> list[str]:
    """Detect same metric / period / company with different values."""
    groups: dict[tuple, list[FinancialFact]] = defaultdict(list)
    for fact in facts:
        key = (fact.metric_name_canonical, fact.period, fact.company)
        groups[key].append(fact)

    conflicts = []
    for (metric, period, company), group in groups.items():
        values = set(f.value for f in group)
        if len(values) > 1:
            vals_str = ", ".join(
                f"{f.value} {f.unit} (source: {f.source_chunk_id})" for f in group
            )
            conflicts.append(
                f"{company} {metric} for {period} has conflicting values: {vals_str}"
            )

    return conflicts
