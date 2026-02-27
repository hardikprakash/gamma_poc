"""
M4: Fact Extractor — Chunk → ExtractionResult (facts, entities, risk_factors).

Makes 1 LLM call per chunk. Pydantic-validated output.
Runs metric normalization after extraction (exact match → fuzzy → store raw).
"""

from __future__ import annotations
import logging
import uuid

from models.chunk import Chunk
from models.fact import FinancialFact, Entity, RiskFactor
from models.response import ExtractionResult, ExtractionLLMResponse
from models.taxonomy import resolve_metric_name, METRIC_CATEGORIES
from llm.validator import validated_llm_call

logger = logging.getLogger(__name__)

_EXTRACTION_SYSTEM = (
    "You extract structured financial data from document chunks. "
    "Extract ONLY facts explicitly stated in the content. Do not infer, calculate, or extrapolate. "
    "Return only valid JSON, no prose, no markdown fences."
)


async def extract_facts(chunk: Chunk) -> ExtractionResult:
    """
    Returns ExtractionResult with .facts, .entities, .risk_factors.
    Makes 1 LLM call per chunk. Pydantic-validated.
    """
    prompt = f"""Extract all financial facts and entities from this content.

Source: {chunk.company} | {chunk.fiscal_year} | {chunk.section_path} | {chunk.chunk_type}

IMPORTANT: Any reference to "the company", "we", "our", or "the Group" in this content
refers exclusively to {chunk.company}. Extract all facts as belonging to {chunk.company} for {chunk.fiscal_year}.

Content:
{chunk.content}

Return JSON:
{{
  "facts": [
    {{
      "metric_name": "<exact name as written in document>",
      "value": 0.0,
      "unit": "millions",
      "currency": "USD",
      "period": "FY2023",
      "fiscal_year": {chunk.fiscal_year},
      "is_comparative": false,
      "verbatim_text": "<exact quote from source confirming the value, max 60 chars>"
    }}
  ],
  "entities": [
    {{
      "name": "<entity name>",
      "entity_type": "company"
    }}
  ],
  "risk_themes": []
}}"""

    try:
        llm_result: ExtractionLLMResponse = await validated_llm_call(
            prompt, ExtractionLLMResponse, system=_EXTRACTION_SYSTEM
        ) # type: ignore
    except Exception as e:
        logger.warning(f"Fact extraction failed for chunk {chunk.chunk_id}: {e}")
        return ExtractionResult(facts=[], entities=[], risk_factors=[])

    # ── Normalize and build FinancialFact objects ────────────────────────────
    facts: list[FinancialFact] = []
    for raw_fact in llm_result.facts:
        canonical_name, confidence = resolve_metric_name(raw_fact.metric_name)
        # Override confidence: table chunks → high, prose → medium, fuzzy → low
        if chunk.chunk_type == "table" and confidence != "low":
            confidence = "high"
        elif confidence == "high":
            confidence = "medium" if chunk.chunk_type == "prose" else confidence

        metric_category = METRIC_CATEGORIES.get(canonical_name, "other")

        fact = FinancialFact(
            fact_id=str(uuid.uuid4()),
            metric_name_raw=raw_fact.metric_name,
            metric_name_canonical=canonical_name,
            metric_category=metric_category,
            value=raw_fact.value,
            unit=raw_fact.unit,
            currency=raw_fact.currency,
            period=raw_fact.period or f"FY{raw_fact.fiscal_year or chunk.fiscal_year}",
            fiscal_year=raw_fact.fiscal_year or chunk.fiscal_year,
            company=chunk.company,
            is_comparative=raw_fact.is_comparative,
            source_chunk_id=chunk.chunk_id,
            confidence=confidence,
            verbatim_text=raw_fact.verbatim_text[:60],
        )
        facts.append(fact)

    # ── Build Entity objects ─────────────────────────────────────────────────
    entities: list[Entity] = []
    doc_id = f"{chunk.ticker}_{chunk.fiscal_year}_{chunk.doc_type}"
    for raw_entity in llm_result.entities:
        entity = Entity(
            entity_id=str(uuid.uuid4()),
            name=raw_entity.name,
            canonical_name=raw_entity.name.strip().lower(),
            entity_type=raw_entity.entity_type,
            first_seen_doc_id=doc_id,
            mention_count=1,
        )
        entities.append(entity)

    # ── Build RiskFactor objects (only for risk-category chunks) ─────────────
    risk_factors: list[RiskFactor] = []
    if chunk.semantic_category == "risk_factors" and llm_result.risk_themes:
        for theme in llm_result.risk_themes:
            rf = RiskFactor(
                risk_id=str(uuid.uuid4()),
                title=theme,
                summary="",
                risk_category=theme,
                company=chunk.company,
                fiscal_year=chunk.fiscal_year,
                first_appeared_year=chunk.fiscal_year,
                source_chunk_id=chunk.chunk_id,
            )
            risk_factors.append(rf)

    logger.debug(
        f"Chunk {chunk.chunk_id}: {len(facts)} facts, "
        f"{len(entities)} entities, {len(risk_factors)} risks"
    )

    return ExtractionResult(facts=facts, entities=entities, risk_factors=risk_factors)
