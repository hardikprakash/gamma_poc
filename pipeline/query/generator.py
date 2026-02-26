"""
M10: Answer Generator — LLM call to produce the answer from assembled context.

One retry on Pydantic validation failure. Raises on second failure.
Uses strict system prompt from DOC3 §1.6.
"""

from __future__ import annotations
import json
import logging

from models.response import RawLLMResponse, ContextPayload, QueryDecomposition
from llm.validator import validated_llm_call

logger = logging.getLogger(__name__)

_GENERATION_SYSTEM = """You are a financial analyst assistant answering questions using retrieved financial filing excerpts.
Follow every rule below without exception.

RULES:
1. Every factual claim MUST be followed immediately by its citation key in brackets, e.g. [A-2023-fin-p47].
   Use the exact keys provided in the context blocks. Do not invent keys.
2. For numeric claims: prefer [FACT] blocks over [SOURCE] prose. If a FACT block exists for a number, cite it.
3. NEVER state a number not present verbatim in the provided context.
4. If a sub-question cannot be answered from the provided context, state explicitly:
   "I could not find information about [X] in the provided documents."
   Include it in unanswerable_sub_questions.
5. Do not extrapolate, interpolate, or calculate derived figures unless all inputs are sourced
   and the user explicitly requests a calculation.
6. If the same metric appears with conflicting values in different sources, report both values
   and flag the conflict. Never silently choose one value.
7. Present numerical comparisons as markdown tables.
8. Return only valid JSON matching the schema below. No prose outside the JSON."""


async def generate_answer(
    query: str,
    context_payload: ContextPayload,
    decomposition: QueryDecomposition,
) -> RawLLMResponse:
    """
    Returns Pydantic-validated RawLLMResponse.
    One retry on validation failure.
    """
    sub_questions_str = json.dumps(decomposition.sub_questions)

    prompt = f"""Query: {query}
Sub-questions to address: {sub_questions_str}

Context:
{context_payload.context_str}

Return JSON:
{{
  "answer": "<answer text with inline [citation-keys]>",
  "citations_used": ["key1", "key2"],
  "unanswerable_sub_questions": ["<sub-questions with no source found>"],
  "conflicts_detected": ["<description of any conflicting values found>"],
  "confidence_note": "<one sentence on answer completeness>"
}}"""

    result = await validated_llm_call(
        prompt, RawLLMResponse, system=_GENERATION_SYSTEM
    )

    logger.info(
        f"Answer generated: {len(result.citations_used)} citations, "
        f"{len(result.unanswerable_sub_questions)} unanswerable, "
        f"{len(result.conflicts_detected)} conflicts"
    )

    return result
