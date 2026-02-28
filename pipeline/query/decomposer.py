"""
M6: Query Decomposer — Natural language query → structured QueryDecomposition.

Makes 1 LLM call. Pydantic-validated.
"""

from __future__ import annotations
import logging
from models.response import QueryDecomposition
from llm.validator import validated_llm_call
from config import LLM_QUERY_MAX_TOKENS

logger = logging.getLogger(__name__)

_DECOMPOSER_SYSTEM = (
    "You decompose financial document queries into structured retrieval parameters. "
    "Return only valid JSON, no prose, no markdown fences."
)


async def decompose_query(
    query: str,
    available_companies: list[str],
    available_years: list[int],
) -> QueryDecomposition:
    """
    Returns QueryDecomposition with companies, years, metrics, sections, intent, sub_questions.
    Makes 1 LLM call.
    """
    companies_list = ", ".join(available_companies) if available_companies else "none"
    years_list = ", ".join(str(y) for y in sorted(available_years)) if available_years else "none"

    prompt = f"""Analyze this query about financial documents.

Query: {query}
Available companies in corpus: {companies_list}
Available fiscal years: {years_list}

Return JSON:
{{
  "companies": ["<company names matching corpus>"],
  "years": [<ints>],
  "metrics": ["<canonical metric names from taxonomy: revenue, gross_profit, operating_income, net_income, ebitda, eps_diluted, eps_basic, gross_margin_pct, operating_margin_pct, net_margin_pct, total_assets, total_liabilities, total_equity, total_debt, cash_and_equivalents, operating_cashflow, capex, free_cashflow, revenue_growth_pct, rd_expense>"],
  "semantic_sections": ["<values from: financial_results | financial_statements | risk_factors | segment_data | governance | business_overview | legal | market_data | other>"],
  "intent": "lookup" | "comparison" | "trend" | "explanation" | "existence_check",
  "sub_questions": ["<atomic question answerable from a single source>"]
}}

If the query covers all available years, use the full list of years.
If the query is about a specific year not in the available years, still include it so the system can flag it as unavailable.
Always generate at least one sub_question."""

    result = await validated_llm_call(prompt, QueryDecomposition, system=_DECOMPOSER_SYSTEM,
                                      max_tokens=LLM_QUERY_MAX_TOKENS)

    # Post-processing: validate years
    if isinstance(result.years, str) and result.years == "all":
        result.years = list(available_years) if available_years else []
    elif isinstance(result.years, list):
        # Keep years even if not in corpus — retriever will handle "not found"
        pass

    # Ensure at least one sub-question
    if not result.sub_questions:
        result.sub_questions = [query]

    logger.info(
        f"Query decomposed: intent={result.intent}, "
        f"companies={result.companies}, years={result.years}, "
        f"metrics={result.metrics}, sub_questions={len(result.sub_questions)}"
    )

    return result
