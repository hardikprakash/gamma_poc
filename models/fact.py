"""
FinancialFact, Entity, and RiskFactor models.
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional


class FinancialFact(BaseModel):
    """A structured numeric assertion extracted from a chunk."""
    fact_id: str
    metric_name_raw: str
    metric_name_canonical: str
    metric_category: str  # revenue | profitability | liquidity | leverage | cashflow | segment | per_share | other
    value: float
    unit: str  # millions | billions | thousands | percentage | ratio | per_share | units
    currency: Optional[str] = None  # ISO 4217
    period: str  # FY2023 | H1 2023 | Q4 2022 etc.
    fiscal_year: int
    company: str
    is_comparative: bool = False
    source_chunk_id: str
    confidence: str = "medium"  # high | medium | low
    verbatim_text: str = ""


class Entity(BaseModel):
    """A named entity extracted from chunks."""
    entity_id: str
    name: str
    canonical_name: str
    entity_type: str  # company | person | geography | product | auditor | regulator | risk_theme
    first_seen_doc_id: str = ""
    mention_count: int = 1


class RiskFactor(BaseModel):
    """Extracted risk theme with its own embedding for cross-year matching."""
    risk_id: str
    title: str
    summary: str = ""
    risk_category: str = ""
    company: str = ""
    fiscal_year: int = 0
    first_appeared_year: int = 0
    source_chunk_id: str = ""
    embedding: Optional[list[float]] = None
