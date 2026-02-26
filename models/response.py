"""
Response models: AgentResponse, intermediary pipeline models,
Pydantic schemas for LLM output validation.
"""

from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Literal
from dataclasses import dataclass, field


# ── AgentResponse (final output) ────────────────────────────────────────────
class AgentResponse(BaseModel):
    query: str
    answer: str
    resolved_citations: list[dict] = Field(default_factory=list)
    retrieval_confidence: dict = Field(default_factory=dict)
    unanswerable_sub_questions: list[str] = Field(default_factory=list)
    conflicts_detected: list[str] = Field(default_factory=list)
    facts_used: list[str] = Field(default_factory=list)
    sub_questions: list[str] = Field(default_factory=list)
    chunks_retrieved: int = 0
    chunks_used: int = 0
    latency_ms: int = 0


# ── Parser output ───────────────────────────────────────────────────────────
@dataclass
class TextBlock:
    text: str
    font_size: float
    font_weight: str  # "bold" | "normal"
    bbox: tuple = ()  # (x0, y0, x1, y1)
    page_idx: int = 0
    is_heading: bool = False
    heading_level: int = 0


@dataclass
class TableData:
    table_id: str
    page_idx: int
    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    bbox: tuple = ()
    markdown: str = ""


@dataclass
class PageData:
    page_idx: int
    text_blocks: list[TextBlock] = field(default_factory=list)
    tables: list[TableData] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class ParsedDocument:
    pages: list[PageData] = field(default_factory=list)
    font_profile: dict = field(default_factory=dict)
    heading_level_map: dict = field(default_factory=dict)
    total_pages: int = 0
    file_path: str = ""


# ── Structure Inference output ──────────────────────────────────────────────
@dataclass
class SectionMeta:
    raw_title: str
    normalized_title: str
    semantic_category: str
    heading_level: int
    page_start: int
    page_end: int = 0
    relevance_score: str = "medium"  # high | medium | low
    section_path: str = ""


class TableClassification(BaseModel):
    table_type: str = "unknown"
    contains_financial_facts: bool = False
    primary_metric: Optional[str] = None
    time_periods: list[str] = Field(default_factory=list)
    confidence: str = "medium"


@dataclass
class StructuredDocument:
    doc_type: str = "other"
    sections: list[SectionMeta] = field(default_factory=list)
    table_classifications: dict = field(default_factory=dict)
    # Carry-through fields
    parsed_doc: ParsedDocument | None = None
    company: str = ""
    ticker: str = ""
    fiscal_year: int = 0
    jurisdiction: str = "unknown"
    reporting_period: str = "unknown"
    currency: str = ""


# ── LLM validation schemas ─────────────────────────────────────────────────
class DocTypeDetection(BaseModel):
    form_type: str = "other"
    jurisdiction: str = "unknown"
    reporting_period: str = "unknown"
    company_name: Optional[str] = None
    fiscal_year: Optional[int] = None
    confidence: str = "medium"


class SectionLabel(BaseModel):
    raw_title: str
    normalized_title: str
    semantic_category: str
    relevance: str = "medium"


class SectionLabelsResponse(BaseModel):
    """Wrapper — LLM returns a JSON array of SectionLabel."""
    __root__: list[SectionLabel] = []

    # Pydantic v2 compat: allow parsing a raw list
    @classmethod
    def parse_list(cls, data: list[dict]) -> list[SectionLabel]:
        return [SectionLabel.model_validate(item) for item in data]


# ── Fact extraction LLM schema ──────────────────────────────────────────────
class ExtractedFact(BaseModel):
    metric_name: str
    value: float
    unit: str = "units"
    currency: Optional[str] = None
    period: str = ""
    fiscal_year: int = 0
    is_comparative: bool = False
    verbatim_text: str = ""


class ExtractedEntity(BaseModel):
    name: str
    entity_type: str = "company"


class ExtractionLLMResponse(BaseModel):
    facts: list[ExtractedFact] = Field(default_factory=list)
    entities: list[ExtractedEntity] = Field(default_factory=list)
    risk_themes: list[str] = Field(default_factory=list)


# ── Extraction result ───────────────────────────────────────────────────────
@dataclass
class ExtractionResult:
    from models.fact import FinancialFact, Entity, RiskFactor
    facts: list = field(default_factory=list)
    entities: list = field(default_factory=list)
    risk_factors: list = field(default_factory=list)


# ── Query Decomposition ────────────────────────────────────────────────────
class QueryDecomposition(BaseModel):
    companies: list[str] = Field(default_factory=list)
    years: list[int] | str = "all"  # list[int] or literal "all"
    metrics: list[str] = Field(default_factory=list)
    semantic_sections: list[str] = Field(default_factory=list)
    intent: str = "lookup"  # lookup | comparison | trend | explanation | existence_check
    sub_questions: list[str] = Field(default_factory=list)


# ── Retrieval ───────────────────────────────────────────────────────────────
@dataclass
class ScoredChunk:
    chunk: object  # Chunk dataclass
    score: float = 0.0
    source: str = ""  # "vector" | "graph_section" | "merged"


@dataclass
class RetrievalResult:
    chunks: list[ScoredChunk] = field(default_factory=list)
    facts: list = field(default_factory=list)  # list[FinancialFact]
    total_retrieved: int = 0


# ── Re-rank ─────────────────────────────────────────────────────────────────
@dataclass
class RerankResult:
    top_chunks: list[ScoredChunk] = field(default_factory=list)
    facts: list = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)


# ── Context Assembly ────────────────────────────────────────────────────────
@dataclass
class CitationEntry:
    key: str
    chunk_id: str
    company: str
    fiscal_year: int
    section_path: str
    page: int
    chunk_type: str
    confidence: str
    content_preview: str
    is_fact: bool = False
    fact_id: str = ""


@dataclass
class CitationRegistry:
    citations: dict[str, CitationEntry] = field(default_factory=dict)

    def add(self, entry: CitationEntry):
        self.citations[entry.key] = entry

    def resolve(self, key: str) -> CitationEntry | None:
        return self.citations.get(key)


@dataclass
class RetrievalConfidence:
    score: float = 0.0
    label: str = "LOW"  # HIGH | MEDIUM | LOW
    answered_by_facts: int = 0
    answered_by_chunks: int = 0
    unanswered: int = 0

    def to_dict(self) -> dict:
        return {
            "score": self.score,
            "label": self.label,
            "answered_by_facts": self.answered_by_facts,
            "answered_by_chunks": self.answered_by_chunks,
            "unanswered": self.unanswered,
        }


@dataclass
class ContextPayload:
    context_str: str = ""
    citation_registry: CitationRegistry = field(default_factory=CitationRegistry)
    confidence: RetrievalConfidence = field(default_factory=RetrievalConfidence)
    token_count: int = 0


# ── Answer Generation ───────────────────────────────────────────────────────
class RawLLMResponse(BaseModel):
    answer: str = ""
    citations_used: list[str] = Field(default_factory=list)
    unanswerable_sub_questions: list[str] = Field(default_factory=list)
    conflicts_detected: list[str] = Field(default_factory=list)
    confidence_note: str = ""


# ── Graph Build Result ──────────────────────────────────────────────────────
@dataclass
class GraphBuildResult:
    nodes_created: int = 0
    edges_created: int = 0
    embeddings_written: int = 0
