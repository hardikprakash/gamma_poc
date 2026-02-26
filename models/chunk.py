"""
Core Chunk data model.
"""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class Chunk:
    """The core unit of retrievable content."""
    chunk_id: str           # format: {TICKER}_{YEAR}_{SEMANTIC_CAT}_{PAGE}_{SEQ}
    company: str            # full legal company name
    ticker: str             # stock ticker or short identifier
    fiscal_year: int
    doc_type: str           # 20-F | 10-K | annual_report | earnings_release | other
    section_path: str       # full heading path e.g. "Annual Report > MD&A > Revenue Analysis"
    semantic_category: str  # from fixed taxonomy
    page_start: int         # 0-indexed
    page_end: int           # 0-indexed
    chunk_type: str         # prose | table | footnote
    content: str            # full text; tables serialized as markdown
    content_structured: dict | None = None   # for tables: {headers, rows}; None for prose
    token_count: int = 0
    embedding: list[float] | None = None     # dimension matches EMBEDDING_DIMENSIONS

    def to_dict(self) -> dict:
        """Serialize to dict for Neo4j / JSON — excludes embedding for readability."""
        d = {
            "chunk_id": self.chunk_id,
            "company": self.company,
            "ticker": self.ticker,
            "fiscal_year": self.fiscal_year,
            "doc_type": self.doc_type,
            "section_path": self.section_path,
            "semantic_category": self.semantic_category,
            "page_start": self.page_start,
            "page_end": self.page_end,
            "chunk_type": self.chunk_type,
            "content": self.content,
            "token_count": self.token_count,
        }
        return d
