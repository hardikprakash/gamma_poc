"""
M3: Chunker — StructuredDocument → list[Chunk].

No LLM calls. Deterministic.
Rules:
  - Chunk at heading boundaries, never across section boundaries
  - Target 300-500 tokens per prose chunk; min 50, max 600
  - Each table = exactly one chunk regardless of size
  - Footnotes = separate chunk type
"""

from __future__ import annotations
import logging
import re
import tiktoken

from models.chunk import Chunk
from models.response import StructuredDocument, SectionMeta

logger = logging.getLogger(__name__)

_encoder = tiktoken.get_encoding("cl100k_base")

MIN_CHUNK_TOKENS = 50
MAX_CHUNK_TOKENS = 600
TARGET_CHUNK_TOKENS = 400


def chunk_document(structured_doc: StructuredDocument) -> list[Chunk]:
    """
    Returns list of Chunk objects with full metadata.
    No LLM calls. Deterministic.
    """
    parsed_doc = structured_doc.parsed_doc
    sections = structured_doc.sections
    company = structured_doc.company
    ticker = structured_doc.ticker
    fiscal_year = structured_doc.fiscal_year
    doc_type = structured_doc.doc_type

    if not sections:
        # Fallback: treat entire document as one section
        sections = [SectionMeta(
            raw_title="Document",
            normalized_title="Document",
            semantic_category="other",
            heading_level=1,
            page_start=0,
            page_end=parsed_doc.total_pages - 1 if parsed_doc else 0,
            relevance_score="low",
            section_path="Document",
        )]

    all_chunks: list[Chunk] = []
    chunk_seq_counters: dict[str, int] = {}

    for section in sections:
        # Collect prose text for this section's page range
        prose_text = _get_section_prose(parsed_doc, section)
        tables = _get_section_tables(parsed_doc, section)

        # Create table chunks
        for table in tables:
            seq = _next_seq(chunk_seq_counters, section, fiscal_year, ticker)
            chunk_id = _make_chunk_id(ticker, fiscal_year, section.semantic_category, table.page_idx, seq)

            content = table.markdown or _fallback_table_text(table)
            token_count = len(_encoder.encode(content))

            all_chunks.append(Chunk(
                chunk_id=chunk_id,
                company=company,
                ticker=ticker,
                fiscal_year=fiscal_year,
                doc_type=doc_type,
                section_path=section.section_path,
                semantic_category=section.semantic_category,
                page_start=table.page_idx,
                page_end=table.page_idx,
                chunk_type="table",
                content=content,
                content_structured={"headers": table.headers, "rows": table.rows},
                token_count=token_count,
            ))

        # Create prose chunks — split by sentence boundary
        if prose_text.strip():
            prose_chunks = _split_prose(prose_text, MAX_CHUNK_TOKENS, MIN_CHUNK_TOKENS)
            for chunk_text in prose_chunks:
                token_count = len(_encoder.encode(chunk_text))
                seq = _next_seq(chunk_seq_counters, section, fiscal_year, ticker)
                chunk_id = _make_chunk_id(
                    ticker, fiscal_year, section.semantic_category,
                    section.page_start, seq,
                )

                # Detect footnote chunks
                chunk_type = "prose"
                if _is_footnote(chunk_text):
                    chunk_type = "footnote"

                all_chunks.append(Chunk(
                    chunk_id=chunk_id,
                    company=company,
                    ticker=ticker,
                    fiscal_year=fiscal_year,
                    doc_type=doc_type,
                    section_path=section.section_path,
                    semantic_category=section.semantic_category,
                    page_start=section.page_start,
                    page_end=section.page_end,
                    chunk_type=chunk_type,
                    content=chunk_text,
                    token_count=token_count,
                ))

    # Merge undersized chunks with their neighbors
    all_chunks = _merge_undersized(all_chunks)

    logger.info(f"Created {len(all_chunks)} chunks from {len(sections)} sections")
    return all_chunks


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_chunk_id(ticker: str, fiscal_year: int, semantic_cat: str, page: int, seq: int) -> str:
    """Format: {TICKER}_{YEAR}_{SEMANTIC_CAT}_{PAGE}_{SEQ}"""
    cat_short = semantic_cat.replace("_", "")[:12]
    return f"{ticker}_{fiscal_year}_{cat_short}_p{page}_{seq:03d}"


def _next_seq(counters: dict, section: SectionMeta, fiscal_year: int, ticker: str) -> int:
    key = f"{ticker}_{fiscal_year}_{section.semantic_category}"
    counters[key] = counters.get(key, 0) + 1
    return counters[key]


def _get_section_prose(parsed_doc, section: SectionMeta) -> str:
    """Gather all prose text from pages in this section's range."""
    if not parsed_doc:
        return ""
    texts = []
    for page in parsed_doc.pages:
        if section.page_start <= page.page_idx <= section.page_end:
            for block in page.text_blocks:
                if not block.is_heading:
                    texts.append(block.text)
    return " ".join(texts)


def _get_section_tables(parsed_doc, section: SectionMeta) -> list:
    """Gather all tables from pages in this section's range."""
    if not parsed_doc:
        return []
    tables = []
    for page in parsed_doc.pages:
        if section.page_start <= page.page_idx <= section.page_end:
            tables.extend(page.tables)
    return tables


def _split_prose(text: str, max_tokens: int, min_tokens: int) -> list[str]:
    """Split prose text into chunks at sentence boundaries, respecting token limits."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current = []
    current_tokens = 0

    for sent in sentences:
        sent_tokens = len(_encoder.encode(sent))
        if current_tokens + sent_tokens > max_tokens and current:
            chunks.append(" ".join(current))
            current = [sent]
            current_tokens = sent_tokens
        else:
            current.append(sent)
            current_tokens += sent_tokens

    if current:
        chunks.append(" ".join(current))

    return chunks


def _merge_undersized(chunks: list[Chunk]) -> list[Chunk]:
    """Merge chunks smaller than MIN_CHUNK_TOKENS with adjacent chunks in same section."""
    if len(chunks) <= 1:
        return chunks

    merged: list[Chunk] = []
    i = 0
    while i < len(chunks):
        chunk = chunks[i]
        if chunk.token_count < MIN_CHUNK_TOKENS and chunk.chunk_type == "prose":
            # Try to merge with next chunk in same section
            if i + 1 < len(chunks) and chunks[i + 1].section_path == chunk.section_path and chunks[i + 1].chunk_type == "prose":
                next_chunk = chunks[i + 1]
                combined_content = chunk.content + " " + next_chunk.content
                combined_tokens = len(_encoder.encode(combined_content))
                merged_chunk = Chunk(
                    chunk_id=chunk.chunk_id,
                    company=chunk.company,
                    ticker=chunk.ticker,
                    fiscal_year=chunk.fiscal_year,
                    doc_type=chunk.doc_type,
                    section_path=chunk.section_path,
                    semantic_category=chunk.semantic_category,
                    page_start=chunk.page_start,
                    page_end=next_chunk.page_end,
                    chunk_type="prose",
                    content=combined_content,
                    token_count=combined_tokens,
                )
                merged.append(merged_chunk)
                i += 2
                continue
            # Try to merge with previous
            elif merged and merged[-1].section_path == chunk.section_path and merged[-1].chunk_type == "prose":
                prev = merged[-1]
                prev.content = prev.content + " " + chunk.content
                prev.token_count = len(_encoder.encode(prev.content))
                prev.page_end = max(prev.page_end, chunk.page_end)
                i += 1
                continue
        merged.append(chunk)
        i += 1

    return merged


def _is_footnote(text: str) -> bool:
    """Heuristic: footnote text often starts with a number/symbol followed by explanation."""
    text_stripped = text.strip()
    if re.match(r'^\(\d+\)|^\d+\s', text_stripped) and len(text_stripped) < 500:
        return True
    if text_stripped.lower().startswith("note") or text_stripped.lower().startswith("footnote"):
        return True
    return False


def _fallback_table_text(table) -> str:
    """Fallback markdown generation from table data."""
    if not table.headers:
        return ""
    lines = ["| " + " | ".join(table.headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in table.headers) + " |")
    for row in table.rows:
        padded = row + [""] * (len(table.headers) - len(row))
        lines.append("| " + " | ".join(padded[:len(table.headers)]) + " |")
    return "\n".join(lines)
