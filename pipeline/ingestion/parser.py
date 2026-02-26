"""
M1: Parser — PDF → ParsedDocument.

No LLM calls. Uses PyMuPDF (fitz) for text + font metadata, pdfplumber for tables.
"""

from __future__ import annotations
import logging
import fitz  # PyMuPDF
import pdfplumber

from models.response import (
    ParsedDocument,
    PageData,
    TextBlock,
    TableData,
)

logger = logging.getLogger(__name__)


# ── Public API ───────────────────────────────────────────────────────────────

def parse_pdf(pdf_path: str) -> ParsedDocument:
    """
    Returns ParsedDocument:
      .pages: list[PageData]   — text blocks with font metadata, table bboxes per page
      .font_profile: dict      — (font_size, weight) → frequency count across doc
      .heading_level_map: dict — (font_size, weight) → heading level 1|2|3
    No LLM calls. Uses PyMuPDF for text+font, pdfplumber for tables.
    """
    fitz_doc = fitz.open(pdf_path)
    plumber_doc = pdfplumber.open(pdf_path)

    total_pages = len(fitz_doc)
    font_profile = _build_font_profile(fitz_doc)
    heading_level_map = _build_heading_level_map(font_profile)

    pages: list[PageData] = []
    for page_idx in range(total_pages):
        fitz_page = fitz_doc[page_idx]
        plumber_page = plumber_doc.pages[page_idx] if page_idx < len(plumber_doc.pages) else None

        text_blocks = _extract_text_blocks(fitz_page, page_idx, heading_level_map)
        tables = _extract_tables(plumber_page, page_idx) if plumber_page else []
        raw_text = fitz_page.get_text("text")

        pages.append(PageData(
            page_idx=page_idx,
            text_blocks=text_blocks,
            tables=tables,
            raw_text=raw_text,
        ))

    fitz_doc.close()
    plumber_doc.close()

    logger.info(
        f"Parsed {pdf_path}: {total_pages} pages, "
        f"body font={font_profile.get('body')}, "
        f"{len(heading_level_map)} heading levels detected"
    )

    return ParsedDocument(
        pages=pages,
        font_profile=font_profile,
        heading_level_map=heading_level_map,
        total_pages=total_pages,
        file_path=pdf_path,
    )


# ── Font Profile (DOC3 §2.8) ────────────────────────────────────────────────

def _build_font_profile(fitz_doc) -> dict:
    """
    Count (font_size_rounded, weight) frequencies across all spans.
    The most frequent profile is 'body'.
    """
    font_counts: dict[tuple, int] = {}
    for page in fitz_doc:
        blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
        for block in blocks:
            if block.get("type") != 0:  # skip image blocks
                continue
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span.get("text", "").strip()
                    if not text:
                        continue
                    size = round(span["size"])
                    weight = "bold" if span["flags"] & 16 else "normal"
                    key = (size, weight)
                    font_counts[key] = font_counts.get(key, 0) + len(text)

    if not font_counts:
        return {"body": (10, "normal"), "all": {}}

    body_profile = max(font_counts, key=font_counts.get)
    return {"body": body_profile, "all": font_counts}


def _build_heading_level_map(font_profile: dict) -> dict:
    """
    Map (font_size, weight) tuples to heading levels 1-4.
    Headings are profiles larger than body font, or same-size bold.
    """
    body_size = font_profile["body"][0]
    candidates = [
        (size, weight)
        for (size, weight) in font_profile.get("all", {})
        if size > body_size or (size == body_size and weight == "bold")
    ]
    # Sort: largest first, bold before normal at same size
    candidates.sort(key=lambda x: (x[0], x[1] == "bold"), reverse=True)
    return {profile: level + 1 for level, profile in enumerate(candidates[:4])}


# ── Text Block Extraction ───────────────────────────────────────────────────

def _extract_text_blocks(
    fitz_page, page_idx: int, heading_level_map: dict
) -> list[TextBlock]:
    """Extract text spans grouped into TextBlock objects with heading detection."""
    blocks_data = fitz_page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
    result: list[TextBlock] = []

    for block in blocks_data:
        if block.get("type") != 0:
            continue

        for line in block.get("lines", []):
            for span in line.get("spans", []):
                text = span.get("text", "").strip()
                if not text:
                    continue

                size = round(span["size"])
                weight = "bold" if span["flags"] & 16 else "normal"
                font_key = (size, weight)
                is_heading = font_key in heading_level_map
                heading_level = heading_level_map.get(font_key, 0)

                bbox = tuple(span.get("bbox", block.get("bbox", ())))
                result.append(TextBlock(
                    text=text,
                    font_size=span["size"],
                    font_weight=weight,
                    bbox=bbox,
                    page_idx=page_idx,
                    is_heading=is_heading,
                    heading_level=heading_level,
                ))

    return result


# ── Table Extraction ────────────────────────────────────────────────────────

def _extract_tables(plumber_page, page_idx: int) -> list[TableData]:
    """Extract tables from a pdfplumber page, convert to markdown."""
    tables: list[TableData] = []
    try:
        raw_tables = plumber_page.extract_tables()
    except Exception as e:
        logger.debug(f"Table extraction failed on page {page_idx}: {e}")
        return tables

    for t_idx, raw_table in enumerate(raw_tables):
        if not raw_table or len(raw_table) < 2:
            continue

        # First row as headers, rest as data rows
        headers = [str(cell).strip() if cell else "" for cell in raw_table[0]]
        rows = []
        for row in raw_table[1:]:
            rows.append([str(cell).strip() if cell else "" for cell in row])

        markdown = _table_to_markdown(headers, rows)

        table_id = f"p{page_idx}_t{t_idx}"
        tables.append(TableData(
            table_id=table_id,
            page_idx=page_idx,
            headers=headers,
            rows=rows,
            markdown=markdown,
        ))

    return tables


def _table_to_markdown(headers: list[str], rows: list[list[str]]) -> str:
    """Convert table data to markdown format."""
    if not headers:
        return ""

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        # Pad row to match headers length
        padded = row + [""] * (len(headers) - len(row))
        lines.append("| " + " | ".join(padded[:len(headers)]) + " |")

    return "\n".join(lines)
