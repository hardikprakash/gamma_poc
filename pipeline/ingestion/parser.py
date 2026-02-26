"""
M1: Parser — PDF → ParsedDocument.

No LLM calls. Primary extraction via pymupdf4llm (markdown-native).
Uses fitz for font profile metadata. Falls back to pdfplumber for
tables that fail markdown validation (merged cells, multi-row headers,
page-spanning tables).
"""

from __future__ import annotations
import logging
import re
import fitz  # PyMuPDF — font profiling
import pymupdf4llm
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
      .pages: list[PageData]   — text blocks + tables per page
      .font_profile: dict      — (font_size, weight) → frequency count across doc
      .heading_level_map: dict — (font_size, weight) → heading level 1-4
    Primary path: pymupdf4llm markdown output (page_chunks=True).
    Fallback: pdfplumber for tables that fail markdown validation.
    """
    # ── 1. pymupdf4llm: page-chunked markdown ───────────────────────────────
    page_chunks = pymupdf4llm.to_markdown(
        pdf_path,
        page_chunks=True,
        ignore_images=True,
        force_text=True,
    )
    total_pages = len(page_chunks)

    # ── 2. fitz: font profile for heading-level metadata ────────────────────
    fitz_doc = fitz.open(pdf_path)
    font_profile = _build_font_profile(fitz_doc)
    heading_level_map = _build_heading_level_map(font_profile)
    fitz_doc.close()

    # ── 3. pdfplumber: opened lazily only when a table fails validation ─────
    plumber_doc = None
    fallback_count = 0

    pages: list[PageData] = []
    for page_idx, chunk in enumerate(page_chunks):
        md_text: str = chunk.get("text", "")

        # Parse markdown into text blocks and table candidates
        text_blocks, md_tables = _parse_markdown_page(md_text, page_idx)

        # Validate each table; fall back to pdfplumber on failure
        validated_tables: list[TableData] = []
        needs_fallback = False
        for tbl in md_tables:
            if _validate_markdown_table(tbl.markdown):
                validated_tables.append(tbl)
            else:
                needs_fallback = True
                logger.debug(
                    f"Page {page_idx}: table {tbl.table_id} failed validation, "
                    "queuing pdfplumber fallback"
                )

        if needs_fallback:
            if plumber_doc is None:
                plumber_doc = pdfplumber.open(pdf_path)
            if page_idx < len(plumber_doc.pages):
                fb_tables = _extract_tables_pdfplumber(
                    plumber_doc.pages[page_idx], page_idx,
                    start_idx=len(validated_tables),
                )
                validated_tables.extend(fb_tables)
                fallback_count += 1

        raw_text = _strip_markdown_formatting(md_text)

        pages.append(PageData(
            page_idx=page_idx,
            text_blocks=text_blocks,
            tables=validated_tables,
            raw_text=raw_text,
        ))

    if plumber_doc is not None:
        plumber_doc.close()

    logger.info(
        f"Parsed {pdf_path}: {total_pages} pages, "
        f"body font={font_profile.get('body')}, "
        f"{len(heading_level_map)} heading levels, "
        f"{fallback_count} pages needed pdfplumber table fallback"
    )

    return ParsedDocument(
        pages=pages,
        font_profile=font_profile,
        heading_level_map=heading_level_map,
        total_pages=total_pages,
        file_path=pdf_path,
    )


# ── Markdown Page Parsing ───────────────────────────────────────────────────

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)")
_TABLE_ROW_RE = re.compile(r"^\|.*\|$")
_TABLE_SEP_RE = re.compile(r"^\|[\s:|\-]*-[\s:|\-]*\|$")


def _parse_markdown_page(
    md_text: str, page_idx: int
) -> tuple[list[TextBlock], list[TableData]]:
    """
    Split a single page's markdown into TextBlock objects and TableData objects.
    Headings (lines starting with #) become TextBlock with is_heading=True.
    Contiguous pipe-delimited lines are extracted as markdown tables.
    Everything else becomes prose TextBlock entries.
    """
    text_blocks: list[TextBlock] = []
    tables: list[TableData] = []
    lines = md_text.split("\n")

    i = 0
    table_counter = 0
    prose_buffer: list[str] = []

    def _flush_prose():
        nonlocal prose_buffer
        text = "\n".join(prose_buffer).strip()
        if text:
            text_blocks.append(TextBlock(
                text=text,
                font_size=0.0,
                font_weight="normal",
                page_idx=page_idx,
                is_heading=False,
                heading_level=0,
            ))
        prose_buffer = []

    while i < len(lines):
        line = lines[i]

        # ── Heading ──────────────────────────────────────────────────────
        heading_match = _HEADING_RE.match(line)
        if heading_match:
            _flush_prose()
            level = len(heading_match.group(1))  # number of # chars
            heading_text = heading_match.group(2).strip()
            if heading_text:
                text_blocks.append(TextBlock(
                    text=heading_text,
                    font_size=0.0,
                    font_weight="bold",
                    page_idx=page_idx,
                    is_heading=True,
                    heading_level=level,
                ))
            i += 1
            continue

        # ── Table block (contiguous pipe-delimited lines) ────────────────
        if _TABLE_ROW_RE.match(line.strip()):
            _flush_prose()
            table_lines: list[str] = []
            while i < len(lines) and _TABLE_ROW_RE.match(lines[i].strip()):
                table_lines.append(lines[i].strip())
                i += 1

            # Parse into headers / rows
            tbl = _parse_markdown_table_block(
                table_lines, page_idx, table_counter
            )
            if tbl:
                tables.append(tbl)
                table_counter += 1
            continue

        # ── Regular prose ────────────────────────────────────────────────
        prose_buffer.append(line)
        i += 1

    _flush_prose()
    return text_blocks, tables


def _parse_markdown_table_block(
    lines: list[str], page_idx: int, table_counter: int
) -> TableData | None:
    """Parse contiguous pipe-delimited lines into a TableData object."""
    if len(lines) < 2:
        return None

    def _split_row(line: str) -> list[str]:
        # Strip leading/trailing pipes, split by |
        cells = line.strip().strip("|").split("|")
        return [c.strip() for c in cells]

    # Detect separator row (---|----|---)
    sep_idx = None
    for idx, ln in enumerate(lines[:3]):
        if _TABLE_SEP_RE.match(ln):
            sep_idx = idx
            break

    if sep_idx is not None and sep_idx > 0:
        # Header rows above separator, data rows below
        header_cells = _split_row(lines[sep_idx - 1])
        data_lines = lines[sep_idx + 1:]
    else:
        # No separator found — treat first row as header
        header_cells = _split_row(lines[0])
        data_lines = lines[1:]

    rows = [_split_row(ln) for ln in data_lines if not _TABLE_SEP_RE.match(ln)]
    markdown = "\n".join(lines)

    return TableData(
        table_id=f"p{page_idx}_t{table_counter}",
        page_idx=page_idx,
        headers=header_cells,
        rows=rows,
        markdown=markdown,
    )


# ── Table Validation ────────────────────────────────────────────────────────

def _validate_markdown_table(markdown: str) -> bool:
    """
    Quick structural check on a markdown table:
    1. Must have ≥ 2 data rows (header + separator + ≥1 data).
    2. Column count must be consistent across all rows (tolerance: ±1).
    3. Separator row must exist and use only dashes/colons/spaces/pipes.
    4. No row should have entirely empty cells (sign of merged-cell breakage).
    Returns True if table looks well-formed.
    """
    lines = [ln.strip() for ln in markdown.strip().split("\n") if ln.strip()]
    if len(lines) < 3:
        return False

    def _col_count(line: str) -> int:
        return len(line.strip().strip("|").split("|"))

    # Find separator
    sep_found = False
    for ln in lines[:3]:
        if _TABLE_SEP_RE.match(ln):
            sep_found = True
            break
    if not sep_found:
        return False

    # Column consistency check
    col_counts = [_col_count(ln) for ln in lines if not _TABLE_SEP_RE.match(ln)]
    if not col_counts:
        return False
    expected = col_counts[0]
    for cc in col_counts:
        if abs(cc - expected) > 1:
            return False

    # All-empty-row check (sign of merged cell rendering failure)
    for ln in lines:
        if _TABLE_SEP_RE.match(ln):
            continue
        cells = ln.strip().strip("|").split("|")
        if all(c.strip() == "" for c in cells):
            return False

    return True


# ── pdfplumber Table Fallback ───────────────────────────────────────────────

def _extract_tables_pdfplumber(
    plumber_page, page_idx: int, start_idx: int = 0
) -> list[TableData]:
    """Extract tables from a pdfplumber page — used only when pymupdf4llm tables fail validation."""
    tables: list[TableData] = []
    try:
        raw_tables = plumber_page.extract_tables()
    except Exception as e:
        logger.debug(f"pdfplumber fallback failed on page {page_idx}: {e}")
        return tables

    for t_idx, raw_table in enumerate(raw_tables):
        if not raw_table or len(raw_table) < 2:
            continue

        headers = [str(cell).strip() if cell else "" for cell in raw_table[0]]
        rows = []
        for row in raw_table[1:]:
            rows.append([str(cell).strip() if cell else "" for cell in row])

        markdown = _table_to_markdown(headers, rows)
        table_id = f"p{page_idx}_t{start_idx + t_idx}"
        tables.append(TableData(
            table_id=table_id,
            page_idx=page_idx,
            headers=headers,
            rows=rows,
            markdown=markdown,
        ))

    return tables


def _table_to_markdown(headers: list[str], rows: list[list[str]]) -> str:
    """Convert pdfplumber table data to markdown format."""
    if not headers:
        return ""

    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        padded = row + [""] * (len(headers) - len(row))
        lines.append("| " + " | ".join(padded[:len(headers)]) + " |")

    return "\n".join(lines)


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
            if block.get("type") != 0:
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
    candidates.sort(key=lambda x: (x[0], x[1] == "bold"), reverse=True)
    return {profile: level + 1 for level, profile in enumerate(candidates[:4])}


# ── Utility ─────────────────────────────────────────────────────────────────

def _strip_markdown_formatting(md_text: str) -> str:
    """Strip markdown syntax to produce plain raw_text (used by downstream modules)."""
    text = re.sub(r"^#{1,6}\s+", "", md_text, flags=re.MULTILINE)  # headings
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)  # bold
    text = re.sub(r"\*(.+?)\*", r"\1", text)  # italic
    text = re.sub(r"`(.+?)`", r"\1", text)  # inline code
    return text
