"""
M1: Parser — PDF → ParsedDocument.

No LLM calls.
  • pdfplumber — PRIMARY table extraction (structured headers/rows/markdown).
  • pymupdf4llm — prose & heading extraction (markdown per page).

Table regions are stripped from the pymupdf4llm markdown so table text
never bleeds into prose TextBlocks.
"""

from __future__ import annotations
import logging
import re
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

    Flow per page:
      1. pdfplumber extracts every table → TableData objects.
      2. pymupdf4llm gives the full-page markdown.
      3. All pipe-delimited table blocks are stripped from the markdown.
      4. Remaining markdown is parsed into heading & prose TextBlocks.
    """
    # ── 1. pymupdf4llm: page-chunked markdown (prose + headings) ────────────
    page_chunks = pymupdf4llm.to_markdown(
        pdf_path,
        page_chunks=True,
        ignore_images=True,
        force_text=True,
    )
    total_pages = len(page_chunks)

    # ── 2. pdfplumber: PRIMARY table extraction ─────────────────────────────
    plumber_doc = pdfplumber.open(pdf_path)
    plumber_table_count = 0

    pages: list[PageData] = []
    for page_idx, chunk in enumerate(page_chunks):
        md_text: str = chunk.get("text", "")

        # 3a. Extract tables from pdfplumber (primary source of truth)
        plumber_tables: list[TableData] = []
        if page_idx < len(plumber_doc.pages):
            plumber_tables = _extract_tables_pdfplumber(
                plumber_doc.pages[page_idx], page_idx,
            )
            plumber_table_count += len(plumber_tables)

        # 3b. Strip all pipe-delimited table blocks from markdown
        clean_md = _strip_table_blocks(md_text)

        # 3c. Parse remaining markdown for headings & prose
        text_blocks, _ = _parse_markdown_page(clean_md, page_idx)

        raw_text = _strip_markdown_formatting(clean_md)

        pages.append(PageData(
            page_idx=page_idx,
            text_blocks=text_blocks,
            tables=plumber_tables,
            raw_text=raw_text,
        ))

    plumber_doc.close()

    logger.info(
        f"Parsed {pdf_path}: {total_pages} pages, "
        f"{plumber_table_count} tables extracted via pdfplumber"
    )

    return ParsedDocument(
        pages=pages,
        total_pages=total_pages,
        file_path=pdf_path,
    )


# ── Table Block Stripping ────────────────────────────────────────────────────

_TABLE_ROW_RE = re.compile(r"^\|.*\|$")
_TABLE_SEP_RE = re.compile(r"^\|[\s:|\-]*-[\s:|\-]*\|$")


def _strip_table_blocks(md_text: str) -> str:
    """Remove all contiguous pipe-delimited table blocks from markdown.

    This prevents table content from leaking into prose TextBlocks when
    tables are extracted separately via pdfplumber.
    """
    out_lines: list[str] = []
    lines = md_text.split("\n")
    i = 0
    while i < len(lines):
        if _TABLE_ROW_RE.match(lines[i].strip()):
            # Skip entire contiguous table block
            while i < len(lines) and _TABLE_ROW_RE.match(lines[i].strip()):
                i += 1
        else:
            out_lines.append(lines[i])
            i += 1
    return "\n".join(out_lines)


# ── Markdown Page Parsing ───────────────────────────────────────────────────

_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)")


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


# ── pdfplumber Table Extraction (PRIMARY) ────────────────────────────────────

def _extract_tables_pdfplumber(
    plumber_page, page_idx: int, start_idx: int = 0,
) -> list[TableData]:
    """Extract tables from a pdfplumber page.

    This is the primary table extraction path.  pdfplumber uses line-
    intersection geometry for bordered tables and a text-alignment
    heuristic for borderless ones — both produce clean headers/rows
    without the rendering artefacts that plague markdown-parsed tables.
    """
    tables: list[TableData] = []
    try:
        raw_tables = plumber_page.extract_tables(
            table_settings={
                "vertical_strategy": "lines_strict",
                "horizontal_strategy": "lines_strict",
            }
        )
        # If strict mode finds nothing, fall back to default detection
        if not raw_tables:
            raw_tables = plumber_page.extract_tables()
    except Exception as e:
        logger.debug(f"pdfplumber extraction failed on page {page_idx}: {e}")
        return tables

    for t_idx, raw_table in enumerate(raw_tables):
        if not raw_table or len(raw_table) < 2:
            continue

        # Clean cells: None → "", strip whitespace, collapse internal newlines
        def _clean(cell) -> str:
            if cell is None:
                return ""
            return " ".join(str(cell).split()).strip()

        headers = [_clean(cell) for cell in raw_table[0]]

        # Skip tables where every header is empty (artefact)
        if all(h == "" for h in headers):
            # Try using second row as headers if it has content
            if len(raw_table) > 2 and any(_clean(c) for c in raw_table[1]):
                headers = [_clean(c) for c in raw_table[1]]
                raw_table = [raw_table[0]] + raw_table[2:]  # drop old empty header
            else:
                continue

        rows: list[list[str]] = []
        for row in raw_table[1:]:
            cleaned_row = [_clean(cell) for cell in row]
            # Skip entirely empty rows
            if any(c for c in cleaned_row):
                rows.append(cleaned_row)

        if not rows:
            continue

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


# ── Utility ─────────────────────────────────────────────────────────────────

def _strip_markdown_formatting(md_text: str) -> str:
    """Strip markdown syntax to produce plain raw_text (used by downstream modules)."""
    text = re.sub(r"^#{1,6}\s+", "", md_text, flags=re.MULTILINE)  # headings
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)  # bold
    text = re.sub(r"\*(.+?)\*", r"\1", text)  # italic
    text = re.sub(r"`(.+?)`", r"\1", text)  # inline code
    return text
