"""
M2: Structure Inference — ParsedDocument → StructuredDocument.

Makes exactly 3 LLM calls:
  1. Document type detection
  2. Heading / section label normalization
  3. Table classification (batched)
All LLM outputs Pydantic-validated with single retry.
"""

from __future__ import annotations
import json
import logging
from models.response import (
    ParsedDocument,
    StructuredDocument,
    SectionMeta,
    DocTypeDetection,
    SectionLabel,
    TableClassification,
)
from llm.validator import validated_llm_call

logger = logging.getLogger(__name__)

# ── Prompt templates (from DOC3 §1.1-1.3) ──────────────────────────────────

_DOC_TYPE_SYSTEM = (
    "You are a financial document classifier. "
    "Return only valid JSON, no prose, no markdown fences."
)

_SECTION_LABEL_SYSTEM = (
    "You normalize financial document headings into standard semantic categories. "
    "Return only valid JSON, no prose, no markdown fences."
)

_TABLE_CLASS_SYSTEM = (
    "You classify financial tables by content type. "
    "Return only valid JSON, no prose, no markdown fences."
)


# ── Public API ───────────────────────────────────────────────────────────────

async def infer_structure(
    parsed_doc: ParsedDocument,
    company: str,
    fiscal_year: int,
    ticker: str = "",
) -> StructuredDocument:
    """
    Returns StructuredDocument with doc_type, sections, table_classifications.
    Makes exactly 3 LLM calls.
    """
    # ── Step 1: Detect document type ─────────────────────────────────────────
    doc_type_result = await _detect_doc_type(parsed_doc)
    logger.info(f"Doc type detected: {doc_type_result.form_type} (confidence={doc_type_result.confidence})")

    # ── Step 2: Normalize section headings ───────────────────────────────────
    headings = _collect_headings(parsed_doc)
    section_labels = await _normalize_sections(headings) if headings else []
    logger.info(f"Normalized {len(section_labels)} section headings")

    # ── Step 3: Classify tables ──────────────────────────────────────────────
    tables = _collect_tables(parsed_doc)
    table_classifications = {}
    if tables:
        table_classifications = await _classify_tables(tables)
        logger.info(f"Classified {len(table_classifications)} tables")

    # ── Build SectionMeta list ───────────────────────────────────────────────
    sections = _build_sections(parsed_doc, section_labels, headings)

    structured = StructuredDocument(
        doc_type=doc_type_result.form_type,
        sections=sections,
        table_classifications=table_classifications,
        parsed_doc=parsed_doc,
        company=company,
        ticker=ticker,
        fiscal_year=fiscal_year,
        jurisdiction=doc_type_result.jurisdiction,
        reporting_period=doc_type_result.reporting_period,
        currency="",
    )
    return structured


# ── Step 1: Document Type Detection ─────────────────────────────────────────

async def _detect_doc_type(parsed_doc: ParsedDocument) -> DocTypeDetection:
    title_text = ""
    intro_text = ""
    if parsed_doc.pages:
        first_page = parsed_doc.pages[0]
        title_text = first_page.raw_text[:300]
        intro_text = first_page.raw_text[:600]

    headings = _collect_headings(parsed_doc)[:15]
    headings_json = json.dumps([h["text"] for h in headings])

    prompt = f"""Classify this financial document.

Title page text: {title_text}
First 600 characters: {intro_text}
Detected headings (first 15): {headings_json}

Return JSON matching this schema exactly:
{{
  "form_type": "20-F" | "10-K" | "10-Q" | "annual_report" | "earnings_release" | "prospectus" | "proxy_statement" | "other",
  "jurisdiction": "US" | "international" | "unknown",
  "reporting_period": "annual" | "quarterly" | "unknown",
  "company_name": "<detected company name or null>",
  "fiscal_year": null,
  "confidence": "high" | "medium" | "low"
}}"""

    return await validated_llm_call(prompt, DocTypeDetection, system=_DOC_TYPE_SYSTEM)


# ── Step 2: Section Label Normalization ─────────────────────────────────────

async def _normalize_sections(headings: list[dict]) -> list[SectionLabel]:
    headings_json = json.dumps(headings, indent=2)

    prompt = f"""Normalize these headings from a financial document.
For each heading, assign a semantic_category from this fixed list only:
[financial_results, financial_statements, risk_factors, segment_data, governance, business_overview, legal, market_data, other]

Headings in document order:
{headings_json}

Return a JSON object with a "sections" key containing an array, one object per heading:
{{
  "sections": [
    {{
      "raw_title": "<exact heading text>",
      "normalized_title": "<clean readable title>",
      "semantic_category": "<value from fixed list above>",
      "relevance": "high" | "medium" | "low"
    }}
  ]
}}"""

    from pydantic import BaseModel, Field

    class SectionsWrapper(BaseModel):
        sections: list[SectionLabel] = Field(default_factory=list)

    result = await validated_llm_call(prompt, SectionsWrapper, system=_SECTION_LABEL_SYSTEM)
    return result.sections


# ── Step 3: Table Classification (batched) ──────────────────────────────────

async def _classify_tables(tables: list[dict]) -> dict[str, TableClassification]:
    """Classify all tables in a single LLM call (batched)."""
    results: dict[str, TableClassification] = {}

    # Batch all tables into one call for efficiency
    tables_json_parts = []
    for t in tables:
        tables_json_parts.append(f"Table ID: {t['table_id']}\n{t['markdown']}\n")

    combined = "\n---\n".join(tables_json_parts)

    from pydantic import BaseModel, Field

    class TableClassItem(BaseModel):
        table_id: str
        table_type: str = "unknown"
        contains_financial_facts: bool = False
        primary_metric: str | None = None
        time_periods: list[str] = Field(default_factory=list)
        confidence: str = "medium"

    class TablesWrapper(BaseModel):
        tables: list[TableClassItem] = Field(default_factory=list)

    prompt = f"""Classify these tables from a financial document.

{combined}

Return a JSON object with a "tables" key containing an array, one object per table:
{{
  "tables": [
    {{
      "table_id": "<table_id from above>",
      "table_type": "income_statement" | "balance_sheet" | "cash_flow" | "segment_data" | "ratio_summary" | "footnote_detail" | "non_financial" | "unknown",
      "contains_financial_facts": true | false,
      "primary_metric": "<main metric or null>",
      "time_periods": ["FY2023", "FY2022"],
      "confidence": "high" | "medium" | "low"
    }}
  ]
}}"""

    try:
        wrapper = await validated_llm_call(prompt, TablesWrapper, system=_TABLE_CLASS_SYSTEM)
        for item in wrapper.tables:
            results[item.table_id] = TableClassification(
                table_type=item.table_type,
                contains_financial_facts=item.contains_financial_facts,
                primary_metric=item.primary_metric,
                time_periods=item.time_periods,
                confidence=item.confidence,
            )
    except Exception as e:
        logger.warning(f"Table classification failed, defaulting: {e}")
        for t in tables:
            results[t["table_id"]] = TableClassification()

    return results


# ── Helpers ──────────────────────────────────────────────────────────────────

def _collect_headings(parsed_doc: ParsedDocument) -> list[dict]:
    """Collect all heading text blocks from the parsed document."""
    headings = []
    for page in parsed_doc.pages:
        for block in page.text_blocks:
            if block.is_heading and block.text.strip():
                headings.append({
                    "text": block.text.strip(),
                    "level": block.heading_level,
                    "page": page.page_idx,
                })
    return headings


def _collect_tables(parsed_doc: ParsedDocument) -> list[dict]:
    """Collect all tables from the parsed document."""
    tables = []
    for page in parsed_doc.pages:
        for table in page.tables:
            tables.append({
                "table_id": table.table_id,
                "page_idx": table.page_idx,
                "headers": table.headers,
                "rows": table.rows,
                "markdown": table.markdown,
            })
    return tables


def _build_sections(
    parsed_doc: ParsedDocument,
    section_labels: list[SectionLabel],
    headings: list[dict],
) -> list[SectionMeta]:
    """Build SectionMeta list by matching LLM labels back to detected headings."""
    # Create a lookup by raw_title
    label_map: dict[str, SectionLabel] = {}
    for label in section_labels:
        label_map[label.raw_title.strip().lower()] = label

    sections: list[SectionMeta] = []
    # Build section path by tracking heading hierarchy
    heading_stack: list[str] = []

    for i, heading in enumerate(headings):
        text = heading["text"].strip()
        level = heading["level"]
        page = heading["page"]

        # Find matching label
        label = label_map.get(text.lower())
        if label:
            normalized_title = label.normalized_title
            semantic_category = label.semantic_category
            relevance = label.relevance
        else:
            normalized_title = text
            semantic_category = "other"
            relevance = "low"

        # Build section_path from heading stack
        # Trim stack to current level
        while len(heading_stack) >= level:
            heading_stack.pop()
        heading_stack.append(normalized_title)
        section_path = " > ".join(heading_stack)

        # Compute page_end — look at next heading or end of document
        if i + 1 < len(headings):
            page_end = headings[i + 1]["page"]
        else:
            page_end = parsed_doc.total_pages - 1

        sections.append(SectionMeta(
            raw_title=text,
            normalized_title=normalized_title,
            semantic_category=semantic_category,
            heading_level=level,
            page_start=page,
            page_end=page_end,
            relevance_score=relevance,
            section_path=section_path,
        ))

    return sections
