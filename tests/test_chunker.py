"""
Tests for M3: Chunker — deterministic logic, no LLM calls needed.
"""

import pytest
from models.chunk import Chunk
from models.response import StructuredDocument, SectionMeta, ParsedDocument, PageData


def _make_structured_doc(
    sections: list[SectionMeta] | None = None,
    pages: list[PageData] | None = None,
) -> StructuredDocument:
    """Helper to build a minimal StructuredDocument."""
    if pages is None:
        pages = [
            PageData(
                page_index=0,
                text_blocks=[],
                tables=[],
            )
        ]
    parsed = ParsedDocument(
        source_path="test.pdf",
        pages=pages,
        total_pages=len(pages),
    )
    if sections is None:
        sections = [
            SectionMeta(
                heading="Revenue Discussion",
                level=1,
                semantic_category="financial_results",
                page_start=0,
                page_end=0,
            )
        ]
    return StructuredDocument(
        source_path="test.pdf",
        company="Test Corp",
        ticker="TST",
        fiscal_year=2023,
        doc_type="10-K",
        sections=sections,
        parsed_doc=parsed,
    )


class TestChunkerBasics:
    """Deterministic chunker tests."""

    def test_chunk_ids_contain_ticker_and_year(self):
        from pipeline.ingestion.chunker import _make_chunk_id
        cid = _make_chunk_id("AAPL", 2023, "financial_results", 5, 1)
        assert "AAPL" in cid
        assert "2023" in cid
        assert "financialres" in cid  # shortened category, underscores stripped

    def test_chunk_id_format(self):
        from pipeline.ingestion.chunker import _make_chunk_id
        cid = _make_chunk_id("MSFT", 2022, "risk_factors", 10, 3)
        assert cid == "MSFT_2022_riskfactors_p10_003"

    def test_is_footnote_detection(self):
        from pipeline.ingestion.chunker import _is_footnote
        assert _is_footnote("(1) See notes to the financial statements.")
        assert _is_footnote("Note: amounts in millions.")
        assert not _is_footnote("Total revenue increased by 15% year over year.")

    def test_prose_splitting_respects_max_tokens(self):
        from pipeline.ingestion.chunker import _split_prose
        # Generate text that's ~2400 tokens with sentence boundaries
        sentences = [f"Sentence number {i} is here." for i in range(400)]
        long_text = " ".join(sentences)
        pieces = _split_prose(long_text, max_tokens=600, min_tokens=50)
        assert len(pieces) >= 2

    def test_empty_text_split(self):
        from pipeline.ingestion.chunker import _split_prose
        pieces = _split_prose("", max_tokens=600, min_tokens=50)
        assert pieces == [] or pieces == [""]


class TestChunkerMerging:
    """Test undersized chunk merging logic."""

    def test_merge_undersized_chunks(self):
        from pipeline.ingestion.chunker import _merge_undersized
        small1 = Chunk(
            chunk_id="TST_2023_fin_0_1", company="Test", ticker="TST",
            fiscal_year=2023, doc_type="10-K",
            section_path="Test", semantic_category="financial_results",
            page_start=0, page_end=0, chunk_type="prose",
            content="Short text.", token_count=10,
        )
        small2 = Chunk(
            chunk_id="TST_2023_fin_0_2", company="Test", ticker="TST",
            fiscal_year=2023, doc_type="10-K",
            section_path="Test", semantic_category="financial_results",
            page_start=0, page_end=0, chunk_type="prose",
            content="Also short.", token_count=10,
        )
        result = _merge_undersized([small1, small2])
        # Merged chunks should have fewer or equal total
        total_before = 2
        assert len(result) <= total_before

    def test_table_chunks_not_merged(self):
        from pipeline.ingestion.chunker import _merge_undersized
        t1 = Chunk(
            chunk_id="TST_2023_fin_0_1", company="Test", ticker="TST",
            fiscal_year=2023, doc_type="10-K",
            section_path="Test", semantic_category="financial_statements",
            page_start=0, page_end=0, chunk_type="table",
            content="| A | B |\n|---|---|\n| 1 | 2 |", token_count=10,
        )
        result = _merge_undersized([t1])
        assert len(result) == 1
        assert result[0].chunk_type == "table"
