"""
Tests for query pipeline components — retriever deduplication, reranker scoring, assembler citations.
"""

import pytest
from models.chunk import Chunk
from models.fact import FinancialFact
from models.response import (
    ScoredChunk,
    RetrievalResult,
    RerankResult,
    CitationRegistry,
    CitationEntry,
)


def _make_chunk(chunk_id: str, company: str = "Apple", content: str = "Test content") -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        company=company,
        ticker="AAPL" if company == "Apple" else "MSFT",
        fiscal_year=2023,
        doc_type="10-K",
        section_path="Financial Statements > Income Statement",
        semantic_category="financial_statements",
        page_start=0,
        page_end=0,
        chunk_type="prose",
        content=content,
        token_count=50,
    )


def _make_scored_chunk(chunk_id: str, score: float = 0.5, **kwargs) -> ScoredChunk:
    return ScoredChunk(
        chunk=_make_chunk(chunk_id, **kwargs),
        score=score,
        source="graph_fact",
        matched_facts=[],
    )


class TestRetrieverDeduplication:
    """Test the _merge_and_deduplicate function."""

    def test_dedup_by_chunk_id(self):
        from pipeline.query.retriever import _merge_and_deduplicate
        sc1 = _make_scored_chunk("c1", score=0.8)
        sc2 = _make_scored_chunk("c1", score=0.6)  # duplicate
        sc3 = _make_scored_chunk("c2", score=0.7)
        result = _merge_and_deduplicate([sc1, sc2, sc3])
        ids = [sc.chunk.chunk_id for sc in result]
        assert len(ids) == 2
        assert ids.count("c1") == 1

    def test_dedup_keeps_higher_score(self):
        from pipeline.query.retriever import _merge_and_deduplicate
        sc1 = _make_scored_chunk("c1", score=0.3)
        sc2 = _make_scored_chunk("c1", score=0.9)
        result = _merge_and_deduplicate([sc1, sc2])
        assert result[0].score == 0.9

    def test_dedup_sorted_desc(self):
        from pipeline.query.retriever import _merge_and_deduplicate
        sc1 = _make_scored_chunk("c1", score=0.3)
        sc2 = _make_scored_chunk("c2", score=0.9)
        sc3 = _make_scored_chunk("c3", score=0.6)
        result = _merge_and_deduplicate([sc1, sc2, sc3])
        scores = [sc.score for sc in result]
        assert scores == sorted(scores, reverse=True)

    def test_empty_input(self):
        from pipeline.query.retriever import _merge_and_deduplicate
        assert _merge_and_deduplicate([]) == []


class TestRerankerConflictDetection:
    """Test _detect_conflicts in reranker."""

    def test_same_metric_different_values_detected(self):
        from pipeline.query.reranker import _detect_conflicts
        f1 = FinancialFact(
            fact_id="f1", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=100000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c1", confidence="high",
        )
        f2 = FinancialFact(
            fact_id="f2", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=200000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c2", confidence="high",
        )
        conflicts = _detect_conflicts([f1, f2])
        assert len(conflicts) >= 1

    def test_no_conflict_when_same_value(self):
        from pipeline.query.reranker import _detect_conflicts
        f1 = FinancialFact(
            fact_id="f1", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=100000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c1", confidence="high",
        )
        f2 = FinancialFact(
            fact_id="f2", metric_name_raw="Total Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=100000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c2", confidence="high",
        )
        conflicts = _detect_conflicts([f1, f2])
        assert len(conflicts) == 0

    def test_no_conflict_across_different_years(self):
        from pipeline.query.reranker import _detect_conflicts
        f1 = FinancialFact(
            fact_id="f1", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=100000, unit="millions",
            period="FY2022", fiscal_year=2022, company="Apple",
            source_chunk_id="c1", confidence="high",
        )
        f2 = FinancialFact(
            fact_id="f2", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=200000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c2", confidence="high",
        )
        conflicts = _detect_conflicts([f1, f2])
        assert len(conflicts) == 0


class TestAssemblerCitations:
    """Test context assembler citation key creation."""

    def test_citation_key_format(self):
        from pipeline.query.assembler import _make_citation_key
        chunk = _make_chunk("c1", company="Apple")
        registry = CitationRegistry(entries={})
        key = _make_citation_key(chunk, registry)
        assert key.startswith("[")
        assert key.endswith("]")

    def test_citations_registered(self):
        from pipeline.query.assembler import _make_citation_key
        chunk = _make_chunk("c1", company="Apple")
        registry = CitationRegistry(entries={})
        key = _make_citation_key(chunk, registry)
        assert key in registry.entries

    def test_duplicate_chunk_same_key(self):
        from pipeline.query.assembler import _make_citation_key
        chunk1 = _make_chunk("c1", company="Apple")
        chunk2 = _make_chunk("c1", company="Apple")
        registry = CitationRegistry(entries={})
        key1 = _make_citation_key(chunk1, registry)
        key2 = _make_citation_key(chunk2, registry)
        assert key1 == key2

    def test_fact_citation_key(self):
        from pipeline.query.assembler import _make_fact_citation_key
        fact = FinancialFact(
            fact_id="f1", metric_name_raw="Revenue", metric_name_canonical="revenue",
            metric_category="revenue", value=100000, unit="millions",
            period="FY2023", fiscal_year=2023, company="Apple",
            source_chunk_id="c1", confidence="high",
        )
        registry = CitationRegistry(entries={})
        key = _make_fact_citation_key(fact, registry)
        assert key.startswith("[")
        assert key in registry.entries
