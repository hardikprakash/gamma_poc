"""
Interface contract tests — verify all 11 module function signatures and data models.
"""

import pytest
import inspect
import importlib


class TestModuleInterfaces:
    """Verify that every pipeline module exposes the expected public function with correct signature."""

    # ── M1: Parser ──────────────────────────────────────────────────────────

    def test_parser_function_exists(self):
        from pipeline.ingestion.parser import parse_pdf
        assert callable(parse_pdf)

    def test_parser_signature(self):
        from pipeline.ingestion.parser import parse_pdf
        sig = inspect.signature(parse_pdf)
        params = list(sig.parameters.keys())
        assert "pdf_path" in params

    # ── M2: Structure Inference ─────────────────────────────────────────────

    def test_structure_function_exists(self):
        from pipeline.ingestion.structure import infer_structure
        assert callable(infer_structure)

    def test_structure_is_async(self):
        from pipeline.ingestion.structure import infer_structure
        assert inspect.iscoroutinefunction(infer_structure)

    def test_structure_signature(self):
        from pipeline.ingestion.structure import infer_structure
        sig = inspect.signature(infer_structure)
        params = list(sig.parameters.keys())
        assert "parsed_doc" in params and "company" in params

    # ── M3: Chunker ─────────────────────────────────────────────────────────

    def test_chunker_function_exists(self):
        from pipeline.ingestion.chunker import chunk_document
        assert callable(chunk_document)

    def test_chunker_is_sync(self):
        from pipeline.ingestion.chunker import chunk_document
        assert not inspect.iscoroutinefunction(chunk_document)

    def test_chunker_signature(self):
        from pipeline.ingestion.chunker import chunk_document
        sig = inspect.signature(chunk_document)
        params = list(sig.parameters.keys())
        assert "structured_doc" in params

    # ── M4: Fact Extractor ──────────────────────────────────────────────────

    def test_extractor_function_exists(self):
        from pipeline.graph.extractor import extract_facts
        assert callable(extract_facts)

    def test_extractor_is_async(self):
        from pipeline.graph.extractor import extract_facts
        assert inspect.iscoroutinefunction(extract_facts)

    def test_extractor_signature(self):
        from pipeline.graph.extractor import extract_facts
        sig = inspect.signature(extract_facts)
        params = list(sig.parameters.keys())
        assert "chunk" in params

    # ── M5: Graph Constructor ───────────────────────────────────────────────

    def test_constructor_function_exists(self):
        from pipeline.graph.constructor import build_graph
        assert callable(build_graph)

    def test_constructor_is_sync(self):
        from pipeline.graph.constructor import build_graph
        assert not inspect.iscoroutinefunction(build_graph)

    def test_constructor_signature(self):
        from pipeline.graph.constructor import build_graph
        sig = inspect.signature(build_graph)
        params = list(sig.parameters.keys())
        assert "chunks" in params and "extraction_results" in params

    # ── M6: Query Decomposer ───────────────────────────────────────────────

    def test_decomposer_function_exists(self):
        from pipeline.query.decomposer import decompose_query
        assert callable(decompose_query)

    def test_decomposer_is_async(self):
        from pipeline.query.decomposer import decompose_query
        assert inspect.iscoroutinefunction(decompose_query)

    def test_decomposer_signature(self):
        from pipeline.query.decomposer import decompose_query
        sig = inspect.signature(decompose_query)
        params = list(sig.parameters.keys())
        assert "user_query" in params

    # ── M7: Hybrid Retriever ───────────────────────────────────────────────

    def test_retriever_function_exists(self):
        from pipeline.query.retriever import retrieve
        assert callable(retrieve)

    def test_retriever_is_async(self):
        from pipeline.query.retriever import retrieve
        assert inspect.iscoroutinefunction(retrieve)

    def test_retriever_signature(self):
        from pipeline.query.retriever import retrieve
        sig = inspect.signature(retrieve)
        params = list(sig.parameters.keys())
        assert "decomposition" in params and "graph_client" in params

    # ── M8: Reranker ───────────────────────────────────────────────────────

    def test_reranker_function_exists(self):
        from pipeline.query.reranker import rerank
        assert callable(rerank)

    def test_reranker_is_sync(self):
        from pipeline.query.reranker import rerank
        assert not inspect.iscoroutinefunction(rerank)

    def test_reranker_signature(self):
        from pipeline.query.reranker import rerank
        sig = inspect.signature(rerank)
        params = list(sig.parameters.keys())
        assert "retrieval_result" in params and "user_query" in params

    # ── M9: Context Assembler ──────────────────────────────────────────────

    def test_assembler_function_exists(self):
        from pipeline.query.assembler import assemble_context
        assert callable(assemble_context)

    def test_assembler_is_sync(self):
        from pipeline.query.assembler import assemble_context
        assert not inspect.iscoroutinefunction(assemble_context)

    def test_assembler_signature(self):
        from pipeline.query.assembler import assemble_context
        sig = inspect.signature(assemble_context)
        params = list(sig.parameters.keys())
        assert "rerank_result" in params

    # ── M10: Answer Generator ──────────────────────────────────────────────

    def test_generator_function_exists(self):
        from pipeline.query.generator import generate_answer
        assert callable(generate_answer)

    def test_generator_is_async(self):
        from pipeline.query.generator import generate_answer
        assert inspect.iscoroutinefunction(generate_answer)

    def test_generator_signature(self):
        from pipeline.query.generator import generate_answer
        sig = inspect.signature(generate_answer)
        params = list(sig.parameters.keys())
        assert "context_payload" in params and "user_query" in params

    # ── M11: Response Finalizer ────────────────────────────────────────────

    def test_finalizer_function_exists(self):
        from pipeline.query.finalizer import finalize_response
        assert callable(finalize_response)

    def test_finalizer_is_sync(self):
        from pipeline.query.finalizer import finalize_response
        assert not inspect.iscoroutinefunction(finalize_response)

    def test_finalizer_signature(self):
        from pipeline.query.finalizer import finalize_response
        sig = inspect.signature(finalize_response)
        params = list(sig.parameters.keys())
        assert "raw_response" in params and "citation_registry" in params


class TestDataModels:
    """Verify all core data models are importable and well-formed."""

    def test_chunk_fields(self):
        from models.chunk import Chunk
        import dataclasses
        field_names = [f.name for f in dataclasses.fields(Chunk)]
        required = ["chunk_id", "company", "ticker", "fiscal_year", "doc_type",
                     "section_path", "semantic_category", "page_start", "page_end",
                     "chunk_type", "content"]
        for r in required:
            assert r in field_names, f"Chunk missing field: {r}"

    def test_financial_fact_fields(self):
        from models.fact import FinancialFact
        required = ["fact_id", "metric_name_raw", "metric_name_canonical",
                     "metric_category", "value", "unit", "period",
                     "fiscal_year", "company", "source_chunk_id"]
        model_fields = set(FinancialFact.model_fields.keys())
        for r in required:
            assert r in model_fields, f"FinancialFact missing field: {r}"

    def test_entity_fields(self):
        from models.fact import Entity
        model_fields = set(Entity.model_fields.keys())
        for r in ["entity_id", "name", "canonical_name", "entity_type"]:
            assert r in model_fields, f"Entity missing field: {r}"

    def test_risk_factor_fields(self):
        from models.fact import RiskFactor
        model_fields = set(RiskFactor.model_fields.keys())
        for r in ["risk_id", "title", "company", "fiscal_year", "source_chunk_id"]:
            assert r in model_fields, f"RiskFactor missing field: {r}"

    def test_agent_response_fields(self):
        from models.response import AgentResponse
        model_fields = set(AgentResponse.model_fields.keys())
        for r in ["answer", "confidence", "resolved_citations"]:
            assert r in model_fields, f"AgentResponse missing field: {r}"

    def test_taxonomy_constants(self):
        from models.taxonomy import SEMANTIC_CATEGORIES, CANONICAL_METRICS, METRIC_CATEGORIES
        assert len(SEMANTIC_CATEGORIES) >= 8
        assert len(CANONICAL_METRICS) >= 15
        assert len(METRIC_CATEGORIES) >= 15

    def test_resolve_metric_name_exact(self):
        from models.taxonomy import resolve_metric_name
        canon, conf = resolve_metric_name("Total Revenues")
        assert canon == "revenue"
        assert conf == "high"

    def test_resolve_metric_name_fuzzy(self):
        from models.taxonomy import resolve_metric_name
        canon, conf = resolve_metric_name("totall revenuess")
        assert conf in ("medium", "high")

    def test_resolve_metric_name_unknown(self):
        from models.taxonomy import resolve_metric_name
        canon, conf = resolve_metric_name("zzz_completely_unknown_metric_xyx")
        assert conf == "low"
        assert canon == "zzz_completely_unknown_metric_xyx"

    def test_chunk_to_dict_excludes_embedding(self):
        from models.chunk import Chunk
        c = Chunk(
            chunk_id="TEST_2023_financial_results_0_1",
            company="Test Co", ticker="TEST", fiscal_year=2023,
            doc_type="10-K", section_path="Test > Section",
            semantic_category="financial_results",
            page_start=0, page_end=0,
            chunk_type="prose", content="hello",
            embedding=[0.1] * 768,
        )
        d = c.to_dict()
        assert "embedding" not in d
        assert d["chunk_id"] == "TEST_2023_financial_results_0_1"
