"""
M7: Hybrid Retriever — Runs 3 retrieval paths concurrently, merges and deduplicates.

Path A: graph_fact_lookup  — Cypher exact-match on FinancialFact nodes
Path B: graph_section_traversal — Cypher to section-matched Chunk nodes
Path C: vector_similarity_search — Neo4j native vector search filtered by metadata

No LLM calls. Deterministic.
"""

from __future__ import annotations
import asyncio
import logging

from models.response import QueryDecomposition, ScoredChunk, RetrievalResult
from models.chunk import Chunk
from models.fact import FinancialFact
from db.neo4j_client import AsyncNeo4jClient
from llm.embedding_client import embed
from config import VECTOR_TOP_K

logger = logging.getLogger(__name__)


async def retrieve(
    decomposition: QueryDecomposition,
    graph_client: AsyncNeo4jClient,
    top_k_vector: int = VECTOR_TOP_K,
) -> RetrievalResult:
    """
    Runs all 3 paths concurrently via asyncio.gather.
    Vector search must always be filtered by company.
    """
    facts_task = asyncio.create_task(
        graph_fact_lookup(decomposition, graph_client)
    )
    section_task = asyncio.create_task(
        graph_section_traversal(decomposition, graph_client)
    )
    vector_task = asyncio.create_task(
        vector_similarity_search(decomposition, graph_client, top_k_vector)
    )

    facts, section_chunks, vector_chunks = await asyncio.gather(
        facts_task, section_task, vector_task
    )

    merged = _merge_and_deduplicate(section_chunks, vector_chunks)

    total = len(merged) + len(facts)
    logger.info(
        f"Retrieval: {len(facts)} facts, {len(section_chunks)} section chunks, "
        f"{len(vector_chunks)} vector chunks → {len(merged)} merged chunks"
    )

    return RetrievalResult(
        chunks=merged,
        facts=facts,
        total_retrieved=total,
    )


# ── Path A: Graph Fact Lookup ────────────────────────────────────────────────

async def graph_fact_lookup(
    decomposition: QueryDecomposition,
    graph_client: AsyncNeo4jClient,
) -> list[FinancialFact]:
    """Exact-match on FinancialFact nodes by canonical metric + company + year."""
    facts: list[FinancialFact] = []

    if not decomposition.metrics:
        return facts

    companies = decomposition.companies or []
    years = decomposition.years if isinstance(decomposition.years, list) else []

    for metric in decomposition.metrics:
        for company in companies:
            params = {"metric": metric, "company": company}
            year_clause = ""
            if years:
                year_clause = "AND f.fiscal_year IN $years"
                params["years"] = years

            query = f"""
                MATCH (f:FinancialFact {{
                    metric_name_canonical: $metric,
                    company: $company
                }})
                WHERE true {year_clause}
                RETURN f
                LIMIT 50
            """
            results = await graph_client.query(query, **params)
            for record in results:
                f = record.get("f", {})
                if not f:
                    continue
                try:
                    fact = FinancialFact(
                        fact_id=f.get("fact_id", ""),
                        metric_name_raw=f.get("metric_name_raw", ""),
                        metric_name_canonical=f.get("metric_name_canonical", ""),
                        metric_category=f.get("metric_category", "other"),
                        value=float(f.get("value", 0)),
                        unit=f.get("unit", "units"),
                        currency=f.get("currency"),
                        period=f.get("period", ""),
                        fiscal_year=int(f.get("fiscal_year", 0)),
                        company=f.get("company", ""),
                        is_comparative=bool(f.get("is_comparative", False)),
                        source_chunk_id=f.get("source_chunk_id", ""),
                        confidence=f.get("confidence", "medium"),
                        verbatim_text=f.get("verbatim_text", ""),
                        page_start=int(f.get("page_start", -1)),
                    )
                    facts.append(fact)
                except Exception as e:
                    logger.debug(f"Fact parse error: {e}")

    # Also fetch SAME_METRIC_AS linked facts for trend queries
    if decomposition.intent in ("trend", "comparison"):
        facts = await _follow_same_metric_edges(graph_client, facts)

    logger.debug(f"Path A (fact lookup): {len(facts)} facts")
    return facts


async def _follow_same_metric_edges(
    graph_client: AsyncNeo4jClient,
    seed_facts: list[FinancialFact],
) -> list[FinancialFact]:
    """Follow SAME_METRIC_AS edges to get all related facts."""
    seen_ids = {f.fact_id for f in seed_facts}
    all_facts = list(seed_facts)

    for fact in seed_facts:
        results = await graph_client.query("""
            MATCH (f:FinancialFact {fact_id: $fact_id})-[:SAME_METRIC_AS*1..4]-(linked:FinancialFact)
            RETURN linked
            LIMIT 20
        """, fact_id=fact.fact_id)

        for record in results:
            linked = record.get("linked", {})
            if linked.get("fact_id") and linked["fact_id"] not in seen_ids:
                try:
                    f = FinancialFact(
                        fact_id=linked.get("fact_id", ""),
                        metric_name_raw=linked.get("metric_name_raw", ""),
                        metric_name_canonical=linked.get("metric_name_canonical", ""),
                        metric_category=linked.get("metric_category", "other"),
                        value=float(linked.get("value", 0)),
                        unit=linked.get("unit", "units"),
                        currency=linked.get("currency"),
                        period=linked.get("period", ""),
                        fiscal_year=int(linked.get("fiscal_year", 0)),
                        company=linked.get("company", ""),
                        is_comparative=bool(linked.get("is_comparative", False)),
                        source_chunk_id=linked.get("source_chunk_id", ""),
                        confidence=linked.get("confidence", "medium"),
                        verbatim_text=linked.get("verbatim_text", ""),
                        page_start=int(linked.get("page_start", -1)),
                    )
                    all_facts.append(f)
                    seen_ids.add(f.fact_id)
                except Exception:
                    pass

    return all_facts


# ── Path B: Graph Section Traversal ─────────────────────────────────────────

async def graph_section_traversal(
    decomposition: QueryDecomposition,
    graph_client: AsyncNeo4jClient,
) -> list[ScoredChunk]:
    """Fetch chunks from matching semantic sections."""
    chunks: list[ScoredChunk] = []

    sections = decomposition.semantic_sections or []
    companies = decomposition.companies or []
    years = decomposition.years if isinstance(decomposition.years, list) else []

    if not sections or not companies:
        return chunks

    for section in sections:
        for company in companies:
            params = {"section": section, "company": company}
            year_clause = ""
            if years:
                year_clause = "AND c.fiscal_year IN $years"
                params["years"] = years

            query = f"""
                MATCH (c:Chunk {{
                    semantic_category: $section,
                    company: $company
                }})
                WHERE true {year_clause}
                RETURN c
                LIMIT 30
            """
            results = await graph_client.query(query, **params)
            for record in results:
                c = record.get("c", {})
                if not c:
                    continue
                try:
                    chunk = Chunk(
                        chunk_id=c.get("chunk_id", ""),
                        company=c.get("company", ""),
                        ticker=c.get("ticker", ""),
                        fiscal_year=int(c.get("fiscal_year", 0)),
                        doc_type=c.get("doc_type", ""),
                        section_path=c.get("section_path", ""),
                        semantic_category=c.get("semantic_category", ""),
                        page_start=int(c.get("page_start", 0)),
                        page_end=int(c.get("page_end", 0)),
                        chunk_type=c.get("chunk_type", "prose"),
                        content=c.get("content", ""),
                        token_count=int(c.get("token_count", 0)),
                    )
                    chunks.append(ScoredChunk(chunk=chunk, score=0.5, source="graph_section"))
                except Exception as e:
                    logger.debug(f"Section chunk parse error: {e}")

    logger.debug(f"Path B (section traversal): {len(chunks)} chunks")
    return chunks


# ── Path C: Vector Similarity Search ────────────────────────────────────────

async def vector_similarity_search(
    decomposition: QueryDecomposition,
    graph_client: AsyncNeo4jClient,
    top_k: int = VECTOR_TOP_K,
) -> list[ScoredChunk]:
    """Neo4j native vector search filtered by company (mandatory)."""
    chunks: list[ScoredChunk] = []

    companies = decomposition.companies or []
    if not companies:
        logger.warning("Vector search skipped: no companies specified")
        return chunks

    # Build query embedding from sub-questions
    query_text = " ".join(decomposition.sub_questions) if decomposition.sub_questions else ""
    if not query_text:
        return chunks

    try:
        query_embedding = await embed(query_text)
    except Exception as e:
        logger.warning(f"Embedding failed, skipping vector search: {e}")
        return chunks

    years = decomposition.years if isinstance(decomposition.years, list) else []

    for company in companies:
        params = {
            "embedding": query_embedding,
            "company": company,
            "top_k": top_k,
        }

        year_filter = ""
        if years:
            year_filter = "AND node.fiscal_year IN $years"
            params["years"] = years

        query = f"""
            CALL db.index.vector.queryNodes('chunk_embedding_index', $top_k, $embedding)
            YIELD node, score
            WHERE node.company = $company {year_filter}
            RETURN node, score
            LIMIT $top_k
        """

        try:
            results = await graph_client.query(query, **params)
            for record in results:
                node = record.get("node", {})
                score = float(record.get("score", 0))
                if not node:
                    continue
                try:
                    chunk = Chunk(
                        chunk_id=node.get("chunk_id", ""),
                        company=node.get("company", ""),
                        ticker=node.get("ticker", ""),
                        fiscal_year=int(node.get("fiscal_year", 0)),
                        doc_type=node.get("doc_type", ""),
                        section_path=node.get("section_path", ""),
                        semantic_category=node.get("semantic_category", ""),
                        page_start=int(node.get("page_start", 0)),
                        page_end=int(node.get("page_end", 0)),
                        chunk_type=node.get("chunk_type", "prose"),
                        content=node.get("content", ""),
                        token_count=int(node.get("token_count", 0)),
                    )
                    chunks.append(ScoredChunk(chunk=chunk, score=score, source="vector"))
                except Exception as e:
                    logger.debug(f"Vector chunk parse error: {e}")
        except Exception as e:
            logger.warning(f"Vector search error for {company}: {e}")

    logger.debug(f"Path C (vector search): {len(chunks)} chunks")
    return chunks


# ── Merge & Deduplicate ─────────────────────────────────────────────────────

def _merge_and_deduplicate(
    section_chunks: list[ScoredChunk],
    vector_chunks: list[ScoredChunk],
) -> list[ScoredChunk]:
    """Merge chunks from both paths, deduplicate by chunk_id, keeping highest score."""
    seen: dict[str, ScoredChunk] = {}

    for sc in section_chunks + vector_chunks:
        cid = sc.chunk.chunk_id
        if cid not in seen or sc.score > seen[cid].score:
            if cid in seen:
                sc.source = "merged"
            seen[cid] = sc

    # Sort by score descending
    result = sorted(seen.values(), key=lambda x: x.score, reverse=True)
    return result
