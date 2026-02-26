"""
M5: Graph Constructor — Writes chunks, facts, entities, risk factors into Neo4j.

Idempotent — uses MERGE not CREATE throughout.
Creates all node types, all edge types including SAME_METRIC_AS and EVOLVED_INTO.
"""

from __future__ import annotations
import logging
from datetime import datetime
from collections import defaultdict

from models.chunk import Chunk
from models.fact import FinancialFact, Entity, RiskFactor
from models.response import GraphBuildResult
from db.neo4j_client import Neo4jClient
from config import RISK_SIMILARITY_THRESHOLD

logger = logging.getLogger(__name__)


def build_graph(
    chunks: list[Chunk],
    facts: list[FinancialFact],
    entities: list[Entity],
    risk_factors: list[RiskFactor],
    graph_client: Neo4jClient,
    *,
    doc_id: str = "",
    company: str = "",
    ticker: str = "",
    fiscal_year: int = 0,
    doc_type: str = "",
    total_pages: int = 0,
) -> GraphBuildResult:
    """
    Idempotent graph builder using MERGE throughout.
    """
    nodes_created = 0
    edges_created = 0
    embeddings_written = 0

    # ── 1. Create/merge Document node ────────────────────────────────────────
    if not doc_id and chunks:
        c = chunks[0]
        doc_id = f"{c.ticker}_{c.fiscal_year}_{c.doc_type}"
        company = c.company
        ticker = c.ticker
        fiscal_year = c.fiscal_year
        doc_type = c.doc_type

    if doc_id:
        graph_client.write("""
            MERGE (d:Document {doc_id: $doc_id})
            SET d.company = $company,
                d.ticker = $ticker,
                d.fiscal_year = $fiscal_year,
                d.doc_type = $doc_type,
                d.total_pages = $total_pages,
                d.ingest_timestamp = $ts
        """, doc_id=doc_id, company=company, ticker=ticker,
            fiscal_year=fiscal_year, doc_type=doc_type,
            total_pages=total_pages, ts=datetime.utcnow().isoformat())
        nodes_created += 1

    # ── 2. Create/merge Section nodes and BELONGS_TO edges ───────────────────
    sections_seen: set[str] = set()
    for chunk in chunks:
        section_id = f"{doc_id}_{chunk.semantic_category}_{chunk.page_start}"
        if section_id not in sections_seen:
            graph_client.write("""
                MERGE (s:Section {section_id: $section_id})
                SET s.doc_id = $doc_id,
                    s.raw_title = $raw_title,
                    s.normalized_title = $normalized_title,
                    s.semantic_category = $semantic_category,
                    s.section_path = $section_path,
                    s.page_start = $page_start,
                    s.page_end = $page_end
            """, section_id=section_id, doc_id=doc_id,
                raw_title=chunk.section_path.split(" > ")[-1] if " > " in chunk.section_path else chunk.section_path,
                normalized_title=chunk.section_path.split(" > ")[-1] if " > " in chunk.section_path else chunk.section_path,
                semantic_category=chunk.semantic_category,
                section_path=chunk.section_path,
                page_start=chunk.page_start, page_end=chunk.page_end)

            # Section → Document
            graph_client.write("""
                MATCH (s:Section {section_id: $section_id})
                MATCH (d:Document {doc_id: $doc_id})
                MERGE (s)-[:BELONGS_TO]->(d)
            """, section_id=section_id, doc_id=doc_id)
            sections_seen.add(section_id)
            nodes_created += 1
            edges_created += 1

    # ── 3. Create/merge Chunk nodes ──────────────────────────────────────────
    for chunk in chunks:
        props = chunk.to_dict()
        graph_client.write("""
            MERGE (c:Chunk {chunk_id: $chunk_id})
            SET c += $props
        """, chunk_id=chunk.chunk_id, props=props)
        nodes_created += 1

        # Write embedding if present
        if chunk.embedding:
            graph_client.write("""
                MATCH (c:Chunk {chunk_id: $chunk_id})
                SET c.embedding = $embedding
            """, chunk_id=chunk.chunk_id, embedding=chunk.embedding)
            embeddings_written += 1

        # Chunk → Section (BELONGS_TO)
        section_id = f"{doc_id}_{chunk.semantic_category}_{chunk.page_start}"
        graph_client.write("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (s:Section {section_id: $section_id})
            MERGE (c)-[:BELONGS_TO]->(s)
        """, chunk_id=chunk.chunk_id, section_id=section_id)
        edges_created += 1

    # ── 4. Create/merge FinancialFact nodes + edges ──────────────────────────
    for fact in facts:
        graph_client.write("""
            MERGE (f:FinancialFact {fact_id: $fact_id})
            SET f.metric_name_raw = $metric_name_raw,
                f.metric_name_canonical = $metric_name_canonical,
                f.metric_category = $metric_category,
                f.value = $value,
                f.unit = $unit,
                f.currency = $currency,
                f.period = $period,
                f.fiscal_year = $fiscal_year,
                f.company = $company,
                f.is_comparative = $is_comparative,
                f.source_chunk_id = $source_chunk_id,
                f.confidence = $confidence,
                f.verbatim_text = $verbatim_text
        """, **fact.model_dump())
        nodes_created += 1

        # Chunk → FinancialFact (CONTAINS_FACT)
        graph_client.write("""
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MATCH (f:FinancialFact {fact_id: $fact_id})
            MERGE (c)-[:CONTAINS_FACT]->(f)
        """, chunk_id=fact.source_chunk_id, fact_id=fact.fact_id)
        edges_created += 1

        # Document → FinancialFact (REPORTS)
        if doc_id:
            graph_client.write("""
                MATCH (d:Document {doc_id: $doc_id})
                MATCH (f:FinancialFact {fact_id: $fact_id})
                MERGE (d)-[:REPORTS]->(f)
            """, doc_id=doc_id, fact_id=fact.fact_id)
            edges_created += 1

    # ── 5. Create/merge Entity nodes + edges ─────────────────────────────────
    entity_chunks: dict[str, list[str]] = defaultdict(list)
    for entity in entities:
        graph_client.write("""
            MERGE (e:Entity {canonical_name: $canonical_name})
            ON CREATE SET
                e.entity_id = $entity_id,
                e.name = $name,
                e.entity_type = $entity_type,
                e.first_seen_doc_id = $first_seen_doc_id,
                e.mention_count = 1
            ON MATCH SET
                e.mention_count = e.mention_count + 1
        """, canonical_name=entity.canonical_name, entity_id=entity.entity_id,
            name=entity.name, entity_type=entity.entity_type,
            first_seen_doc_id=entity.first_seen_doc_id)
        nodes_created += 1

    # Chunk → Entity (MENTIONS) — link via source_chunk extraction mapping
    # We track entity→chunks during extraction; here we reconstruct from facts' source_chunk_ids
    _link_entities_to_chunks(graph_client, entities, chunks)

    # ── 6. Create/merge RiskFactor nodes + edges ─────────────────────────────
    for rf in risk_factors:
        graph_client.write("""
            MERGE (r:RiskFactor {risk_id: $risk_id})
            SET r.title = $title,
                r.summary = $summary,
                r.risk_category = $risk_category,
                r.company = $company,
                r.fiscal_year = $fiscal_year,
                r.first_appeared_year = $first_appeared_year,
                r.source_chunk_id = $source_chunk_id
        """, risk_id=rf.risk_id, title=rf.title, summary=rf.summary,
            risk_category=rf.risk_category, company=rf.company,
            fiscal_year=rf.fiscal_year, first_appeared_year=rf.first_appeared_year,
            source_chunk_id=rf.source_chunk_id)
        nodes_created += 1

        # Write risk factor embedding
        if rf.embedding:
            graph_client.write("""
                MATCH (r:RiskFactor {risk_id: $risk_id})
                SET r.embedding = $embedding
            """, risk_id=rf.risk_id, embedding=rf.embedding)
            embeddings_written += 1

        # RiskFactor → Chunk (MENTIONED_IN)
        graph_client.write("""
            MATCH (r:RiskFactor {risk_id: $risk_id})
            MATCH (c:Chunk {chunk_id: $chunk_id})
            MERGE (r)-[:MENTIONED_IN]->(c)
        """, risk_id=rf.risk_id, chunk_id=rf.source_chunk_id)
        edges_created += 1

    # ── 7. Build SAME_METRIC_AS edges (per company) ─────────────────────────
    if company:
        edges_created += _build_same_metric_edges(graph_client, company)

    # ── 8. Build NEXT_YEAR edges (per company) ──────────────────────────────
    if company:
        edges_created += _build_next_year_edges(graph_client, company)

    # ── 9. Build EVOLVED_INTO edges for risk factors ────────────────────────
    if company:
        edges_created += _build_evolved_into_edges(graph_client, company)

    # ── 10. AUDITED_BY edge ─────────────────────────────────────────────────
    _link_auditor(graph_client, doc_id, entities)

    logger.info(
        f"Graph build complete: {nodes_created} nodes, {edges_created} edges, "
        f"{embeddings_written} embeddings"
    )

    return GraphBuildResult(
        nodes_created=nodes_created,
        edges_created=edges_created,
        embeddings_written=embeddings_written,
    )


# ── SAME_METRIC_AS (DOC3 §2.7) ─────────────────────────────────────────────

def _build_same_metric_edges(graph_client: Neo4jClient, company: str) -> int:
    """Link same canonical metric across fiscal years, same company only."""
    facts = graph_client.query("""
        MATCH (f:FinancialFact {company: $company})
        RETURN f.fact_id AS fact_id,
               f.metric_name_canonical AS metric_name_canonical,
               f.fiscal_year AS fiscal_year
        ORDER BY f.fiscal_year
    """, company=company)

    groups: dict[str, list[dict]] = defaultdict(list)
    for fact in facts:
        groups[fact["metric_name_canonical"]].append(fact)

    edges = 0
    for metric, fact_list in groups.items():
        fact_list.sort(key=lambda x: x["fiscal_year"])
        for i in range(len(fact_list) - 1):
            graph_client.write("""
                MATCH (a:FinancialFact {fact_id: $id1})
                MATCH (b:FinancialFact {fact_id: $id2})
                MERGE (a)-[:SAME_METRIC_AS]->(b)
            """, id1=fact_list[i]["fact_id"], id2=fact_list[i + 1]["fact_id"])
            edges += 1

    logger.debug(f"Built {edges} SAME_METRIC_AS edges for {company}")
    return edges


# ── NEXT_YEAR edges ─────────────────────────────────────────────────────────

def _build_next_year_edges(graph_client: Neo4jClient, company: str) -> int:
    docs = graph_client.query("""
        MATCH (d:Document {company: $company})
        RETURN d.doc_id AS doc_id, d.fiscal_year AS fiscal_year
        ORDER BY d.fiscal_year
    """, company=company)

    edges = 0
    for i in range(len(docs) - 1):
        graph_client.write("""
            MATCH (a:Document {doc_id: $id1})
            MATCH (b:Document {doc_id: $id2})
            MERGE (a)-[:NEXT_YEAR]->(b)
        """, id1=docs[i]["doc_id"], id2=docs[i + 1]["doc_id"])
        edges += 1
    return edges


# ── EVOLVED_INTO edges ──────────────────────────────────────────────────────

def _build_evolved_into_edges(graph_client: Neo4jClient, company: str) -> int:
    """
    Same company, adjacent years, embedding similarity > threshold.
    """
    risks = graph_client.query("""
        MATCH (r:RiskFactor {company: $company})
        WHERE r.embedding IS NOT NULL
        RETURN r.risk_id AS risk_id,
               r.fiscal_year AS fiscal_year,
               r.embedding AS embedding
        ORDER BY r.fiscal_year
    """, company=company)

    if len(risks) < 2:
        return 0

    import numpy as np

    edges = 0
    by_year: dict[int, list[dict]] = defaultdict(list)
    for r in risks:
        by_year[r["fiscal_year"]].append(r)

    years = sorted(by_year.keys())
    for i in range(len(years) - 1):
        y1, y2 = years[i], years[i + 1]
        if y2 - y1 > 1:  # Must be adjacent
            continue
        for r1 in by_year[y1]:
            for r2 in by_year[y2]:
                try:
                    v1 = np.array(r1["embedding"])
                    v2 = np.array(r2["embedding"])
                    sim = float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))
                    if sim > RISK_SIMILARITY_THRESHOLD:
                        graph_client.write("""
                            MATCH (a:RiskFactor {risk_id: $id1})
                            MATCH (b:RiskFactor {risk_id: $id2})
                            MERGE (a)-[:EVOLVED_INTO]->(b)
                        """, id1=r1["risk_id"], id2=r2["risk_id"])
                        edges += 1
                except Exception:
                    continue

    logger.debug(f"Built {edges} EVOLVED_INTO edges for {company}")
    return edges


# ── Helper: Link entities to chunks ─────────────────────────────────────────

def _link_entities_to_chunks(
    graph_client: Neo4jClient,
    entities: list[Entity],
    chunks: list[Chunk],
):
    """Create MENTIONS edges from chunks to entities based on name matching."""
    for entity in entities:
        for chunk in chunks:
            if entity.name.lower() in chunk.content.lower():
                try:
                    graph_client.write("""
                        MATCH (c:Chunk {chunk_id: $chunk_id})
                        MATCH (e:Entity {canonical_name: $canonical_name})
                        MERGE (c)-[:MENTIONS]->(e)
                    """, chunk_id=chunk.chunk_id, canonical_name=entity.canonical_name)
                except Exception:
                    pass


def _link_auditor(graph_client: Neo4jClient, doc_id: str, entities: list[Entity]):
    """Create AUDITED_BY edge from Document to auditor Entity."""
    for entity in entities:
        if entity.entity_type == "auditor" and doc_id:
            try:
                graph_client.write("""
                    MATCH (d:Document {doc_id: $doc_id})
                    MATCH (e:Entity {canonical_name: $canonical_name})
                    MERGE (d)-[:AUDITED_BY]->(e)
                """, doc_id=doc_id, canonical_name=entity.canonical_name)
            except Exception:
                pass
