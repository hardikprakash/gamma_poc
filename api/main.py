"""
FastAPI application — POST /query, GET /corpus, POST /ingest.
"""

from __future__ import annotations
import asyncio
import logging
import time
import tempfile
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware

from api.models import QueryRequest, IngestResponse, CorpusResponse
from models.response import AgentResponse
from db.neo4j_client import Neo4jClient, AsyncNeo4jClient
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

logger = logging.getLogger(__name__)

# ── Globals ──────────────────────────────────────────────────────────────────
_sync_client: Neo4jClient | None = None
_async_client: AsyncNeo4jClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _sync_client, _async_client
    _sync_client = Neo4jClient()
    _async_client = AsyncNeo4jClient()
    _sync_client.setup_schema()
    logger.info("Neo4j clients initialized, schema ready.")
    yield
    _sync_client.close()
    await _async_client.close()


app = FastAPI(
    title="Financial Filings Graph RAG Agent",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── POST /query ─────────────────────────────────────────────────────────────

@app.post("/query", response_model=AgentResponse)
async def query_endpoint(request: QueryRequest):
    """
    Full query pipeline: decompose → retrieve → rerank → assemble → generate → finalize.
    Returns HTTP 200 with AgentResponse even when confidence is LOW.
    Returns HTTP 422 if query mentions companies not in corpus.
    """
    start = time.time()

    # Fetch available companies and years from graph
    available = await _get_available_corpus()
    available_companies = [d["company"] for d in available]
    available_years = sorted(set(d["fiscal_year"] for d in available))

    # If request specifies companies, validate them
    if request.companies:
        unknown = [c for c in request.companies if c not in available_companies]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"Companies not in corpus: {unknown}. Available: {available_companies}"
            )
        filter_companies = request.companies
    else:
        filter_companies = available_companies

    if request.years:
        filter_years = request.years
    else:
        filter_years = available_years

    # ── Step 1: Query Decomposition ──────────────────────────────────────────
    from pipeline.query.decomposer import decompose_query
    decomposition = await decompose_query(
        request.query, filter_companies, filter_years
    )

    # Validate decomposed companies exist in corpus
    if decomposition.companies:
        unknown = [c for c in decomposition.companies if c not in available_companies]
        if unknown and not any(c in available_companies for c in decomposition.companies):
            raise HTTPException(
                status_code=422,
                detail=f"Query references companies not in corpus: {unknown}. Available: {available_companies}"
            )
        # Keep only valid companies
        decomposition.companies = [c for c in decomposition.companies if c in available_companies]
        if not decomposition.companies:
            decomposition.companies = filter_companies

    if not decomposition.companies:
        decomposition.companies = filter_companies

    # ── Step 2: Hybrid Retrieval ─────────────────────────────────────────────
    from pipeline.query.retriever import retrieve
    retrieval_result = await retrieve(decomposition, _async_client)

    # ── Step 3: Re-ranking ───────────────────────────────────────────────────
    from pipeline.query.reranker import rerank
    rerank_result = rerank(request.query, retrieval_result)

    # ── Step 4: Context Assembly ─────────────────────────────────────────────
    from pipeline.query.assembler import assemble_context
    context_payload = assemble_context(rerank_result, decomposition)

    # ── Step 5: Answer Generation ────────────────────────────────────────────
    from pipeline.query.generator import generate_answer
    raw_response = await generate_answer(
        request.query, context_payload, decomposition
    )

    # ── Step 6: Finalization ─────────────────────────────────────────────────
    from pipeline.query.finalizer import finalize_response
    latency_ms = int((time.time() - start) * 1000)
    response = finalize_response(
        raw_response, context_payload, decomposition, rerank_result, latency_ms
    )
    response.query = request.query

    return response


# ── GET /corpus ──────────────────────────────────────────────────────────────

@app.get("/corpus")
async def get_corpus():
    """Returns all ingested documents with stats."""
    docs = await _get_corpus_with_stats()
    return {"documents": docs}


async def _get_available_corpus() -> list[dict]:
    results = await _async_client.query("""
        MATCH (d:Document)
        RETURN d.company AS company, d.fiscal_year AS fiscal_year, d.doc_id AS doc_id
    """)
    return results


async def _get_corpus_with_stats() -> list[dict]:
    results = await _async_client.query("""
        MATCH (d:Document)
        OPTIONAL MATCH (d)<-[:BELONGS_TO]-(:Section)<-[:BELONGS_TO]-(c:Chunk)
        OPTIONAL MATCH (d)-[:REPORTS]->(f:FinancialFact)
        WITH d,
             count(DISTINCT c) AS chunk_count,
             count(DISTINCT f) AS fact_count
        RETURN d.doc_id AS doc_id,
               d.company AS company,
               d.ticker AS ticker,
               d.fiscal_year AS fiscal_year,
               d.doc_type AS doc_type,
               chunk_count,
               fact_count,
               d.ingest_timestamp AS ingest_timestamp
        ORDER BY d.company, d.fiscal_year
    """)
    return results


# ── POST /ingest ─────────────────────────────────────────────────────────────

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(
    file: UploadFile,
    company: str = Form(...),
    ticker: str = Form(...),
    fiscal_year: int = Form(...),
    doc_type_hint: str = Form(""),
):
    """
    Accepts a PDF upload and runs the full ingest pipeline (M1 → M2 → M3 → M4 → M5).
    """
    start = time.time()

    # Save uploaded file to temp
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    try:
        result = await _run_ingest_pipeline(
            tmp_path, company, ticker, fiscal_year, doc_type_hint
        )
    finally:
        os.unlink(tmp_path)

    duration = time.time() - start

    return IngestResponse(
        doc_id=result["doc_id"],
        chunks_created=result["chunks_created"],
        facts_created=result["facts_created"],
        entities_created=result["entities_created"],
        risk_factors_created=result["risk_factors_created"],
        ingest_duration_seconds=round(duration, 2),
    )


async def _run_ingest_pipeline(
    pdf_path: str,
    company: str,
    ticker: str,
    fiscal_year: int,
    doc_type_hint: str = "",
) -> dict:
    """Run M1 → M2 → M3 → M4 → M5 pipeline."""
    from pipeline.ingestion.parser import parse_pdf
    from pipeline.ingestion.structure import infer_structure
    from pipeline.ingestion.chunker import chunk_document
    from pipeline.graph.extractor import extract_facts
    from pipeline.graph.constructor import build_graph
    from llm.embedding_client import embed_batch

    # M1: Parse
    parsed_doc = parse_pdf(pdf_path)

    # M2: Structure Inference
    structured_doc = await infer_structure(parsed_doc, company, fiscal_year, ticker)
    if doc_type_hint and doc_type_hint != "auto-detect":
        structured_doc.doc_type = doc_type_hint

    # M3: Chunk
    chunks = chunk_document(structured_doc)

    # M4: Fact Extraction — 1 LLM call per chunk
    all_facts = []
    all_entities = []
    all_risk_factors = []

    # Process chunks in batches to manage concurrency
    batch_size = 5
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i:i + batch_size]
        tasks = [extract_facts(chunk) for chunk in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Extraction error: {result}")
                continue
            all_facts.extend(result.facts)
            all_entities.extend(result.entities)
            all_risk_factors.extend(result.risk_factors)

    # Generate embeddings for chunks
    chunk_texts = [c.content for c in chunks]
    if chunk_texts:
        try:
            embeddings = await embed_batch(chunk_texts)
            for chunk, emb in zip(chunks, embeddings):
                chunk.embedding = emb
        except Exception as e:
            logger.warning(f"Chunk embedding failed: {e}")

    # Generate embeddings for risk factors
    risk_texts = [rf.title + " " + rf.summary for rf in all_risk_factors]
    if risk_texts:
        try:
            risk_embeddings = await embed_batch(risk_texts)
            for rf, emb in zip(all_risk_factors, risk_embeddings):
                rf.embedding = emb
        except Exception as e:
            logger.warning(f"Risk factor embedding failed: {e}")

    # M5: Graph Construction
    doc_id = f"{ticker}_{fiscal_year}_{structured_doc.doc_type}"
    graph_result = build_graph(
        chunks=chunks,
        facts=all_facts,
        entities=all_entities,
        risk_factors=all_risk_factors,
        graph_client=_sync_client,
        doc_id=doc_id,
        company=company,
        ticker=ticker,
        fiscal_year=fiscal_year,
        doc_type=structured_doc.doc_type,
        total_pages=parsed_doc.total_pages,
    )

    return {
        "doc_id": doc_id,
        "chunks_created": len(chunks),
        "facts_created": len(all_facts),
        "entities_created": len(all_entities),
        "risk_factors_created": len(all_risk_factors),
    }


# ── Health Check ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}
