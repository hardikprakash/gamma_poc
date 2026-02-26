#!/usr/bin/env python3
"""
CLI ingest script — processes PDF financial documents into the Neo4j knowledge graph.

Usage:
    python ingest.py --pdf-dir ./data
    python ingest.py --pdf-dir ./data --company "Apple Inc." --ticker AAPL --fiscal-year 2023
    python ingest.py --pdf ./data/AAPL_2023.pdf --company "Apple Inc." --ticker AAPL --fiscal-year 2023
"""

from __future__ import annotations
import argparse
import asyncio
import glob
import logging
import os
import sys
import time

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import PDF_INPUT_DIR
from db.neo4j_client import Neo4jClient
from pipeline.ingestion.parser import parse_pdf
from pipeline.ingestion.structure import infer_structure
from pipeline.ingestion.chunker import chunk_document
from pipeline.graph.extractor import extract_facts
from pipeline.graph.constructor import build_graph
from llm.embedding_client import embed_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("ingest")


async def ingest_single_pdf(
    pdf_path: str,
    company: str,
    ticker: str,
    fiscal_year: int,
    graph_client: Neo4jClient,
    doc_type_hint: str = "",
) -> dict:
    """Run the full M1→M5 pipeline on a single PDF."""
    logger.info(f"=== Ingesting: {pdf_path} ===")
    logger.info(f"    Company: {company} | Ticker: {ticker} | FY: {fiscal_year}")

    t0 = time.time()

    # ── M1: Parse ────────────────────────────────────────────────────────────
    logger.info("  [M1] Parsing PDF...")
    parsed_doc = parse_pdf(pdf_path)
    logger.info(f"  [M1] Done: {parsed_doc.total_pages} pages")

    # ── M2: Structure Inference ──────────────────────────────────────────────
    logger.info("  [M2] Inferring structure (3 LLM calls)...")
    structured_doc = await infer_structure(parsed_doc, company, fiscal_year, ticker)
    if doc_type_hint:
        structured_doc.doc_type = doc_type_hint
    logger.info(f"  [M2] Done: type={structured_doc.doc_type}, {len(structured_doc.sections)} sections")

    # ── M3: Chunking ─────────────────────────────────────────────────────────
    logger.info("  [M3] Chunking...")
    chunks = chunk_document(structured_doc)
    logger.info(f"  [M3] Done: {len(chunks)} chunks")

    # ── M4: Fact Extraction ──────────────────────────────────────────────────
    logger.info(f"  [M4] Extracting facts ({len(chunks)} chunks, 1 LLM call each)...")
    all_facts = []
    all_entities = []
    all_risk_factors = []

    batch_size = 5
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i:i + batch_size]
        tasks = [extract_facts(chunk) for chunk in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"    Extraction error: {result}")
                continue
            all_facts.extend(result.facts)
            all_entities.extend(result.entities)
            all_risk_factors.extend(result.risk_factors)
        logger.info(f"    Batch {i // batch_size + 1}: processed {len(batch)} chunks")

    logger.info(
        f"  [M4] Done: {len(all_facts)} facts, {len(all_entities)} entities, "
        f"{len(all_risk_factors)} risk factors"
    )

    # ── Embeddings ───────────────────────────────────────────────────────────
    logger.info("  [EMB] Generating chunk embeddings...")
    chunk_texts = [c.content for c in chunks]
    if chunk_texts:
        try:
            embeddings = await embed_batch(chunk_texts)
            for chunk, emb in zip(chunks, embeddings):
                chunk.embedding = emb
            logger.info(f"  [EMB] Done: {len(embeddings)} chunk embeddings")
        except Exception as e:
            logger.warning(f"  [EMB] Chunk embedding failed: {e}")

    # Risk factor embeddings
    risk_texts = [rf.title + " " + rf.summary for rf in all_risk_factors]
    if risk_texts:
        try:
            risk_embs = await embed_batch(risk_texts)
            for rf, emb in zip(all_risk_factors, risk_embs):
                rf.embedding = emb
            logger.info(f"  [EMB] Done: {len(risk_embs)} risk factor embeddings")
        except Exception as e:
            logger.warning(f"  [EMB] Risk embedding failed: {e}")

    # ── M5: Graph Construction ───────────────────────────────────────────────
    logger.info("  [M5] Building graph...")
    doc_id = f"{ticker}_{fiscal_year}_{structured_doc.doc_type}"
    graph_result = build_graph(
        chunks=chunks,
        facts=all_facts,
        entities=all_entities,
        risk_factors=all_risk_factors,
        graph_client=graph_client,
        doc_id=doc_id,
        company=company,
        ticker=ticker,
        fiscal_year=fiscal_year,
        doc_type=structured_doc.doc_type,
        total_pages=parsed_doc.total_pages,
    )
    logger.info(
        f"  [M5] Done: {graph_result.nodes_created} nodes, "
        f"{graph_result.edges_created} edges, "
        f"{graph_result.embeddings_written} embeddings"
    )

    duration = time.time() - t0
    logger.info(f"=== Ingest complete: {pdf_path} ({duration:.1f}s) ===\n")

    return {
        "doc_id": doc_id,
        "chunks_created": len(chunks),
        "facts_created": len(all_facts),
        "entities_created": len(all_entities),
        "risk_factors_created": len(all_risk_factors),
        "duration_seconds": round(duration, 2),
    }


def _infer_metadata_from_path(pdf_path: str) -> tuple[str, str, int]:
    """
    Try to infer company/ticker/year from filename.
    Expected patterns: AAPL_2023.pdf, CompanyA_FY2022_20F.pdf, etc.
    Returns (company, ticker, fiscal_year) — empty/0 if can't determine.
    """
    basename = os.path.splitext(os.path.basename(pdf_path))[0]
    parts = basename.replace("-", "_").split("_")

    ticker = parts[0] if parts else ""
    company = ticker  # Default — user should override
    fiscal_year = 0

    for part in parts:
        # Try to extract year
        cleaned = part.replace("FY", "").replace("fy", "")
        try:
            year = int(cleaned)
            if 2000 <= year <= 2030:
                fiscal_year = year
                break
        except ValueError:
            continue

    return company, ticker, fiscal_year


async def main():
    parser = argparse.ArgumentParser(description="Ingest PDF financial documents into Neo4j graph.")
    parser.add_argument("--pdf-dir", type=str, default=PDF_INPUT_DIR, help="Directory containing PDF files")
    parser.add_argument("--pdf", type=str, default=None, help="Single PDF file to ingest")
    parser.add_argument("--company", type=str, default=None, help="Company name")
    parser.add_argument("--ticker", type=str, default=None, help="Stock ticker")
    parser.add_argument("--fiscal-year", type=int, default=None, help="Fiscal year")
    parser.add_argument("--doc-type", type=str, default="", help="Document type hint")
    args = parser.parse_args()

    # Initialize Neo4j
    graph_client = Neo4jClient()
    graph_client.setup_schema()
    logger.info("Neo4j schema initialized.")

    # Collect PDFs
    if args.pdf:
        pdf_files = [args.pdf]
    else:
        pdf_files = sorted(glob.glob(os.path.join(args.pdf_dir, "*.pdf")))

    if not pdf_files:
        logger.error(f"No PDF files found in {args.pdf_dir}")
        sys.exit(1)

    logger.info(f"Found {len(pdf_files)} PDF files to ingest.")

    # Process each PDF
    results = []
    for pdf_path in pdf_files:
        if args.company and args.ticker and args.fiscal_year:
            company, ticker, fiscal_year = args.company, args.ticker, args.fiscal_year
        else:
            company, ticker, fiscal_year = _infer_metadata_from_path(pdf_path)
            if args.company:
                company = args.company
            if args.ticker:
                ticker = args.ticker
            if args.fiscal_year:
                fiscal_year = args.fiscal_year

        if not company or not ticker or not fiscal_year:
            logger.warning(
                f"Skipping {pdf_path}: could not determine metadata. "
                f"Use --company, --ticker, --fiscal-year flags."
            )
            continue

        result = await ingest_single_pdf(
            pdf_path, company, ticker, fiscal_year,
            graph_client, args.doc_type
        )
        results.append(result)

    # Summary
    logger.info("=" * 60)
    logger.info("INGEST SUMMARY")
    logger.info("=" * 60)
    total_chunks = sum(r["chunks_created"] for r in results)
    total_facts = sum(r["facts_created"] for r in results)
    total_entities = sum(r["entities_created"] for r in results)
    total_duration = sum(r["duration_seconds"] for r in results)
    logger.info(f"  Documents: {len(results)}")
    logger.info(f"  Chunks:    {total_chunks}")
    logger.info(f"  Facts:     {total_facts}")
    logger.info(f"  Entities:  {total_entities}")
    logger.info(f"  Duration:  {total_duration:.1f}s")

    graph_client.close()


if __name__ == "__main__":
    asyncio.run(main())
