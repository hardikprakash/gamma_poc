#!/usr/bin/env python3
"""
CLI ingest script — processes PDF financial documents into the Neo4j knowledge graph.

Features:
  • Per-document, per-chunk checkpointing (survives crashes / quota exhaustion)
  • Resumable M4 extraction — only re-processes un-extracted chunks
  • Embedding-model tracking — swap models and re-embed without redoing M1-M4
  • ``--from-stage`` flag to force restart from a specific pipeline stage

Usage:
    python ingest.py --pdf-dir ./data
    python ingest.py --pdf ./data/AAPL_2023.pdf --company "Apple Inc." --ticker AAPL --fiscal-year 2023
    python ingest.py --pdf-dir ./data --from-stage emb   # re-embed with a new model
"""

from __future__ import annotations
import argparse
import asyncio
import glob
import logging
import os
import sys
import time

import httpx

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    PDF_INPUT_DIR, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
    OLLAMA_BASE_URL, EMBEDDING_MODEL,
    OPENROUTER_BASE_URL, OPENROUTER_API_KEY, LLM_MODEL,
)
from db.neo4j_client import Neo4jClient
from pipeline.ingestion.parser import parse_pdf
from pipeline.ingestion.structure import infer_structure
from pipeline.ingestion.chunker import chunk_document
from pipeline.graph.extractor import extract_facts
from pipeline.graph.constructor import build_graph
from llm.embedding_client import embed_batch
from checkpoint import (
    IngestCheckpoint,
    CHECKPOINT_DIR,
    STAGES,
    get_completed_doc_keys,
)

_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
)
logger = logging.getLogger("ingest")


# ── Service connectivity checks ──────────────────────────────────────────────

async def _check_services() -> bool:
    """Verify Neo4j, Ollama, and OpenRouter are reachable before starting. Returns True if all pass."""
    ok = True

    # ── Neo4j ────────────────────────────────────────────────────────────────
    logger.info("[CHECK] Neo4j ...")
    try:
        client = Neo4jClient()
        client.query("RETURN 1")
        client.close()
        logger.info(f"[CHECK] Neo4j OK  ({NEO4J_URI})")
    except Exception as exc:
        logger.error(f"[CHECK] Neo4j FAIL ({NEO4J_URI}): {exc}")
        ok = False

    # ── Ollama ───────────────────────────────────────────────────────────────
    logger.info("[CHECK] Ollama ...")
    try:
        async with httpx.AsyncClient(timeout=5.0) as http:
            resp = await http.get(f"{OLLAMA_BASE_URL}/api/tags")
            resp.raise_for_status()
            models = [m["name"] for m in resp.json().get("models", [])]
            if not any(EMBEDDING_MODEL in m for m in models):
                logger.warning(
                    f"[CHECK] Ollama OK but model '{EMBEDDING_MODEL}' not found. "
                    f"Pull it with: docker exec ollama ollama pull {EMBEDDING_MODEL}"
                )
            else:
                logger.info(f"[CHECK] Ollama OK  ({OLLAMA_BASE_URL}, model={EMBEDDING_MODEL})")
    except Exception as exc:
        logger.error(f"[CHECK] Ollama FAIL ({OLLAMA_BASE_URL}): {exc}")
        ok = False

    # ── OpenRouter / LLM ────────────────────────────────────────────────────
    logger.info("[CHECK] OpenRouter ...")
    if not OPENROUTER_API_KEY:
        logger.error("[CHECK] OpenRouter FAIL: OPENROUTER_API_KEY is not set")
        ok = False
    else:
        try:
            async with httpx.AsyncClient(timeout=8.0) as http:
                resp = await http.get(
                    f"{OPENROUTER_BASE_URL}/models",
                    headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
                )
                resp.raise_for_status()
                logger.info(f"[CHECK] OpenRouter OK  (model={LLM_MODEL})")
        except Exception as exc:
            logger.error(f"[CHECK] OpenRouter FAIL ({OPENROUTER_BASE_URL}): {exc}")
            ok = False

    return ok


async def ingest_single_pdf(
    pdf_path: str,
    company: str,
    ticker: str,
    fiscal_year: int,
    graph_client: Neo4jClient,
    doc_type_hint: str = "",
    checkpoint_dir: str = CHECKPOINT_DIR,
    from_stage: str | None = None,
) -> dict:
    """Run the full M1→M5 pipeline on a single PDF with per-chunk checkpointing.

    If *from_stage* is set (e.g. ``"emb"``), all stages before it are assumed
    done and their cached results are loaded from the checkpoint directory.
    """
    doc_key = f"{ticker}_{fiscal_year}"
    ckpt = IngestCheckpoint(doc_key, checkpoint_dir)

    # Store metadata for future resume
    ckpt.set_meta(pdf_path=pdf_path, company=company, ticker=ticker, fiscal_year=fiscal_year)

    logger.info(f"=== Ingesting: {pdf_path} ===")
    logger.info(f"    Company: {company} | Ticker: {ticker} | FY: {fiscal_year}")
    logger.info(f"    Checkpoint: {ckpt.summary()}")

    # If --from-stage was requested, invalidate that stage and everything after
    if from_stage and from_stage in STAGES:
        logger.info(f"  --from-stage={from_stage}: invalidating {from_stage}+ and re-running")
        ckpt.invalidate_from(from_stage)

    t0 = time.time()

    # ── M1 + M2 + M3: Parse → Structure → Chunk ─────────────────────────────
    # These are grouped: if chunks are cached we skip all three.
    chunks = None
    if ckpt.is_stage_done("m1m2m3"):
        chunks = ckpt.load_chunks()
        if chunks is not None:
            logger.info(f"  [M1-M3] Loaded {len(chunks)} cached chunks from checkpoint")

    if chunks is None:
        # M1: Parse
        logger.info("  [M1] Parsing PDF...")
        parsed_doc = parse_pdf(pdf_path)
        logger.info(f"  [M1] Done: {parsed_doc.total_pages} pages")

        # M2: Structure Inference
        logger.info("  [M2] Inferring structure (3 LLM calls)...")
        structured_doc = await infer_structure(parsed_doc, company, fiscal_year, ticker)
        if doc_type_hint:
            structured_doc.doc_type = doc_type_hint
        logger.info(
            f"  [M2] Done: type={structured_doc.doc_type}, "
            f"{len(structured_doc.sections)} sections"
        )

        # M3: Chunking
        logger.info("  [M3] Chunking...")
        chunks = chunk_document(structured_doc)
        logger.info(f"  [M3] Done: {len(chunks)} chunks")

        # Persist M3 output + metadata
        ckpt.save_chunks(chunks)
        ckpt.set_meta(
            doc_type=structured_doc.doc_type,
            total_pages=parsed_doc.total_pages,
            total_chunks=len(chunks),
        )
        ckpt.mark_stage_done("m1m2m3")
    else:
        # pull doc_type / total_pages from cached state
        pass

    doc_type = ckpt.get_meta("doc_type") or "other"
    total_pages = ckpt.get_meta("total_pages") or 0

    # ── M4: Fact Extraction (per-chunk resumable) ────────────────────────────
    if ckpt.is_stage_done("m4_extract"):
        logger.info("  [M4] Already complete (checkpoint) — loading cached extractions")
        all_facts, all_entities, all_risk_factors = ckpt.load_all_extractions()
        logger.info(
            f"  [M4] Loaded: {len(all_facts)} facts, {len(all_entities)} entities, "
            f"{len(all_risk_factors)} risk factors"
        )
    else:
        already_extracted = ckpt.get_extracted_chunk_ids()
        pending = [c for c in chunks if c.chunk_id not in already_extracted]

        if already_extracted:
            logger.info(
                f"  [M4] Resuming extraction: {len(already_extracted)} done, "
                f"{len(pending)} remaining"
            )
        else:
            logger.info(
                f"  [M4] Extracting facts ({len(chunks)} chunks, 1 LLM call each)..."
            )

        batch_size = 5
        for i in range(0, len(pending), batch_size):
            batch = pending[i : i + batch_size]
            tasks = [extract_facts(chunk) for chunk in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for chunk, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.warning(
                        f"    Extraction error for {chunk.chunk_id}: {result}"
                    )
                    continue
                # Persist immediately — survives next crash
                ckpt.append_extraction(
                    chunk.chunk_id,
                    result.facts,
                    result.entities,
                    result.risk_factors,
                )
            batch_num = (len(already_extracted) + i) // batch_size + 1
            logger.info(f"    Batch {batch_num}: processed {len(batch)} chunks")

        # Verify all chunks done (some may have failed persistently)
        final_extracted = ckpt.get_extracted_chunk_ids()
        skipped = len(chunks) - len(final_extracted)
        if skipped:
            logger.warning(
                f"  [M4] {skipped} chunk(s) had extraction errors and were skipped"
            )

        ckpt.mark_stage_done("m4_extract")

        # Load consolidated extraction results
        all_facts, all_entities, all_risk_factors = ckpt.load_all_extractions()
        logger.info(
            f"  [M4] Done: {len(all_facts)} facts, {len(all_entities)} entities, "
            f"{len(all_risk_factors)} risk factors"
        )

    # ── Embeddings ───────────────────────────────────────────────────────────
    model_changed = ckpt.embedding_model_changed(EMBEDDING_MODEL)
    if ckpt.is_stage_done("emb") and not model_changed:
        logger.info(
            f"  [EMB] Already complete (checkpoint, model={ckpt.get_embedding_model()})"
        )
        # Embeddings live in-memory on chunks for M5 — must regenerate
        # (they are not persisted due to size). Quick via local Ollama.
        logger.info("  [EMB] Re-generating embeddings from Ollama (needed for M5)...")
    else:
        if model_changed:
            logger.info(
                f"  [EMB] Model changed ({ckpt.get_embedding_model()} → {EMBEDDING_MODEL})"
                " — regenerating all embeddings"
            )

    # Always generate chunk + risk embeddings in-memory for M5
    logger.info("  [EMB] Generating chunk embeddings...")
    chunk_texts = [c.content for c in chunks]
    if chunk_texts:
        try:
            logger.info(f"  [EMB] Sending {len(chunk_texts)} chunks to Ollama (single request)...")
            t0 = time.time()
            embeddings = await embed_batch(chunk_texts)
            for chunk, emb in zip(chunks, embeddings):
                chunk.embedding = emb
            logger.info(f"  [EMB] Done: {len(embeddings)} chunk embeddings in {time.time() - t0:.1f}s")
        except Exception as e:
            logger.warning(f"  [EMB] Chunk embedding failed: {e}")

    # Risk factor embeddings
    risk_texts = [rf.title + " " + rf.summary for rf in all_risk_factors]
    if risk_texts:
        try:
            logger.info(f"  [EMB] Sending {len(risk_texts)} risk factors to Ollama...")
            t0 = time.time()
            risk_embs = await embed_batch(risk_texts)
            for rf, emb in zip(all_risk_factors, risk_embs):
                rf.embedding = emb
            logger.info(f"  [EMB] Done: {len(risk_embs)} risk factor embeddings in {time.time() - t0:.1f}s")
        except Exception as e:
            logger.warning(f"  [EMB] Risk embedding failed: {e}")

    ckpt.set_embedding_model(EMBEDDING_MODEL)
    ckpt.mark_stage_done("emb")

    # ── M5: Graph Construction ───────────────────────────────────────────────
    logger.info("  [M5] Building graph...")
    doc_id = f"{ticker}_{fiscal_year}_{doc_type}"
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
        doc_type=doc_type,
        total_pages=total_pages,
    )
    logger.info(
        f"  [M5] Done: {graph_result.nodes_created} nodes, "
        f"{graph_result.edges_created} edges, "
        f"{graph_result.embeddings_written} embeddings"
    )
    ckpt.mark_stage_done("m5_graph")
    ckpt.mark_complete()

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
    parser.add_argument("--checkpoint-dir", type=str, default=CHECKPOINT_DIR,
                        help=f"Directory for per-doc checkpoints (default: {CHECKPOINT_DIR})")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Wipe all checkpoint data and re-ingest from scratch")
    parser.add_argument("--from-stage", type=str, default=None,
                        choices=list(STAGES),
                        help="Force restart from this pipeline stage (e.g. 'emb' to re-embed)")
    parser.add_argument("--skip-checks", action="store_true",
                        help="Skip service connectivity checks")
    args = parser.parse_args()

    # ── Service connectivity checks ──────────────────────────────────────────
    if not args.skip_checks:
        logger.info("Checking service connectivity...")
        all_ok = await _check_services()
        if not all_ok:
            logger.error("One or more services are unreachable. Fix above errors and retry.")
            logger.error("(Use --skip-checks to bypass if you know what you're doing.)")
            sys.exit(1)

    # ── Checkpoint reset ─────────────────────────────────────────────────────
    if args.reset_checkpoint and os.path.isdir(args.checkpoint_dir):
        import shutil
        shutil.rmtree(args.checkpoint_dir)
        logger.info(f"Checkpoint directory wiped: {args.checkpoint_dir}")

    completed_doc_keys = get_completed_doc_keys(args.checkpoint_dir)
    if completed_doc_keys:
        logger.info(
            f"Checkpoint: {len(completed_doc_keys)} fully-completed doc(s) will be skipped."
        )

    # ── Initialize Neo4j ─────────────────────────────────────────────────────
    graph_client = Neo4jClient()
    graph_client.setup_schema()
    logger.info("Neo4j schema initialized.")

    # ── Collect PDFs ─────────────────────────────────────────────────────────
    if args.pdf:
        pdf_files = [args.pdf]
    else:
        pdf_files = sorted(glob.glob(os.path.join(args.pdf_dir, "*.pdf")))

    if not pdf_files:
        logger.error(f"No PDF files found in {args.pdf_dir}")
        sys.exit(1)

    logger.info(f"Found {len(pdf_files)} PDF files to ingest.")

    # ── Process each PDF ─────────────────────────────────────────────────────
    results = []
    failed = []
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

        doc_key = f"{ticker}_{fiscal_year}"

        # Skip fully-completed docs (unless --from-stage forces a re-run)
        if doc_key in completed_doc_keys and not args.from_stage:
            logger.info(f"Skipping (already completed): {pdf_path}  [{doc_key}]")
            continue

        try:
            result = await ingest_single_pdf(
                pdf_path, company, ticker, fiscal_year,
                graph_client, args.doc_type,
                checkpoint_dir=args.checkpoint_dir,
                from_stage=args.from_stage,
            )
            results.append(result)
        except KeyboardInterrupt:
            logger.warning(f"Interrupted during {pdf_path} — progress saved in checkpoint.")
            raise
        except Exception as exc:
            logger.error(
                f"FAILED: {pdf_path} — {type(exc).__name__}: {exc}\n"
                f"  Progress saved in checkpoint. Re-run to resume."
            )
            failed.append({"path": pdf_path, "error": str(exc)})

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("INGEST SUMMARY")
    logger.info("=" * 60)
    total_chunks = sum(r["chunks_created"] for r in results)
    total_facts = sum(r["facts_created"] for r in results)
    total_entities = sum(r["entities_created"] for r in results)
    total_duration = sum(r["duration_seconds"] for r in results)
    logger.info(f"  Documents completed: {len(results)}")
    logger.info(f"  Documents failed:    {len(failed)}")
    logger.info(f"  Chunks:    {total_chunks}")
    logger.info(f"  Facts:     {total_facts}")
    logger.info(f"  Entities:  {total_entities}")
    logger.info(f"  Duration:  {total_duration:.1f}s")
    if failed:
        logger.info("  Failed files (progress saved — re-run to resume):")
        for f in failed:
            logger.info(f"    {f['path']}: {f['error']}")

    graph_client.close()


if __name__ == "__main__":
    asyncio.run(main())
