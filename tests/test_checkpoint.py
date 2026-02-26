"""
Tests for checkpoint.py — per-document, per-chunk checkpointing.
"""

import json
import os
import tempfile

import pytest

# Ensure project root on path
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from checkpoint import IngestCheckpoint, get_completed_doc_keys, list_checkpoints, STAGES


@pytest.fixture
def tmp_dir(tmp_path):
    """Return a temporary checkpoint directory path."""
    return str(tmp_path / "ckpt")


# ── basic state ──────────────────────────────────────────────────────────────

def test_new_checkpoint_has_empty_state(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt._state["doc_key"] == "AAPL_2023"
    assert ckpt._state["completed_stages"] == []
    assert not ckpt.is_complete


def test_mark_stage_done(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.mark_stage_done("m1m2m3")
    assert ckpt.is_stage_done("m1m2m3")
    assert not ckpt.is_stage_done("m4_extract")

    # Reload from disk
    ckpt2 = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt2.is_stage_done("m1m2m3")


def test_mark_complete(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.mark_complete()
    assert ckpt.is_complete

    # New instance also sees it
    ckpt2 = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt2.is_complete


def test_set_and_get_meta(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.set_meta(company="Apple Inc.", ticker="AAPL", fiscal_year=2023)
    assert ckpt.get_meta("company") == "Apple Inc."
    assert ckpt.get_meta("fiscal_year") == 2023
    assert ckpt.get_meta("nonexistent", "default") == "default"


# ── invalidate_from ──────────────────────────────────────────────────────────

def test_invalidate_from_removes_later_stages(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    for s in STAGES:
        ckpt.mark_stage_done(s)
    ckpt.mark_complete()

    ckpt.invalidate_from("emb")

    assert ckpt.is_stage_done("m1m2m3")
    assert ckpt.is_stage_done("m4_extract")
    assert not ckpt.is_stage_done("emb")
    assert not ckpt.is_stage_done("m5_graph")
    assert not ckpt.is_complete  # COMPLETE marker removed


def test_invalidate_m4_deletes_extractions_file(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.mark_stage_done("m1m2m3")
    ckpt.mark_stage_done("m4_extract")
    # write a fake extraction line
    with open(ckpt._extractions_path(), "w") as f:
        f.write('{"chunk_id":"c1","facts":[],"entities":[],"risk_factors":[]}\n')
    assert os.path.exists(ckpt._extractions_path())

    ckpt.invalidate_from("m4_extract")
    assert not ckpt.is_stage_done("m4_extract")
    assert not os.path.exists(ckpt._extractions_path())


# ── chunks cache ─────────────────────────────────────────────────────────────

def test_save_and_load_chunks(tmp_dir):
    from models.chunk import Chunk

    chunks = [
        Chunk(
            chunk_id="AAPL_2023_revenue_p5_001",
            company="Apple Inc.",
            ticker="AAPL",
            fiscal_year=2023,
            doc_type="10-K",
            section_path="Revenue",
            semantic_category="revenue",
            page_start=5,
            page_end=6,
            chunk_type="prose",
            content="Apple reported $383B in revenue.",
            token_count=8,
            embedding=[0.1, 0.2, 0.3],  # should be stripped on save
        ),
        Chunk(
            chunk_id="AAPL_2023_revenue_p7_002",
            company="Apple Inc.",
            ticker="AAPL",
            fiscal_year=2023,
            doc_type="10-K",
            section_path="Revenue > Products",
            semantic_category="revenue",
            page_start=7,
            page_end=7,
            chunk_type="table",
            content="| Product | Revenue |\n|---|---|\n| iPhone | $200B |",
            token_count=12,
        ),
    ]

    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.save_chunks(chunks)

    loaded = ckpt.load_chunks()
    assert loaded is not None
    assert len(loaded) == 2
    assert loaded[0].chunk_id == "AAPL_2023_revenue_p5_001"
    assert loaded[0].content == "Apple reported $383B in revenue."
    # Embedding should have been stripped
    assert loaded[0].embedding is None
    assert loaded[1].chunk_type == "table"


def test_load_chunks_returns_none_when_missing(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt.load_chunks() is None


# ── M4 per-chunk extractions ────────────────────────────────────────────────

def test_extraction_append_and_load(tmp_dir):
    from models.fact import FinancialFact, Entity, RiskFactor

    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)

    fact1 = FinancialFact(
        fact_id="f1", metric_name_raw="Revenue", metric_name_canonical="total_revenue",
        metric_category="revenue", value=383.0, unit="billions", currency="USD",
        period="FY2023", fiscal_year=2023, company="Apple Inc.",
        source_chunk_id="c1", confidence="high", verbatim_text="$383B"
    )
    entity1 = Entity(
        entity_id="e1", name="Apple Inc.", canonical_name="apple inc.",
        entity_type="company", first_seen_doc_id="AAPL_2023_10-K"
    )
    risk1 = RiskFactor(
        risk_id="r1", title="Supply Chain Risk", summary="Global supply chain disruptions",
        risk_category="operational", company="Apple Inc.", fiscal_year=2023,
        source_chunk_id="c1"
    )

    # Append first chunk
    ckpt.append_extraction("c1", [fact1], [entity1], [risk1])
    assert ckpt.get_extracted_chunk_ids() == {"c1"}

    # Append second chunk (empty)
    ckpt.append_extraction("c2", [], [], [])
    assert ckpt.get_extracted_chunk_ids() == {"c1", "c2"}

    # Load all
    facts, entities, risks = ckpt.load_all_extractions()
    assert len(facts) == 1
    assert facts[0].metric_name_canonical == "total_revenue"
    assert len(entities) == 1
    assert entities[0].name == "Apple Inc."
    assert len(risks) == 1
    assert risks[0].title == "Supply Chain Risk"


def test_extraction_ids_empty_when_no_file(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt.get_extracted_chunk_ids() == set()


def test_extraction_survives_reload(tmp_dir):
    """Extraction data persists across IngestCheckpoint instances."""
    from models.fact import FinancialFact

    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    fact = FinancialFact(
        fact_id="f1", metric_name_raw="EPS", metric_name_canonical="eps_diluted",
        metric_category="per_share", value=6.13, unit="per_share", currency="USD",
        period="FY2023", fiscal_year=2023, company="Apple Inc.",
        source_chunk_id="c5", confidence="high", verbatim_text="$6.13"
    )
    ckpt.append_extraction("c5", [fact], [], [])

    # New instance reads same disk data
    ckpt2 = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert "c5" in ckpt2.get_extracted_chunk_ids()
    facts, _, _ = ckpt2.load_all_extractions()
    assert len(facts) == 1
    assert facts[0].value == 6.13


# ── embedding model tracking ────────────────────────────────────────────────

def test_embedding_model_tracking(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    assert ckpt.get_embedding_model() == ""
    assert not ckpt.embedding_model_changed("nomic-embed-text")

    ckpt.set_embedding_model("nomic-embed-text")
    assert ckpt.get_embedding_model() == "nomic-embed-text"
    assert not ckpt.embedding_model_changed("nomic-embed-text")
    assert ckpt.embedding_model_changed("bge-large-en-v1.5")


# ── wipe ─────────────────────────────────────────────────────────────────────

def test_wipe_removes_directory(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.set_meta(company="Apple")
    ckpt.mark_stage_done("m1m2m3")
    assert os.path.isdir(ckpt.dir)

    ckpt.wipe()
    assert not os.path.isdir(ckpt.dir)


# ── top-level helpers ────────────────────────────────────────────────────────

def test_list_checkpoints(tmp_dir):
    IngestCheckpoint("AAPL_2023", tmp_dir)
    IngestCheckpoint("INFY_2022", tmp_dir)
    result = list_checkpoints(tmp_dir)
    assert set(result) == {"AAPL_2023", "INFY_2022"}


def test_get_completed_doc_keys(tmp_dir):
    ckpt1 = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt1.mark_complete()
    IngestCheckpoint("INFY_2022", tmp_dir)  # not completed

    completed = get_completed_doc_keys(tmp_dir)
    assert completed == {"AAPL_2023"}


# ── summary ──────────────────────────────────────────────────────────────────

def test_summary_string(tmp_dir):
    ckpt = IngestCheckpoint("AAPL_2023", tmp_dir)
    ckpt.mark_stage_done("m1m2m3")
    ckpt.set_embedding_model("nomic-embed-text")
    s = ckpt.summary()
    assert "AAPL_2023" in s
    assert "m1m2m3" in s
    assert "nomic-embed-text" in s


# ── infer metadata from path (from ingest.py) ───────────────────────────────

def test_infer_metadata_standard():
    from ingest import _infer_metadata_from_path
    c, t, y = _infer_metadata_from_path("/data/AAPL_2023.pdf")
    assert t == "AAPL"
    assert y == 2023


def test_infer_metadata_fy_prefix():
    from ingest import _infer_metadata_from_path
    c, t, y = _infer_metadata_from_path("/data/INFY_FY2022_20F.pdf")
    assert t == "INFY"
    assert y == 2022
