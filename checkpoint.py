"""
Per-document, per-chunk checkpoint system for the ingest pipeline.

Layout:
    .ingest_checkpoints/
      INFY_2022/
        state.json          # metadata + completed-stage list
        chunks.json         # serialised M3 output (no embeddings)
        extractions.jsonl   # one JSON-line per extracted chunk (append-only)
        COMPLETE            # marker written after M5 succeeds

Stages tracked: m1m2m3  |  m4_extract  |  emb  |  m5_graph
"""

from __future__ import annotations
import json
import os
import shutil
from dataclasses import asdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.chunk import Chunk
    from models.fact import FinancialFact, Entity, RiskFactor

CHECKPOINT_DIR = ".ingest_checkpoints"

# Stages in pipeline order
STAGES = ("m1m2m3", "m4_extract", "emb", "m5_graph")


class IngestCheckpoint:
    """Manages on-disk checkpoint state for a single document."""

    def __init__(self, doc_key: str, checkpoint_dir: str = CHECKPOINT_DIR) -> None:
        self.doc_key = doc_key
        self.dir = os.path.join(checkpoint_dir, doc_key)
        os.makedirs(self.dir, exist_ok=True)
        self._state: dict = self._load_state()

    # ── paths ────────────────────────────────────────────────────────────────
    def _state_path(self) -> str:
        return os.path.join(self.dir, "state.json")

    def _chunks_path(self) -> str:
        return os.path.join(self.dir, "chunks.json")

    def _extractions_path(self) -> str:
        return os.path.join(self.dir, "extractions.jsonl")

    def _complete_path(self) -> str:
        return os.path.join(self.dir, "COMPLETE")

    # ── state I/O ────────────────────────────────────────────────────────────
    def _load_state(self) -> dict:
        p = self._state_path()
        if os.path.exists(p):
            with open(p) as f:
                return json.load(f)
        return {
            "doc_key": self.doc_key,
            "completed_stages": [],
            "pdf_path": "",
            "company": "",
            "ticker": "",
            "fiscal_year": 0,
            "doc_type": "",
            "total_pages": 0,
            "total_chunks": 0,
            "embedding_model": "",
        }

    def _save_state(self) -> None:
        with open(self._state_path(), "w") as f:
            json.dump(self._state, f, indent=2)

    # ── metadata ─────────────────────────────────────────────────────────────
    def set_meta(self, **kwargs) -> None:
        self._state.update(kwargs)
        self._save_state()

    def get_meta(self, key: str, default=None):
        return self._state.get(key, default)

    # ── stage tracking ───────────────────────────────────────────────────────
    def mark_stage_done(self, stage: str) -> None:
        if stage not in self._state["completed_stages"]:
            self._state["completed_stages"].append(stage)
            self._save_state()

    def is_stage_done(self, stage: str) -> bool:
        return stage in self._state["completed_stages"]

    def invalidate_from(self, stage: str) -> None:
        """Remove *stage* and all later stages from completed list."""
        if stage not in STAGES:
            return
        idx = STAGES.index(stage)
        to_remove = set(STAGES[idx:])
        self._state["completed_stages"] = [
            s for s in self._state["completed_stages"] if s not in to_remove
        ]
        # If we invalidate m4, delete its JSONL so it rewrites from scratch
        if "m4_extract" in to_remove and os.path.exists(self._extractions_path()):
            os.remove(self._extractions_path())
        # Remove COMPLETE marker if present
        if os.path.exists(self._complete_path()):
            os.remove(self._complete_path())
        self._save_state()

    # ── completion marker ────────────────────────────────────────────────────
    @property
    def is_complete(self) -> bool:
        return os.path.exists(self._complete_path())

    def mark_complete(self) -> None:
        with open(self._complete_path(), "w") as f:
            f.write("done\n")

    # ── M3 chunks cache ─────────────────────────────────────────────────────
    def save_chunks(self, chunks: list[Chunk]) -> None:
        """Persist serialised chunks (embeddings stripped to save space)."""
        data = []
        for c in chunks:
            d = asdict(c)
            d.pop("embedding", None)
            data.append(d)
        with open(self._chunks_path(), "w") as f:
            json.dump(data, f)
        self._state["total_chunks"] = len(chunks)
        self._save_state()

    def load_chunks(self) -> list[Chunk] | None:
        """Return cached chunks or None if not present."""
        p = self._chunks_path()
        if not os.path.exists(p):
            return None
        from models.chunk import Chunk

        with open(p) as f:
            data = json.load(f)
        return [Chunk(**d) for d in data]

    # ── M4 per-chunk extraction cache ────────────────────────────────────────
    def get_extracted_chunk_ids(self) -> set[str]:
        """Return set of chunk_ids that already have extraction results."""
        p = self._extractions_path()
        if not os.path.exists(p):
            return set()
        ids: set[str] = set()
        with open(p) as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        ids.add(record["chunk_id"])
                    except (json.JSONDecodeError, KeyError):
                        continue
        return ids

    def append_extraction(
        self,
        chunk_id: str,
        facts: list[FinancialFact],
        entities: list[Entity],
        risk_factors: list[RiskFactor],
    ) -> None:
        """Append one chunk's extraction result (JSONL, append-mode)."""
        record = {
            "chunk_id": chunk_id,
            "facts": [f.model_dump() for f in facts],
            "entities": [e.model_dump() for e in entities],
            "risk_factors": [r.model_dump() for r in risk_factors],
        }
        with open(self._extractions_path(), "a") as f:
            f.write(json.dumps(record) + "\n")

    def load_all_extractions(
        self,
    ) -> tuple[list[FinancialFact], list[Entity], list[RiskFactor]]:
        """Read back every extraction line and rebuild model objects."""
        from models.fact import FinancialFact, Entity, RiskFactor

        p = self._extractions_path()
        if not os.path.exists(p):
            return [], [], []
        all_facts: list[FinancialFact] = []
        all_entities: list[Entity] = []
        all_risks: list[RiskFactor] = []
        with open(p) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                all_facts.extend(FinancialFact(**fd) for fd in rec.get("facts", []))
                all_entities.extend(Entity(**ed) for ed in rec.get("entities", []))
                all_risks.extend(RiskFactor(**rd) for rd in rec.get("risk_factors", []))
        return all_facts, all_entities, all_risks

    # ── embedding model tracking ─────────────────────────────────────────────
    def set_embedding_model(self, model: str) -> None:
        self._state["embedding_model"] = model
        self._save_state()

    def get_embedding_model(self) -> str:
        return self._state.get("embedding_model", "")

    def embedding_model_changed(self, current_model: str) -> bool:
        saved = self.get_embedding_model()
        return bool(saved) and saved != current_model

    # ── housekeeping ─────────────────────────────────────────────────────────
    def wipe(self) -> None:
        """Delete the entire checkpoint directory for this doc."""
        if os.path.isdir(self.dir):
            shutil.rmtree(self.dir)

    def summary(self) -> str:
        stages = ", ".join(self._state["completed_stages"]) or "(none)"
        extracted = len(self.get_extracted_chunk_ids())
        total = self._state.get("total_chunks", "?")
        return (
            f"[{self.doc_key}] stages={stages} | "
            f"chunks extracted={extracted}/{total} | "
            f"emb_model={self.get_embedding_model() or '(none)'} | "
            f"complete={self.is_complete}"
        )


# ── Top-level helpers ────────────────────────────────────────────────────────

def list_checkpoints(checkpoint_dir: str = CHECKPOINT_DIR) -> list[str]:
    """Return doc_keys that have checkpoint directories."""
    if not os.path.isdir(checkpoint_dir):
        return []
    return sorted(
        d
        for d in os.listdir(checkpoint_dir)
        if os.path.isdir(os.path.join(checkpoint_dir, d))
    )


def get_completed_doc_keys(checkpoint_dir: str = CHECKPOINT_DIR) -> set[str]:
    """Return doc_keys whose COMPLETE marker exists."""
    result: set[str] = set()
    for dk in list_checkpoints(checkpoint_dir):
        cp = IngestCheckpoint(dk, checkpoint_dir)
        if cp.is_complete:
            result.add(dk)
    return result
