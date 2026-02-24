"""
Embedding Module
================
Generates text embeddings using an Ollama-hosted model.

Provides:
  - ``embed_text``   — embed a single string
  - ``embed_batch``  — embed a list of strings in batches
  - ``node_to_text`` — build a human-readable description from
                        a Neo4j node dict for embedding
"""

import logging
import time
from typing import Optional

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

# ── Configurable defaults ────────────────────────────────────────────
_BASE_URL: str = settings.EMBEDDING_BASE_URL
_MODEL: str = settings.EMBEDDING_MODEL
_BATCH_SIZE: int = settings.EMBEDDING_BATCH_SIZE


# ── Public API ───────────────────────────────────────────────────────

def embed_text(
    text: str,
    model: str | None = None,
    base_url: str | None = None,
) -> list[float]:
    """
    Embed a single text string via the Ollama ``/api/embed`` endpoint.

    Returns a list of floats (the embedding vector).
    """
    url = f"{base_url or _BASE_URL}/api/embed"
    payload = {"model": model or _MODEL, "input": text}

    resp = httpx.post(url, json=payload, timeout=60.0)
    resp.raise_for_status()
    data = resp.json()

    embeddings = data.get("embeddings", [])
    if not embeddings or not embeddings[0]:
        raise ValueError(f"Empty embedding returned for text: {text[:80]!r}")

    return embeddings[0]


def embed_batch(
    texts: list[str],
    model: str | None = None,
    base_url: str | None = None,
    batch_size: int | None = None,
) -> list[list[float]]:
    """
    Embed a list of texts in batches.

    The Ollama ``/api/embed`` endpoint accepts a list of inputs natively,
    so each batch is a single HTTP call.  Returns embeddings in the same
    order as the input texts.
    """
    url = f"{base_url or _BASE_URL}/api/embed"
    mdl = model or _MODEL
    bs = batch_size or _BATCH_SIZE

    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), bs):
        batch = texts[i : i + bs]
        payload = {"model": mdl, "input": batch}

        logger.info(
            "Embedding batch %d–%d of %d texts …",
            i + 1, min(i + bs, len(texts)), len(texts),
        )
        t0 = time.perf_counter()
        resp = httpx.post(url, json=payload, timeout=120.0)
        resp.raise_for_status()
        data = resp.json()
        elapsed = time.perf_counter() - t0

        embeddings = data.get("embeddings", [])
        if len(embeddings) != len(batch):
            raise ValueError(
                f"Expected {len(batch)} embeddings, got {len(embeddings)}"
            )

        all_embeddings.extend(embeddings)
        logger.info("  → %d embeddings in %.1fs", len(embeddings), elapsed)

    return all_embeddings


def node_to_text(node: dict) -> str:
    """
    Build a short descriptive string from a Neo4j node dict,
    suitable for embedding.

    Format:  ``[EntityType] name. key1: val1. key2: val2.``

    Skips internal/meta keys like ``entity_id``, ``entity_type``,
    ``sources``, ``_labels``, ``_id``, and ``embedding``.
    """
    SKIP_KEYS = {
        "entity_id", "entity_type", "sources", "_labels", "_id",
        "embedding", "name",
    }

    entity_type = node.get("entity_type", "Entity")
    name = node.get("name", node.get("entity_id", "unknown"))

    parts = [f"[{entity_type}] {name}"]

    for key, value in node.items():
        if key in SKIP_KEYS or value is None:
            continue
        if isinstance(value, (list, dict)):
            continue  # skip complex nested values
        parts.append(f"{key}: {value}")

    return ". ".join(parts)
