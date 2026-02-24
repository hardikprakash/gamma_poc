"""
Backend Base
============
Abstract interface that every retrieval backend must implement.
The frontend and scripts interact with backends solely through this
interface, allowing Graph RAG and PageIndex (and future backends) to
be swapped transparently.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryResult:
    """Unified result returned by any backend's ``query()`` method."""

    question: str
    answer: str
    confidence: str = "UNKNOWN"          # HIGH / MEDIUM / LOW / UNKNOWN
    has_sufficient_data: bool = False
    missing_data: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseBackend(ABC):
    """
    Every retrieval backend exposes two capabilities:

    1. **Ingestion pipeline** — process raw filings into a queryable store.
    2. **Query agent** — answer natural-language questions.
    """

    name: str = "base"

    # ── Query interface ───────────────────────────────────────────────

    @abstractmethod
    def query(self, question: str) -> QueryResult:
        """Answer a natural-language question and return a QueryResult."""
        ...

    # ── Pipeline interface ────────────────────────────────────────────

    @abstractmethod
    def run_pipeline(
        self,
        company_filter: str | None = None,
        force: bool = False,
    ) -> list[dict]:
        """
        Run the full ingestion pipeline (parse → index / ingest).

        Args:
            company_filter: Process only this company (case-insensitive).
            force: Re-process even if outputs already exist.

        Returns:
            A list of per-filing summary dicts.
        """
        ...

    # ── Lifecycle ─────────────────────────────────────────────────────

    def close(self) -> None:
        """Release resources (DB connections, etc.)."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
