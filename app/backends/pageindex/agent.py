"""
PageIndex Backend — Agent (skeleton)
====================================
Agentic query loop for the PageIndex retrieval approach.

PageIndex builds a JSON table-of-contents for each document, which is
used at query time to locate the most relevant pages before sending
them to an LLM for answer generation.

This module is a **skeleton** — the interface mirrors
:class:`app.backends.graphrag.agent.QueryAgent` so the frontend can
swap backends transparently, but no real logic is implemented yet.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PageIndexAgent:
    """
    Query agent backed by PageIndex document retrieval.

    Usage::

        with PageIndexAgent() as agent:
            result = agent.query("What was Infosys revenue in FY2024?")
            print(result["answer"])
    """

    def __init__(self, **kwargs) -> None:
        # TODO: initialise PageIndex client / index connection
        logger.info("PageIndexAgent initialised (skeleton — not yet implemented)")

    def query(self, question: str) -> dict:
        """
        Answer a question using PageIndex retrieval.

        Returns a dict matching the shape of GraphRAG's QueryAgent:

            {
                "question": str,
                "answer": str,
                "confidence": str,
                "has_sufficient_data": bool,
                "missing_data": str | None,
                "metadata": dict,
            }
        """
        logger.warning("PageIndexAgent.query() called but not implemented yet")
        return {
            "question": question,
            "answer": (
                "[PageIndex backend is not implemented yet] "
                "This is a skeleton — real retrieval logic will be added soon."
            ),
            "confidence": "NONE",
            "has_sufficient_data": False,
            "missing_data": "PageIndex backend not yet implemented.",
            "metadata": {},
        }

    # ── Lifecycle ─────────────────────────────────────────────────────

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
