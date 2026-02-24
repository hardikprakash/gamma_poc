"""
PageIndex Backend — Indexer (skeleton)
======================================
Builds a JSON table-of-contents for each document's pages, enabling
fast page-level retrieval at query time.

This module is a **skeleton** — the public interface is defined but
the implementation is not yet complete.
"""

import logging

logger = logging.getLogger(__name__)


class PageIndexer:
    """
    Build and persist page-level indices for a set of filing PDFs.

    Expected workflow (once implemented):

    1. Parse PDF into pages (reuse shared PDF parsing if appropriate).
    2. For each page, generate a structured summary / TOC entry.
    3. Store the index (JSON file, DB, or in-memory).

    Usage::

        indexer = PageIndexer()
        indexer.build("data/filings/infosys/2025/form20f-2025.pdf")
    """

    def __init__(self, **kwargs) -> None:
        # TODO: accept config (API keys, output dirs, etc.)
        logger.info("PageIndexer initialised (skeleton)")

    def build(self, pdf_path: str, document_id: str | None = None) -> dict:
        """
        Build a page-level index for a single PDF.

        Returns a summary dict with at least:
            {"document_id": str, "pages_indexed": int, "status": str}
        """
        logger.warning("PageIndexer.build() called but not implemented yet")
        return {
            "document_id": document_id or pdf_path,
            "pages_indexed": 0,
            "status": "skeleton",
        }

    def build_all(self, filings_dir: str, company_filter: str | None = None) -> list[dict]:
        """
        Discover and index all PDFs under the filings directory.

        Returns a list of per-document summary dicts.
        """
        logger.warning("PageIndexer.build_all() called but not implemented yet")
        return []
