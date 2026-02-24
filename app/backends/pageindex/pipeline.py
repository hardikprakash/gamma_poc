"""
PageIndex Backend — Pipeline (skeleton)
=======================================
End-to-end pipeline for the PageIndex approach:
parse PDFs → build page-level TOC indices.

This module is a **skeleton** — the public interface parallels
:class:`app.backends.graphrag.pipeline.Pipeline` so ``run_pipeline.py``
can drive either backend uniformly, but no real logic is implemented yet.
"""

import logging

from app.backends.pageindex.indexer import PageIndexer

logger = logging.getLogger(__name__)


class Pipeline:
    """
    PageIndex ingestion pipeline.

    Usage::

        pipeline = Pipeline()
        results = pipeline.run()            # all companies
        results = pipeline.run("infosys")   # one company
    """

    def __init__(self, skip_existing: bool = True) -> None:
        self.skip_existing = skip_existing
        self.indexer = PageIndexer()

    def run(self, company_filter: str | None = None) -> list[dict]:
        """
        Run the PageIndex pipeline for all (or filtered) filings.

        Returns a list of summary dicts, one per filing.
        """
        logger.warning("PageIndex Pipeline.run() called but not implemented yet")
        return self.indexer.build_all(
            filings_dir="data/filings",
            company_filter=company_filter,
        )
