"""
End-to-End Pipeline
===================
Discovers PDF files under ``data/filings/<company>/<year>/<name>.pdf``,
infers metadata from the directory structure, and chains all three
stages: **parsing → extraction → graph ingestion**.

Usage (as a module)::

    from app.pipeline import Pipeline
    pipeline = Pipeline()
    pipeline.run()            # process all PDFs
    pipeline.run("infosys")   # only this company

Each stage writes its output to ``output/parsing/`` and
``output/extraction/`` so individual stages can be re-run or inspected.
"""

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

from app.core.config import settings
from app.graphrag.pdf_parsing import PDFParser
from app.graphrag.entity_relation_extraction import EntityRelationExtractor
from app.graphrag.graph_ingestion import GraphIngestor

logger = logging.getLogger(__name__)

# Directories
FILINGS_DIR = os.path.join(settings.PDF_INPUT_DIR, "filings")
PARSING_OUTPUT_DIR = os.path.join(settings.PDF_OUTPUT_DIR, "parsing")
EXTRACTION_OUTPUT_DIR = os.path.join(settings.PDF_OUTPUT_DIR, "extraction")


@dataclass
class FilingInfo:
    """Metadata inferred from the directory path of a PDF."""
    pdf_path: str
    company_name: str        # e.g. "infosys"
    year: str                # e.g. "2025"
    file_name: str           # e.g. "form20f-2025.pdf"
    document_id: str         # e.g. "infosys_form20f_2025"
    document_name: str       # e.g. "form20f-2025.pdf"

    @property
    def parsed_json_path(self) -> str:
        base = _slugify(Path(self.file_name).stem)
        return os.path.join(PARSING_OUTPUT_DIR, f"parsed_{self.company_name}_{base}.json")

    @property
    def extracted_json_path(self) -> str:
        base = _slugify(Path(self.file_name).stem)
        return os.path.join(EXTRACTION_OUTPUT_DIR, f"extracted_{self.company_name}_{base}.json")


def discover_filings(company_filter: str | None = None) -> list[FilingInfo]:
    """
    Walk ``data/filings/<company>/<year>/<file>.pdf`` and build FilingInfo
    objects with all metadata inferred from the path.

    Args:
        company_filter: If set, only include this company (case-insensitive).
    """
    filings: list[FilingInfo] = []

    if not os.path.isdir(FILINGS_DIR):
        logger.warning("Filings directory not found: %s", FILINGS_DIR)
        return filings

    for company_dir in sorted(os.listdir(FILINGS_DIR)):
        company_path = os.path.join(FILINGS_DIR, company_dir)
        if not os.path.isdir(company_path):
            continue

        company_name = company_dir.lower().strip()

        if company_filter and company_name != company_filter.lower().strip():
            continue

        for year_dir in sorted(os.listdir(company_path)):
            year_path = os.path.join(company_path, year_dir)
            if not os.path.isdir(year_path):
                continue

            # Validate year
            year = year_dir.strip()
            if not re.match(r"^\d{4}$", year):
                logger.warning("Skipping non-year directory: %s", year_path)
                continue

            for file_name in sorted(os.listdir(year_path)):
                if not file_name.lower().endswith(".pdf"):
                    continue

                pdf_path = os.path.join(year_path, file_name)
                base_slug = _slugify(Path(file_name).stem)
                document_id = f"{company_name}_{base_slug}"

                filings.append(FilingInfo(
                    pdf_path=pdf_path,
                    company_name=company_name,
                    year=year,
                    file_name=file_name,
                    document_id=document_id,
                    document_name=file_name,
                ))

    return filings


class Pipeline:
    """
    Orchestrates the full parse → extract → ingest pipeline.

    Usage::

        pipeline = Pipeline()
        results = pipeline.run()          # all companies
        results = pipeline.run("infosys") # one company
    """

    def __init__(self, skip_existing: bool = True) -> None:
        """
        Args:
            skip_existing: If True, skip stages whose output files already
                           exist.  Set False to force re-processing.
        """
        self.skip_existing = skip_existing

    def run(self, company_filter: str | None = None) -> list[dict]:
        """
        Run the full pipeline for all (or filtered) filings.

        Returns a list of summary dicts, one per filing.
        """
        filings = discover_filings(company_filter)

        if not filings:
            logger.warning("No filings found under %s", FILINGS_DIR)
            return []

        logger.info("Discovered %d filing(s) to process.", len(filings))
        for f in filings:
            logger.info("  %s  (company=%s, year=%s, id=%s)",
                        f.pdf_path, f.company_name, f.year, f.document_id)

        # Ensure output dirs exist
        os.makedirs(PARSING_OUTPUT_DIR, exist_ok=True)
        os.makedirs(EXTRACTION_OUTPUT_DIR, exist_ok=True)

        summaries: list[dict] = []

        # ── Stage 1 & 2: Parse and Extract each filing ────────────────
        extraction_paths: list[str] = []

        for filing in filings:
            summary: dict = {
                "document_id": filing.document_id,
                "company": filing.company_name,
                "year": filing.year,
                "pdf_path": filing.pdf_path,
                "stages": {},
            }

            # ── Parse ─────────────────────────────────────────────────
            try:
                parsed_path = self._run_parsing(filing)
                summary["stages"]["parsing"] = {"status": "ok", "output": parsed_path}
            except Exception as exc:
                logger.error("Parsing failed for %s: %s", filing.document_id, exc)
                summary["stages"]["parsing"] = {"status": "error", "message": str(exc)}
                summaries.append(summary)
                continue

            # ── Extract ───────────────────────────────────────────────
            try:
                extracted_path = self._run_extraction(filing)
                summary["stages"]["extraction"] = {"status": "ok", "output": extracted_path}
                extraction_paths.append(extracted_path)
            except Exception as exc:
                logger.error("Extraction failed for %s: %s", filing.document_id, exc)
                summary["stages"]["extraction"] = {"status": "error", "message": str(exc)}
                summaries.append(summary)
                continue

            summaries.append(summary)

        # ── Stage 3: Ingest all extracted files into Neo4j ────────────
        if extraction_paths:
            try:
                ingestion_results = self._run_ingestion(extraction_paths)
                # Attach ingestion results to matching summaries
                for ig in ingestion_results:
                    for s in summaries:
                        if s["document_id"] == ig.get("document_id"):
                            s["stages"]["ingestion"] = {"status": "ok", **ig}
            except Exception as exc:
                logger.error("Graph ingestion failed: %s", exc)
                for s in summaries:
                    if "ingestion" not in s.get("stages", {}):
                        s["stages"]["ingestion"] = {"status": "error", "message": str(exc)}

        return summaries

    # ── Stage runners ─────────────────────────────────────────────────

    def _run_parsing(self, filing: FilingInfo) -> str:
        """Parse a PDF and save the result.  Returns output path."""
        output_path = filing.parsed_json_path

        if self.skip_existing and os.path.isfile(output_path):
            logger.info("Parsing: skipping %s (output exists)", filing.document_id)
            return output_path

        logger.info("Parsing: %s → %s", filing.pdf_path, output_path)
        with PDFParser(
            pdf_path=filing.pdf_path,
            document_id=filing.document_id,
            document_name=filing.document_name,
            document_type="filing",
        ) as parser:
            document = parser.process_document()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(document, f, indent=2, ensure_ascii=False)

        return output_path

    def _run_extraction(self, filing: FilingInfo) -> str:
        """Extract entities/relations and save the result.  Returns output path."""
        output_path = filing.extracted_json_path

        if self.skip_existing and os.path.isfile(output_path):
            logger.info("Extraction: skipping %s (output exists)", filing.document_id)
            return output_path

        parsed_path = filing.parsed_json_path
        if not os.path.isfile(parsed_path):
            raise FileNotFoundError(f"Parsed JSON not found: {parsed_path}")

        logger.info("Extraction: %s → %s", parsed_path, output_path)
        with EntityRelationExtractor(
            parsed_json_path=parsed_path,
            filing_year=filing.year,
        ) as extractor:
            result = extractor.process_document()

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2, ensure_ascii=False)

        return output_path

    def _run_ingestion(self, extraction_paths: list[str]) -> list[dict]:
        """Ingest all extraction JSONs into Neo4j.  Returns summaries."""
        logger.info("Ingestion: %d file(s) into Neo4j", len(extraction_paths))
        results: list[dict] = []

        with GraphIngestor() as gi:
            for path in extraction_paths:
                summary = gi.ingest(path)
                results.append(summary)
            gi.create_indexes()

        return results


# ── Helpers ──────────────────────────────────────────────────────────

def _slugify(text: str) -> str:
    """Convert text to a clean slug: lowercase, underscores, no special chars."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_")
    return text
