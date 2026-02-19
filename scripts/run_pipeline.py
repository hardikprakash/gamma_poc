"""
Run the full pipeline
=====================
Discovers PDFs in ``data/filings/<company>/<year>/`` and runs the
complete parse → extract → ingest workflow.

Usage:
    python scripts/run_pipeline.py                  # all companies
    python scripts/run_pipeline.py infosys           # one company
    python scripts/run_pipeline.py --force           # re-process even if outputs exist
    python scripts/run_pipeline.py infosys --force   # combine

Directory structure expected:
    data/filings/
    └── <company_name>/
        └── <year>/
            └── <name>.pdf

Example:
    data/filings/infosys/2025/form20f-2025.pdf
    data/filings/infosys/2024/form20f-2024.pdf

Requires a running Neo4j instance (see docker-compose.yml).
"""

import sys
import os
import logging

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(name)s - %(message)s",
)
logging.getLogger("neo4j").setLevel(logging.WARNING)

from app.pipeline import Pipeline, FILINGS_DIR


def main() -> None:
    # Parse arguments
    company_filter = None
    force = False
    for arg in sys.argv[1:]:
        if arg == "--force":
            force = True
        else:
            company_filter = arg

    # Check that the filings directory exists
    if not os.path.isdir(FILINGS_DIR):
        print(f"\nFilings directory not found: {FILINGS_DIR}")
        print(f"Create the directory structure:")
        print(f"  {FILINGS_DIR}/<company_name>/<year>/<name>.pdf")
        print(f"\nExample:")
        print(f"  {FILINGS_DIR}/infosys/2025/form20f-2025.pdf\n")
        sys.exit(1)

    pipeline = Pipeline(skip_existing=not force)

    print(f"\n{'='*60}")
    print("  CONTEXT BUILDING PIPELINE")
    if company_filter:
        print(f"  Company filter: {company_filter}")
    if force:
        print(f"  Mode: FORCE (re-processing all stages)")
    else:
        print(f"  Mode: INCREMENTAL (skipping existing outputs)")
    print(f"{'='*60}\n")

    summaries = pipeline.run(company_filter)

    if not summaries:
        print("No filings found to process.")
        sys.exit(1)

    # Print results
    print(f"\n{'='*60}")
    print("PIPELINE RESULTS")
    print(f"{'='*60}")
    for s in summaries:
        print(f"\n  Document: {s['document_id']}")
        print(f"  Company:  {s['company']}")
        print(f"  Year:     {s['year']}")
        print(f"  PDF:      {s['pdf_path']}")
        for stage_name, stage_info in s.get("stages", {}).items():
            status = stage_info.get("status", "?")
            icon = "OK" if status == "ok" else "FAIL"
            detail = ""
            if status == "ok" and "output" in stage_info:
                detail = f" → {stage_info['output']}"
            elif status == "ok" and "entities_written" in stage_info:
                detail = (
                    f" → {stage_info['entities_written']} entities, "
                    f"{stage_info['relationships_written']} relationships"
                )
            elif status == "error":
                detail = f" — {stage_info.get('message', '')}"
            print(f"    {stage_name:12s} [{icon}]{detail}")

    print(f"\n{'='*60}")
    ok_count = sum(1 for s in summaries
                   if all(st.get("status") == "ok"
                          for st in s.get("stages", {}).values()))
    print(f"  {ok_count}/{len(summaries)} filings fully processed.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
