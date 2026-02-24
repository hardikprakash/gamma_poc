"""
Run the full pipeline
=====================
Discovers PDFs in ``data/filings/<company>/<year>/`` and runs the
ingestion pipeline for the chosen backend.

Usage:
    python scripts/run_pipeline.py                          # Graph RAG, all companies
    python scripts/run_pipeline.py infosys                   # one company
    python scripts/run_pipeline.py --force                   # re-process
    python scripts/run_pipeline.py --backend pageindex       # PageIndex backend
    python scripts/run_pipeline.py infosys --force           # combine

Directory structure expected:
    data/filings/
    └── <company_name>/
        └── <year>/
            └── <name>.pdf

Example:
    data/filings/infosys/2025/form20f-2025.pdf
    data/filings/infosys/2024/form20f-2024.pdf

Requires a running Neo4j instance for Graph RAG (see docker-compose.yml).
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


BACKENDS = {
    "graphrag": {
        "label": "Graph RAG",
        "pipeline_factory": lambda: __import__(
            "app.backends.graphrag.pipeline", fromlist=["Pipeline"]
        ).Pipeline,
        "filings_dir_factory": lambda: __import__(
            "app.backends.graphrag.pipeline", fromlist=["FILINGS_DIR"]
        ).FILINGS_DIR,
    },
    "pageindex": {
        "label": "PageIndex",
        "pipeline_factory": lambda: __import__(
            "app.backends.pageindex.pipeline", fromlist=["Pipeline"]
        ).Pipeline,
        "filings_dir_factory": lambda: None,  # PageIndex skeleton doesn't need this
    },
}


def main() -> None:
    company_filter = None
    force = False
    backend_name = "graphrag"

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--force":
            force = True
        elif args[i] == "--backend" and i + 1 < len(args):
            i += 1
            backend_name = args[i].lower()
        else:
            company_filter = args[i]
        i += 1

    if backend_name not in BACKENDS:
        print(f"Unknown backend: {backend_name}")
        print(f"Available: {', '.join(BACKENDS.keys())}")
        sys.exit(1)

    backend = BACKENDS[backend_name]
    PipelineClass = backend["pipeline_factory"]()
    filings_dir = backend["filings_dir_factory"]()

    # Check filings dir (only for backends that define one)
    if filings_dir and not os.path.isdir(filings_dir):
        print(f"\nFilings directory not found: {filings_dir}")
        print(f"Create the directory structure:")
        print(f"  {filings_dir}/<company_name>/<year>/<name>.pdf")
        print(f"\nExample:")
        print(f"  {filings_dir}/infosys/2025/form20f-2025.pdf\n")
        sys.exit(1)

    pipeline = PipelineClass(skip_existing=not force)

    print(f"\n{'='*60}")
    print(f"  PIPELINE — {backend['label']}")
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
