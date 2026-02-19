"""
Run graph ingestion
===================
Reads one or more extraction JSON files and ingests them into Neo4j.

Usage:
    python scripts/run_ingestion.py                           # default single file
    python scripts/run_ingestion.py path/to/extracted.json     # specific file
    python scripts/run_ingestion.py dir/with/jsons/            # all JSONs in a directory

Requires a running Neo4j instance (see docker-compose.yml).
"""

import sys
import os
import glob
import json
import logging

# Add project root to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(name)s - %(message)s",
)

from app.graphrag.graph_ingestion import GraphIngestor


def collect_extraction_files(path: str) -> list[str]:
    """Return a list of extraction JSON file paths from a file or directory."""
    if os.path.isfile(path):
        return [path]
    if os.path.isdir(path):
        pattern = os.path.join(path, "*.json")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No JSON files found in {path}")
            sys.exit(1)
        return files
    print(f"Path not found: {path}")
    sys.exit(1)


def main() -> None:
    # Determine input path
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = "./output/extraction/extracted_infosys_form20f-2025_sample.json"

    files = collect_extraction_files(target)
    print(f"\nFiles to ingest ({len(files)}):")
    for f in files:
        print(f"  - {f}")
    print()

    with GraphIngestor() as gi:
        summaries = []
        for filepath in files:
            summary = gi.ingest(filepath)
            summaries.append(summary)

        # Create indexes after all documents are ingested
        gi.create_indexes()

    # Print summary
    print(f"\n{'='*60}")
    print("INGESTION SUMMARY")
    print(f"{'='*60}")
    for s in summaries:
        print(f"\nDocument: {s['document_id']}")
        print(f"  Entities resolved:  {s['entities_resolved']}")
        print(f"  Entities remapped:  {s['entities_remapped']} (fuzzy-matched)")
        print(f"  Entities written:   {s['entities_written']}")
        print(f"  Relationships written: {s['relationships_written']}")
    print(f"\n{'='*60}")
    print("Done. Verify in Neo4j Browser at http://localhost:7474")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
