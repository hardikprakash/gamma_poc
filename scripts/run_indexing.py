"""
Standalone script to run graph indexing (embedding + vector index).

Usage::

    python -m scripts.run_indexing            # embed only new nodes
    python -m scripts.run_indexing --force     # re-embed all nodes
"""

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

from app.graphrag.graph_indexing import GraphIndexer


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed graph nodes and create vector index")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-embed nodes that already have an embedding",
    )
    args = parser.parse_args()

    with GraphIndexer(force=args.force) as indexer:
        stats = indexer.run()

    print("\n=== Indexing Summary ===")
    print(json.dumps(stats.to_dict(), indent=2))


if __name__ == "__main__":
    main()
