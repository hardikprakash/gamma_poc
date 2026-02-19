"""
Run the query agent
===================
Interactive CLI that lets you ask questions against the knowledge graph.

Usage:
    python scripts/run_query.py                          # interactive mode
    python scripts/run_query.py "What RSUs did Salil Parekh exercise?"  # single query

Requires:
  - A running Neo4j instance with ingested data (see run_ingestion.py)
  - LLM API credentials in .env
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
# Suppress noisy neo4j driver logging
logging.getLogger("neo4j").setLevel(logging.WARNING)

from app.agent.agent import QueryAgent


def print_result(result: dict) -> None:
    print(f"\n{'─'*60}")
    print(f"Question: {result['question']}")
    print(f"{'─'*60}")
    print(f"\n{result['answer']}")
    print(f"\n{'─'*60}")
    print(f"Confidence:       {result['confidence']}")
    print(f"Sufficient data:  {result['has_sufficient_data']}")
    print(f"Tool calls:       {len(result['tool_calls'])}")
    if result.get('missing_data'):
        print(f"Missing data:     {result['missing_data']}")
    print(f"{'─'*60}\n")


def main() -> None:
    with QueryAgent() as agent:
        # Single query mode
        if len(sys.argv) > 1:
            question = " ".join(sys.argv[1:])
            result = agent.query(question)
            print_result(result)
            return

        # Interactive mode
        print("=" * 60)
        print("  Financial Filing Query Agent")
        print("  Type your question, or 'quit' to exit.")
        print("  Type 'schema' to see what's in the graph.")
        print("=" * 60)

        while True:
            try:
                question = input("\n> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break
            if question.lower() == "schema":
                print(agent.retriever.get_schema_summary())
                continue

            result = agent.query(question)
            print_result(result)


if __name__ == "__main__":
    main()
