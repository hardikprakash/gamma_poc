"""
Run the query agent
===================
Interactive CLI that lets you ask questions using one of the available
retrieval backends (Graph RAG or PageIndex).

Usage:
    python scripts/run_query.py                                # interactive, Graph RAG
    python scripts/run_query.py "What was Infosys revenue?"     # single query
    python scripts/run_query.py --backend pageindex             # use PageIndex backend

Requires:
  - For Graph RAG: a running Neo4j instance with ingested data
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
logging.getLogger("neo4j").setLevel(logging.WARNING)


BACKENDS = {
    "graphrag": {
        "label": "Graph RAG",
        "agent_factory": lambda: __import__(
            "app.backends.graphrag.agent", fromlist=["QueryAgent"]
        ).QueryAgent,
    },
    "pageindex": {
        "label": "PageIndex",
        "agent_factory": lambda: __import__(
            "app.backends.pageindex.agent", fromlist=["PageIndexAgent"]
        ).PageIndexAgent,
    },
}


def print_result(result: dict) -> None:
    print(f"\n{'─'*60}")
    print(f"Question: {result['question']}")
    print(f"{'─'*60}")
    print(f"\n{result['answer']}")
    print(f"\n{'─'*60}")
    print(f"Confidence:       {result.get('confidence', '—')}")
    print(f"Sufficient data:  {result.get('has_sufficient_data', '—')}")
    print(f"Tool calls:       {len(result.get('tool_calls', []))}")
    if result.get('missing_data'):
        print(f"Missing data:     {result['missing_data']}")
    print(f"{'─'*60}\n")


def main() -> None:
    backend_name = "graphrag"
    positional_args = []

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--backend" and i + 1 < len(args):
            i += 1
            backend_name = args[i].lower()
        else:
            positional_args.append(args[i])
        i += 1

    if backend_name not in BACKENDS:
        print(f"Unknown backend: {backend_name}")
        print(f"Available: {', '.join(BACKENDS.keys())}")
        sys.exit(1)

    backend = BACKENDS[backend_name]
    AgentClass = backend["agent_factory"]()

    with AgentClass() as agent:
        # Single query mode
        if positional_args:
            question = " ".join(positional_args)
            result = agent.query(question)
            print_result(result)
            return

        # Interactive mode
        print("=" * 60)
        print(f"  Financial Filing Query Agent [{backend['label']}]")
        print("  Type your question, or 'quit' to exit.")
        if backend_name == "graphrag":
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
            if question.lower() == "schema" and backend_name == "graphrag":
                print(agent.retriever.get_schema_summary())
                continue

            result = agent.query(question)
            print_result(result)


if __name__ == "__main__":
    main()
