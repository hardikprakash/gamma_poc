"""
Query Agent
===========
Agentic chatbot that answers financial questions by querying a Neo4j
knowledge graph following a strict 4-step workflow:

  1. **Assess** — understand what the user is asking
  2. **Plan**   — list the data items needed from the graph
  3. **Fetch**  — retrieve data via search / neighbors / Cypher
  4. **Answer** — synthesise a cited answer or explain what's missing
"""

import json
import logging
from typing import Any

from app.core.openrouter import client
from app.core.config import settings
from app.core.prompts import (
    query_agent_system_prompt as AGENT_SYSTEM_PROMPT,
    answer_generation_prompt as ANSWER_PROMPT,
)
from app.domain.ontology import ENTITY_TYPES, RELATIONSHIP_TYPES
from app.graphrag.graph_retrieval import GraphRetriever

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 12

# ── Tool definitions ─────────────────────────────────────────────────

TOOL_DEFINITIONS = [
    # Step 1
    {
        "type": "function",
        "function": {
            "name": "assess_query",
            "description": (
                "Step 1: Assess the user's question. Identify the intent, "
                "entities, time periods, metrics, and any comparisons or "
                "aggregations required."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": (
                            "High-level intent: lookup_value, compare_values, "
                            "list_entities, explain_relationship, summarise, etc."
                        ),
                    },
                    "entities_mentioned": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Names of entities the user is asking about.",
                    },
                    "time_periods": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Time periods or fiscal years mentioned.",
                    },
                    "metrics_or_facts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific metrics, facts, or data points requested.",
                    },
                },
                "required": ["intent", "entities_mentioned"],
            },
        },
    },
    # Step 2
    {
        "type": "function",
        "function": {
            "name": "plan_retrieval",
            "description": (
                "Step 2: Based on the assessment, produce a concrete plan of "
                "data items to retrieve from the graph. Each item should "
                "describe what to search for, which tool to use, and why."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "data_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "description": {"type": "string"},
                                "search_strategy": {
                                    "type": "string",
                                    "description": (
                                        "Which tool to use: search_nodes, "
                                        "get_neighbors, or run_cypher."
                                    ),
                                },
                            },
                            "required": ["description", "search_strategy"],
                        },
                        "description": "List of data items needed to answer the query.",
                    },
                },
                "required": ["data_items"],
            },
        },
    },
    # Step 3a
    {
        "type": "function",
        "function": {
            "name": "search_nodes",
            "description": (
                "Step 3 (Fetch): Search the knowledge graph for nodes whose "
                "name contains the query string. Optionally filter by entity type."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Substring to search for in node names.",
                    },
                    "entity_type": {
                        "type": "string",
                        "description": (
                            "Optional entity type label. One of: "
                            + ", ".join(ENTITY_TYPES.keys())
                        ),
                    },
                },
                "required": ["query"],
            },
        },
    },
    # Step 3b
    {
        "type": "function",
        "function": {
            "name": "get_neighbors",
            "description": (
                "Step 3 (Fetch): Expand the neighborhood around a known node. "
                "Returns nodes and relationships within `depth` hops."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "entity_id": {
                        "type": "string",
                        "description": "The entity_id of the node to expand from.",
                    },
                    "depth": {
                        "type": "integer",
                        "description": "Number of hops (1 or 2). Default 1.",
                        "default": 1,
                    },
                    "rel_type": {
                        "type": "string",
                        "description": (
                            "Optional relationship type to follow. One of: "
                            + ", ".join(RELATIONSHIP_TYPES)
                        ),
                    },
                },
                "required": ["entity_id"],
            },
        },
    },
    # Step 3c
    {
        "type": "function",
        "function": {
            "name": "run_cypher",
            "description": (
                "Step 3 (Fetch): Execute a read-only Cypher query. "
                "Use for precise lookups, aggregations, or multi-hop patterns. "
                "Write operations are blocked."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "cypher": {
                        "type": "string",
                        "description": "A valid read-only Cypher query.",
                    },
                    "params": {
                        "type": "object",
                        "description": "Optional parameters for the query.",
                    },
                },
                "required": ["cypher"],
            },
        },
    },
    # Step 4
    {
        "type": "function",
        "function": {
            "name": "submit_answer",
            "description": (
                "Step 4: Submit the final answer. Call this when you have "
                "gathered enough data — or determined that data is insufficient. "
                "Provide a precise answer with citations, or explain what is missing."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "has_sufficient_data": {
                        "type": "boolean",
                        "description": "True if the graph has enough data to answer.",
                    },
                    "answer": {
                        "type": "string",
                        "description": (
                            "The final answer if sufficient data exists. "
                            "Include specific values, dates, and source citations."
                        ),
                    },
                    "missing_data": {
                        "type": "string",
                        "description": (
                            "If data is insufficient, describe exactly what "
                            "information is missing from the graph."
                        ),
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["HIGH", "MEDIUM", "LOW"],
                        "description": "Confidence level based on evidence coverage.",
                    },
                },
                "required": ["has_sufficient_data", "confidence"],
            },
        },
    },
]


# ── Agent class ──────────────────────────────────────────────────────

class QueryAgent:
    """
    Agentic query loop following the 4-step workflow:
    assess → plan → fetch → answer.

    Usage::

        with QueryAgent() as agent:
            result = agent.query("What RSUs did Salil Parekh exercise?")
            print(result["answer"])
    """

    def __init__(self, **neo4j_kwargs) -> None:
        self.retriever = GraphRetriever(**neo4j_kwargs)
        self.system_prompt = AGENT_SYSTEM_PROMPT.format(
            entity_types=json.dumps(ENTITY_TYPES, indent=2),
            relationship_types=json.dumps(RELATIONSHIP_TYPES, indent=2),
        )

    def query(self, question: str) -> dict:
        """
        Run the full agent workflow for a user question.

        Returns::

            {
                "question": str,
                "answer": str,
                "confidence": str,
                "has_sufficient_data": bool,
                "missing_data": str | None,
                "assessment": dict | None,
                "plan": list | None,
                "tool_calls": list[dict],
                "graph_context": str,
            }
        """
        self.retriever.reset_context()

        schema_summary = self.retriever.get_schema_summary()

        messages: list[dict] = [
            {"role": "system", "content": self.system_prompt},
            {
                "role": "user",
                "content": (
                    f"{question}\n\n"
                    f"### Current graph contents\n{schema_summary}"
                ),
            },
        ]

        # Track the workflow state
        assessment: dict | None = None
        plan: list | None = None
        tool_log: list[dict] = []
        final_result: dict | None = None

        for round_num in range(1, MAX_TOOL_ROUNDS + 1):
            logger.info("Agent round %d …", round_num)

            response = client.chat.completions.create(
                model=settings.MODEL_NAME,  # type: ignore
                messages=messages,           # type: ignore[arg-type]
                tools=TOOL_DEFINITIONS,      # type: ignore[arg-type]
                tool_choice="auto",
                temperature=0,
            )

            assistant_message = response.choices[0].message
            messages.append(assistant_message)  # type: ignore[arg-type]

            # No tool calls → model decided to answer directly
            if not assistant_message.tool_calls:
                if final_result is None:
                    final_result = {
                        "has_sufficient_data": True,
                        "answer": assistant_message.content or "(no answer)",
                        "confidence": "MEDIUM",
                        "missing_data": None,
                    }
                break

            # Process tool calls
            for tool_call in assistant_message.tool_calls:
                fn_name = tool_call.function.name        # type: ignore[union-attr]
                fn_args = json.loads(tool_call.function.arguments)  # type: ignore[union-attr]

                logger.info("  [%s] %s", fn_name, _truncate(str(fn_args), 120))
                tool_log.append({"tool": fn_name, "args": fn_args})

                result = self._dispatch(fn_name, fn_args, question)

                # Capture workflow metadata
                if fn_name == "assess_query":
                    assessment = fn_args
                elif fn_name == "plan_retrieval":
                    plan = fn_args.get("data_items", [])
                elif fn_name == "submit_answer":
                    final_result = {
                        "has_sufficient_data": fn_args.get("has_sufficient_data", False),
                        "answer": fn_args.get("answer", ""),
                        "confidence": fn_args.get("confidence", "LOW"),
                        "missing_data": fn_args.get("missing_data"),
                    }

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(result, default=str),
                })

                if fn_name == "submit_answer":
                    break

            if final_result and final_result.get("answer"):
                break

        # If we exhausted rounds without a final answer, force one
        if final_result is None:
            logger.warning("Agent hit max rounds, forcing answer generation.")
            final_result = self._force_answer(question)

        # If the agent submitted but answer is empty, generate from context
        if final_result and not final_result.get("answer") and final_result.get("has_sufficient_data"):
            generated = self._generate_answer_from_context(question)
            final_result["answer"] = generated

        return {
            "question": question,
            "answer": final_result.get("answer", "(no answer)"),
            "confidence": final_result.get("confidence", "UNKNOWN"),
            "has_sufficient_data": final_result.get("has_sufficient_data", False),
            "missing_data": final_result.get("missing_data"),
            "assessment": assessment,
            "plan": plan,
            "tool_calls": tool_log,
            "graph_context": self.retriever.context.to_text(),
        }

    # ── Tool dispatch ─────────────────────────────────────────────────

    def _dispatch(self, name: str, args: dict, question: str) -> dict:
        if name == "assess_query":
            return {"status": "ok", "message": "Assessment recorded. Proceed to plan_retrieval."}

        elif name == "plan_retrieval":
            items = args.get("data_items", [])
            return {
                "status": "ok",
                "items_planned": len(items),
                "message": f"Plan recorded with {len(items)} item(s). Proceed to fetch data.",
            }

        elif name == "search_nodes":
            nodes = self.retriever.search_nodes(
                query=args["query"],
                entity_type=args.get("entity_type"),
            )
            return {"nodes_found": len(nodes), "nodes": _compact_nodes(nodes)}

        elif name == "get_neighbors":
            result = self.retriever.get_neighbors(
                entity_id=args["entity_id"],
                depth=args.get("depth", 1),
                rel_type=args.get("rel_type"),
            )
            return {
                "nodes_found": len(result.get("nodes", [])),
                "relationships_found": len(result.get("relationships", [])),
                "nodes": _compact_nodes(result.get("nodes", [])),
                "relationships": result.get("relationships", []),
            }

        elif name == "run_cypher":
            rows = self.retriever.run_cypher(
                cypher=args["cypher"],
                params=args.get("params"),
            )
            return {"rows_returned": len(rows), "rows": rows}

        elif name == "submit_answer":
            return {
                "status": "ok",
                "message": "Answer submitted.",
                "graph_evidence_summary": self.retriever.context.to_text()[:500],
            }

        else:
            return {"error": f"Unknown tool: {name}"}

    # ── Fallback answer generation ────────────────────────────────────

    def _force_answer(self, question: str) -> dict:
        """Generate an answer from accumulated context when agent stalls."""
        answer = self._generate_answer_from_context(question)
        return {
            "has_sufficient_data": not self.retriever.context.is_empty(),
            "answer": answer,
            "confidence": "LOW",
            "missing_data": "Agent reached maximum rounds before completing workflow.",
        }

    def _generate_answer_from_context(self, question: str) -> str:
        """Call the LLM to synthesise an answer from graph evidence."""
        graph_context = self.retriever.context.to_text()

        prompt = ANSWER_PROMPT.format(
            graph_context=graph_context,
            question=question,
        )

        response = client.chat.completions.create(
            model=settings.MODEL_NAME,  # type: ignore
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )

        return response.choices[0].message.content or "(no answer)"

    # ── Context manager ───────────────────────────────────────────────

    def close(self) -> None:
        self.retriever.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ── Helpers ──────────────────────────────────────────────────────────

def _compact_nodes(nodes: list[dict]) -> list[dict]:
    """Trim nodes to the fields the LLM needs for reasoning."""
    compact = []
    seen: set[str] = set()
    for n in nodes:
        eid = n.get("entity_id", "?")
        if eid in seen:
            continue
        seen.add(eid)
        compact.append({
            "entity_id": eid,
            "type": n.get("entity_type", n.get("_labels", "")),
            "name": n.get("name", eid),
            **{k: v for k, v in n.items()
               if k not in ("entity_id", "entity_type", "name",
                             "sources", "_labels", "_id")},
        })
    return compact


def _truncate(text: str, max_len: int = 100) -> str:
    return text[:max_len] + "…" if len(text) > max_len else text
