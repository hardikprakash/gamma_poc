"""
Graph Retrieval Module
======================
Read-only Neo4j queries for the query-time agent.

Provides three retrieval primitives:
  1. **search_nodes** — fuzzy name search across node labels
  2. **get_neighbors** — N-hop neighborhood expansion from a node
  3. **run_cypher** — arbitrary read-only Cypher execution

Plus a helper to convert raw Neo4j records into a compact text
context string the LLM can reason over.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from neo4j import GraphDatabase, Driver

from app.core.config import settings

logger = logging.getLogger(__name__)

# ── Maximum limits to keep context sizes sane ─────────────────────────
MAX_SEARCH_RESULTS = 20
MAX_NEIGHBOR_DEPTH = 2
MAX_CYPHER_ROWS = 50
# ──────────────────────────────────────────────────────────────────────


@dataclass
class GraphContext:
    """Accumulated evidence from one or more retrieval calls."""
    nodes: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    raw_rows: list[dict] = field(default_factory=list)   # free-form Cypher results

    def to_text(self) -> str:
        """Render all accumulated evidence as compact text for the LLM."""
        parts: list[str] = []

        if self.nodes:
            parts.append("=== NODES ===")
            seen_ids: set[str] = set()
            for n in self.nodes:
                nid = n.get("entity_id", "?")
                if nid in seen_ids:
                    continue
                seen_ids.add(nid)
                label = n.get("entity_type", n.get("_labels", ""))
                name = n.get("name", nid)
                props = {k: v for k, v in n.items()
                         if k not in ("entity_id", "entity_type", "name", "sources", "_labels", "_id")}
                src = n.get("sources", [])
                line = f"- [{label}] {name} (id={nid})"
                if props:
                    line += f"  {props}"
                if src:
                    line += f"  [sources: {', '.join(str(s) for s in src)}]"
                parts.append(line)

        if self.relationships:
            parts.append("\n=== RELATIONSHIPS ===")
            seen_rels: set[tuple] = set()
            for r in self.relationships:
                key = (r.get("src"), r.get("type"), r.get("tgt"))
                if key in seen_rels:
                    continue
                seen_rels.add(key)
                props = {k: v for k, v in r.items()
                         if k not in ("src", "tgt", "type", "sources")}
                line = f"- ({r.get('src')}) -[{r.get('type')}]-> ({r.get('tgt')})"
                if props:
                    line += f"  {props}"
                parts.append(line)

        if self.raw_rows:
            parts.append("\n=== CYPHER RESULTS ===")
            for row in self.raw_rows:
                parts.append(f"  {row}")

        return "\n".join(parts) if parts else "(no graph evidence collected)"

    def is_empty(self) -> bool:
        return not self.nodes and not self.relationships and not self.raw_rows


class GraphRetriever:
    """
    Stateful reader that keeps a Neo4j driver open and accumulates
    a GraphContext across multiple retrieval calls.
    """

    def __init__(
        self,
        neo4j_uri: str | None = None,
        neo4j_user: str | None = None,
        neo4j_password: str | None = None,
    ) -> None:
        self._uri = neo4j_uri or settings.NEO4J_URI
        self._user = neo4j_user or settings.NEO4J_USER
        self._password = neo4j_password or settings.NEO4J_PASSWORD
        self.driver: Driver = GraphDatabase.driver(
            self._uri, auth=(self._user, self._password),
        )
        self.context = GraphContext()

    # ── Tool 1: search_nodes ──────────────────────────────────────────

    def search_nodes(
        self,
        query: str,
        entity_type: str | None = None,
        limit: int = MAX_SEARCH_RESULTS,
    ) -> list[dict]:
        """
        Case-insensitive substring search on node `name` property.
        Optionally filter by entity type label.
        Returns list of node property dicts and appends to context.
        """
        if entity_type:
            cypher: str = (
                f"MATCH (n:`{entity_type}`) "
                "WHERE toLower(n.name) CONTAINS toLower($q) "
                "RETURN n LIMIT $lim"
            )
        else:
            cypher = (
                "MATCH (n) "
                "WHERE n.name IS NOT NULL AND toLower(n.name) CONTAINS toLower($q) "
                "RETURN n LIMIT $lim"
            )

        nodes = self._run_and_collect_nodes(cypher, {"q": query, "lim": limit})
        logger.info("search_nodes(%r, type=%s) → %d nodes", query, entity_type, len(nodes))
        return nodes

    # ── Tool 2: get_neighbors ─────────────────────────────────────────

    def get_neighbors(
        self,
        entity_id: str,
        depth: int = 1,
        rel_type: str | None = None,
    ) -> dict:
        """
        Expand the neighborhood around a node up to `depth` hops.
        Optionally filter to a specific relationship type.
        Returns {"nodes": [...], "relationships": [...]}.
        """
        depth = min(depth, MAX_NEIGHBOR_DEPTH)

        rel_pattern = f":`{rel_type}`" if rel_type else ""
        cypher = (
            "MATCH (start {entity_id: $eid}) "
            f"CALL (start) {{ "
            f"  MATCH (start)-[r{rel_pattern}*1..{depth}]-(neighbor) "
            f"  RETURN r, neighbor "
            f"}} "
            "RETURN start, r, neighbor "
            f"LIMIT {MAX_CYPHER_ROWS}"
        )

        nodes: list[dict] = []
        rels: list[dict] = []
        try:
            with self.driver.session() as session:
                result = session.run(cypher, {"eid": entity_id})
                for record in result:
                    # start node
                    start_node = record["start"]
                    nodes.append(_node_to_dict(start_node))

                    # neighbor node
                    neighbor = record["neighbor"]
                    nodes.append(_node_to_dict(neighbor))

                    # relationships (can be a path/list)
                    raw_rels = record["r"]
                    if isinstance(raw_rels, list):
                        for rel in raw_rels:
                            rels.append(_rel_to_dict(rel))
                    else:
                        rels.append(_rel_to_dict(raw_rels))
        except Exception as exc:
            logger.error("get_neighbors failed for %s: %s", entity_id, exc)
            # Fallback to simpler query without CALL subquery
            return self._get_neighbors_simple(entity_id, depth, rel_type)

        # Deduplicate and accumulate
        self._accumulate_nodes(nodes)
        self._accumulate_rels(rels)

        logger.info(
            "get_neighbors(%s, depth=%d) → %d nodes, %d rels",
            entity_id, depth, len(nodes), len(rels),
        )
        return {"nodes": nodes, "relationships": rels}

    def _get_neighbors_simple(
        self,
        entity_id: str,
        depth: int = 1,
        rel_type: str | None = None,
    ) -> dict:
        """Simpler fallback neighbor query without CALL subquery syntax."""
        depth = min(depth, MAX_NEIGHBOR_DEPTH)
        rel_pattern = f":`{rel_type}`" if rel_type else ""

        cypher = (
            f"MATCH (start {{entity_id: $eid}})-[r{rel_pattern}*1..{depth}]-(neighbor) "
            f"RETURN start, r, neighbor LIMIT {MAX_CYPHER_ROWS}"
        )

        nodes: list[dict] = []
        rels: list[dict] = []
        try:
            with self.driver.session() as session:
                result = session.run(cypher, {"eid": entity_id})
                for record in result:
                    nodes.append(_node_to_dict(record["start"]))
                    nodes.append(_node_to_dict(record["neighbor"]))
                    raw_rels = record["r"]
                    if isinstance(raw_rels, list):
                        for rel in raw_rels:
                            rels.append(_rel_to_dict(rel))
                    else:
                        rels.append(_rel_to_dict(raw_rels))
        except Exception as exc:
            logger.error("get_neighbors_simple also failed for %s: %s", entity_id, exc)

        self._accumulate_nodes(nodes)
        self._accumulate_rels(rels)
        return {"nodes": nodes, "relationships": rels}

    # ── Tool 3: run_cypher ────────────────────────────────────────────

    def run_cypher(self, cypher: str, params: dict | None = None) -> list[dict]:
        """
        Execute an arbitrary read-only Cypher query.
        Returns list of row dicts and appends to context.
        """
        params = params or {}

        # Safety: block write operations
        upper = cypher.strip().upper()
        for keyword in ("CREATE", "MERGE", "DELETE", "REMOVE", "SET", "DROP", "DETACH"):
            if keyword in upper.split():
                logger.warning("Blocked write Cypher: %s", cypher[:100])
                return [{"error": "Write operations are not allowed."}]

        rows: list[dict] = []
        try:
            with self.driver.session() as session:
                result = session.run(cypher, params)
                for record in result:
                    row: dict = {}
                    for key in record.keys():
                        val = record[key]
                        row[key] = _neo4j_value_to_serialisable(val)
                    rows.append(row)
                    if len(rows) >= MAX_CYPHER_ROWS:
                        break
        except Exception as exc:
            logger.error("Cypher execution failed: %s\n  Query: %s", exc, cypher[:200])
            rows = [{"error": str(exc)}]

        self.context.raw_rows.extend(rows)
        logger.info("run_cypher → %d rows", len(rows))
        return rows

    # ── Utility ───────────────────────────────────────────────────────

    def get_schema_summary(self) -> str:
        """Return a compact summary of what's actually in the graph."""
        rows = []
        try:
            with self.driver.session() as session:
                # Node label counts
                result = session.run(
                    "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC"
                )
                rows.append("Node counts:")
                for rec in result:
                    rows.append(f"  {rec['label']}: {rec['cnt']}")

                # Relationship type counts
                result = session.run(
                    "MATCH ()-[r]->() RETURN type(r) AS rtype, count(r) AS cnt ORDER BY cnt DESC"
                )
                rows.append("Relationship counts:")
                for rec in result:
                    rows.append(f"  {rec['rtype']}: {rec['cnt']}")
        except Exception as exc:
            rows.append(f"(schema query failed: {exc})")

        return "\n".join(rows)

    def reset_context(self) -> None:
        """Clear accumulated context for a new query."""
        self.context = GraphContext()

    def close(self) -> None:
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ── Private helpers ───────────────────────────────────────────────

    def _run_and_collect_nodes(self, cypher: str, params: dict) -> list[dict]:
        nodes: list[dict] = []
        try:
            with self.driver.session() as session:
                result = session.run(cypher, params)
                for record in result:
                    node = record["n"]
                    nodes.append(_node_to_dict(node))
        except Exception as exc:
            logger.error("Node query failed: %s", exc)
        self._accumulate_nodes(nodes)
        return nodes

    def _accumulate_nodes(self, nodes: list[dict]) -> None:
        seen = {n.get("entity_id") for n in self.context.nodes}
        for n in nodes:
            if n.get("entity_id") not in seen:
                self.context.nodes.append(n)
                seen.add(n.get("entity_id"))

    def _accumulate_rels(self, rels: list[dict]) -> None:
        seen = {(r.get("src"), r.get("type"), r.get("tgt")) for r in self.context.relationships}
        for r in rels:
            key = (r.get("src"), r.get("type"), r.get("tgt"))
            if key not in seen:
                self.context.relationships.append(r)
                seen.add(key)


# ── Neo4j value conversions ──────────────────────────────────────────

def _node_to_dict(node) -> dict:
    """Convert a Neo4j Node object to a plain dict."""
    d = dict(node)
    d["_labels"] = list(node.labels) if hasattr(node, "labels") else []
    d["_id"] = node.element_id if hasattr(node, "element_id") else None
    return d


def _rel_to_dict(rel) -> dict:
    """Convert a Neo4j Relationship object to a plain dict."""
    d = dict(rel)
    d["type"] = rel.type if hasattr(rel, "type") else "UNKNOWN"
    if hasattr(rel, "start_node"):
        d["src"] = dict(rel.start_node).get("entity_id", "?")
    if hasattr(rel, "end_node"):
        d["tgt"] = dict(rel.end_node).get("entity_id", "?")
    return d


def _neo4j_value_to_serialisable(val: Any) -> Any:
    """Make a Neo4j value JSON-friendly."""
    if hasattr(val, "labels"):          # Node
        return _node_to_dict(val)
    if hasattr(val, "type") and hasattr(val, "start_node"):  # Relationship
        return _rel_to_dict(val)
    if isinstance(val, list):
        return [_neo4j_value_to_serialisable(v) for v in val]
    if isinstance(val, dict):
        return {k: _neo4j_value_to_serialisable(v) for k, v in val.items()}
    return val
