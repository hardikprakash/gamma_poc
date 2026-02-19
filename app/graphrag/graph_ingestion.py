"""
Graph Ingestion Module
======================
Reads extraction JSON (entities + relationships), resolves duplicate
entities via ID and fuzzy-name matching, and writes everything into
Neo4j using idempotent MERGE operations.

Entity resolution strategy
--------------------------
1. **Exact ID match** – entities with the same slug ID are merged
   automatically (properties from later documents win on conflict).
2. **Fuzzy name match** – within the same entity *type*, if two IDs
   have names with a RapidFuzz token_sort_ratio ≥ FUZZY_THRESHOLD,
   the incoming entity is remapped to the existing ID.  This catches
   cases like ``company_infosys`` vs ``company_infosys_limited``.
3. All relationship source/target IDs are remapped through the
   resolved alias table before writing to Neo4j.
"""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from neo4j import GraphDatabase, Driver
from rapidfuzz import fuzz

from app.core.config import settings

logger = logging.getLogger(__name__)

# ── Tuneable knobs ────────────────────────────────────────────────────
FUZZY_THRESHOLD = 88          # token_sort_ratio score to treat names as same entity
BATCH_SIZE = 200              # Neo4j UNWIND batch size
# ──────────────────────────────────────────────────────────────────────


# ── Lightweight data containers ───────────────────────────────────────

@dataclass
class ResolvedEntity:
    """An entity after ID-resolution, ready for Neo4j."""
    id: str
    type: str
    properties: dict = field(default_factory=dict)
    sources: list = field(default_factory=list)       # list of {document_id, page_num, section}


@dataclass
class ResolvedRelationship:
    """A relationship whose source/target IDs have been resolved."""
    source_id: str
    target_id: str
    type: str
    properties: dict = field(default_factory=dict)
    sources: list = field(default_factory=list)


# ── Entity resolver ──────────────────────────────────────────────────

class EntityResolver:
    """
    Maintains a running registry of canonical entities.

    * Call ``resolve(entities)`` to register a batch of entities.
      Returns the list of ResolvedEntity objects and a ``alias_map``
      dict mapping original IDs → canonical IDs.
    * The resolver persists across multiple ``resolve()`` calls so
      that documents ingested sequentially share the same registry.
    """

    def __init__(self, fuzzy_threshold: int = FUZZY_THRESHOLD) -> None:
        self.threshold = fuzzy_threshold
        # canonical_id → ResolvedEntity
        self._registry: dict[str, ResolvedEntity] = {}
        # type → {canonical_id: name}  (for fuzzy lookup)
        self._name_index: dict[str, dict[str, str]] = defaultdict(dict)

    # ── public API ────────────────────────────────────────────────────

    def resolve(self, entities: list[dict]) -> tuple[list[ResolvedEntity], dict[str, str]]:
        """
        Register a batch of raw entity dicts (as they appear in the
        extraction JSON) and return (resolved_entities, alias_map).
        """
        alias_map: dict[str, str] = {}

        for raw in entities:
            raw_id: str = raw["id"]
            etype: str = raw["type"]
            props: dict = raw.get("properties", {})
            source: dict = raw.get("source", {})
            name: str = props.get("name", raw_id)

            canonical_id = self._find_canonical(raw_id, etype, name)

            if canonical_id is None:
                # brand-new entity
                entity = ResolvedEntity(
                    id=raw_id,
                    type=etype,
                    properties=dict(props),
                    sources=[source] if source else [],
                )
                self._registry[raw_id] = entity
                self._name_index[etype][raw_id] = name
                alias_map[raw_id] = raw_id
            else:
                # merge into existing canonical entity
                existing = self._registry[canonical_id]
                existing.properties.update(props)          # later props win
                if source and source not in existing.sources:
                    existing.sources.append(source)
                alias_map[raw_id] = canonical_id

        resolved = list(self._registry.values())
        return resolved, alias_map

    # ── private helpers ───────────────────────────────────────────────

    def _find_canonical(self, raw_id: str, etype: str, name: str) -> Optional[str]:
        """Return the canonical ID this entity should merge into, or None."""
        # 1. exact ID match
        if raw_id in self._registry:
            return raw_id

        # 2. fuzzy name match within the same entity type
        for cid, cname in self._name_index.get(etype, {}).items():
            score = fuzz.token_sort_ratio(name.lower(), cname.lower())
            if score >= self.threshold:
                logger.info(
                    "Entity resolution: '%s' (id=%s) ≈ '%s' (id=%s)  score=%d",
                    name, raw_id, cname, cid, score,
                )
                return cid

        return None


# ── Neo4j writer ─────────────────────────────────────────────────────

class Neo4jWriter:
    """Thin wrapper around the Neo4j driver to write entities and relationships."""

    def __init__(self, driver: Driver) -> None:
        self.driver = driver

    # ── public API ────────────────────────────────────────────────────

    def ensure_constraints(self) -> None:
        """Create uniqueness constraints so MERGE is efficient."""
        with self.driver.session() as session:
            # one constraint per entity type gives us fast MERGE by entity_id
            for etype in _all_entity_types():
                cypher = (
                    f"CREATE CONSTRAINT IF NOT EXISTS "
                    f"FOR (n:`{etype}`) REQUIRE n.entity_id IS UNIQUE"
                )
                session.run(cypher)
            logger.info("Neo4j uniqueness constraints ensured.")

    def write_entities(self, entities: list[ResolvedEntity]) -> int:
        """MERGE entity nodes into Neo4j.  Returns count written."""
        batches = _make_batches(entities, BATCH_SIZE)
        total = 0
        with self.driver.session() as session:
            for batch in batches:
                for entity in batch:
                    params = {
                        "entity_id": entity.id,
                        "props": _clean_props(entity.properties),
                        "sources": entity.sources,
                        "entity_type": entity.type,
                    }
                    # Dynamic label via APOC-free approach: MERGE then SET label
                    cypher = (
                        "MERGE (n {entity_id: $entity_id}) "
                        "SET n += $props, "
                        "    n.entity_type = $entity_type, "
                        "    n.sources = [s IN $sources | s.document_id] "
                        f"SET n:`{entity.type}` "
                    )
                    session.run(cypher, params)
                    total += 1
        logger.info("Wrote %d entity nodes.", total)
        return total

    def write_relationships(self, relationships: list[ResolvedRelationship]) -> int:
        """MERGE relationship edges into Neo4j.  Returns count written."""
        total = 0
        with self.driver.session() as session:
            for rel in relationships:
                params = {
                    "src_id": rel.source_id,
                    "tgt_id": rel.target_id,
                    "props": _clean_props(rel.properties),
                    "sources": rel.sources,
                }
                cypher = (
                    "MATCH (a {entity_id: $src_id}), (b {entity_id: $tgt_id}) "
                    f"MERGE (a)-[r:`{rel.type}`]->(b) "
                    "SET r += $props, "
                    "    r.sources = [s IN $sources | s.document_id] "
                )
                session.run(cypher, params)
                total += 1
        logger.info("Wrote %d relationships.", total)
        return total

    def create_indexes(self) -> None:
        """Create lookup indexes for common query patterns."""
        index_specs = [
            "CREATE INDEX IF NOT EXISTS FOR (n:Company) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Metric) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Period) ON (n.fiscal_year)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Segment) ON (n.name)",
        ]
        with self.driver.session() as session:
            for spec in index_specs:
                session.run(spec)
        logger.info("Neo4j indexes created.")


# ── Orchestrator ─────────────────────────────────────────────────────

class GraphIngestor:
    """
    Top-level orchestrator.

    Usage::

        with GraphIngestor() as gi:
            gi.ingest("output/extraction/extracted_doc1.json")
            gi.ingest("output/extraction/extracted_doc2.json")
            gi.create_indexes()

    Entity resolution is cumulative across ``ingest()`` calls, so
    entities from doc2 will be merged with matching entities from doc1.
    """

    def __init__(
        self,
        neo4j_uri: str | None = None,
        neo4j_user: str | None = None,
        neo4j_password: str | None = None,
        fuzzy_threshold: int = FUZZY_THRESHOLD,
    ) -> None:
        self._uri = neo4j_uri or settings.NEO4J_URI
        self._user = neo4j_user or settings.NEO4J_USER
        self._password = neo4j_password or settings.NEO4J_PASSWORD

        self.driver: Driver = GraphDatabase.driver(
            self._uri, auth=(self._user, self._password),
        )
        self.writer = Neo4jWriter(self.driver)
        self.resolver = EntityResolver(fuzzy_threshold=fuzzy_threshold)

        # Ensure constraints up-front so MERGE is index-backed
        self.writer.ensure_constraints()

    # ── public API ────────────────────────────────────────────────────

    def ingest(self, extraction_json_path: str) -> dict:
        """
        Ingest one extraction JSON file into Neo4j.

        Returns a summary dict with counts.
        """
        logger.info("Loading %s …", extraction_json_path)
        with open(extraction_json_path, "r") as f:
            data = json.load(f)

        document_id = data.get("document_id", "unknown")
        raw_entities = data.get("entities", [])
        raw_relationships = data.get("relationships", [])

        logger.info(
            "Document '%s': %d entities, %d relationships",
            document_id, len(raw_entities), len(raw_relationships),
        )

        # ── Step 1: resolve entities ──────────────────────────────────
        resolved_entities, alias_map = self.resolver.resolve(raw_entities)

        remapped = sum(1 for oid, cid in alias_map.items() if oid != cid)
        logger.info(
            "Entity resolution: %d unique entities (%d remapped via fuzzy match)",
            len(resolved_entities), remapped,
        )

        # ── Step 2: remap relationship IDs ────────────────────────────
        resolved_rels = _remap_relationships(raw_relationships, alias_map)

        # ── Step 3: write to Neo4j ────────────────────────────────────
        n_entities = self.writer.write_entities(resolved_entities)
        n_rels = self.writer.write_relationships(resolved_rels)

        summary = {
            "document_id": document_id,
            "entities_resolved": len(resolved_entities),
            "entities_remapped": remapped,
            "entities_written": n_entities,
            "relationships_written": n_rels,
        }
        logger.info("Ingestion complete: %s", summary)
        return summary

    def create_indexes(self) -> None:
        """Create lookup indexes (call once after all documents are ingested)."""
        self.writer.create_indexes()

    # ── context manager ───────────────────────────────────────────────

    def close(self) -> None:
        self.driver.close()
        logger.info("Neo4j driver closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ── Module-level helpers ─────────────────────────────────────────────

def _all_entity_types() -> list[str]:
    """Return entity type names from ontology."""
    from app.domain.ontology import ENTITY_TYPES
    return list(ENTITY_TYPES.keys())


def _remap_relationships(
    raw_rels: list[dict],
    alias_map: dict[str, str],
) -> list[ResolvedRelationship]:
    """Remap source/target IDs through the alias table."""
    resolved: list[ResolvedRelationship] = []
    for r in raw_rels:
        src = alias_map.get(r["source_id"], r["source_id"])
        tgt = alias_map.get(r["target_id"], r["target_id"])
        resolved.append(ResolvedRelationship(
            source_id=src,
            target_id=tgt,
            type=r["type"],
            properties=r.get("properties", {}),
            sources=[r.get("source", {})],
        ))
    return resolved


def _clean_props(props: dict) -> dict:
    """
    Sanitise property values for Neo4j.
    Neo4j cannot store None / nested dicts as property values.
    """
    clean: dict = {}
    for k, v in props.items():
        if v is None:
            continue
        if isinstance(v, dict):
            # flatten one level: {"address": {"city": "X"}} → {"address_city": "X"}
            for sub_k, sub_v in v.items():
                if sub_v is not None and not isinstance(sub_v, (dict, list)):
                    clean[f"{k}_{sub_k}"] = sub_v
        elif isinstance(v, list):
            # Neo4j supports homogeneous lists of primitives
            if all(isinstance(i, (str, int, float, bool)) for i in v):
                clean[k] = v
            else:
                clean[k] = json.dumps(v)
        else:
            clean[k] = v
    return clean


def _make_batches(items: list, size: int) -> list[list]:
    return [items[i : i + size] for i in range(0, len(items), size)]
