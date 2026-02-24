"""
Graph Indexing Module
=====================
Post-ingestion step that:

  1. Reads all entity nodes from Neo4j
  2. Builds a text description for each node
  3. Generates embeddings via Ollama (embeddinggemma)
  4. Stores embeddings as node properties in Neo4j
  5. Creates a Neo4j vector index for semantic search

Usage::

    with GraphIndexer() as indexer:
        stats = indexer.run()
        print(stats)

Or standalone::

    python -m scripts.run_indexing
"""

import logging
import time
from dataclasses import dataclass, field

from neo4j import GraphDatabase, Driver

from app.core.config import settings
from app.backends.graphrag.embedding import embed_batch, node_to_text

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────
VECTOR_INDEX_NAME = "node_embedding_index"
EMBEDDING_PROPERTY = "embedding"
FETCH_BATCH_SIZE = 500  # nodes to read from Neo4j at a time
# ─────────────────────────────────────────────────────────────────────


@dataclass
class IndexingStats:
    """Summary of an indexing run."""
    total_nodes: int = 0
    nodes_embedded: int = 0
    nodes_skipped: int = 0
    embedding_dimension: int = 0
    vector_index_created: bool = False
    elapsed_seconds: float = 0.0

    def to_dict(self) -> dict:
        return {
            "total_nodes": self.total_nodes,
            "nodes_embedded": self.nodes_embedded,
            "nodes_skipped": self.nodes_skipped,
            "embedding_dimension": self.embedding_dimension,
            "vector_index_created": self.vector_index_created,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
        }


class GraphIndexer:
    """
    Reads nodes from Neo4j, generates embeddings, writes them back,
    and creates a vector index.
    """

    def __init__(
        self,
        neo4j_uri: str | None = None,
        neo4j_user: str | None = None,
        neo4j_password: str | None = None,
        force: bool = False,
    ) -> None:
        """
        Args:
            force: If True, re-embed nodes that already have an embedding.
        """
        self._uri = neo4j_uri or settings.NEO4J_URI
        self._user = neo4j_user or settings.NEO4J_USER
        self._password = neo4j_password or settings.NEO4J_PASSWORD
        self.force = force

        self.driver: Driver = GraphDatabase.driver(
            self._uri, auth=(self._user, self._password),
        )
        self.dimension = settings.EMBEDDING_DIMENSION

    # ── Public API ────────────────────────────────────────────────────

    def run(self) -> IndexingStats:
        """
        Full indexing pipeline:
          1. Fetch nodes  →  2. Embed  →  3. Write embeddings  →  4. Create vector index
        """
        t0 = time.perf_counter()
        stats = IndexingStats(embedding_dimension=self.dimension)

        # Step 1: Fetch all entity nodes
        nodes = self._fetch_all_nodes()
        stats.total_nodes = len(nodes)
        logger.info("Fetched %d nodes from Neo4j.", len(nodes))

        if not nodes:
            logger.warning("No nodes found — nothing to index.")
            stats.elapsed_seconds = time.perf_counter() - t0
            return stats

        # Step 2: Filter nodes that need embedding
        to_embed: list[dict] = []
        for node in nodes:
            has_embedding = node.get(EMBEDDING_PROPERTY) is not None
            if has_embedding and not self.force:
                stats.nodes_skipped += 1
                continue
            to_embed.append(node)

        logger.info(
            "%d nodes to embed (%d skipped, force=%s).",
            len(to_embed), stats.nodes_skipped, self.force,
        )

        if to_embed:
            # Step 3: Generate embeddings
            texts = [node_to_text(n) for n in to_embed]
            entity_ids = [n["entity_id"] for n in to_embed]

            logger.info("Generating embeddings for %d nodes …", len(texts))
            embeddings = embed_batch(texts)
            stats.nodes_embedded = len(embeddings)

            # Step 4: Write embeddings back to Neo4j
            self._write_embeddings(entity_ids, embeddings)

        # Step 5: Create vector index
        stats.vector_index_created = self._ensure_vector_index()

        stats.elapsed_seconds = time.perf_counter() - t0
        logger.info("Indexing complete: %s", stats.to_dict())
        return stats

    # ── Private helpers ───────────────────────────────────────────────

    def _fetch_all_nodes(self) -> list[dict]:
        """Fetch all nodes that have an entity_id."""
        cypher = (
            "MATCH (n) "
            "WHERE n.entity_id IS NOT NULL "
            "RETURN n.entity_id AS entity_id, "
            "       n.entity_type AS entity_type, "
            "       n.name AS name, "
            "       n.embedding AS embedding, "
            "       properties(n) AS props"
        )
        nodes: list[dict] = []
        with self.driver.session() as session:
            result = session.run(cypher)
            for record in result:
                node = dict(record["props"])
                node["entity_id"] = record["entity_id"]
                node["entity_type"] = record["entity_type"]
                node["name"] = record["name"]
                node[EMBEDDING_PROPERTY] = record["embedding"]
                nodes.append(node)
        return nodes

    def _write_embeddings(
        self,
        entity_ids: list[str],
        embeddings: list[list[float]],
    ) -> None:
        """Write embedding vectors back to their nodes."""
        logger.info("Writing %d embeddings to Neo4j …", len(entity_ids))
        with self.driver.session() as session:
            for eid, emb in zip(entity_ids, embeddings):
                session.run(
                    "MATCH (n {entity_id: $eid}) "
                    f"SET n.{EMBEDDING_PROPERTY} = $emb",
                    {"eid": eid, "emb": emb},
                )
        logger.info("Embeddings written.")

    def _ensure_vector_index(self) -> bool:
        """
        Create a Neo4j vector index on the embedding property if it
        does not already exist.

        Uses Neo4j 5.x vector index syntax.
        Returns True if index was created (or already exists).
        """
        # Check if it already exists
        with self.driver.session() as session:
            result = list(session.run(
                "SHOW INDEXES YIELD name, type WHERE name = $name RETURN name",
                {"name": VECTOR_INDEX_NAME},
            ))
            if result:
                logger.info("Vector index '%s' already exists.", VECTOR_INDEX_NAME)
                return True

        # Create the index — we need a concrete label.
        # Neo4j vector indexes require a single label.  We create one
        # index on a generic ``Entity`` label, and ensure all nodes
        # that have embeddings also carry that label.
        logger.info("Adding :Entity label to all embedded nodes …")
        with self.driver.session() as session:
            session.run(
                f"MATCH (n) WHERE n.{EMBEDDING_PROPERTY} IS NOT NULL "
                "SET n:Entity"
            )

        logger.info(
            "Creating vector index '%s' (dim=%d, cosine) …",
            VECTOR_INDEX_NAME, self.dimension,
        )
        with self.driver.session() as session:
            session.run(
                f"CREATE VECTOR INDEX {VECTOR_INDEX_NAME} IF NOT EXISTS "
                f"FOR (n:Entity) ON (n.{EMBEDDING_PROPERTY}) "
                "OPTIONS {indexConfig: {"
                f"  `vector.dimensions`: {self.dimension},"
                "  `vector.similarity_function`: 'cosine'"
                "}}"
            )
        logger.info("Vector index created.")
        return True

    # ── Context manager ───────────────────────────────────────────────

    def close(self) -> None:
        self.driver.close()
        logger.info("Neo4j driver closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
