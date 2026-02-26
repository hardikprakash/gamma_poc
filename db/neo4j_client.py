"""
Neo4j client wrapper with schema setup, CRUD helpers, and vector search.
"""

from __future__ import annotations
import logging
from neo4j import GraphDatabase, AsyncGraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, EMBEDDING_DIMENSIONS

logger = logging.getLogger(__name__)


class Neo4jClient:
    """Synchronous Neo4j driver for graph construction and schema setup."""

    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
    ):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    # ── Helpers ──────────────────────────────────────────────────────────────
    def query(self, cypher: str, **params) -> list[dict]:
        with self._driver.session() as session:
            result = session.run(cypher, **params)
            return [record.data() for record in result]

    def write(self, cypher: str, **params) -> None:
        with self._driver.session() as session:
            session.run(cypher, **params)

    # ── Schema Setup ─────────────────────────────────────────────────────────
    def setup_schema(self):
        """Create indexes and constraints. Idempotent."""

        # Lookup indexes
        indexes = [
            "CREATE INDEX doc_lookup IF NOT EXISTS FOR (d:Document) ON (d.company, d.fiscal_year)",
            "CREATE INDEX fact_lookup IF NOT EXISTS FOR (f:FinancialFact) ON (f.metric_name_canonical, f.company)",
            "CREATE INDEX chunk_lookup IF NOT EXISTS FOR (c:Chunk) ON (c.chunk_id)",
            "CREATE INDEX entity_lookup IF NOT EXISTS FOR (e:Entity) ON (e.canonical_name)",
            "CREATE INDEX section_lookup IF NOT EXISTS FOR (s:Section) ON (s.section_id)",
            "CREATE INDEX risk_lookup IF NOT EXISTS FOR (r:RiskFactor) ON (r.risk_id)",
            "CREATE INDEX fact_id_lookup IF NOT EXISTS FOR (f:FinancialFact) ON (f.fact_id)",
            "CREATE INDEX doc_id_lookup IF NOT EXISTS FOR (d:Document) ON (d.doc_id)",
        ]
        for idx in indexes:
            try:
                self.write(idx)
            except Exception as e:
                logger.debug(f"Index may already exist: {e}")

        # Vector index — drop and recreate if dimensions changed
        try:
            self.write(f"""
                CREATE VECTOR INDEX chunk_embedding_index IF NOT EXISTS
                FOR (c:Chunk) ON (c.embedding)
                OPTIONS {{ indexConfig: {{
                    `vector.dimensions`: {EMBEDDING_DIMENSIONS},
                    `vector.similarity_function`: 'cosine'
                }} }}
            """)
        except Exception as e:
            logger.debug(f"Vector index creation note: {e}")

        logger.info("Neo4j schema setup complete.")


class AsyncNeo4jClient:
    """Async Neo4j driver for query-time operations."""

    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD,
    ):
        self._driver = AsyncGraphDatabase.driver(uri, auth=(user, password))

    async def close(self):
        await self._driver.close()

    async def query(self, cypher: str, **params) -> list[dict]:
        async with self._driver.session() as session:
            result = await session.run(cypher, **params)
            records = await result.data()
            return records

    async def write(self, cypher: str, **params) -> None:
        async with self._driver.session() as session:
            await session.run(cypher, **params)
