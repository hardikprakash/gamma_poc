-- Neo4j schema definition
-- Run via: python -c "from db.neo4j_client import Neo4jClient; Neo4jClient().setup_schema()"

-- Lookup indexes
CREATE INDEX doc_lookup IF NOT EXISTS FOR (d:Document) ON (d.company, d.fiscal_year);
CREATE INDEX fact_lookup IF NOT EXISTS FOR (f:FinancialFact) ON (f.metric_name_canonical, f.company);
CREATE INDEX chunk_lookup IF NOT EXISTS FOR (c:Chunk) ON (c.chunk_id);
CREATE INDEX entity_lookup IF NOT EXISTS FOR (e:Entity) ON (e.canonical_name);
CREATE INDEX section_lookup IF NOT EXISTS FOR (s:Section) ON (s.section_id);
CREATE INDEX risk_lookup IF NOT EXISTS FOR (r:RiskFactor) ON (r.risk_id);
CREATE INDEX fact_id_lookup IF NOT EXISTS FOR (f:FinancialFact) ON (f.fact_id);
CREATE INDEX doc_id_lookup IF NOT EXISTS FOR (d:Document) ON (d.doc_id);

-- Vector index for chunk embeddings (dimensions must match EMBEDDING_DIMENSIONS)
-- nomic-embed-text: 768, mxbai-embed-large: 1024
CREATE VECTOR INDEX chunk_embedding_index IF NOT EXISTS
FOR (c:Chunk) ON (c.embedding)
OPTIONS { indexConfig: {
  `vector.dimensions`: 768,
  `vector.similarity_function`: 'cosine'
}};
