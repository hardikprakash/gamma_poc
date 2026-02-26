# Graph RAG Agent — Financial Document Analysis

A Graph RAG (Retrieval-Augmented Generation) system that ingests PDF financial documents, extracts structured facts into a Neo4j knowledge graph, and answers natural-language queries with cited, accurate responses.

## Architecture

```
PDF → M1 Parser → M2 Structure Inference → M3 Chunker → M4 Fact Extractor → M5 Graph Constructor
                                                                                        ↓
User Query → M6 Decomposer → M7 Retriever → M8 Reranker → M9 Assembler → M10 Generator → M11 Finalizer → Answer
```

**11 pipeline modules** process documents through ingestion (M1-M5) and query (M6-M11) phases.

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Graph DB | Neo4j 5.x (native vector index) |
| Embeddings | Ollama local (nomic-embed-text, 768d) |
| LLM | OpenRouter API (GPT-4o via OpenAI SDK) |
| Re-ranker | cross-encoder/ms-marco-MiniLM-L-6-v2 |
| Validation | Pydantic v2 on all LLM outputs |
| API | FastAPI (async) |
| Frontend | Streamlit |
| PDF Parsing | PyMuPDF + pdfplumber |

## Quick Start

### 1. Prerequisites

- Python 3.11+
- Docker & Docker Compose (for Ollama + Neo4j)
- Neo4j 5.x instance

### 2. Environment Setup

```bash
cp .env.example .env
# Edit .env with your keys:
```

Required environment variables:

```dotenv
# LLM (OpenRouter)
OPENAI_API_KEY=sk-or-v1-...          # OpenRouter API key
OPENAI_API_BASE_URL=https://openrouter.ai/api/v1
MODEL_NAME=openai/gpt-4o-2024-11-20

# Embeddings (Ollama)
OLLAMA_BASE_URL=http://localhost:11434  # default

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORDstr=testpassword         # mapped internally as NEO4J_PASSWORD
```

### 3. Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Start Services

```bash
# Start Ollama (with GPU)
docker compose up -d

# Pull embedding model
docker exec -it $(docker ps -q -f name=ollama) ollama pull nomic-embed-text

# Start Neo4j (if not using Docker)
# neo4j start
```

### 5. Ingest Documents

```bash
# Ingest all PDFs in a directory
python ingest.py --pdf-dir ./data/pdfs/

# Ingest a single file with explicit metadata
python ingest.py --pdf ./data/apple_10k_2023.pdf \
    --company "Apple Inc." --ticker AAPL \
    --fiscal-year 2023 --doc-type 10-K
```

The CLI infers company/ticker/year from filenames when not specified.

### 6. Start API Server

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 7. Start Frontend

```bash
streamlit run frontend/app.py
```

Open http://localhost:8501 in your browser.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/query` | Submit a natural-language query |
| `GET` | `/corpus` | List ingested documents with stats |
| `POST` | `/ingest` | Upload & ingest a PDF |
| `GET` | `/health` | Health check |

### Example Query

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"query": "What was Apple'\''s revenue in 2023?"}'
```

Response includes `answer`, `confidence`, `resolved_citations`, and optional `conflicts_detected`.

## Evaluation

Run the evaluation suite against the test set:

```bash
python eval/eval.py --test-set eval/test_set.json --output eval/results.json --verbose
```

The eval measures 7 metrics per DOC4:

| Metric | Target |
|--------|--------|
| Numerical Fact Accuracy | ≥ 90% |
| Citation Precision | ≥ 85% |
| Hallucination Rate | < 5% |
| Unanswerable Declaration | ≥ 95% |
| Retrieval Recall (facts) | ≥ 80% |
| P95 Latency | < 8s |
| Conflict Detection Rate | ≥ 80% |

## Testing

```bash
pytest tests/ -v
```

Test coverage:
- `test_interfaces.py` — Contract tests for all 11 module signatures + data models
- `test_parser.py` — PDF parser helper functions
- `test_chunker.py` — Deterministic chunker logic
- `test_extractor.py` — Metric normalization and fact model validation
- `test_retriever.py` — Deduplication, conflict detection, citation assembly

## Project Structure

```
├── api/                   # FastAPI endpoints
│   ├── main.py
│   └── models.py
├── db/                    # Neo4j client + schema
│   ├── neo4j_client.py
│   └── schema.cypher
├── eval/                  # Evaluation runner
│   ├── eval.py
│   └── test_set.json
├── frontend/              # Streamlit UI
│   └── app.py
├── llm/                   # LLM + embedding clients
│   ├── openrouter_client.py
│   ├── embedding_client.py
│   └── validator.py
├── models/                # Pydantic/dataclass models
│   ├── chunk.py
│   ├── fact.py
│   ├── response.py
│   └── taxonomy.py
├── pipeline/
│   ├── ingestion/         # M1-M3: Parse → Structure → Chunk
│   │   ├── parser.py
│   │   ├── structure.py
│   │   └── chunker.py
│   ├── graph/             # M4-M5: Extract → Build Graph
│   │   ├── extractor.py
│   │   └── constructor.py
│   └── query/             # M6-M11: Decompose → Retrieve → Rerank → Assemble → Generate → Finalize
│       ├── decomposer.py
│       ├── retriever.py
│       ├── reranker.py
│       ├── assembler.py
│       ├── generator.py
│       └── finalizer.py
├── tests/                 # Unit & contract tests
├── config.py              # Central configuration
├── ingest.py              # CLI ingest script
├── requirements.txt
└── docker-compose.yaml
```

## Key Design Constraints

1. **C-01**: Every numeric claim cites ≥ 1 source (chunk or fact node)
2. **C-02**: Pydantic v2 validation on all LLM outputs; single retry on failure
3. **C-03**: Citation keys created BEFORE answer generation
4. **C-04**: Idempotent graph writes — re-ingesting same PDF is safe
5. **C-05**: Token budget enforced per pipeline step
6. **C-06**: All queries include ≥ 1 company filter
7. **C-07**: Unanswerable sub-questions declared explicitly, never hallucinated
