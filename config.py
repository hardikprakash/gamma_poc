"""
Centralized configuration. All env vars, model names, thresholds.
Nothing hardcoded in module files — everything reads from here.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Neo4j ────────────────────────────────────────────────────────────────────
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", os.environ.get("NEO4J_PASSWORDstr", "testpassword"))

# ── LLM via OpenRouter ──────────────────────────────────────────────────────
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", os.environ.get("OPENAI_API_KEY", ""))
OPENROUTER_BASE_URL = os.environ.get("OPENROUTER_BASE_URL", os.environ.get("OPENAI_API_BASE_URL", "https://openrouter.ai/api/v1"))
LLM_MODEL = os.environ.get("LLM_MODEL", os.environ.get("MODEL_NAME", "openai/gpt-4o-2024-11-20"))
LLM_MAX_TOKENS = int(os.environ.get("LLM_MAX_TOKENS", "4096"))

# ── Embeddings via Ollama ───────────────────────────────────────────────────
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "nomic-embed-text-v2-moe")
EMBEDDING_DIMENSIONS = int(os.environ.get("EMBEDDING_DIMENSIONS", "768"))

# ── Re-ranker ───────────────────────────────────────────────────────────────
RERANKER_MODEL = os.environ.get("RERANKER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")

# ── Retrieval / generation thresholds ───────────────────────────────────────
CONTEXT_TOKEN_BUDGET = int(os.environ.get("CONTEXT_TOKEN_BUDGET", "6000"))
VECTOR_TOP_K = int(os.environ.get("VECTOR_TOP_K", "20"))
RERANK_TOP_K = int(os.environ.get("RERANK_TOP_K", "15"))
RISK_SIMILARITY_THRESHOLD = float(os.environ.get("RISK_SIMILARITY_THRESHOLD", "0.82"))

# ── Paths ───────────────────────────────────────────────────────────────────
PDF_INPUT_DIR = os.environ.get("PDF_INPUT_DIR", "./data")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")
