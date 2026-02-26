"""
API request/response models for FastAPI.
"""

from pydantic import BaseModel, Field
from typing import Optional


class QueryRequest(BaseModel):
    query: str
    companies: list[str] = Field(default_factory=list)
    years: list[int] = Field(default_factory=list)


class IngestResponse(BaseModel):
    doc_id: str
    chunks_created: int
    facts_created: int
    entities_created: int
    risk_factors_created: int
    ingest_duration_seconds: float


class CorpusResponse(BaseModel):
    documents: list[dict] = Field(default_factory=list)
