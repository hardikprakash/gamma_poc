"""LLM Client — thin wrapper around OpenAI-compatible API."""

from openai import OpenAI
from app.core.config import settings

client = OpenAI(
    base_url=settings.OPENAI_API_BASE_URL,
    api_key=settings.OPENAI_API_KEY,
)