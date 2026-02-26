"""
Ollama embedding client — OpenAI-compatible /v1/embeddings endpoint.
"""

from openai import AsyncOpenAI
from config import OLLAMA_BASE_URL, EMBEDDING_MODEL

embed_client = AsyncOpenAI(
    api_key="ollama",  # Ollama doesn't check the key but the field is required
    base_url=OLLAMA_BASE_URL + "/v1",
)


async def embed(text: str) -> list[float]:
    response = await embed_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text,
    )
    return response.data[0].embedding


async def embed_batch(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    response = await embed_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )
    return [item.embedding for item in response.data]
