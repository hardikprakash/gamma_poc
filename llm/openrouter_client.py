"""
OpenRouter LLM client — OpenAI-compatible API.
"""

from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, LLM_MODEL, LLM_MAX_TOKENS

client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
)


async def complete(prompt: str, system: str | None = None) -> str:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = await client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        temperature=0.1,
        max_tokens=LLM_MAX_TOKENS,
        response_format={"type": "json_object"},
    )
    return response.choices[0].message.content
