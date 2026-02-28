"""
OpenRouter LLM client — OpenAI-compatible API.
"""

import asyncio
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, LLM_MODEL, LLM_MAX_TOKENS, LLM_TIMEOUT_SECS

# max_retries=0: disable SDK-level retries; we handle retries in validator.py.
# transport timeout slightly above LLM_TIMEOUT_SECS so asyncio.wait_for fires first.
client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
    timeout=LLM_TIMEOUT_SECS + 30.0,
    max_retries=0,
)


async def complete(prompt: str, system: str | None = None, max_tokens: int | None = None) -> str:
    """Call the LLM.  Pass max_tokens to override the default (LLM_MAX_TOKENS)."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            temperature=0.1,
            max_tokens=max_tokens if max_tokens is not None else LLM_MAX_TOKENS,
            response_format={"type": "json_object"},
        ),
        timeout=LLM_TIMEOUT_SECS,
    )
    return response.choices[0].message.content
