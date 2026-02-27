"""
OpenRouter LLM client — OpenAI-compatible API.
"""

import asyncio
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL, LLM_MODEL, LLM_MAX_TOKENS

# max_retries=0: disable SDK-level retries; we handle retries in validator.py.
# timeout=120: per-socket read timeout; asyncio.wait_for in complete() enforces hard total cap.
client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url=OPENROUTER_BASE_URL,
    timeout=120.0,
    max_retries=0,
)

_TOTAL_TIMEOUT_SECS = 90  # hard wall-clock cap per LLM call


async def complete(prompt: str, system: str | None = None) -> str:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    response = await asyncio.wait_for(
        client.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            temperature=0.1,
            max_tokens=LLM_MAX_TOKENS,
            response_format={"type": "json_object"},
        ),
        timeout=_TOTAL_TIMEOUT_SECS,
    )
    return response.choices[0].message.content
