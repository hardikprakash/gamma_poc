"""
Pydantic validation wrapper for LLM calls — single retry on failure.
"""

import json
import logging
import time
from pydantic import BaseModel, ValidationError
from llm import openrouter_client

logger = logging.getLogger(__name__)


class LLMValidationError(Exception):
    pass


async def validated_llm_call(
    prompt: str,
    schema: type[BaseModel],
    system: str | None = None,
    *,
    is_list: bool = False,
) -> BaseModel | list:
    """
    Call LLM, parse & validate output with Pydantic.
    One retry on validation failure.
    If is_list=True, parse as JSON list and validate each element against schema.
    """
    current_prompt = prompt
    for attempt in range(2):
        label = f"{schema.__name__} attempt={attempt + 1}"
        logger.debug(f"[LLM] → {label}: sending request")
        t0 = time.monotonic()
        raw = await openrouter_client.complete(current_prompt, system=system)
        elapsed = time.monotonic() - t0
        logger.debug(f"[LLM] ← {label}: response in {elapsed:.1f}s ({len(raw)} chars)")
        if elapsed > 30:
            logger.warning(f"[LLM] SLOW response: {label} took {elapsed:.1f}s")
        try:
            text = raw.strip()
            # Strip markdown fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()

            if is_list:
                data = json.loads(text)
                # Handle both raw list and wrapped {"items": [...]} patterns
                if isinstance(data, dict):
                    # Find the first list value
                    for v in data.values():
                        if isinstance(v, list):
                            data = v
                            break
                if not isinstance(data, list):
                    data = [data]
                return [schema.model_validate(item) for item in data]
            else:
                return schema.model_validate_json(text)
        except (ValidationError, json.JSONDecodeError) as e:
            # Classify the failure to help debugging:
            # - Short response (<50 chars): OpenRouter routing/metadata leak
            # - Ends abruptly mid-JSON: token limit truncation
            # - Other: malformed model output
            raw_snippet = repr(text[:120]) if len(text) <= 120 else f"{repr(text[:60])}...({len(text)} chars)...{repr(text[-40:])}"
            if attempt == 0:
                logger.warning(f"[LLM] Validation error on {label}: {e} — raw: {raw_snippet} — retrying")
                current_prompt = (
                    prompt
                    + f"\n\nPREVIOUS ATTEMPT FAILED: {e}\n"
                    "Return ONLY valid JSON, no prose, no fences."
                )
                continue
            raise LLMValidationError(f"LLM output invalid after retry: {e}")
