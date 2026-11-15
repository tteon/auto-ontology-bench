"""Streaming chat completion against the NVIDIA API catalog, with client-side timing.

Hosted API, so server-side queue/KV-cache metrics are not observable. What is:
TTFT (first reasoning token and first answer token separately -- reasoning
models stream their thinking first), total time, inter-chunk gaps, and the
`usage` block, which on NVIDIA includes `prompt_tokens_details.cached_tokens`
(a per-request prompt-cache signal) and `completion_tokens_details.reasoning_tokens`.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

ENDPOINT = os.environ.get("NVIDIA_ENDPOINT", "https://integrate.api.nvidia.com/v1")
MODEL = os.environ.get("LLM_MODEL", "nvidia/nemotron-3.5-super-vl-120b-a12b")


class RateLimited(Exception):
    pass


async def complete(client: httpx.AsyncClient, messages: list[dict], *, max_tokens: int = 4096) -> dict[str, Any]:
    body = {
        "model": MODEL,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    t0 = time.perf_counter()
    first_any = first_content = None
    gaps: list[float] = []
    last = None
    content: list[str] = []
    reasoning_chars = 0
    usage: dict[str, Any] = {}
    async with client.stream(
        "POST", f"{ENDPOINT}/chat/completions", json=body,
        headers={"Authorization": f"Bearer {os.environ['NVIDIA_API_KEY']}"}, timeout=180,
    ) as r:
        if r.status_code == 429:
            raise RateLimited(await r.aread())
        if r.status_code >= 400:
            raise httpx.HTTPStatusError(f"{r.status_code}: {(await r.aread())[:300]!r}", request=r.request, response=r)
        async for line in r.aiter_lines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            chunk = json.loads(data)
            if chunk.get("usage"):
                usage = chunk["usage"]
            for ch in chunk.get("choices", []):
                delta = ch.get("delta") or {}
                now = time.perf_counter()
                piece = delta.get("content")
                thought = delta.get("reasoning_content") or delta.get("reasoning")
                if piece or thought:
                    if first_any is None:
                        first_any = now
                    if last is not None:
                        gaps.append((now - last) * 1000)
                    last = now
                if thought:
                    reasoning_chars += len(thought)
                if piece:
                    if first_content is None:
                        first_content = now
                    content.append(piece)
    t1 = time.perf_counter()
    gaps.sort()
    return {
        "text": "".join(content),
        "ttft_ms": (first_any - t0) * 1000 if first_any else None,
        "ttft_content_ms": (first_content - t0) * 1000 if first_content else None,
        "total_ms": (t1 - t0) * 1000,
        "itl_p50_ms": gaps[len(gaps) // 2] if gaps else None,
        "itl_p99_ms": gaps[int(len(gaps) * 0.99)] if gaps else None,
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "cached_tokens": (usage.get("prompt_tokens_details") or {}).get("cached_tokens"),
        "reasoning_tokens": (usage.get("completion_tokens_details") or {}).get("reasoning_tokens"),
        "reasoning_chars": reasoning_chars,
        "model": MODEL,
    }
