"""Helpers for retrying Redis Stream pending entries and dead-lettering poison messages."""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger("stream-recovery")


async def reclaim_and_process(
    redis: Any,
    stream: str,
    group: str,
    consumer: str,
    handler: Callable[[str, dict[str, Any]], Awaitable[None]],
    *,
    min_idle_ms: int,
    max_deliveries: int,
    dead_stream: str,
) -> None:
    """Claim idle PEL entries, processing them or moving poison entries to a DLQ."""
    cursor: str | bytes = "0-0"
    while True:
        result = await redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=min_idle_ms,
            start_id=cursor,
            count=20,
        )
        cursor, messages = result[0], result[1]
        for message_id, fields in messages:
            info = await redis.xpending_range(stream, group, min=message_id, max=message_id, count=1)
            deliveries = int(info[0]["times_delivered"]) if info else 1
            if deliveries > max_deliveries:
                await redis.xadd(dead_stream, {
                    "origin": stream,
                    "id": message_id,
                    "data": fields.get("data", ""),
                })
                await redis.xack(stream, group, message_id)
                log.error("message moved to dead-letter stream=%s id=%s", stream, message_id)
                continue
            try:
                # The supplied handler acknowledges only after successful processing.
                await handler(message_id, fields)
            except Exception:
                log.exception("reclaimed message failed stream=%s id=%s", stream, message_id)
        if cursor in ("0-0", b"0-0"):
            break
