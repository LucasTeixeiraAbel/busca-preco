"""Dispatch due tracked products into the scrape Redis Stream."""
from __future__ import annotations

import asyncio
import json
import logging
import os

import asyncpg
from dotenv import load_dotenv
from redis.asyncio import Redis

from .discovery import discover_due
from .worker import REDIS_URL

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("product-scheduler")
BATCH_SIZE = int(os.getenv("SCHEDULER_BATCH_SIZE", "50"))
POLL_SECONDS = float(os.getenv("SCHEDULER_POLL_SECONDS", "5"))
DISCOVERY_ENABLED = os.getenv("DISCOVERY_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


async def dispatch_due_products(pool: asyncpg.Pool, redis: Redis) -> int:
    count = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT o.offer_id,o.product_id,o.url,o.interval_minutes FROM product_offers o "
                "JOIN tracked_products p USING(product_id) "
                "WHERE o.enabled=TRUE AND p.enabled=TRUE AND o.next_check_at<=now() "
                "ORDER BY o.next_check_at LIMIT $1 FOR UPDATE OF o SKIP LOCKED",
                BATCH_SIZE,
            )
            for row in rows:
                job = {"product_id": row["product_id"], "url": row["url"]}
                await redis.xadd("scrape:jobs", {"data": json.dumps(job)})
                await conn.execute(
                    "UPDATE product_offers SET next_check_at=now()+($2 * interval '1 minute'), "
                    "last_queued_at=now(),updated_at=now() WHERE offer_id=$1",
                    row["offer_id"], row["interval_minutes"],
                )
                count += 1
    return count


async def run() -> None:
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=4)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
        while True:
            try:
                count = await dispatch_due_products(pool, redis)
                if count:
                    log.info("queued %s due product checks", count)
                if DISCOVERY_ENABLED:
                    discovered = await discover_due(pool)
                    if discovered:
                        log.info("automatically enrolled %s product offers", discovered)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("product scheduler iteration failed")
            await asyncio.sleep(POLL_SECONDS)
    finally:
        await redis.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(run())
