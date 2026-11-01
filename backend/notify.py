"""Alert fan-out worker for Amazon SES or SendGrid and Web Push."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from pywebpush import WebPushException, webpush
from redis.asyncio import Redis
from redis.exceptions import ResponseError
import httpx

from .worker import REDIS_URL
from .mailer import send_email as send_transactional_email
from .telegram_api import call as telegram_call
from .streams import reclaim_and_process
from types import SimpleNamespace

app = SimpleNamespace(state=SimpleNamespace(db=None))

log = logging.getLogger("price-notifier")
STREAM, GROUP, CONSUMER = "alerts:detected", "alert-deliveries", os.getenv("NOTIFIER_ID", "notifier-1")
DELIVERY_CONCURRENCY = max(1, int(os.getenv("DELIVERY_CONCURRENCY", "10")))


async def send_email(email: str, item: dict) -> None:
    subject = f"Oferta detectada: {item['store']}"
    body = f"Preço: R$ {item['price']}\nProduto: {item.get('title', item['product_id'])}\nLoja: {item['store']}\n{item['url']}"
    await send_transactional_email(email, subject, body)


async def send_telegram(chat_id: int, item: dict) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não configurado")
    text = f"Preço fora do padrão\n{item.get('title', item['product_id'])}\n{item['store']} · R$ {item['price']}\n{item['url']}"
    async with httpx.AsyncClient(timeout=4) as client:
        await telegram_call(client, token, "sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": False})


async def deliver(redis: Redis, stream_id: str, item: dict) -> None:
    pool = app.state.db
    delivery_id = str(item.get("outbox_id") or stream_id)
    async with pool.acquire() as conn:
        emails = await conn.fetch("SELECT email FROM alert_subscribers WHERE enabled")
        pushes = await conn.fetch("SELECT endpoint, subscription FROM push_subscriptions WHERE enabled")
        telegram_chats = await conn.fetch("SELECT chat_id FROM telegram_chats WHERE enabled")
    tasks: list[tuple[str, str, Any]] = []
    for row in emails:
        destination = row["email"]
        tasks.append(("email", destination, lambda destination=destination: send_email(destination, item)))
    vapid_private = os.getenv("VAPID_PRIVATE_KEY")
    if vapid_private:
        claims = {"sub": os.environ.get("VAPID_SUBJECT", "mailto:alerts@example.com")}
        payload = json.dumps({"title": "Preço fora do padrão", "body": f"{item['store']}: R$ {item['price']}", "url": "/"})
        for row in pushes:
            endpoint = row["endpoint"]
            async def send_push(row=row, endpoint=endpoint):
                try:
                    await asyncio.to_thread(
                        webpush,
                        subscription_info=json.loads(row["subscription"]),
                        data=payload,
                        vapid_private_key=vapid_private,
                        vapid_claims=claims,
                        timeout=4,
                    )
                except WebPushException as exc:
                    if getattr(exc.response, "status_code", None) not in (404, 410):
                        raise
                    await pool.execute("UPDATE push_subscriptions SET enabled=false WHERE endpoint=$1", endpoint)
            tasks.append(("push", endpoint, send_push))
    if telegram_chats and not os.getenv("TELEGRAM_BOT_TOKEN"):
        raise RuntimeError("Telegram chats are enabled but TELEGRAM_BOT_TOKEN is missing")
    for row in telegram_chats:
        destination = str(row["chat_id"])
        tasks.append(("telegram", destination, lambda destination=destination: send_telegram(int(destination), item)))

    semaphore = asyncio.Semaphore(DELIVERY_CONCURRENCY)

    async def send_once(channel: str, destination: str, send: Any) -> bool:
        already = await pool.fetchval(
            "SELECT 1 FROM alert_deliveries WHERE alert_id=$1 AND channel=$2 AND destination=$3",
            delivery_id, channel, destination,
        )
        if already:
            return True
        try:
            async with semaphore:
                await send()
        except Exception as exc:
            log.warning("delivery failed channel=%s error=%s", channel, type(exc).__name__)
            return False
        await pool.execute(
            "INSERT INTO alert_deliveries(alert_id,channel,destination) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            delivery_id, channel, destination,
        )
        return True

    results = await asyncio.gather(
        *(send_once(channel, destination, send) for channel, destination, send in tasks),
        return_exceptions=True,
    )
    if not all(result is True for result in results):
        raise RuntimeError("One or more alert destinations failed")


async def run() -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        try:
            await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        last_reclaim = asyncio.get_running_loop().time()
        while True:
            batches = await redis.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=20, block=1000)
            for _, messages in batches:
                for stream_id, fields in messages:
                    try:
                        await deliver(redis, stream_id, json.loads(fields["data"]))
                        await redis.xack(STREAM, GROUP, stream_id)
                    except Exception:
                        log.warning("alert delivery failed id=%s; pending for retry", stream_id)
            if asyncio.get_running_loop().time() - last_reclaim >= 30:
                await reclaim_and_process(
                    redis, STREAM, GROUP, CONSUMER,
                    lambda message_id, fields: deliver(redis, message_id, json.loads(fields["data"])),
                    min_idle_ms=int(os.getenv("RECLAIM_IDLE_MS", "60000")),
                    max_deliveries=int(os.getenv("MAX_DELIVERIES", "5")),
                    dead_stream="alerts:detected:dead",
                )
                last_reclaim = asyncio.get_running_loop().time()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    import asyncpg
    async def main() -> None:
        app.state.db = await asyncpg.create_pool(os.environ["DATABASE_URL"])
        try:
            await run()
        finally:
            await app.state.db.close()
    asyncio.run(main())
