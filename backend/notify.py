"""Alert fan-out worker for Amazon SES or SendGrid and Web Push."""
from __future__ import annotations

import asyncio
import json
import logging
import os

from pywebpush import WebPushException, webpush
from redis.asyncio import Redis
from redis.exceptions import ResponseError
import httpx

from .worker import REDIS_URL
from .mailer import send_email as send_transactional_email
from .telegram_api import call as telegram_call
from types import SimpleNamespace

app = SimpleNamespace(state=SimpleNamespace(db=None))

log = logging.getLogger("price-notifier")
STREAM, GROUP, CONSUMER = "alerts:detected", "alert-deliveries", os.getenv("NOTIFIER_ID", "notifier-1")


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
    async with pool.acquire() as conn:
        emails = await conn.fetch("SELECT email FROM alert_subscribers WHERE enabled")
        pushes = await conn.fetch("SELECT endpoint, subscription FROM push_subscriptions WHERE enabled")
        telegram_chats = await conn.fetch("SELECT chat_id FROM telegram_chats WHERE enabled")
    for row in emails:
        destination = row["email"]
        already = await pool.fetchval("SELECT 1 FROM alert_deliveries WHERE alert_id=$1 AND channel='email' AND destination=$2", stream_id, destination)
        if already:
            continue
        await send_email(destination, item)
        await pool.execute("INSERT INTO alert_deliveries(alert_id,channel,destination) VALUES($1,'email',$2) ON CONFLICT DO NOTHING", stream_id, destination)
    vapid_private = os.getenv("VAPID_PRIVATE_KEY")
    if vapid_private:
        claims = {"sub": os.environ.get("VAPID_SUBJECT", "mailto:alerts@example.com")}
        payload = json.dumps({"title": "Preço fora do padrão", "body": f"{item['store']}: R$ {item['price']}", "url": "/"})
        for row in pushes:
            endpoint = row["endpoint"]
            already = await pool.fetchval("SELECT 1 FROM alert_deliveries WHERE alert_id=$1 AND channel='push' AND destination=$2", stream_id, endpoint)
            if already:
                continue
            try:
                await asyncio.to_thread(webpush, subscription_info=json.loads(row["subscription"]), data=payload, vapid_private_key=vapid_private, vapid_claims=claims, timeout=4)
                await pool.execute("INSERT INTO alert_deliveries(alert_id,channel,destination) VALUES($1,'push',$2) ON CONFLICT DO NOTHING", stream_id, endpoint)
            except WebPushException as exc:
                if getattr(exc.response, "status_code", None) in (404, 410):
                    await pool.execute("UPDATE push_subscriptions SET enabled=false WHERE endpoint=$1", endpoint)
                else:
                    raise
    if telegram_chats and not os.getenv("TELEGRAM_BOT_TOKEN"):
        raise RuntimeError("Telegram chats are enabled but TELEGRAM_BOT_TOKEN is missing")
    for row in telegram_chats:
        destination = str(row["chat_id"])
        already = await pool.fetchval("SELECT 1 FROM alert_deliveries WHERE alert_id=$1 AND channel='telegram' AND destination=$2", stream_id, destination)
        if already:
            continue
        await send_telegram(int(destination), item)
        await pool.execute("INSERT INTO alert_deliveries(alert_id,channel,destination) VALUES($1,'telegram',$2) ON CONFLICT DO NOTHING", stream_id, destination)


async def run() -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        try:
            await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        while True:
            batches = await redis.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=20, block=1000)
            for _, messages in batches:
                for stream_id, fields in messages:
                    try:
                        await deliver(redis, stream_id, json.loads(fields["data"]))
                        await redis.xack(STREAM, GROUP, stream_id)
                    except Exception:
                        log.exception("alert delivery failed id=%s; pending for retry", stream_id)
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
