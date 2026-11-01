"""Long-polling Telegram bot for linking a user's private chat to their account."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from contextlib import suppress

import asyncpg
import httpx
from dotenv import load_dotenv

from .telegram_api import call

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("telegram-link-bot")


async def handle_message(client: httpx.AsyncClient, pool: asyncpg.Pool, token: str, message: dict) -> None:
    chat = message.get("chat", {})
    text = message.get("text", "")
    chat_id = chat.get("id")
    if not chat_id or chat.get("type") != "private":
        return
    if re.match(r"^/stop(?:@\w+)?(?:\s|$)", text):
        await pool.execute("UPDATE telegram_chats SET enabled=FALSE WHERE chat_id=$1", chat_id)
        await call(client, token, "sendMessage", {"chat_id": chat_id, "text": "Alertas do Radar desativados neste chat."})
        return
    match = re.match(r"^/start(?:@\w+)?(?:\s+([A-Za-z0-9_-]{32,64}))?\s*$", text)
    payload = match.group(1) if match else None
    if not payload:
        await call(client, token, "sendMessage", {"chat_id": chat_id, "text": "Para vincular sua conta, gere um link na página de alertas do Radar e abra-o aqui."})
        return

    token_hash = hashlib.sha256(payload.encode()).hexdigest()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                email = await conn.fetchval(
                    "UPDATE telegram_link_tokens SET consumed_at=now() "
                    "WHERE token_hash=$1 AND consumed_at IS NULL AND expires_at>now() RETURNING email",
                    token_hash,
                )
                if not email:
                    raise ValueError("Link de vinculação inválido ou expirado")
                saved = await conn.fetchval(
                    "INSERT INTO telegram_chats(email,chat_id,enabled) VALUES($1,$2,TRUE) "
                    "ON CONFLICT(email) DO UPDATE SET chat_id=EXCLUDED.chat_id,enabled=TRUE,linked_at=now() "
                    "RETURNING chat_id",
                    email, chat_id,
                )
                if not saved:
                    raise ValueError("Chat não pôde ser vinculado")
    except asyncpg.UniqueViolationError:
        await call(client, token, "sendMessage", {"chat_id": chat_id, "text": "Este chat já está vinculado a outra conta do Radar."})
        return
    except ValueError as exc:
        await call(client, token, "sendMessage", {"chat_id": chat_id, "text": str(exc)})
        return
    await call(client, token, "sendMessage", {"chat_id": chat_id, "text": "Telegram conectado. Você receberá alertas de preços por aqui. Envie /stop para desativar."})


async def run() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    database_url = os.getenv("DATABASE_URL", "")
    if not token or not database_url:
        raise RuntimeError("Configure TELEGRAM_BOT_TOKEN e DATABASE_URL")
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=4)
    offset = 0
    timeout = httpx.Timeout(35.0, connect=5.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Refuse to steal updates from a webhook configured by another deployment.
            webhook = await call(client, token, "getWebhookInfo", {})
            if webhook.get("url"):
                raise RuntimeError("Telegram bot has a webhook configured; disable it before starting long polling")
            await call(client, token, "getMe", {})
            log.info("Telegram bot polling started")
            while True:
                try:
                    updates = await call(
                        client, token, "getUpdates",
                        {"offset": offset, "timeout": 25, "allowed_updates": ["message"]},
                    )
                    for update in updates or []:
                        message = update.get("message")
                        if message:
                            await handle_message(client, pool, token, message)
                        offset = max(offset, int(update["update_id"]) + 1)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Telegram polling iteration failed")
                    await asyncio.sleep(2)
    finally:
        await pool.close()


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(run())
