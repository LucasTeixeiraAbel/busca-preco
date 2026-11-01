import asyncio
import hashlib
from contextlib import asynccontextmanager

from backend import telegram_bot


def test_telegram_start_consumes_one_time_token_and_links_private_chat(monkeypatch):
    start_token = "x" * 32
    expected_hash = hashlib.sha256(start_token.encode()).hexdigest()
    sent = []

    class Connection:
        consumed = False

        @asynccontextmanager
        async def transaction(self):
            yield

        async def fetchval(self, query, *args):
            if "UPDATE telegram_link_tokens" in query:
                assert args[0] == expected_hash
                if self.consumed:
                    return None
                self.consumed = True
                return "person@example.com"
            assert "INSERT INTO telegram_chats" in query
            assert args == ("person@example.com", 12345)
            return 12345

    connection = Connection()

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield connection

    async def fake_call(_client, _token, method, payload):
        sent.append((method, payload))
        return {}

    monkeypatch.setattr(telegram_bot, "call", fake_call)
    message = {"chat": {"id": 12345, "type": "private"}, "text": f"/start {start_token}"}
    asyncio.run(telegram_bot.handle_message(object(), Pool(), "secret", message))

    assert sent[0][0] == "sendMessage"
    assert "Telegram conectado" in sent[0][1]["text"]


def test_telegram_stop_disables_linked_chat(monkeypatch):
    state = {}

    class Pool:
        async def execute(self, query, chat_id):
            state["query"] = query
            state["chat_id"] = chat_id

    async def fake_call(_client, _token, _method, payload):
        state["message"] = payload["text"]

    monkeypatch.setattr(telegram_bot, "call", fake_call)
    message = {"chat": {"id": 55, "type": "private"}, "text": "/stop"}
    asyncio.run(telegram_bot.handle_message(object(), Pool(), "secret", message))

    assert "UPDATE telegram_chats SET enabled=FALSE" in state["query"]
    assert state["chat_id"] == 55
    assert "desativados" in state["message"]
