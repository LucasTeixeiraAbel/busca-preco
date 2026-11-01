import asyncio

import pytest

from backend import notify


class FakePool:
    def __init__(self):
        self.delivered = set()

    def acquire(self):
        return _Acquire(self)

    async def fetchval(self, _query, alert_id, channel, destination):
        return 1 if (alert_id, channel, destination) in self.delivered else None

    async def execute(self, _query, alert_id, channel, destination):
        self.delivered.add((alert_id, channel, destination))


class _Acquire:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def fetch(self, query):
        if "alert_subscribers" in query:
            return [{"email": "first@example.com"}, {"email": "second@example.com"}, {"email": "third@example.com"}]
        return []


class FakeRedis:
    pass


def test_failed_recipient_does_not_block_others_and_retry_skips_successes(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(notify.app.state, "db", pool)
    monkeypatch.delenv("VAPID_PRIVATE_KEY", raising=False)
    attempts = []
    second_failed = True

    async def send(email, _item):
        nonlocal second_failed
        attempts.append(email)
        if email == "second@example.com" and second_failed:
            raise RuntimeError("mail provider error")

    monkeypatch.setattr(notify, "send_email", send)
    item = {"product_id": "console", "store": "Amazon", "price": 1, "url": "https://www.amazon.com.br/dp/x"}

    with pytest.raises(RuntimeError, match="destinations failed"):
        asyncio.run(notify.deliver(FakeRedis(), "1-0", item))
    assert set(attempts) == {"first@example.com", "second@example.com", "third@example.com"}
    assert pool.delivered == {
        ("1-0", "email", "first@example.com"),
        ("1-0", "email", "third@example.com"),
    }

    second_failed = False
    asyncio.run(notify.deliver(FakeRedis(), "1-0", item))
    assert attempts.count("first@example.com") == 1
    assert attempts.count("third@example.com") == 1
    assert attempts.count("second@example.com") == 2
