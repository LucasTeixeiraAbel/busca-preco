import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from backend import notify, worker


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _DbConnection:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self.history = [
            {"price": price, "observed_at": now - timedelta(hours=6, minutes=index)}
            for index, price in enumerate([3499.0, 3499.9] * 10)
        ]
        self.inserted = 0

    async def fetch(self, query, *_args):
        assert "price_observations" in query
        return self.history

    async def fetchval(self, query, *_args):
        if "SELECT title" in query:
            return "[TESTE] PlayStation 5"
        self.inserted += 1
        return self.inserted

    async def execute(self, *_args):
        return "UPDATE 1"


class _DbPool:
    def __init__(self):
        self.connection = _DbConnection()

    def acquire(self):
        return _Acquire(self.connection)


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def lpush(self, key, value):
        self.commands.append(("lpush", key, value))
        return self

    def ltrim(self, key, start, end):
        self.commands.append(("ltrim", key, start, end))
        return self

    def xadd(self, stream, fields):
        self.commands.append(("xadd", stream, fields))
        return self

    def xack(self, stream, group, message_id):
        self.commands.append(("xack", stream, group, message_id))
        return self

    async def execute(self):
        for command in self.commands:
            if command[0] == "lpush":
                self.redis.lists.setdefault(command[1], []).insert(0, command[2])
            elif command[0] == "ltrim":
                self.redis.lists[command[1]] = self.redis.lists[command[1]][command[2]:command[3] + 1]
            elif command[0] == "xadd":
                await self.redis.xadd(command[1], command[2])
            elif command[0] == "xack":
                await self.redis.xack(*command[1:])


class _Redis:
    def __init__(self):
        self.values = {}
        self.streams = {}
        self.lists = {}
        self.acked = []

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        self.values[key] = str(value)

    async def delete(self, key):
        self.values.pop(key, None)

    async def xadd(self, stream, fields):
        entries = self.streams.setdefault(stream, [])
        entries.append((f"{len(entries) + 1}-0", fields))
        return entries[-1][0]

    async def xack(self, stream, group, message_id):
        self.acked.append((stream, group, message_id))

    def pipeline(self, transaction=True):
        return _Pipeline(self)


class _NotifyConnection:
    def __init__(self, pool):
        self.pool = pool

    async def fetch(self, query):
        if "alert_subscribers" in query:
            return [{"email": email} for email in self.pool.emails]
        return []


class _NotifyPool:
    def __init__(self):
        self.emails = ["first@example.com", "second@example.com", "third@example.com"]
        self.delivered = set()

    def acquire(self):
        return _Acquire(_NotifyConnection(self))

    async def fetchval(self, _query, alert_id, channel, destination):
        return 1 if (alert_id, channel, destination) in self.delivered else None

    async def execute(self, _query, alert_id, channel, destination):
        self.delivered.add((alert_id, channel, destination))


def test_dry_run_anomaly_confirmation_cooldown_and_partial_delivery(monkeypatch):
    redis = _Redis()
    monkeypatch.setattr(worker, "db", _DbPool())
    monkeypatch.setattr(notify.app.state, "db", _NotifyPool())
    monkeypatch.delenv("VAPID_PRIVATE_KEY", raising=False)
    item = {
        "product_id": "dry-run-ps5",
        "store": "Amazon",
        "price": 2000,
        "url": "https://www.amazon.com.br/dp/example",
    }

    async def process_three_reads():
        for index in range(1, 4):
            await worker.process_message(redis, f"{index}-0", {"data": json.dumps(item)})

    asyncio.run(process_three_reads())
    alerts = redis.streams.get("alerts:detected", [])
    assert len(alerts) == 1
    alert_id, alert_fields = alerts[0]
    alert = json.loads(alert_fields["data"])
    assert alert["confirmed"] is True and alert["anomaly"] is True
    assert alert["title"].startswith("[TESTE]")

    attempts = []
    fail_second_once = True

    async def send_email(email, _alert):
        nonlocal fail_second_once
        attempts.append(email)
        if email == "second@example.com" and fail_second_once:
            raise RuntimeError("simulated provider failure")

    monkeypatch.setattr(notify, "send_email", send_email)
    pool = notify.app.state.db

    with pytest.raises(RuntimeError, match="destinations failed"):
        asyncio.run(notify.deliver(redis, alert_id, alert))
    assert set(attempts) == set(pool.emails)

    fail_second_once = False
    asyncio.run(notify.deliver(redis, alert_id, alert))
    assert attempts.count("first@example.com") == 1
    assert attempts.count("third@example.com") == 1
    assert attempts.count("second@example.com") == 2
    assert len(redis.streams["alerts:detected"]) == 1
