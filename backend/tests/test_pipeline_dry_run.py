import asyncio
import json
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

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
        self.outbox = []
        self.fail_outbox_mark = False

    async def fetch(self, query, *_args):
        if "alerts_outbox" in query:
            return [row for row in self.outbox if row["published_at"] is None]
        assert "price_observations" in query
        return self.history

    async def fetchval(self, query, *_args):
        if "SELECT title" in query:
            return "[TESTE] PlayStation 5"
        if "pg_advisory_xact_lock" in query:
            return None
        if "SELECT 1 FROM alerts_outbox" in query:
            return 1 if any(row["published_at"] is None for row in self.outbox) else None
        self.inserted += 1
        return self.inserted

    async def execute(self, query, *args):
        if "INSERT INTO alerts_outbox" in query:
            self.outbox.append({
                "id": len(self.outbox) + 1,
                "product_id": args[0],
                "store": args[1],
                "offer_key": args[2],
                "payload": json.loads(args[3]),
                "published_at": None,
            })
        elif "UPDATE alerts_outbox" in query:
            if self.fail_outbox_mark:
                self.fail_outbox_mark = False
                raise RuntimeError("simulated PostgreSQL commit failure")
            for row in self.outbox:
                if row["id"] == args[0]:
                    row["published_at"] = datetime.now(timezone.utc)
        return "UPDATE 1"

    @asynccontextmanager
    async def transaction(self):
        yield


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
        results = []
        for index in range(1, 4):
            results.append(await worker.process_message(redis, f"{index}-0", {"data": json.dumps(item)}))
        return results

    assert asyncio.run(process_three_reads()) == [False, True, False]
    assert len(worker.db.connection.outbox) == 1
    assert not redis.streams.get("alerts:detected")
    asyncio.run(worker.publish_outbox(worker.db, redis))
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


def test_outbox_republish_uses_stable_delivery_id_after_partial_publish(monkeypatch):
    pool = _DbPool()
    pool.connection.outbox.append({
        "id": 44,
        "product_id": "dry-run-ps5",
        "store": "Amazon",
        "offer_key": "offer-hash",
        "payload": {"product_id": "dry-run-ps5", "store": "Amazon", "price": 2000, "url": "https://www.amazon.com.br/dp/example"},
        "published_at": None,
    })
    pool.connection.fail_outbox_mark = True
    redis = _Redis()

    with pytest.raises(RuntimeError, match="commit failure"):
        asyncio.run(worker.publish_outbox(pool, redis))
    asyncio.run(worker.publish_outbox(pool, redis))
    entries = redis.streams["alerts:detected"]
    assert len(entries) == 2
    assert {json.loads(fields["data"])["outbox_id"] for _, fields in entries} == {"44"}

    notifier_pool = _NotifyPool()
    monkeypatch.setattr(notify.app.state, "db", notifier_pool)
    monkeypatch.delenv("VAPID_PRIVATE_KEY", raising=False)
    attempts = []

    async def send_email(email, _alert):
        attempts.append(email)

    monkeypatch.setattr(notify, "send_email", send_email)
    alert = json.loads(entries[0][1]["data"])
    asyncio.run(notify.deliver(redis, entries[0][0], alert))
    asyncio.run(notify.deliver(redis, entries[1][0], json.loads(entries[1][1]["data"])))
    assert len(attempts) == len(notifier_pool.emails)
    assert len(set(attempts)) == len(notifier_pool.emails)
