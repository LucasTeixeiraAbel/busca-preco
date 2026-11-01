import asyncio

from backend.streams import reclaim_and_process


class PendingRedis:
    def __init__(self, deliveries=1):
        self.deliveries = deliveries
        self.acked = []
        self.dead = []
        self.claimed = False

    async def xautoclaim(self, _stream, _group, _consumer, **_kwargs):
        if self.claimed:
            return ("0-0", [], None)
        self.claimed = True
        return ("0-0", [("1-0", {"data": "payload"})], None)

    async def xpending_range(self, *_args, **_kwargs):
        return [{"times_delivered": self.deliveries}]

    async def xadd(self, stream, fields):
        self.dead.append((stream, fields))

    async def xack(self, stream, group, message_id):
        self.acked.append((stream, group, message_id))


def test_reclaim_retries_a_message_on_a_later_pass():
    redis = PendingRedis()
    attempts = 0

    async def handler(_message_id, _fields):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary failure")
        await redis.xack("source", "group", "1-0")

    async def run():
        await reclaim_and_process(redis, "source", "group", "consumer", handler,
                                  min_idle_ms=1, max_deliveries=5, dead_stream="dead")
        redis.claimed = False
        await reclaim_and_process(redis, "source", "group", "consumer", handler,
                                  min_idle_ms=1, max_deliveries=5, dead_stream="dead")

    asyncio.run(run())
    assert attempts == 2
    assert redis.acked == [("source", "group", "1-0")]
    assert not redis.dead


def test_reclaim_dead_letters_and_acks_a_poison_message():
    redis = PendingRedis(deliveries=6)
    attempts = 0

    async def handler(_message_id, _fields):
        nonlocal attempts
        attempts += 1

    asyncio.run(reclaim_and_process(redis, "source", "group", "consumer", handler,
                                    min_idle_ms=1, max_deliveries=5, dead_stream="dead"))
    assert attempts == 0
    assert redis.dead == [("dead", {"origin": "source", "id": "1-0", "data": "payload"})]
    assert redis.acked == [("source", "group", "1-0")]
