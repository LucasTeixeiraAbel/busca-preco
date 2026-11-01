import asyncio
import json
from contextlib import asynccontextmanager

from backend.scheduler import dispatch_due_products


def test_scheduler_enqueues_due_product_and_advances_schedule():
    enqueued = []
    updates = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            yield

        async def fetch(self, _query, limit):
            assert limit > 0
            return [{"offer_id": 7, "product_id": "ssd-1tb", "url": "https://www.kabum.com.br/produto/123", "interval_minutes": 30}]

        async def execute(self, query, *args):
            updates.append((query, args))

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield Connection()

    class Redis:
        async def xadd(self, stream, fields):
            enqueued.append((stream, json.loads(fields["data"])))
            return "1-0"

    count = asyncio.run(dispatch_due_products(Pool(), Redis()))

    assert count == 1
    assert enqueued == [("scrape:jobs", {"product_id": "ssd-1tb", "url": "https://www.kabum.com.br/produto/123"})]
    assert updates[0][1] == (7, 30)


def test_scheduler_dispatches_each_store_offer_independently():
    enqueued = []

    class Connection:
        @asynccontextmanager
        async def transaction(self):
            yield

        async def fetch(self, _query, _limit):
            return [
                {"offer_id": 1, "product_id": "ps5", "url": "https://www.kabum.com.br/produto/1", "interval_minutes": 60},
                {"offer_id": 2, "product_id": "ps5", "url": "https://secure3.fastshop.com.br/console/p", "interval_minutes": 60},
            ]

        async def execute(self, _query, *_args):
            pass

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield Connection()

    class Redis:
        async def xadd(self, stream, fields):
            enqueued.append((stream, json.loads(fields["data"])))

    count = asyncio.run(dispatch_due_products(Pool(), Redis()))
    assert count == 2
    assert len({job[1]["product_id"] for job in enqueued}) == 1
    assert {job[1]["url"] for job in enqueued} == {
        "https://www.kabum.com.br/produto/1",
        "https://secure3.fastshop.com.br/console/p",
    }
