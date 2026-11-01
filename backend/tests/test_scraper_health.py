import asyncio

from backend import scraper


def test_scrape_failure_records_short_sanitized_error_and_threshold():
    class FakePool:
        args = None
        query = ""

        async def fetchrow(self, query, *args):
            self.query = query
            self.args = args
            return {"consecutive_failures": 5, "alert_due": True}

    pool = FakePool()
    error = RuntimeError("navigation failed https://example.com/?token=private")
    failures, alert_due = asyncio.run(scraper.record_scrape_failure(pool, "ps5", "https://www.amazon.com.br/dp/item", error))

    assert (failures, alert_due) == (5, True)
    assert "interval '1 day'" in pool.query
    assert pool.args[3] == scraper.SCRAPE_FAIL_ALERT_THRESHOLD
    assert "private" not in pool.args[2]
    assert "[URL]" in pool.args[2]


def test_scrape_success_clears_consecutive_failure_state():
    class FakePool:
        query = ""
        args = None

        async def execute(self, query, *args):
            self.query = query
            self.args = args

    pool = FakePool()
    asyncio.run(scraper.record_scrape_success(pool, "ps5", "https://www.amazon.com.br/dp/item"))
    assert "consecutive_failures=0,last_error=NULL" in pool.query
    assert pool.args == ("ps5", "https://www.amazon.com.br/dp/item")
