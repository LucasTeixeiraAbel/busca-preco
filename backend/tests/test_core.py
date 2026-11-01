import pytest
import asyncio
import jwt
import hashlib
from contextlib import asynccontextmanager
from fastapi.security import HTTPAuthorizationCredentials

from backend.scraper import parse_price
from fastapi import HTTPException

import backend.worker as worker
from backend.worker import (
    MagicLinkVerification,
    Observation,
    ProductCreate,
    authorize_admin,
    current_user,
    detect_anomaly,
    has_minimum_history_coverage,
    issue_user_token,
    make_telegram_deep_link,
    load_store_history,
    should_alert,
    clear_pending_anomaly,
    ScrapeJob,
    store_label_for_url,
    validate_push_endpoint,
    validate_store_url,
    verify_magic_link,
)


def test_detects_large_downward_price_deviation():
    history = [100.0, 101.0, 99.0, 100.0, 102.0, 98.0, 100.0, 101.0]
    anomalous, score = detect_anomaly(history, 50.0)
    assert anomalous is True
    assert score < -6


def test_does_not_flag_normal_price_movement():
    history = [100.0, 101.0, 99.0, 100.0, 102.0, 98.0, 100.0, 101.0]
    anomalous, _ = detect_anomaly(history, 98.5)
    assert anomalous is False


def test_requires_minimum_history():
    anomalous, score = detect_anomaly([100.0] * 7, 1.0)
    assert (anomalous, score) == (False, 0.0)


def test_constant_price_history_can_detect_material_drop():
    anomalous, score = detect_anomaly([100.0] * 8, 50.0)
    assert anomalous is True
    assert score == -0.5


def test_small_cent_drop_is_not_an_anomaly_when_mad_is_nonzero():
    history = [3499.0, 3499.9] * 10
    anomalous, _ = detect_anomaly(history, 3489.0)
    assert anomalous is False


def test_stable_price_drop_below_relative_floor_is_not_an_anomaly():
    anomalous, _ = detect_anomaly([3499.0] * 20, 3149.0)
    assert anomalous is False


def test_stable_price_drop_above_relative_floor_is_an_anomaly():
    anomalous, _ = detect_anomaly([3499.0] * 20, 2099.0)
    assert anomalous is True


def test_material_drop_is_detected_against_only_the_store_baseline():
    history = [3499.0, 3499.9] * 10
    anomalous, _ = detect_anomaly(history, 2449.0)
    assert anomalous is True


def test_store_history_query_filters_by_product_store_and_time_window():
    class FakeConnection:
        async def fetch(self, query, product_id, store, days):
            assert "product_id=$1 AND store=$2" in query
            assert "interval '1 day'" in query
            assert "LIMIT 2000" in query
            assert (product_id, store, days) == ("console", "Amazon", worker.BASELINE_DAYS)
            return [{"price": 100.0, "observed_at": None}]

    rows = asyncio.run(load_store_history(FakeConnection(), "console", "Amazon"))
    assert rows[0]["price"] == 100.0


def test_history_coverage_requires_minimum_dayspan():
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    six_hour_history = [{"observed_at": now - timedelta(hours=6 + i / 100)} for i in range(8)]
    short_history = [{"observed_at": now - timedelta(hours=5)} for _ in range(8)]
    assert has_minimum_history_coverage(six_hour_history, now)
    assert not has_minimum_history_coverage(short_history, now)


class MemoryRedis:
    def __init__(self):
        self.values = {}
        self.streams = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        self.values[key] = value

    async def delete(self, key):
        self.values.pop(key, None)

    async def xadd(self, stream, fields):
        self.streams.setdefault(stream, []).append(fields)
        return f"{len(self.streams[stream])}-0"


def test_anomaly_requires_confirmation_and_cooldown_suppresses_repeats():
    redis = MemoryRedis()
    observation = Observation(product_id="console", store="Amazon", price=2100, url="https://www.amazon.com.br/dp/example")

    assert asyncio.run(should_alert(redis, observation, str(observation.url))) is False
    assert len(redis.streams["scrape:jobs"]) == 1
    assert asyncio.run(should_alert(redis, observation, str(observation.url))) is True
    assert asyncio.run(should_alert(redis, observation, str(observation.url))) is False
    assert len(redis.streams["scrape:jobs"]) == 1


def test_normal_reading_clears_pending_anomaly_without_alert():
    redis = MemoryRedis()
    observation = Observation(product_id="console", store="Amazon", price=2100, url="https://www.amazon.com.br/dp/example")

    asyncio.run(should_alert(redis, observation, str(observation.url)))
    asyncio.run(clear_pending_anomaly(redis, observation, str(observation.url)))
    assert not redis.values
    assert len(redis.streams["scrape:jobs"]) == 1


@pytest.mark.parametrize("url", [
    "http://www.amazon.com.br/product",
    "https://amazon.com.br.attacker.example/product",
    "https://user:pass@amazon.com.br/product",
    "https://evil.example/product",
])
def test_rejects_unsafe_store_urls(url):
    with pytest.raises(ValueError):
        validate_store_url(url)


def test_accepts_allowlisted_https_store_url():
    url = "https://www.amazon.com.br/dp/example"
    assert validate_store_url(url) == url


def test_scrape_job_does_not_require_a_fake_price_and_store_is_domain_derived():
    job = ScrapeJob(product_id="ssd-1tb", url="https://www.kabum.com.br/produto/123")
    assert job.product_id == "ssd-1tb"
    assert store_label_for_url(str(job.url)) == "KaBuM"


def test_product_catalog_validates_schedule_and_identifier():
    product = ProductCreate(
        product_id="ssd-1tb-kabum",
        title="SSD 1 TB",
        url="https://www.kabum.com.br/produto/123",
        interval_minutes=30,
    )
    assert product.interval_minutes == 30
    with pytest.raises(ValueError):
        ProductCreate(product_id="invalid id", title="SSD", url="https://www.kabum.com.br/produto/123")
    with pytest.raises(ValueError):
        ProductCreate(product_id="ssd", title="SSD", url="https://www.kabum.com.br/produto/123", interval_minutes=1)


def test_product_catalog_accepts_multiple_urls_for_one_product():
    product = ProductCreate(
        product_id="ps5-slim-1tb",
        title="PlayStation 5 Slim 1 TB com leitor",
        urls=[
            "https://www.kabum.com.br/produto/953110/ps5",
            "https://secure3.fastshop.com.br/console-playstation-5-slim-disk-1tb/p",
        ],
    )
    assert len(product.offer_urls) == 2
    with pytest.raises(ValueError):
        ProductCreate(product_id="ps5", title="PS5")


def test_allowlist_supports_additional_verified_retailers():
    assert store_label_for_url("https://secure3.fastshop.com.br/product/p") == "Fast Shop"
    assert store_label_for_url("https://www.magazineluiza.com.br/product/p") == "Magazine Luiza"


def test_push_endpoint_requires_https_and_known_provider():
    assert validate_push_endpoint("https://fcm.googleapis.com/push/example")
    with pytest.raises(ValueError):
        validate_push_endpoint("http://fcm.googleapis.com/push/example")
    with pytest.raises(ValueError):
        validate_push_endpoint("https://attacker.example/push")


def test_admin_auth_rejects_missing_weak_and_incorrect_tokens(monkeypatch):
    monkeypatch.setenv("ALERT_ADMIN_TOKEN", "x" * 40)
    with pytest.raises(HTTPException) as missing:
        authorize_admin(None)
    assert missing.value.status_code == 401
    with pytest.raises(HTTPException) as wrong:
        authorize_admin("Bearer " + "y" * 40)
    assert wrong.value.status_code == 401
    authorize_admin("Bearer " + "x" * 40)


def test_admin_auth_fails_closed_when_secret_is_weak(monkeypatch):
    monkeypatch.setenv("ALERT_ADMIN_TOKEN", "short")
    with pytest.raises(HTTPException) as failure:
        authorize_admin("Bearer short")
    assert failure.value.status_code == 503


def test_user_session_is_signed_and_requires_verified_account(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", "s" * 48)
    monkeypatch.setenv("AUTH_JWT_ISSUER", "test-radar")

    class VerifiedUserDb:
        async def fetchval(self, *_args):
            return 1

    monkeypatch.setattr(worker, "db", VerifiedUserDb())
    token = issue_user_token("person@example.com")
    claims = jwt.decode(token, "s" * 48, algorithms=["HS256"], issuer="test-radar")
    assert claims["sub"] == "person@example.com"
    principal = asyncio.run(current_user(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)))
    assert principal == "person@example.com"


def test_user_session_rejects_expired_token(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", "s" * 48)
    token = jwt.encode({"sub": "person@example.com", "iss": "radar-ofertas", "exp": 1}, "s" * 48, algorithm="HS256")
    with pytest.raises(HTTPException) as failure:
        asyncio.run(current_user(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)))
    assert failure.value.status_code == 401


def test_user_can_star_and_unstar_a_tracked_product(monkeypatch):
    saved = set()

    class FakeDb:
        async def fetchval(self, query, *args):
            if "FROM tracked_products" in query:
                return 1
            return 1 if (args[0], args[1]) in saved else None

        async def execute(self, query, *args):
            if query.startswith("INSERT INTO user_product_stars"):
                saved.add((args[0], args[1]))
            elif query.startswith("DELETE FROM user_product_stars"):
                saved.discard((args[0], args[1]))

    async def fake_current_user(_credentials):
        return "person@example.com"

    monkeypatch.setattr(worker, "current_user", fake_current_user)
    monkeypatch.setattr(worker, "db", FakeDb())
    asyncio.run(worker.star_product("ps5", None))
    assert ("person@example.com", "ps5") in saved
    asyncio.run(worker.unstar_product("ps5", None))
    assert ("person@example.com", "ps5") not in saved


def test_product_history_restricts_periods_and_returns_store_series(monkeypatch):
    from datetime import date, datetime, timezone

    class FakeDb:
        async def fetchval(self, query, *_args):
            if "user_product_stars" in query:
                return 1
            return datetime(2026, 1, 1, tzinfo=timezone.utc)

        async def fetch(self, _query, *_args):
            return [{"day": date(2026, 1, 1), "store": "KaBuM", "low_price": 100.0, "high_price": 120.0, "average_price": 110.0, "closing_price": 105.0}]

    async def fake_current_user(_credentials):
        return "person@example.com"

    monkeypatch.setattr(worker, "current_user", fake_current_user)
    monkeypatch.setattr(worker, "db", FakeDb())
    result = asyncio.run(worker.get_product_history("ps5", 1, None))
    assert result["years"] == 1
    assert result["points"][0]["store"] == "KaBuM"
    with pytest.raises(HTTPException) as failure:
        asyncio.run(worker.get_product_history("ps5", 4, None))
    assert failure.value.status_code == 422


def test_magic_link_is_one_time_and_returns_session(monkeypatch):
    monkeypatch.setenv("AUTH_JWT_SECRET", "m" * 48)
    raw_token = "one-time-login-token-value-which-is-long-enough"
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

    class FakeConnection:
        consumed = False

        @asynccontextmanager
        async def transaction(self):
            yield

        async def fetchval(self, query, supplied_hash=None):
            if "UPDATE magic_login_tokens" in query:
                if supplied_hash != token_hash or self.consumed:
                    return None
                self.consumed = True
                return "person@example.com"
            return "person@example.com"

    connection = FakeConnection()

    class FakePool:
        @asynccontextmanager
        async def acquire(self):
            yield connection

    monkeypatch.setattr(worker, "db", FakePool())
    result = asyncio.run(verify_magic_link(MagicLinkVerification(token=raw_token)))
    claims = jwt.decode(result["access_token"], "m" * 48, algorithms=["HS256"], issuer="radar-ofertas")
    assert claims["sub"] == "person@example.com"
    with pytest.raises(HTTPException) as failure:
        asyncio.run(verify_magic_link(MagicLinkVerification(token=raw_token)))
    assert failure.value.status_code == 400


def test_telegram_deep_link_contains_token_and_rejects_bad_bot_username():
    assert make_telegram_deep_link("@RadarDealsBot", "abc123") == "https://t.me/RadarDealsBot?start=abc123"
    with pytest.raises(ValueError):
        make_telegram_deep_link("bad username", "abc123")


def test_parses_structured_decimal_price():
    assert parse_price('<meta property="product:price:amount" content="1299.90">') == 1299.9


def test_parses_product_json_ld_and_brazilian_price_format():
    html = '<script type="application/ld+json">{"@type":"Product","offers":{"price":"6780.38","priceCurrency":"BRL"}}</script>'
    assert parse_price(html) == 6780.38
    assert parse_price('<meta itemprop="price" content="R$ 6.780,38">') == 6780.38


def test_parses_amazon_active_offer_price_not_struck_through_price():
    html = '''<span class="a-price"><span class="a-offscreen">R$ 4.749,90</span></span>
    <span class="a-price a-text-price"><span class="a-offscreen">R$ 5.999,90</span></span>'''
    assert parse_price(html) == 4749.90


def test_rejects_missing_or_invalid_structured_price():
    with pytest.raises(ValueError, match="Preço estruturado"):
        parse_price('<html><body>R$ 1.299,90</body></html>')

