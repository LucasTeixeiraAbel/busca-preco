"""Allowlisted asynchronous Playwright collector; parses structured price metadata."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import httpx
from urllib.parse import urlsplit

import asyncpg
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .worker import REDIS_URL, store_label_for_url, validate_store_url
from .telegram_api import call as telegram_call

log = logging.getLogger("price-scraper")
JOBS, GROUP, CONSUMER = "scrape:jobs", "scrapers", os.getenv("SCRAPER_ID", "scraper-1")
AMAZON_PRICE_SCOPES = ("#corePriceDisplay_desktop_feature_div", "#corePrice_feature_div", "#apex_desktop")
SCRAPE_FAIL_ALERT_THRESHOLD = int(os.getenv("SCRAPE_FAIL_ALERT_THRESHOLD", "5"))
ADMIN_TELEGRAM_CHAT_ID = os.getenv("ADMIN_TELEGRAM_CHAT_ID", "").strip()


async def guard_navigation(route: object) -> None:
    request = route.request
    if request.is_navigation_request() and request.resource_type == "document":
        try:
            validate_store_url(request.url)
        except ValueError:
            await route.abort()
            return
    await route.continue_()


def parse_price(html: str, store: str | None = None) -> float:
    soup = BeautifulSoup(html, "html.parser")
    def numeric_price(raw: object) -> float | None:
        value = str(raw).replace("\xa0", " ").strip()
        value = re.sub(r"^[^0-9]+", "", value)
        value = re.sub(r"[^0-9,.]+$", "", value)
        if re.fullmatch(r"\d+(?:\.\d{1,2})?", value):
            return float(value)
        if re.fullmatch(r"\d+(?:\.\d{3})*,\d{2}", value):
            return float(value.replace(".", "").replace(",", "."))
        return None

    if store == "Amazon":
        # Amazon can show accessory and recommendation prices before the product price.
        for scope in AMAZON_PRICE_SCOPES:
            for tag in soup.select(f"{scope} span.a-price:not(.a-text-price) span.a-offscreen"):
                price = numeric_price(tag.get_text(" ", strip=True))
                if price and price > 0:
                    return price
    else:
        candidates = [
            soup.find("meta", attrs={"property": "product:price:amount"}),
            soup.find("meta", attrs={"itemprop": "price"}),
            soup.find("meta", attrs={"name": "twitter:data1"}),
        ]
        for tag in candidates:
            if tag and tag.get("content"):
                price = numeric_price(tag["content"])
                if price and price > 0:
                    return price

    def find_product_offer(node: object) -> float | None:
        if isinstance(node, list):
            for child in node:
                price = find_product_offer(child)
                if price:
                    return price
        elif isinstance(node, dict):
            kind = node.get("@type", [])
            if isinstance(kind, str):
                kind = [kind]
            if "Product" in kind:
                offers = node.get("offers", [])
                if isinstance(offers, dict):
                    offers = [offers]
                for offer in offers if isinstance(offers, list) else []:
                    if not isinstance(offer, dict) or offer.get("priceCurrency", "BRL") != "BRL":
                        continue
                    price = numeric_price(offer.get("price", offer.get("lowPrice", "")))
                    if price and price > 0:
                        return price
            for child in node.values():
                price = find_product_offer(child)
                if price:
                    return price
        return None

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            price = find_product_offer(json.loads(script.string or script.get_text()))
            if price:
                return price
        except (json.JSONDecodeError, TypeError):
            continue
    raise ValueError("Preço estruturado não encontrado na página")


def safe_error_summary(exc: Exception) -> str:
    message = str(exc).splitlines()[0][:240]
    message = re.sub(r"https?://[^\s\"'<>]+", "[URL]", message)
    message = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b", "[REDACTED]", message)
    return f"{type(exc).__name__}: {message}"[:300]


async def record_scrape_failure(pool: asyncpg.Pool, product_id: str, url: str, exc: Exception) -> tuple[int, bool]:
    summary = safe_error_summary(exc)
    row = await pool.fetchrow(
        "UPDATE product_offers SET "
        "consecutive_failures=consecutive_failures+1,last_error=$3,last_failure_at=now(), "
        "last_failure_alerted_at=CASE WHEN consecutive_failures+1 >= $4 AND "
        "(last_failure_alerted_at IS NULL OR last_failure_alerted_at < now()-interval '1 day') "
        "THEN now() ELSE last_failure_alerted_at END "
        "WHERE product_id=$1 AND url=$2 "
        "RETURNING consecutive_failures,last_failure_alerted_at=now() AS alert_due",
        product_id, url, summary, SCRAPE_FAIL_ALERT_THRESHOLD,
    )
    if row is None:
        return 0, False
    return int(row["consecutive_failures"]), bool(row["alert_due"])


async def record_scrape_success(pool: asyncpg.Pool, product_id: str, url: str) -> None:
    await pool.execute(
        "UPDATE product_offers SET consecutive_failures=0,last_error=NULL "
        "WHERE product_id=$1 AND url=$2",
        product_id, url,
    )


async def notify_operator_failure(product_id: str, failures: int, error: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not token or not ADMIN_TELEGRAM_CHAT_ID:
        raise RuntimeError("operator Telegram is not configured")
    message = f"Falha de coleta no Radar de Ofertas\nProduto: {product_id}\nFalhas consecutivas: {failures}\nErro: {error}"
    async with httpx.AsyncClient(timeout=4) as client:
        await telegram_call(client, token, "sendMessage", {"chat_id": ADMIN_TELEGRAM_CHAT_ID, "text": message})


async def run() -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    pool: asyncpg.Pool | None = None
    try:
        pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=5)
        try:
            await redis.xgroup_create(JOBS, GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                while True:
                    batches = await redis.xreadgroup(GROUP, CONSUMER, {JOBS: ">"}, count=10, block=1000)
                    for _, messages in batches:
                        for message_id, fields in messages:
                            job: dict | None = None
                            try:
                                job = json.loads(fields["data"])
                                url = validate_store_url(job["url"])
                                page = await browser.new_page()
                                try:
                                    await page.route("**/*", guard_navigation)
                                    response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                                    if not response or response.status >= 400:
                                        raise ValueError("Loja retornou resposta HTTP inválida")
                                    # Redirects must remain on an explicitly trusted HTTPS store.
                                    final_url = validate_store_url(page.url)
                                    price = parse_price(await page.content(), store_label_for_url(final_url))
                                finally:
                                    await page.close()
                                observation = {"product_id": job["product_id"], "store": store_label_for_url(final_url), "price": price, "url": final_url, "source_url": url}
                                await redis.xadd("price:observations", {"data": json.dumps(observation)})
                                await record_scrape_success(pool, job["product_id"], url)
                                await redis.xack(JOBS, GROUP, message_id)
                            except Exception as exc:
                                # The scheduler will enqueue the offer again; do not grow the PEL.
                                error_summary = safe_error_summary(exc)
                                failures = 0
                                alert_due = False
                                if job and job.get("product_id") and job.get("url"):
                                    try:
                                        failures, alert_due = await record_scrape_failure(
                                            pool, str(job["product_id"]), str(job["url"]), exc,
                                        )
                                    except Exception as record_error:
                                        log.error("failed to record scrape failure error=%s", type(record_error).__name__)
                                if failures >= SCRAPE_FAIL_ALERT_THRESHOLD and alert_due:
                                    if ADMIN_TELEGRAM_CHAT_ID:
                                        try:
                                            await notify_operator_failure(str(job["product_id"]), failures, error_summary)
                                        except Exception as notify_error:
                                            log.error("operator scrape alert failed error=%s", type(notify_error).__name__)
                                    else:
                                        log.error("scrape failure threshold reached product=%s failures=%s error=%s", job["product_id"], failures, error_summary)
                                log.warning("scrape failed id=%s error=%s", message_id, error_summary)
                                await redis.xack(JOBS, GROUP, message_id)
            finally:
                await browser.close()
    finally:
        if pool:
            await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
