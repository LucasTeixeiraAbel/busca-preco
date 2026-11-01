"""Allowlisted asynchronous Playwright collector; parses structured price metadata."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .worker import REDIS_URL, store_label_for_url, validate_store_url

log = logging.getLogger("price-scraper")
JOBS, GROUP, CONSUMER = "scrape:jobs", "scrapers", os.getenv("SCRAPER_ID", "scraper-1")


def parse_price(html: str) -> float:
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

    # Amazon renders the active offer in an a-price block; its JSON-LD may omit it.
    for tag in soup.select("span.a-price:not(.a-text-price) span.a-offscreen"):
        price = numeric_price(tag.get_text(" ", strip=True))
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


async def run() -> None:
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
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
                            try:
                                job = json.loads(fields["data"])
                                url = validate_store_url(job["url"])
                                page = await browser.new_page()
                                try:
                                    response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                                    if not response or response.status >= 400:
                                        raise ValueError("Loja retornou resposta HTTP inválida")
                                    # Redirects must remain on an explicitly trusted HTTPS store.
                                    final_url = validate_store_url(page.url)
                                    price = parse_price(await page.content())
                                finally:
                                    await page.close()
                                observation = {"product_id": job["product_id"], "store": store_label_for_url(final_url), "price": price, "url": final_url, "source_url": url}
                                await redis.xadd("price:observations", {"data": json.dumps(observation)})
                                await redis.xack(JOBS, GROUP, message_id)
                            except Exception as exc:
                                # The scheduler will enqueue the offer again; do not grow the PEL.
                                log.warning("scrape failed id=%s error=%s", message_id, type(exc).__name__)
                                await redis.xack(JOBS, GROUP, message_id)
            finally:
                await browser.close()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
