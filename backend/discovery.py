"""Periodic catalog discovery for supported retailers.

Discovery is intentionally bounded: each store gets one search term per interval,
and only a small number of product pages are enrolled for price monitoring.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlsplit, urlunsplit

import asyncpg
import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from .worker import validate_store_url

log = logging.getLogger("catalog-discovery")

DISCOVERY_INTERVAL_MINUTES = max(15, int(os.getenv("DISCOVERY_INTERVAL_MINUTES", "15")))
DISCOVERY_MAX_RESULTS = max(1, min(20, int(os.getenv("DISCOVERY_MAX_RESULTS_PER_QUERY", "4"))))
DISCOVERY_OFFER_INTERVAL = max(15, int(os.getenv("DISCOVERY_OFFER_INTERVAL_MINUTES", "15")))
DISCOVERY_BATCH_SIZE = max(1, min(5, int(os.getenv("DISCOVERY_BATCH_SIZE", "5"))))
DISCOVERY_ERROR_RETRY_MINUTES = max(60, int(os.getenv("DISCOVERY_ERROR_RETRY_MINUTES", "360")))
DEFAULT_QUERIES = (
    "smartphone|notebook|smart tv|console videogame|placa de video|ssd|monitor gamer|"
    "geladeira|lavadora de roupas|ar condicionado|aspirador robo|air fryer|"
    "fone bluetooth|cafeteira|cadeira de escritorio|ferramentas|pneu|fralda"
)
QUERIES = [q.strip() for q in os.getenv("DISCOVERY_QUERIES", DEFAULT_QUERIES).split("|") if q.strip()]

STORE_HOSTS = {
    "amazon": "www.amazon.com.br",
    "kabum": "www.kabum.com.br",
    "casasbahia": "www.casasbahia.com.br",
    "magalu": "www.magazineluiza.com.br",
    "mercadolivre": "api",  # Marketplace search uses the official public search endpoint.
}
STORE_LABELS = {
    "amazon": "Amazon",
    "kabum": "KaBuM",
    "casasbahia": "Casas Bahia",
    "magalu": "Magazine Luiza",
    "mercadolivre": "Mercado Livre",
}


def canonical_url(value: str, base_url: str, store: str) -> str | None:
    """Resolve a result link, remove tracking parameters, and enforce store hosts."""
    absolute = urljoin(base_url, value.strip())
    parts = urlsplit(absolute)
    if parts.scheme != "https" or not parts.hostname:
        return None
    host = parts.hostname.lower()
    allowed_hosts = {
        "amazon": {"amazon.com.br", "www.amazon.com.br"},
        "kabum": {"kabum.com.br", "www.kabum.com.br"},
        "casasbahia": {"casasbahia.com.br", "www.casasbahia.com.br"},
        "magalu": {"magazineluiza.com.br", "www.magazineluiza.com.br"},
        "mercadolivre": {"mercadolivre.com.br", "www.mercadolivre.com.br", "produto.mercadolivre.com.br"},
    }[store]
    if host not in allowed_hosts or parts.username or parts.password:
        return None
    query = urlencode([
        (key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in {"gclid", "fbclid", "ref", "tag"} and not key.lower().startswith("utm_")
    ])
    try:
        return validate_store_url(urlunsplit(("https", host, parts.path.rstrip("/"), query, "")))
    except ValueError:
        return None


def _valid_gtin(value: Any) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits if len(digits) in (8, 12, 13, 14) else None


def product_key(store: str, url: str, gtin: str | None = None, external_id: str | None = None) -> str:
    """Only merge offers across stores when a globally meaningful ID is present."""
    clean_gtin = _valid_gtin(gtin)
    if clean_gtin:
        return f"gtin:{clean_gtin}"
    if store == "mercadolivre" and external_id and external_id.startswith("MLB"):
        return f"mercadolivre:{external_id}"
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
    return f"{store}:{digest}"


def _product_nodes(node: Any):
    if isinstance(node, list):
        for child in node:
            yield from _product_nodes(child)
    elif isinstance(node, dict):
        kinds = node.get("@type", [])
        if isinstance(kinds, str):
            kinds = [kinds]
        if "Product" in kinds:
            yield node
        for child in node.values():
            if isinstance(child, (dict, list)):
                yield from _product_nodes(child)


def _find_gtin(product: dict[str, Any]) -> str | None:
    for field in ("gtin14", "gtin13", "gtin12", "gtin8", "gtin", "productID", "sku"):
        value = _valid_gtin(product.get(field))
        if value:
            return value
    return None


def _is_product_path(store: str, path: str) -> bool:
    path = unquote(path).lower()
    if store == "amazon":
        return bool(re.search(r"/(?:dp|gp/product)/[a-z0-9]{10}(?:/|$)", path))
    if store == "kabum":
        return bool(re.search(r"/produto/\d+", path))
    if store == "magalu":
        return "/p/" in path and len(path.strip("/").split("/")) >= 2
    if store == "casasbahia":
        return "/p/" in path or bool(re.search(r"/\d{5,}(?:/|$)", path))
    if store == "mercadolivre":
        return "/p/" in path or bool(re.search(r"/MLB-?\d+", path, re.I))
    return False


def extract_page_products(html: str, base_url: str, store: str, limit: int = DISCOVERY_MAX_RESULTS) -> list[dict[str, Any]]:
    """Extract product records from structured data first, then store-shaped links."""
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, dict[str, Any]] = {}
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        for product in _product_nodes(payload):
            raw_url = product.get("url")
            name = re.sub(r"\s+", " ", str(product.get("name", ""))).strip()
            if not isinstance(raw_url, str) or len(name) < 5:
                continue
            url = canonical_url(raw_url, base_url, store)
            if not url or not _is_product_path(store, urlsplit(url).path):
                continue
            found[url] = {"url": url, "title": name[:200], "gtin": _find_gtin(product)}

    # Some retailers omit JSON-LD from search results; accept only product-shaped
    # paths from the exact retailer host and discard navigation/search links.
    for anchor in soup.select("a[href]"):
        if len(found) >= limit:
            break
        raw_url = str(anchor.get("href", ""))
        url = canonical_url(raw_url, base_url, store)
        if not url or not _is_product_path(store, urlsplit(url).path):
            continue
        title = " ".join((anchor.get("aria-label") or anchor.get_text(" ", strip=True)).split())
        if len(title) < 8:
            continue
        found.setdefault(url, {"url": url, "title": title[:200], "gtin": None})
    return list(found.values())[:limit]


async def search_marketplace(client: httpx.AsyncClient, term: str) -> list[dict[str, Any]]:
    access_token = os.getenv("MERCADOLIVRE_ACCESS_TOKEN", "").strip()
    if not access_token:
        raise RuntimeError("Mercado Livre API access token is not configured")
    response = await client.get(
        "https://api.mercadolibre.com/sites/MLB/search",
        params={"q": term, "limit": DISCOVERY_MAX_RESULTS},
        headers={"Accept": "application/json", "User-Agent": "RadarOfertas/1.0",
                 "Authorization": f"Bearer {access_token}"},
    )
    response.raise_for_status()
    data = response.json()
    products: list[dict[str, Any]] = []
    for item in data.get("results", []):
        url = canonical_url(str(item.get("permalink", "")), "https://www.mercadolivre.com.br/", "mercadolivre")
        title = re.sub(r"\s+", " ", str(item.get("title", ""))).strip()
        if not url or len(title) < 5:
            continue
        gtin = next((_valid_gtin(attribute.get("value_name")) for attribute in item.get("attributes", [])
                     if attribute.get("id") in {"GTIN", "EAN", "UPC"} and _valid_gtin(attribute.get("value_name"))), None)
        products.append({"url": url, "title": title[:200], "gtin": gtin,
                         "external_id": str(item.get("catalog_product_id") or item.get("id") or "")})
    return products


def search_url(store: str, term: str) -> str:
    slug = quote(term.lower().replace(" ", "-"), safe="-")
    if store == "amazon":
        return f"https://www.amazon.com.br/s?k={quote(term, safe='')}"
    if store == "kabum":
        return f"https://www.kabum.com.br/busca/{slug}"
    if store == "magalu":
        return f"https://www.magazineluiza.com.br/busca/{slug}/"
    if store == "casasbahia":
        return f"https://www.casasbahia.com.br/busca/{quote(term, safe='')}"
    raise ValueError("Loja de busca não suportada")


async def register_products(pool: asyncpg.Pool, store: str, term: str, products: list[dict[str, Any]]) -> int:
    registered = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            for product in products[:DISCOVERY_MAX_RESULTS]:
                url = canonical_url(product["url"], product["url"], store)
                title = " ".join(str(product.get("title", "")).split())[:200]
                if not url or len(title) < 5:
                    continue
                key = product_key(store, url, product.get("gtin"), product.get("external_id"))
                product_id = "auto-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:28]
                row = await conn.fetchrow(
                    "INSERT INTO tracked_products(product_id,title,store,url,interval_minutes,enabled,next_check_at,canonical_key,auto_discovered) "
                    "VALUES($1,$2,$3,$4,$5,TRUE,now(),$6,TRUE) "
                    "ON CONFLICT (canonical_key) WHERE canonical_key IS NOT NULL DO UPDATE SET "
                    "title=CASE WHEN tracked_products.auto_discovered THEN EXCLUDED.title ELSE tracked_products.title END, "
                    "interval_minutes=CASE WHEN tracked_products.auto_discovered THEN EXCLUDED.interval_minutes ELSE tracked_products.interval_minutes END, "
                    "updated_at=now() RETURNING product_id",
                    product_id, title, STORE_LABELS[store], url, DISCOVERY_OFFER_INTERVAL, key,
                )
                await conn.execute(
                    "INSERT INTO product_offers(product_id,store,url,interval_minutes,enabled,next_check_at,auto_discovered,discovery_query) "
                    "VALUES($1,$2,$3,$4,TRUE,now(),TRUE,$5) ON CONFLICT(product_id,url) DO UPDATE SET "
                    "enabled=TRUE,interval_minutes=CASE WHEN product_offers.auto_discovered THEN EXCLUDED.interval_minutes ELSE product_offers.interval_minutes END, "
                    "auto_discovered=product_offers.auto_discovered OR TRUE,discovery_query=EXCLUDED.discovery_query,updated_at=now()",
                    row["product_id"], STORE_LABELS[store], url, DISCOVERY_OFFER_INTERVAL, term[:200],
                )
                registered += 1
    return registered


async def ensure_discovery_sources(pool: asyncpg.Pool) -> None:
    # When cadence settings change, bring already discovered offers along without
    # touching manually registered products or offers.
    await pool.execute(
        "UPDATE tracked_products SET interval_minutes=$1,next_check_at=LEAST(next_check_at,now()),updated_at=now() "
        "WHERE auto_discovered=TRUE AND interval_minutes<>$1",
        DISCOVERY_OFFER_INTERVAL,
    )
    await pool.execute(
        "UPDATE product_offers SET interval_minutes=$1,next_check_at=LEAST(next_check_at,now()),updated_at=now() "
        "WHERE auto_discovered=TRUE AND interval_minutes<>$1",
        DISCOVERY_OFFER_INTERVAL,
    )
    await pool.executemany(
        "INSERT INTO catalog_discovery_sources(store,query_index,next_run_at) VALUES($1,0,now()) "
        "ON CONFLICT(store) DO NOTHING",
        [(store,) for store in STORE_HOSTS],
    )


async def discover_due(pool: asyncpg.Pool) -> int:
    """Run one bounded search per due store and enroll the resulting product links."""
    if not QUERIES:
        return 0
    await ensure_discovery_sources(pool)
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT store,query_index FROM catalog_discovery_sources WHERE next_run_at<=now() "
                "ORDER BY next_run_at,store LIMIT $1 FOR UPDATE SKIP LOCKED",
                DISCOVERY_BATCH_SIZE,
            )
            reservations: list[tuple[str, str]] = []
            for row in rows:
                store = row["store"]
                term = QUERIES[int(row["query_index"]) % len(QUERIES)]
                await conn.execute(
                    "UPDATE catalog_discovery_sources SET query_index=(query_index+1)%$2, "
                    "next_run_at=now()+($3 * interval '1 minute'),last_query=$4,last_error=NULL,updated_at=now() "
                    "WHERE store=$1",
                    store, len(QUERIES), DISCOVERY_INTERVAL_MINUTES, term,
                )
                reservations.append((store, term))

    if not reservations:
        return 0
    total = 0
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                for store, term in reservations:
                    try:
                        if store == "mercadolivre":
                            products = await search_marketplace(client, term)
                        else:
                            page = await browser.new_page()
                            try:
                                async def guard(route: Any) -> None:
                                    request = route.request
                                    if request.is_navigation_request() and request.resource_type == "document":
                                        try:
                                            validate_store_url(request.url)
                                        except ValueError:
                                            await route.abort()
                                            return
                                    await route.continue_()
                                await page.route("**/*", guard)
                                response = await page.goto(search_url(store, term), wait_until="domcontentloaded", timeout=20000)
                                if not response or response.status >= 400:
                                    raise RuntimeError(f"search page HTTP {response.status if response else 'empty'}")
                                final_url = validate_store_url(page.url)
                                products = extract_page_products(await page.content(), final_url, store)
                            finally:
                                await page.close()
                        count = await register_products(pool, store, term, products)
                        total += count
                        await pool.execute(
                            "UPDATE catalog_discovery_sources SET last_success_at=now(),last_error=NULL WHERE store=$1", store,
                        )
                        log.info("catalog discovery store=%s results=%s enrolled=%s", store, len(products), count)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        # Do not log URLs or response bodies; external sites can echo sensitive query data.
                        summary = f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"
                        summary = re.sub(r"https?://\S+", "[URL]", summary)
                        await pool.execute(
                            "UPDATE catalog_discovery_sources SET last_error=$2, "
                            "next_run_at=now()+($3 * interval '1 minute') WHERE store=$1",
                            store, summary, DISCOVERY_ERROR_RETRY_MINUTES,
                        )
                        log.warning("catalog discovery failed store=%s error=%s", store, type(exc).__name__)
            finally:
                await browser.close()
    return total
