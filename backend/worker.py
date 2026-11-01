"""Price anomaly worker. Consumes Redis Stream `price:observations` via consumer group."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import statistics
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from pydantic import BaseModel, EmailStr, Field, HttpUrl, model_validator
from redis.asyncio import Redis
from redis.exceptions import ResponseError
import asyncpg
from dotenv import load_dotenv
from .mailer import send_email as send_transactional_email

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("price-worker")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM, GROUP, CONSUMER = "price:observations", "price-detectors", os.getenv("WORKER_ID", "detector-1")
FEED_KEY = "feed:latest"
ALLOWED_HOSTS = {h.strip().lower() for h in os.getenv(
    "STORE_ALLOWLIST",
    "amazon.com.br,www.amazon.com.br,kabum.com.br,www.kabum.com.br",
).split(",") if h.strip()}
MIN_HISTORY = int(os.getenv("MIN_HISTORY", "8"))
MAD_Z_THRESHOLD = float(os.getenv("MAD_Z_THRESHOLD", "6.0"))
ZERO_MAD_DROP_RATIO = float(os.getenv("ZERO_MAD_DROP_RATIO", "0.35"))

redis: Redis | None = None
db: asyncpg.Pool | None = None
bearer = HTTPBearer(auto_error=False)


@asynccontextmanager
async def lifespan(application: FastAPI):
    global redis, db
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await redis.ping()
        db = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)
        application.state.worker_task = asyncio.create_task(consume())
        yield
    finally:
        task = getattr(application.state, "worker_task", None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if db:
            await db.close()
            db = None
        if redis:
            await redis.aclose()
            redis = None


app = FastAPI(title="Price anomaly detector", docs_url=None, redoc_url=None, lifespan=lifespan)


class Observation(BaseModel):
    product_id: str = Field(min_length=1, max_length=100)
    store: str = Field(min_length=1, max_length=100)
    price: float = Field(gt=0, allow_inf_nan=False)
    url: HttpUrl
    source_url: HttpUrl | None = None


class ScrapeJob(BaseModel):
    product_id: str = Field(min_length=1, max_length=100)
    url: HttpUrl


class ProductCreate(BaseModel):
    product_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$")
    title: str = Field(min_length=1, max_length=200)
    url: HttpUrl | None = None
    urls: list[HttpUrl] = Field(default_factory=list, max_length=20)
    interval_minutes: int = Field(default=60, ge=5, le=10080)

    @model_validator(mode="after")
    def require_offer_url(self) -> "ProductCreate":
        if not self.url and not self.urls:
            raise ValueError("Informe pelo menos uma URL de oferta")
        return self

    @property
    def offer_urls(self) -> list[HttpUrl]:
        return [*self.urls, *([self.url] if self.url else [])]


class EmailEnrollment(BaseModel):
    email: EmailStr


class PushSubscription(BaseModel):
    endpoint: HttpUrl
    keys: dict[str, str]


class MagicLinkRequest(BaseModel):
    email: EmailStr


class MagicLinkVerification(BaseModel):
    token: str = Field(min_length=32, max_length=256)


def make_telegram_deep_link(username: str, token: str) -> str:
    clean_username = username.removeprefix("@").strip()
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", clean_username):
        raise ValueError("Username do bot inválido")
    return f"https://t.me/{clean_username}?start={token}"


def validate_push_endpoint(value: str) -> str:
    parts = urlsplit(value)
    allowed = {h.strip().lower() for h in os.getenv(
        "PUSH_ENDPOINT_HOSTS",
        "fcm.googleapis.com,updates.push.services.mozilla.com,web.push.apple.com,notify.windows.com",
    ).split(",") if h.strip()}
    if parts.scheme != "https" or (parts.hostname or "").lower() not in allowed or parts.port not in (None, 443):
        raise ValueError("Endpoint push não autorizado")
    return value


def authorize_admin(authorization: str | None) -> None:
    expected = os.getenv("ALERT_ADMIN_TOKEN", "")
    if len(expected) < 32:
        raise HTTPException(status_code=503, detail="Provisionamento administrativo não configurado")
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Credencial administrativa inválida")


def issue_user_token(email: str) -> str:
    secret = os.getenv("AUTH_JWT_SECRET", "")
    if len(secret) < 32:
        raise HTTPException(status_code=503, detail="Autenticação não configurada")
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {"sub": email, "iss": os.getenv("AUTH_JWT_ISSUER", "radar-ofertas"), "iat": now, "exp": now + timedelta(hours=1)},
        secret,
        algorithm="HS256",
    )


async def current_user(credentials: HTTPAuthorizationCredentials | None) -> str:
    secret = os.getenv("AUTH_JWT_SECRET", "")
    if not credentials or not secret or len(secret) < 32:
        raise HTTPException(status_code=401, detail="Sessão ausente ou inválida")
    try:
        claims = jwt.decode(
            credentials.credentials,
            secret,
            algorithms=["HS256"],
            issuer=os.getenv("AUTH_JWT_ISSUER", "radar-ofertas"),
            options={"require": ["sub", "exp", "iss"]},
        )
        email = MagicLinkRequest(email=claims["sub"]).email
    except (jwt.PyJWTError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Sessão ausente ou inválida") from exc
    verified = await db.fetchval("SELECT 1 FROM users WHERE email=$1 AND verified_at IS NOT NULL", str(email).lower())
    if not verified:
        raise HTTPException(status_code=401, detail="E-mail ainda não verificado")
    return str(email).lower()


async def rate_limit(request: Request, key: str, maximum: int, seconds: int) -> None:
    count = await redis.eval(
        "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end; return n",
        1, key, seconds,
    )
    if int(count) > maximum:
        raise HTTPException(status_code=429, detail="Limite de requisições excedido")


async def admin_rate_limit(request: Request, operation: str) -> None:
    client_ip = request.client.host if request.client else "unknown"
    key = f"rate:admin:{operation}:{client_ip}"
    count = await redis.eval(
        "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n",
        1, key,
    )
    if int(count) > int(os.getenv("ADMIN_RATE_PER_MIN", "60")):
        raise HTTPException(status_code=429, detail="Limite administrativo excedido")


def validate_store_url(value: str) -> str:
    parts = urlsplit(value)
    host = (parts.hostname or "").lower()
    if parts.scheme != "https" or host not in ALLOWED_HOSTS or parts.username or parts.password:
        raise ValueError("URL fora da allowlist HTTPS de lojas")
    return value


def store_label_for_url(value: str) -> str:
    host = (urlsplit(value).hostname or "").lower()
    labels = {
        "amazon.com.br": "Amazon",
        "www.amazon.com.br": "Amazon",
        "kabum.com.br": "KaBuM",
        "www.kabum.com.br": "KaBuM",
        "secure3.fastshop.com.br": "Fast Shop",
        "magazineluiza.com.br": "Magazine Luiza",
        "www.magazineluiza.com.br": "Magazine Luiza",
        "pichau.com.br": "Pichau",
        "www.pichau.com.br": "Pichau",
    }
    return labels.get(host, host)


def detect_anomaly(history: list[float], current: float) -> tuple[bool, float]:
    """Robust modified z-score using median absolute deviation; resistant to old outliers."""
    if len(history) < MIN_HISTORY:
        return False, 0.0
    center = statistics.median(history)
    mad = statistics.median(abs(item - center) for item in history)
    if center <= 0:
        return False, 0.0
    # A perfectly stable history has MAD=0; treat a material relative drop as anomalous.
    if mad == 0:
        relative_score = (current - center) / center
        return relative_score <= -ZERO_MAD_DROP_RATIO, relative_score
    score = 0.6745 * (current - center) / mad
    return score <= -MAD_Z_THRESHOLD, score


async def process_message(client: Redis, message_id: str, fields: dict[str, Any]) -> None:
    data = json.loads(fields["data"])
    obs = Observation.model_validate(data)
    safe_url = validate_store_url(str(obs.url))
    source_url = validate_store_url(str(obs.source_url)) if obs.source_url else safe_url
    history_key = f"price:history:{obs.product_id}"
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT price FROM price_observations WHERE product_id=$1 ORDER BY observed_at DESC LIMIT 100", obs.product_id)
        title = await conn.fetchval("SELECT title FROM tracked_products WHERE product_id=$1", obs.product_id)
    history = [float(row["price"]) for row in rows]
    anomalous, score = detect_anomaly(history, obs.price)
    item = {
        **obs.model_dump(mode="json", exclude={"source_url"}),
        "title": title or obs.product_id,
        "previousPrice": statistics.median(history) if history else None,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "url": safe_url,
        "anomaly": anomalous,
        "score": round(score, 3),
    }
    async with db.acquire() as conn:
        inserted = await conn.fetchval(
            "INSERT INTO price_observations(stream_id,product_id,store,price,url) VALUES($1,$2,$3,$4,$5) ON CONFLICT(stream_id) DO NOTHING RETURNING id",
            message_id, obs.product_id, obs.store, obs.price, safe_url,
        )
        if inserted is not None:
            await conn.execute(
                "UPDATE product_offers SET last_price=$3,last_observed_at=now(),updated_at=now() "
                "WHERE product_id=$1 AND url=$2",
                obs.product_id, source_url, obs.price,
            )
    if inserted is None:
        await client.xack(STREAM, GROUP, message_id)
        return
    # PostgreSQL is the durable source; Redis keeps the hot history/feed cache.
    pipe = client.pipeline(transaction=True)
    pipe.lpush(history_key, obs.price)
    pipe.ltrim(history_key, 0, 99)
    pipe.lpush(FEED_KEY, json.dumps(item))
    pipe.ltrim(FEED_KEY, 0, 199)
    if anomalous:
        # A downstream alert consumer handles SES/SendGrid and push delivery.
        pipe.xadd("alerts:detected", {"data": json.dumps(item)})
    pipe.xack(STREAM, GROUP, message_id)
    await pipe.execute()
    log.info("processed product=%s anomaly=%s score=%.2f", obs.product_id, anomalous, score)


async def consume() -> None:
    assert redis is not None
    try:
        await redis.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
    while True:
        try:
            batches = await redis.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=50, block=1000)
            for _, messages in batches:
                for message_id, fields in messages:
                    try:
                        await process_message(redis, message_id, fields)
                    except Exception:
                        log.exception("message failed; leaving pending for retry id=%s", message_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("stream read failed")
            await asyncio.sleep(1)


@app.post("/observations", status_code=202)
async def enqueue_observation(payload: Observation, request: Request) -> dict[str, str]:
    # Redis-backed per-IP token bucket; configure trusted proxies before using X-Forwarded-For.
    client_ip = request.client.host if request.client else "unknown"
    key = f"rate:observations:{client_ip}"
    allowed = await redis.eval(
        "local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n",
        1, key,
    )
    if int(allowed) > int(os.getenv("OBSERVATION_RATE_PER_MIN", "120")):
        raise HTTPException(status_code=429, detail="Limite de requisições excedido")
    try:
        validate_store_url(str(payload.url))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="URL de loja não autorizada") from exc
    if payload.store != store_label_for_url(str(payload.url)):
        raise HTTPException(status_code=422, detail="Nome da loja não corresponde ao domínio validado")
    if payload.source_url:
        try:
            validate_store_url(str(payload.source_url))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="URL original não autorizada") from exc
        if store_label_for_url(str(payload.source_url)) != payload.store:
            raise HTTPException(status_code=422, detail="URL original deve pertencer à mesma loja")
    entry_id = await redis.xadd(STREAM, {"data": payload.model_dump_json()})
    return {"queued": entry_id}


@app.get("/feed")
async def get_feed() -> list[dict[str, Any]]:
    # Redis cache read; API gateways should also enforce a global rate limit.
    items = await redis.lrange(FEED_KEY, 0, 49)
    feed = [json.loads(item) for item in items]
    if feed:
        ids = list({item["product_id"] for item in feed if item.get("product_id")})
        titles = await db.fetch("SELECT product_id,title FROM tracked_products WHERE product_id=ANY($1::text[])", ids)
        title_by_id = {row["product_id"]: row["title"] for row in titles}
        for item in feed:
            item.setdefault("title", title_by_id.get(item.get("product_id"), item.get("product_id", "Produto")))
    return feed


@app.post("/scrape", status_code=202)
async def enqueue_scrape(payload: ScrapeJob, request: Request) -> dict[str, str]:
    """Schedule one allowlisted store URL for the dedicated Playwright process."""
    client_ip = request.client.host if request.client else "unknown"
    key = f"rate:scrape:{client_ip}"
    count = await redis.eval("local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n", 1, key)
    if int(count) > int(os.getenv("SCRAPE_RATE_PER_MIN", "30")):
        raise HTTPException(status_code=429, detail="Limite de coleta excedido")
    try:
        validate_store_url(str(payload.url))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="URL de loja não autorizada") from exc
    job_id = await redis.xadd("scrape:jobs", {"data": payload.model_dump_json()})
    return {"queued": job_id}


@app.post("/admin/products", status_code=201)
async def create_tracked_product(
    payload: ProductCreate,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    authorize_admin(authorization)
    await admin_rate_limit(request, "products")
    try:
        validated_urls = list(dict.fromkeys(validate_store_url(str(value)) for value in payload.offer_urls))
        safe_url = validated_urls[0]
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="URL de loja não autorizada") from exc
    store = store_label_for_url(safe_url)
    row = await db.fetchrow(
        "INSERT INTO tracked_products(product_id,title,store,url,interval_minutes,enabled,next_check_at) "
        "VALUES($1,$2,$3,$4,$5,TRUE,now()) ON CONFLICT(product_id) DO UPDATE SET "
        "title=EXCLUDED.title,store=EXCLUDED.store,url=EXCLUDED.url,interval_minutes=EXCLUDED.interval_minutes,"
        "enabled=TRUE,next_check_at=now(),updated_at=now() "
        "RETURNING product_id,title,store,url,interval_minutes,enabled,next_check_at,last_queued_at,last_observed_at,last_price",
        payload.product_id, payload.title.strip(), store, safe_url, payload.interval_minutes,
    )
    offers = []
    for raw_url in dict.fromkeys(str(value) for value in payload.offer_urls):
        try:
            offer_url = validate_store_url(raw_url)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Uma ou mais URLs não pertencem a lojas HTTPS permitidas") from exc
        offer_store = store_label_for_url(offer_url)
        await db.execute(
            "INSERT INTO product_offers(product_id,store,url,interval_minutes,enabled,next_check_at) "
            "VALUES($1,$2,$3,$4,TRUE,now()) ON CONFLICT(product_id,url) DO UPDATE SET "
            "store=EXCLUDED.store,interval_minutes=EXCLUDED.interval_minutes,enabled=TRUE,next_check_at=now(),updated_at=now()",
            payload.product_id, offer_store, offer_url, payload.interval_minutes,
        )
        offers.append({"store": offer_store, "url": offer_url})
    await db.execute(
        "UPDATE product_offers SET enabled=FALSE,updated_at=now() "
        "WHERE product_id=$1 AND NOT (url=ANY($2::text[]))",
        payload.product_id, validated_urls,
    )
    return {**dict(row), "offers": offers}


@app.get("/admin/products")
async def list_tracked_products(authorization: str | None = Header(default=None)) -> list[dict[str, Any]]:
    authorize_admin(authorization)
    rows = await db.fetch(
        "SELECT p.product_id,p.title,o.store,o.url,o.interval_minutes,o.enabled,o.next_check_at,"
        "o.last_queued_at,o.last_observed_at,o.last_price FROM product_offers o "
        "JOIN tracked_products p USING(product_id) WHERE p.enabled=TRUE AND o.enabled=TRUE "
        "ORDER BY p.created_at DESC,o.store LIMIT 500"
    )
    return [dict(row) for row in rows]


@app.delete("/admin/products/{product_id}", status_code=204)
async def disable_tracked_product(
    product_id: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    authorize_admin(authorization)
    await admin_rate_limit(request, "products")
    await db.execute("UPDATE tracked_products SET enabled=FALSE,updated_at=now() WHERE product_id=$1", product_id)


@app.post("/auth/magic-link", status_code=202)
async def request_magic_link(payload: MagicLinkRequest, request: Request) -> dict[str, str]:
    """Send a one-time sign-in link; never reveal whether the address has an account."""
    secret = os.getenv("AUTH_JWT_SECRET", "")
    if len(secret) < 32:
        raise HTTPException(status_code=503, detail="Autenticação não configurada")
    email = str(payload.email).lower()
    ip = request.client.host if request.client else "unknown"
    await rate_limit(request, f"rate:auth:ip:{ip}", 10, 900)
    email_key = hashlib.sha256(email.encode()).hexdigest()
    await rate_limit(request, f"rate:auth:email:{email_key}", 3, 3600)
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    await db.execute(
        "INSERT INTO magic_login_tokens(token_hash,email,expires_at) VALUES($1,$2,$3)",
        token_hash, email, datetime.now(timezone.utc) + timedelta(minutes=15),
    )
    origin = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000").rstrip("/")
    # Keep the secret in the URL fragment: browsers do not send fragments in HTTP requests.
    link = f"{origin}/auth/verify#token={token}"
    await send_transactional_email(email, "Seu link de acesso ao Radar", f"Acesse sua conta pelo link de uso único (válido por 15 minutos):\n{link}\n\nSe não solicitou este acesso, ignore esta mensagem.")
    return {"message": "Se o endereço puder receber acesso, enviaremos um link."}


@app.post("/auth/verify")
async def verify_magic_link(payload: MagicLinkVerification) -> dict[str, str]:
    token_hash = hashlib.sha256(payload.token.encode()).hexdigest()
    async with db.acquire() as conn:
        async with conn.transaction():
            email = await conn.fetchval(
                "UPDATE magic_login_tokens SET consumed_at=now() "
                "WHERE token_hash=$1 AND consumed_at IS NULL AND expires_at>now() RETURNING email",
                token_hash,
            )
            if not email:
                raise HTTPException(status_code=400, detail="Link inválido ou expirado")
            email = await conn.fetchval(
                "INSERT INTO users(email,verified_at) VALUES($1,now()) "
                "ON CONFLICT(email) DO UPDATE SET verified_at=COALESCE(users.verified_at,EXCLUDED.verified_at) "
                "RETURNING email",
                email,
            )
    return {"access_token": issue_user_token(email), "token_type": "bearer"}


@app.get("/me")
async def get_current_profile(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict[str, str | bool]:
    email = await current_user(credentials)
    alerts_enabled = await db.fetchval("SELECT enabled FROM alert_subscribers WHERE email=$1", email)
    telegram_enabled = await db.fetchval("SELECT enabled FROM telegram_chats WHERE email=$1", email)
    return {"email": email, "email_alerts": bool(alerts_enabled), "telegram_enabled": bool(telegram_enabled)}


@app.get("/me/watchlist")
async def get_watchlist(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict[str, Any]:
    email = await current_user(credentials)
    rows = await db.fetch(
        "SELECT p.product_id,p.title,o.store,o.url,o.last_price,o.last_observed_at "
        "FROM user_product_stars w JOIN tracked_products p USING(product_id) "
        "LEFT JOIN product_offers o ON o.product_id=p.product_id AND o.enabled=TRUE "
        "WHERE w.email=$1 AND p.enabled=TRUE ORDER BY p.title,o.store",
        email,
    )
    return {"product_ids": list(dict.fromkeys(row["product_id"] for row in rows)), "offers": [dict(row) for row in rows]}


@app.put("/me/watchlist/{product_id}", status_code=204)
async def star_product(
    product_id: str,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> None:
    email = await current_user(credentials)
    exists = await db.fetchval("SELECT 1 FROM tracked_products WHERE product_id=$1 AND enabled=TRUE", product_id)
    if not exists:
        raise HTTPException(status_code=404, detail="Produto monitorado não encontrado")
    await db.execute(
        "INSERT INTO user_product_stars(email,product_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
        email, product_id,
    )


@app.delete("/me/watchlist/{product_id}", status_code=204)
async def unstar_product(
    product_id: str,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> None:
    email = await current_user(credentials)
    await db.execute("DELETE FROM user_product_stars WHERE email=$1 AND product_id=$2", email, product_id)


@app.get("/me/products/{product_id}/history")
async def get_product_history(
    product_id: str,
    years: int = Query(default=1),
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict[str, Any]:
    email = await current_user(credentials)
    if years not in {1, 2, 3, 5}:
        raise HTTPException(status_code=422, detail="Período deve ser 1, 2, 3 ou 5 anos")
    starred = await db.fetchval(
        "SELECT 1 FROM user_product_stars WHERE email=$1 AND product_id=$2",
        email, product_id,
    )
    if not starred:
        raise HTTPException(status_code=403, detail="Marque o produto com estrela para consultar o histórico")
    cutoff = datetime.now(timezone.utc) - timedelta(days=365 * years)
    available_from = await db.fetchval(
        "SELECT min(observed_at) FROM price_observations WHERE product_id=$1", product_id,
    )
    points = await db.fetch(
        "SELECT date_trunc('day',observed_at)::date AS day,store,"
        "min(price)::float8 AS low_price,max(price)::float8 AS high_price,"
        "avg(price)::float8 AS average_price,(array_agg(price ORDER BY observed_at DESC))[1]::float8 AS closing_price "
        "FROM price_observations WHERE product_id=$1 AND observed_at >= $2 "
        "GROUP BY date_trunc('day',observed_at)::date,store ORDER BY day,store",
        product_id, cutoff,
    )
    return {
        "product_id": product_id,
        "years": years,
        "available_from": available_from,
        "points": [dict(row) for row in points],
    }


@app.put("/me/alerts/email", status_code=204)
async def enable_email_alerts(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
    email = await current_user(credentials)
    await db.execute(
        "INSERT INTO alert_subscribers(email,enabled) VALUES($1,TRUE) "
        "ON CONFLICT(email) DO UPDATE SET enabled=TRUE",
        email,
    )


@app.delete("/me/alerts/email", status_code=204)
async def disable_email_alerts(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
    email = await current_user(credentials)
    await db.execute("UPDATE alert_subscribers SET enabled=FALSE WHERE email=$1", email)


@app.put("/me/push-subscriptions", status_code=204)
async def enable_push_alerts(
    payload: PushSubscription,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> None:
    email = await current_user(credentials)
    endpoint = str(payload.endpoint)
    try:
        validate_push_endpoint(endpoint)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Endpoint push não permitido") from exc
    if not payload.keys.get("p256dh") or not payload.keys.get("auth"):
        raise HTTPException(status_code=422, detail="Chaves da subscription incompletas")
    saved = await db.fetchval(
        "INSERT INTO push_subscriptions(endpoint,subscription,enabled,owner_email) "
        "VALUES($1,$2::jsonb,TRUE,$3) ON CONFLICT(endpoint) DO UPDATE "
        "SET subscription=EXCLUDED.subscription,enabled=TRUE "
        "WHERE push_subscriptions.owner_email=EXCLUDED.owner_email RETURNING endpoint",
        endpoint, json.dumps(payload.model_dump(mode="json")), email,
    )
    if not saved:
        raise HTTPException(status_code=409, detail="Subscription já vinculada a outra conta")


@app.delete("/me/push-subscriptions", status_code=204)
async def disable_push_alerts(
    endpoint: HttpUrl,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> None:
    email = await current_user(credentials)
    await db.execute(
        "UPDATE push_subscriptions SET enabled=FALSE WHERE endpoint=$1 AND owner_email=$2",
        str(endpoint), email,
    )


@app.post("/me/telegram/link")
async def create_telegram_link(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict[str, str]:
    email = await current_user(credentials)
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    username = os.getenv("TELEGRAM_BOT_USERNAME", "")
    if not bot_token or not username:
        raise HTTPException(status_code=503, detail="Bot do Telegram não configurado")
    raw_token = secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    await db.execute(
        "UPDATE telegram_link_tokens SET consumed_at=now() WHERE email=$1 AND consumed_at IS NULL",
        email,
    )
    await db.execute(
        "INSERT INTO telegram_link_tokens(token_hash,email,expires_at) VALUES($1,$2,$3)",
        token_hash, email, datetime.now(timezone.utc) + timedelta(minutes=10),
    )
    try:
        link = make_telegram_deep_link(username, raw_token)
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="Username do bot inválido") from exc
    return {"url": link, "expires_in": "600"}


@app.delete("/me/telegram", status_code=204)
async def disable_telegram_alerts(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
    email = await current_user(credentials)
    await db.execute("UPDATE telegram_chats SET enabled=FALSE WHERE email=$1", email)


@app.put("/admin/alert-subscribers", status_code=204)
async def provision_email_subscriber(
    payload: EmailEnrollment,
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Operator-only provisioning; never put ALERT_ADMIN_TOKEN in browser code."""
    authorize_admin(authorization)
    await admin_rate_limit(request, "email")
    await db.execute(
        "INSERT INTO alert_subscribers(email,enabled) VALUES($1,TRUE) "
        "ON CONFLICT(email) DO UPDATE SET enabled=TRUE",
        str(payload.email).lower(),
    )


@app.delete("/admin/alert-subscribers/{email}", status_code=204)
async def remove_email_subscriber(
    email: EmailStr,
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    authorize_admin(authorization)
    await admin_rate_limit(request, "email")
    await db.execute("UPDATE alert_subscribers SET enabled=FALSE WHERE email=$1", str(email).lower())


@app.put("/admin/push-subscriptions", status_code=204)
async def provision_push_subscription(
    payload: PushSubscription,
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Store consented browser subscription from a trusted, authenticated service."""
    authorize_admin(authorization)
    await admin_rate_limit(request, "push")
    endpoint = str(payload.endpoint)
    try:
        validate_push_endpoint(endpoint)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Endpoint push não permitido") from exc
    if not payload.keys.get("p256dh") or not payload.keys.get("auth"):
        raise HTTPException(status_code=422, detail="Chaves da subscription incompletas")
    await db.execute(
        "INSERT INTO push_subscriptions(endpoint,subscription,enabled) VALUES($1,$2::jsonb,TRUE) "
        "ON CONFLICT(endpoint) DO UPDATE SET subscription=EXCLUDED.subscription,enabled=TRUE",
        endpoint, json.dumps(payload.model_dump(mode="json")),
    )


@app.delete("/admin/push-subscriptions", status_code=204)
async def remove_push_subscription(
    endpoint: HttpUrl,
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    authorize_admin(authorization)
    await admin_rate_limit(request, "push")
    await db.execute("UPDATE push_subscriptions SET enabled=FALSE WHERE endpoint=$1", str(endpoint))
