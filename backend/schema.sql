-- Apply with: psql "$DATABASE_URL" -f schema.sql
CREATE TABLE IF NOT EXISTS price_observations (
  id BIGSERIAL PRIMARY KEY,
  stream_id TEXT NOT NULL UNIQUE,
  product_id TEXT NOT NULL,
  store TEXT NOT NULL,
  price NUMERIC(14, 2) NOT NULL CHECK (price > 0),
  url TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS price_observations_product_time_idx
  ON price_observations (product_id, observed_at DESC) INCLUDE (price);
CREATE INDEX IF NOT EXISTS idx_price_obs_product_store_time
  ON price_observations(product_id, store, observed_at DESC);
CREATE INDEX IF NOT EXISTS price_observations_time_idx ON price_observations (observed_at DESC);

CREATE TABLE IF NOT EXISTS tracked_products (
  product_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  store TEXT NOT NULL,
  url TEXT NOT NULL,
  interval_minutes INTEGER NOT NULL DEFAULT 60 CHECK (interval_minutes BETWEEN 5 AND 10080),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_queued_at TIMESTAMPTZ,
  last_observed_at TIMESTAMPTZ,
  last_price NUMERIC(14, 2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tracked_products_due_idx
  ON tracked_products (next_check_at) WHERE enabled;

CREATE TABLE IF NOT EXISTS product_offers (
  offer_id BIGSERIAL PRIMARY KEY,
  product_id TEXT NOT NULL REFERENCES tracked_products(product_id) ON DELETE CASCADE,
  store TEXT NOT NULL,
  url TEXT NOT NULL,
  interval_minutes INTEGER NOT NULL DEFAULT 60 CHECK (interval_minutes BETWEEN 5 AND 10080),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_queued_at TIMESTAMPTZ,
  last_observed_at TIMESTAMPTZ,
  last_price NUMERIC(14, 2),
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  last_failure_at TIMESTAMPTZ,
  last_failure_alerted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (product_id, url)
);
CREATE INDEX IF NOT EXISTS product_offers_due_idx
  ON product_offers (next_check_at, offer_id) WHERE enabled;

CREATE TABLE IF NOT EXISTS alert_subscribers (
  email TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS users (
  email TEXT PRIMARY KEY,
  verified_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS user_product_stars (
  email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
  product_id TEXT NOT NULL REFERENCES tracked_products(product_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (email, product_id)
);
CREATE INDEX IF NOT EXISTS user_product_stars_product_idx
  ON user_product_stars (product_id, email);
CREATE TABLE IF NOT EXISTS magic_login_tokens (
  token_hash TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS magic_login_tokens_expiry_idx ON magic_login_tokens (expires_at);
CREATE TABLE IF NOT EXISTS telegram_link_tokens (
  token_hash TEXT PRIMARY KEY,
  email TEXT NOT NULL REFERENCES users(email),
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS telegram_link_tokens_expiry_idx ON telegram_link_tokens (expires_at);
CREATE TABLE IF NOT EXISTS telegram_chats (
  email TEXT PRIMARY KEY REFERENCES users(email),
  chat_id BIGINT NOT NULL UNIQUE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS push_subscriptions (
  endpoint TEXT PRIMARY KEY,
  subscription JSONB NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS owner_email TEXT REFERENCES users(email);
CREATE TABLE IF NOT EXISTS alert_deliveries (
  alert_id TEXT NOT NULL,
  channel TEXT NOT NULL,
  destination TEXT NOT NULL,
  sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (alert_id, channel, destination)
);

CREATE TABLE IF NOT EXISTS alerts_outbox (
  id BIGSERIAL PRIMARY KEY,
  product_id TEXT NOT NULL,
  store TEXT NOT NULL,
  offer_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS alerts_outbox_pending_idx
  ON alerts_outbox (id)
  WHERE published_at IS NULL;
