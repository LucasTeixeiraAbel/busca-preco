-- Bounded automatic retailer discovery; safe to apply to existing volumes.
ALTER TABLE tracked_products
  ADD COLUMN IF NOT EXISTS canonical_key TEXT,
  ADD COLUMN IF NOT EXISTS auto_discovered BOOLEAN NOT NULL DEFAULT FALSE;

CREATE UNIQUE INDEX IF NOT EXISTS tracked_products_canonical_key_uidx
  ON tracked_products(canonical_key) WHERE canonical_key IS NOT NULL;

ALTER TABLE product_offers
  ADD COLUMN IF NOT EXISTS auto_discovered BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS discovery_query TEXT;

CREATE TABLE IF NOT EXISTS catalog_discovery_sources (
  store TEXT PRIMARY KEY,
  query_index INTEGER NOT NULL DEFAULT 0 CHECK (query_index >= 0),
  last_query TEXT,
  next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_success_at TIMESTAMPTZ,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
