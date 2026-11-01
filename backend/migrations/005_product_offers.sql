-- Separate one canonical product from its store-specific product pages.
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
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (product_id, url)
);

CREATE INDEX IF NOT EXISTS product_offers_due_idx
  ON product_offers (next_check_at, offer_id) WHERE enabled;

-- Preserve the current single URL as the first offer for every existing product.
INSERT INTO product_offers(product_id,store,url,interval_minutes,enabled,next_check_at,last_queued_at,last_observed_at,last_price)
SELECT product_id,store,url,interval_minutes,enabled,next_check_at,last_queued_at,last_observed_at,last_price
FROM tracked_products
ON CONFLICT (product_id,url) DO NOTHING;
