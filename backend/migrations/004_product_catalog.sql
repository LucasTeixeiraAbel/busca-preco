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
