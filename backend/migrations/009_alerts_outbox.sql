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
