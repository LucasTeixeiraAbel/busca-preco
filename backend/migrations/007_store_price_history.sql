CREATE INDEX IF NOT EXISTS idx_price_obs_product_store_time
  ON price_observations(product_id, store, observed_at DESC);
