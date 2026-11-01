CREATE TABLE IF NOT EXISTS user_product_stars (
  email TEXT NOT NULL REFERENCES users(email) ON DELETE CASCADE,
  product_id TEXT NOT NULL REFERENCES tracked_products(product_id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (email, product_id)
);

CREATE INDEX IF NOT EXISTS user_product_stars_product_idx
  ON user_product_stars (product_id, email);
