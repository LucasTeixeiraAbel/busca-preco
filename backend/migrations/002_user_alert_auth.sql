CREATE TABLE IF NOT EXISTS users (
  email TEXT PRIMARY KEY,
  verified_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS magic_login_tokens (
  token_hash TEXT PRIMARY KEY,
  email TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS magic_login_tokens_expiry_idx ON magic_login_tokens (expires_at);
ALTER TABLE push_subscriptions ADD COLUMN IF NOT EXISTS owner_email TEXT REFERENCES users(email);
