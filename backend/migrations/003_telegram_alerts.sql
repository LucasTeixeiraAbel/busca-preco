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
