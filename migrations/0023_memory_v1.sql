-- Memory schema v1: a slot name, free-form tags and a per-row expiry.
--
-- `slot` is the short topic label the v1 contract exchanges instead of a free
-- sentence, `tags` is a JSON string array, and `expires_at` is the epoch second
-- after which the row is retired by the expire sweep (`NULL` = permanent).
--
-- Existing rows keep `expires_at = NULL`: the backfill is a separate opt-in tool
-- (`--migrate-memories-v1`), and a migration must never retire old rows by itself.
ALTER TABLE memories ADD COLUMN slot TEXT NOT NULL DEFAULT '';
ALTER TABLE memories ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
ALTER TABLE memories ADD COLUMN expires_at INTEGER;

CREATE INDEX IF NOT EXISTS memories_expiry_idx ON memories (bot_id, group_id, expires_at)
    WHERE archived_at IS NULL;
