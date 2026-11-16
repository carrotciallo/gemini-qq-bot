-- Confirmed insert coverage belongs to the current opaque input token.
-- Original-batch membership remains implicit for older callers.
ALTER TABLE armed_turns ADD COLUMN confirmed_event_ids TEXT NOT NULL DEFAULT '[]';
