-- Spontaneous-topic engine state, one JSON blob per group:
-- {last_at, day, day_count, attempts, cooldown:[{key,until,ignored}],
--  pending:{topic,key,enqueued_at,fired_at}|null}
-- A garbled blob reads as a fresh state in Rust; the column itself only ever
-- holds what the engine wrote.
ALTER TABLE group_state ADD COLUMN spontaneous_json TEXT NOT NULL DEFAULT '{}';
