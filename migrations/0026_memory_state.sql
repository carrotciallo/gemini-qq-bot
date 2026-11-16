-- Memory lifecycle state (memfix plan v2, [mf-1]): a memory's standing is
-- structured data, never inferred from its text. `state` names why a row is
-- or is not in play; `superseded_by` links a replaced row to its successor so
-- the correction chain stays auditable; `state_changed_at` stamps the last
-- transition.
--
-- Invariant maintained by every writer from this migration on:
--   state = 'active'  <=>  archived_at IS NULL
-- Rows retired before this migration only ever knew one reason (archived),
-- so the backfill maps archived_at onto it; everything else starts active.
--
-- Reversible: dropping the three columns undoes it (the supersede links are
-- the only information lost by a rollback).

ALTER TABLE memories ADD COLUMN state TEXT NOT NULL DEFAULT 'active';
ALTER TABLE memories ADD COLUMN superseded_by TEXT;
ALTER TABLE memories ADD COLUMN state_changed_at INTEGER;

UPDATE memories SET state = 'archived' WHERE archived_at IS NOT NULL;
UPDATE memories SET state_changed_at = updated_at;
