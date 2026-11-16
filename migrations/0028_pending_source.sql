-- Synthetic openings must never be replayed by ordinary intake after failure.
-- Additive migration; human rows retain the old behavior by default.
ALTER TABLE pending_events ADD COLUMN source TEXT NOT NULL DEFAULT 'human'
    CHECK (source IN ('human', 'spontaneous'));

-- Recover only the old producer's bot-authored synthetic rows. Do not classify
-- human content by its text or by a caller-controlled message-id prefix alone.
UPDATE pending_events SET source = 'spontaneous'
WHERE sender_id = bot_id
  AND message_id GLOB 'spontaneous-[0-9]*-[0-9]*'
  AND CASE WHEN json_valid(normalized_payload)
      THEN json_extract(normalized_payload, '$.speaker_role') = 'bot'
       AND json_extract(normalized_payload, '$.sender_id') = bot_id
       AND json_extract(normalized_payload, '$.message_id') = message_id
      ELSE 0 END;
