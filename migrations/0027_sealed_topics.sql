-- Sealed topics ([seal-1], 2026-11-12): topics the operator has declared
-- dead for a group. A sealed topic must not re-enter the group's derived
-- context: memory writes, summary compaction, retrieval injection and
-- spontaneous topic picks all consult this table. Entries are explicit
-- operator will — unlike tombstones, which are derived from row history
-- and expire out of a fetch window.
--
-- `keywords` is a JSON array of substrings; `slot` is an optional
-- exact-match predicate label (empty = no slot condition). An entry hits
-- when its slot matches (when set) OR any keyword appears in the text.
--
-- Reversible: DROP TABLE sealed_topics; nothing else is touched.
CREATE TABLE sealed_topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    keywords TEXT NOT NULL DEFAULT '[]',
    slot TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    created_by TEXT NOT NULL DEFAULT ''
);
CREATE INDEX idx_sealed_topics_group ON sealed_topics (bot_id, group_id);
