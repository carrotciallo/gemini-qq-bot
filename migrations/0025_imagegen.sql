-- Image generation (the `generate_image` MCP tool): the job ledger, the
-- per-group quota reset watermark, and the single-row settings the panel
-- edits at runtime.
--
-- Two quota buckets share `image_jobs`: `self` (she decided to draw) is
-- capped per group per day, `request` (a member explicitly asked) is capped
-- per person per group per day. The owner never counts. A job counts from
-- the moment it is registered, whatever its final state — the panel reset
-- (a watermark in `imagegen_resets`) is the pressure valve.
--
-- Reversible: additive only. Dropping the three tables restores the prior
-- schema; no existing table is touched.
CREATE TABLE image_jobs (
    id TEXT PRIMARY KEY,
    bot_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    -- Who the picture is charged to: a QQ id for `request`, the literal
    -- 'bot' for `self`.
    requester_id TEXT NOT NULL,
    bucket TEXT NOT NULL,
    prompt TEXT NOT NULL,
    self_portrait INTEGER NOT NULL DEFAULT 0,
    caption TEXT,
    state TEXT NOT NULL DEFAULT 'queued',
    file_path TEXT,
    error_class TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX image_jobs_group_day_idx ON image_jobs (bot_id, group_id, created_at);
CREATE INDEX image_jobs_state_idx ON image_jobs (state, created_at);

CREATE TABLE imagegen_resets (
    bot_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    reset_at INTEGER NOT NULL,
    PRIMARY KEY (bot_id, group_id)
);

-- One row, ever (id = 1). Caps are read at decision time so a panel edit
-- takes effect without a restart; `owner_id` is written by the main
-- process at startup from its file config, because the MCP server process
-- only has the database.
CREATE TABLE imagegen_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    self_daily_cap INTEGER NOT NULL DEFAULT 15,
    request_daily_cap INTEGER NOT NULL DEFAULT 5,
    request_cooldown_minutes INTEGER NOT NULL DEFAULT 10,
    owner_id TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0
);

INSERT INTO imagegen_settings (id) VALUES (1);
