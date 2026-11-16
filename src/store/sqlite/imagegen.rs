//! Image generation persistence for SqliteStore (migration 0025): the job
//! ledger, the panel-editable settings row, and the per-group reset
//! watermark. Counting lives here and only here, so the MCP gate and the
//! panel usage view can never drift into two different answers.

use super::*;

fn map_image_job(row: sqlx::sqlite::SqliteRow) -> ImageJob {
    ImageJob {
        id: row.get("id"),
        bot_id: row.get("bot_id"),
        group_id: row.get("group_id"),
        requester_id: row.get("requester_id"),
        bucket: row.get("bucket"),
        prompt: row.get("prompt"),
        self_portrait: row.get::<i64, _>("self_portrait") != 0,
        caption: row.get("caption"),
        state: row.get("state"),
        file_path: row.get("file_path"),
        error_class: row.get("error_class"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

const IMAGE_JOB_COLUMNS: &str = "id, bot_id, group_id, requester_id, bucket, prompt, \
    self_portrait, caption, state, file_path, error_class, created_at, updated_at";

impl SqliteStore {
    /// The settings row plus the owner id the main process recorded at
    /// startup. A missing row (a database older than the migration, which
    /// cannot happen after `open_and_migrate`) degrades to the defaults.
    pub async fn imagegen_settings(&self) -> BotResult<(ImagegenSettings, String)> {
        let row = sqlx::query(
            "SELECT self_daily_cap, request_daily_cap, request_cooldown_minutes, owner_id, updated_at
             FROM imagegen_settings WHERE id = 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok((ImagegenSettings::default(), String::new()));
        };
        Ok((
            ImagegenSettings {
                self_daily_cap: row.get("self_daily_cap"),
                request_daily_cap: row.get("request_daily_cap"),
                request_cooldown_minutes: row.get("request_cooldown_minutes"),
                updated_at: row.get("updated_at"),
            },
            row.get("owner_id"),
        ))
    }

    pub async fn update_imagegen_settings(
        &self,
        settings: &ImagegenSettings,
        now: i64,
    ) -> BotResult<()> {
        sqlx::query(
            "UPDATE imagegen_settings SET self_daily_cap = ?, request_daily_cap = ?,
             request_cooldown_minutes = ?, updated_at = ? WHERE id = 1",
        )
        .bind(settings.self_daily_cap)
        .bind(settings.request_daily_cap)
        .bind(settings.request_cooldown_minutes)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record the owner id from the file config. Called by the main process
    /// at startup; the MCP server process has only the database, so this row
    /// is how the gate learns whose requests are exempt.
    pub async fn set_imagegen_owner(&self, owner_id: &str, now: i64) -> BotResult<()> {
        sqlx::query("UPDATE imagegen_settings SET owner_id = ?, updated_at = ? WHERE id = 1")
            .bind(owner_id)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// The group's reset watermark: jobs at or before it no longer count.
    pub async fn imagegen_reset_at(&self, bot_id: &str, group_id: &str) -> BotResult<i64> {
        let at: Option<i64> = sqlx::query_scalar(
            "SELECT reset_at FROM imagegen_resets WHERE bot_id = ? AND group_id = ?",
        )
        .bind(bot_id)
        .bind(group_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(at.unwrap_or(0))
    }

    pub async fn imagegen_reset(&self, bot_id: &str, group_id: &str, now: i64) -> BotResult<()> {
        sqlx::query(
            "INSERT INTO imagegen_resets (bot_id, group_id, reset_at) VALUES (?, ?, ?)
             ON CONFLICT (bot_id, group_id) DO UPDATE SET reset_at = excluded.reset_at",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Register and charge a live tool request under one SQLite write lock.
    /// Inner errors are policy refusals; database/busy failures remain retryable
    /// outer errors. No generation or network work is performed in this transaction.
    pub async fn register_image_job(
        &self,
        bot_id: &str,
        group_id: &str,
        turn_token: &str,
        requester_id: &str,
        bucket: &str,
        prompt: &str,
        self_portrait: bool,
        caption: Option<&str>,
        now: i64,
    ) -> BotResult<Result<String, &'static str>> {
        if bucket != "self" && bucket != "request" {
            return Ok(Err("invalid_bucket"));
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let live: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM armed_turns WHERE group_id = ?
             AND turn_token = ? AND expires_at >= ?)",
        )
        .bind(group_id)
        .bind(turn_token)
        .bind(now)
        .fetch_one(&mut *tx)
        .await?;
        if !live {
            return Ok(Err("window_closed"));
        }
        let enabled: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM groups WHERE bot_id = ? AND group_id = ?
             AND enabled = 1 AND paused = 0)",
        )
        .bind(bot_id)
        .bind(group_id)
        .fetch_one(&mut *tx)
        .await?;
        if !enabled {
            return Ok(Err("group_disabled"));
        }
        let settings = sqlx::query(
            "SELECT self_daily_cap, request_daily_cap, request_cooldown_minutes, owner_id
             FROM imagegen_settings WHERE id = 1",
        )
        .fetch_one(&mut *tx)
        .await?;
        let owner: String = settings.get("owner_id");
        let exempt = bucket == "request" && !owner.is_empty() && requester_id == owner;
        if !exempt {
            let reset: i64 = sqlx::query_scalar(
                "SELECT COALESCE((SELECT reset_at FROM imagegen_resets
                 WHERE bot_id = ? AND group_id = ?), 0)",
            )
            .bind(bot_id)
            .bind(group_id)
            .fetch_one(&mut *tx)
            .await?;
            let used: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM image_jobs WHERE bot_id = ? AND group_id = ?
                 AND bucket = ? AND created_at >= ? AND created_at > ?
                 AND (? = 'self' OR requester_id = ?)",
            )
            .bind(bot_id)
            .bind(group_id)
            .bind(bucket)
            .bind(super::super::shanghai_day_start(now))
            .bind(reset)
            .bind(bucket)
            .bind(requester_id)
            .fetch_one(&mut *tx)
            .await?;
            let cap: i64 = settings.get(if bucket == "self" {
                "self_daily_cap"
            } else {
                "request_daily_cap"
            });
            if used >= cap {
                return Ok(Err("daily_cap"));
            }
            if bucket == "request" {
                let last: Option<i64> = sqlx::query_scalar(
                    "SELECT MAX(created_at) FROM image_jobs WHERE bot_id = ? AND group_id = ?
                     AND bucket = 'request' AND requester_id = ?",
                )
                .bind(bot_id)
                .bind(group_id)
                .bind(requester_id)
                .fetch_one(&mut *tx)
                .await?;
                let cooldown: i64 = settings.get("request_cooldown_minutes");
                if last.is_some_and(|last| now.saturating_sub(last) < cooldown.saturating_mul(60)) {
                    return Ok(Err("cooldown"));
                }
            }
        }
        let id = format!("job_{}", uuid_like());
        sqlx::query(
            "INSERT INTO image_jobs
             (id, bot_id, group_id, requester_id, bucket, prompt, self_portrait,
              caption, state, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(&id)
        .bind(bot_id)
        .bind(group_id)
        .bind(requester_id)
        .bind(bucket)
        .bind(prompt)
        .bind(i64::from(self_portrait))
        .bind(caption)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Ok(id))
    }

    pub async fn insert_image_job(
        &self,
        bot_id: &str,
        group_id: &str,
        requester_id: &str,
        bucket: &str,
        prompt: &str,
        self_portrait: bool,
        caption: Option<&str>,
        now: i64,
    ) -> BotResult<String> {
        let id = format!("job_{}", uuid_like());
        sqlx::query(
            "INSERT INTO image_jobs
                (id, bot_id, group_id, requester_id, bucket, prompt, self_portrait,
                 caption, state, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
        )
        .bind(&id)
        .bind(bot_id)
        .bind(group_id)
        .bind(requester_id)
        .bind(bucket)
        .bind(prompt)
        .bind(i64::from(self_portrait))
        .bind(caption)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    /// Jobs at or after the day start `since`, strictly after the stored reset
    /// watermark. `requester` narrows to one person.
    pub async fn count_image_jobs(
        &self,
        bot_id: &str,
        group_id: &str,
        bucket: &str,
        requester: Option<&str>,
        since: i64,
    ) -> BotResult<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM image_jobs
             WHERE bot_id = ?1 AND group_id = ?2 AND bucket = ?3 AND created_at >= ?4
               AND created_at > COALESCE((SELECT reset_at FROM imagegen_resets
                   WHERE bot_id = ?1 AND group_id = ?2), 0)
               AND (?5 IS NULL OR requester_id = ?6)",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(bucket)
        .bind(since)
        .bind(requester)
        .bind(requester)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    /// Per-person request counts at or after day start `since`, strictly after
    /// the stored reset watermark, heaviest first.
    pub async fn image_requester_counts(
        &self,
        bot_id: &str,
        group_id: &str,
        since: i64,
    ) -> BotResult<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT requester_id, COUNT(*) AS n FROM image_jobs
             WHERE bot_id = ?1 AND group_id = ?2 AND bucket = 'request' AND created_at >= ?3
               AND created_at > COALESCE((SELECT reset_at FROM imagegen_resets
                   WHERE bot_id = ?1 AND group_id = ?2), 0)
             GROUP BY requester_id ORDER BY n DESC, requester_id",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("requester_id"), r.get::<i64, _>("n")))
            .collect())
    }

    pub async fn list_image_jobs(
        &self,
        bot_id: &str,
        group_id: &str,
        since: i64,
        limit: i64,
    ) -> BotResult<Vec<ImageJob>> {
        let rows = sqlx::query(&format!(
            "SELECT {IMAGE_JOB_COLUMNS} FROM image_jobs
             WHERE bot_id = ? AND group_id = ? AND created_at > ?
             ORDER BY created_at DESC, id DESC LIMIT ?"
        ))
        .bind(bot_id)
        .bind(group_id)
        .bind(since)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_image_job).collect())
    }

    pub async fn get_image_job(&self, id: &str) -> BotResult<Option<ImageJob>> {
        let row = sqlx::query(&format!(
            "SELECT {IMAGE_JOB_COLUMNS} FROM image_jobs WHERE id = ?"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(map_image_job))
    }

    /// Atomically queue a generated artifact and finish its generation ledger.
    /// A failed insert or cancelled transaction leaves `generated` retryable;
    /// neither a partial outbox nor a falsely succeeded job can become visible.
    pub async fn commit_generated_image(
        &self,
        job: &ImageJob,
        parts: &[crate::store::OutboxPart<'_>],
        file_path: &str,
        now: i64,
    ) -> BotResult<Vec<String>> {
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE image_jobs SET state = 'succeeded', error_class = NULL, updated_at = ?
             WHERE id = ? AND bot_id = ? AND group_id = ? AND state = 'generated' AND file_path = ?",
        ).bind(now).bind(&job.id).bind(&job.bot_id).bind(&job.group_id).bind(file_path)
            .execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(sqlx::Error::RowNotFound.into());
        }
        let mut ids = Vec::with_capacity(parts.len());
        for part in parts {
            ids.push(Self::insert_outbox_part(&mut tx, part).await?);
        }
        tx.commit().await?;
        Ok(ids)
    }

    /// Claim the oldest queued job or generated artifact awaiting its outbox.
    /// A generated job retains its file_path and must never invoke generation again.
    /// The guarded update keeps two executors from claiming the same job twice.
    pub async fn claim_next_image_job(&self, now: i64) -> BotResult<Option<ImageJob>> {
        let row = sqlx::query(&format!(
            "SELECT {IMAGE_JOB_COLUMNS} FROM image_jobs
             WHERE state IN ('queued', 'generated') ORDER BY created_at, id LIMIT 1"
        ))
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let job = map_image_job(row);
        let res = sqlx::query(
            "UPDATE image_jobs SET state = 'running', updated_at = ?
             WHERE id = ? AND state = ?",
        )
        .bind(now)
        .bind(&job.id)
        .bind(&job.state)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() != 1 {
            return Ok(None);
        }
        Ok(Some(ImageJob {
            state: "running".to_string(),
            updated_at: now,
            ..job
        }))
    }

    /// Return every `running` job to `queued` and report what was
    /// reclaimed. The executor is single-flight and runs one job at a
    /// time, so a row still marked `running` while it is between jobs
    /// can only be an orphan: the process (or the whole host) died
    /// mid-generation. Veda's 2026-11-11 shutdown stranded a job
    /// exactly like this, and it sat `running` until a human re-queued
    /// it by hand. Reclaiming unconditionally, rather than by age,
    /// keeps one rule for startup and between jobs alike: between
    /// jobs, `running` is never legitimate.
    pub async fn reclaim_running_image_jobs(&self, now: i64) -> BotResult<Vec<ImageJob>> {
        let rows = sqlx::query(&format!(
            "SELECT {IMAGE_JOB_COLUMNS} FROM image_jobs
             WHERE state = 'running' ORDER BY created_at, id"
        ))
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let jobs: Vec<ImageJob> = rows.into_iter().map(map_image_job).collect();
        sqlx::query(
            "UPDATE image_jobs SET state = 'queued', updated_at = ?
             WHERE state = 'running'",
        )
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(jobs)
    }

    /// Terminal cancellation preserves artifact ownership and the quota ledger.
    pub async fn cancel_image_job(&self, id: &str, now: i64) -> BotResult<()> {
        sqlx::query("UPDATE image_jobs SET state = 'cancelled', error_class = 'group_inactive', updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn finish_image_job(
        &self,
        id: &str,
        state: &str,
        file_path: Option<&str>,
        error_class: Option<&str>,
        now: i64,
    ) -> BotResult<()> {
        sqlx::query(
            "UPDATE image_jobs SET state = ?, file_path = ?, error_class = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(state)
        .bind(file_path)
        .bind(error_class)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Senders in the live generation's original batch or confirmed insert ids.
    pub async fn armed_sender_ids(
        &self,
        bot_id: &str,
        group_id: &str,
        token: &str,
    ) -> BotResult<Vec<String>> {
        let rows = sqlx::query(
            "SELECT DISTINCT p.sender_id FROM pending_events p JOIN armed_turns a ON a.group_id = p.group_id
             WHERE p.bot_id = ? AND p.group_id = ? AND a.turn_token = ? AND p.batch_id IS NOT NULL
             AND (p.batch_id = a.batch_id OR p.id IN (SELECT value FROM json_each(a.confirmed_event_ids)))",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(token)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get("sender_id")).collect())
    }
}
