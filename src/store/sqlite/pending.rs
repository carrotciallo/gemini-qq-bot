//! Pending-event queue for SqliteStore: insert, claim/release batches and orphan recovery.
//! Moved out of sqlite.rs verbatim (std-split-sqlite); logic unchanged.

use super::*;

impl SqliteStore {
    /// Insert a pending event. Returns `false` when the message was already stored.
    ///
    /// The argument list mirrors the row: splitting it into a struct would only move the
    /// same fields one level away, and every caller is internal.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_pending(
        &self,
        bot_id: &str,
        group_id: &str,
        message_id: &str,
        sender_id: &str,
        received_at: i64,
        payload: &str,
        image_refs: &str,
    ) -> BotResult<bool> {
        let res = sqlx::query(
            "INSERT INTO pending_events
                (bot_id, group_id, message_id, sender_id, received_at, normalized_payload, image_refs)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (bot_id, message_id) DO NOTHING",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(message_id)
        .bind(sender_id)
        .bind(received_at)
        .bind(payload)
        .bind(image_refs)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    /// Create an already-claimed synthetic opening in one statement. There is
    /// never an unclaimed interval for intake to observe or a follow-up lookup
    /// whose failure can strand an ordinary-looking message.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_spontaneous_pending(
        &self,
        bot_id: &str,
        group_id: &str,
        message_id: &str,
        received_at: i64,
        payload: &str,
        batch_id: &str,
    ) -> BotResult<Option<i64>> {
        Ok(sqlx::query_scalar(
            "INSERT INTO pending_events
             (bot_id, group_id, message_id, sender_id, received_at,
              normalized_payload, image_refs, batch_id, source)
             VALUES (?, ?, ?, ?, ?, ?, '[]', ?, 'spontaneous')
             ON CONFLICT (bot_id, message_id) DO NOTHING RETURNING id",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(message_id)
        .bind(bot_id)
        .bind(received_at)
        .bind(payload)
        .bind(batch_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Idempotent final cleanup, including early-return and inactive-group paths.
    /// The persisted source, not job metadata, protects inserted human messages.
    pub async fn cancel_spontaneous_events(&self, ids: &[i64]) -> BotResult<()> {
        let mut tx = self.pool.begin().await?;
        for id in ids {
            sqlx::query("DELETE FROM pending_events WHERE id = ? AND source = 'spontaneous'")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn pending_for_group(
        &self,
        bot_id: &str,
        group_id: &str,
    ) -> BotResult<Vec<PendingEventRow>> {
        let rows = sqlx::query(
            "SELECT id, bot_id, group_id, message_id, sender_id, received_at,
                    normalized_payload, image_refs, batch_id
             FROM pending_events WHERE bot_id = ? AND group_id = ? AND batch_id IS NULL
               AND source = 'human' ORDER BY id",
        )
        .bind(bot_id)
        .bind(group_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_pending).collect())
    }

    pub async fn pending_count(&self, bot_id: &str, group_id: &str) -> BotResult<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pending_events WHERE bot_id = ? AND group_id = ? AND batch_id IS NULL AND source = 'human'",
        )
        .bind(bot_id)
        .bind(group_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    pub async fn pending_total(&self, bot_id: &str) -> BotResult<i64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pending_events WHERE bot_id = ? AND batch_id IS NULL AND source = 'human'",
        )
        .bind(bot_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(n)
    }

    /// Tag rows with a batch id inside one transaction so a crash can never leave a
    /// row both claimed and unclaimed.
    ///
    /// Returns the ids this caller actually took. A 0-row `UPDATE` is not an
    /// error: two callers (flush vs resident insert) can race on the same
    /// unclaimed set, and the loser must see an empty vec rather than `Ok(())`
    /// so it does not enqueue a follow-up job on already-consumed events.
    pub async fn claim_batch(&self, ids: &[i64], batch_id: &str) -> BotResult<Vec<i64>> {
        let mut tx = self.pool.begin().await?;
        let mut claimed = Vec::new();
        for id in ids {
            let result = sqlx::query(
                "UPDATE pending_events SET batch_id = ? WHERE id = ? AND batch_id IS NULL AND source = 'human'",
            )
            .bind(batch_id)
            .bind(id)
            .execute(&mut *tx)
            .await?;
            if result.rows_affected() > 0 {
                claimed.push(*id);
            }
        }
        tx.commit().await?;
        Ok(claimed)
    }

    /// Delete delivered events, dropping message bodies as required.
    pub async fn delete_pending(&self, ids: &[i64]) -> BotResult<()> {
        let mut tx = self.pool.begin().await?;
        for id in ids {
            sqlx::query("DELETE FROM pending_events WHERE id = ?")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Release human inputs, but consume failed synthetic openings.
    pub async fn release_batch(&self, batch_id: &str) -> BotResult<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM pending_events WHERE batch_id = ? AND source = 'spontaneous'")
            .bind(batch_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "UPDATE pending_events SET batch_id = NULL WHERE batch_id = ? AND source = 'human'",
        )
        .bind(batch_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Release human rows back to the pool; consume synthetic rows, by event id.
    ///
    /// Batch-scoped [`Self::release_batch`] cannot express this once queued batches
    /// have been merged: the surviving job carries the union of event ids, whose rows
    /// keep their original batch tags, so releasing by the surviving tag alone would
    /// strand the folded-in rows as claimed forever.
    pub async fn release_events(&self, ids: &[i64]) -> BotResult<()> {
        let mut tx = self.pool.begin().await?;
        for id in ids {
            sqlx::query("DELETE FROM pending_events WHERE id = ? AND source = 'spontaneous'")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            sqlx::query(
                "UPDATE pending_events SET batch_id = NULL WHERE id = ? AND source = 'human'",
            )
            .bind(id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Settle a cancelled turn without replaying a possibly delivered reply.
    /// Insertion tags are recorded before claim awaits, including unconfirmed input.
    pub async fn expire_turn(
        &self,
        bot_id: &str,
        group_id: &str,
        batch_id: &str,
        original_ids: &[i64],
        insert_batches: &[String],
        now: i64,
    ) -> BotResult<()> {
        let mut tx = self.pool.begin().await?;
        // Take the write lock before reading ownership/delivery state.
        sqlx::query("DELETE FROM armed_turns WHERE group_id = ? AND batch_id = ?")
            .bind(group_id)
            .bind(batch_id)
            .execute(&mut *tx)
            .await?;
        let queued: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM outbox WHERE bot_id = ? AND group_id = ? AND batch_id = ?)",
        ).bind(bot_id).bind(group_id).bind(batch_id).fetch_one(&mut *tx).await?;
        sqlx::query(
            "UPDATE outbox SET state = 'unknown', updated_at = ? WHERE bot_id = ? AND group_id = ?
             AND batch_id = ? AND state IN ('pending', 'sending')",
        )
        .bind(now)
        .bind(bot_id)
        .bind(group_id)
        .bind(batch_id)
        .execute(&mut *tx)
        .await?;
        let mut ids = original_ids.to_vec();
        for owner in insert_batches {
            let claimed: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM pending_events WHERE bot_id = ? AND group_id = ? AND batch_id = ?",
            )
            .bind(bot_id)
            .bind(group_id)
            .bind(owner)
            .fetch_all(&mut *tx)
            .await?;
            ids.extend(claimed);
        }
        ids.sort_unstable();
        ids.dedup();
        for id in ids {
            // If a reply was queued, conservatively consume its inputs: resuming
            // them as another turn could duplicate a send whose ack was cancelled.
            sqlx::query(
                "DELETE FROM pending_events WHERE bot_id = ? AND group_id = ? AND id = ?
                 AND (? != 0 OR source = 'spontaneous')",
            )
            .bind(bot_id)
            .bind(group_id)
            .bind(id)
            .bind(queued)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE pending_events SET batch_id = NULL WHERE bot_id = ? AND group_id = ?
                 AND id = ? AND source = 'human'",
            )
            .bind(bot_id)
            .bind(group_id)
            .bind(id)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "UPDATE agent_calls SET state = 'failed', error_class = 'turn_deadline', finished_at = ?
             WHERE bot_id = ? AND group_id = ? AND batch_id = ? AND state = 'running'",
        ).bind(now).bind(bot_id).bind(group_id).bind(batch_id).execute(&mut *tx).await?;
        sqlx::query("UPDATE groups SET agy_conversation_id = NULL, agy_turn_count = 0, updated_at = ? WHERE bot_id = ? AND group_id = ?")
            .bind(now).bind(bot_id).bind(group_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Release every claimed pending row back to the unclaimed pool, per group.
    ///
    /// Startup crash recovery, called once from `App::build` after
    /// [`Self::reconcile_calls`]: at that point no call is running and the
    /// scheduler's lanes are empty, so a non-NULL `batch_id` can only be a claim
    /// the previous process made and never finished. Left tagged, those rows are
    /// invisible to the flush tick (`pending_for_group` only reads unclaimed
    /// rows), so every restart would silently swallow whatever was claimed but
    /// not yet executed. Returns `(group_id, released)` pairs for the audit log.
    pub async fn release_orphaned_claims(&self, bot_id: &str) -> BotResult<Vec<(String, u64)>> {
        // No spontaneous job survives a process restart. Include unclaimed rows
        // left by the pre-source producer; never return them to ordinary intake.
        sqlx::query("DELETE FROM pending_events WHERE bot_id = ? AND source = 'spontaneous'")
            .bind(bot_id)
            .execute(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT group_id, COUNT(*) AS n FROM pending_events
             WHERE bot_id = ? AND batch_id IS NOT NULL
             GROUP BY group_id ORDER BY group_id",
        )
        .bind(bot_id)
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::new();
        for row in rows {
            let group_id: String = row.get("group_id");
            let n: i64 = row.get("n");
            sqlx::query(
                "UPDATE pending_events SET batch_id = NULL
                 WHERE bot_id = ? AND group_id = ? AND batch_id IS NOT NULL",
            )
            .bind(bot_id)
            .bind(&group_id)
            .execute(&self.pool)
            .await?;
            out.push((group_id, n as u64));
        }
        Ok(out)
    }

    /// Drop accumulated bodies for a group that is disabled or paused.
    pub async fn drop_pending(&self, bot_id: &str, group_id: &str) -> BotResult<u64> {
        let res = sqlx::query("DELETE FROM pending_events WHERE bot_id = ? AND group_id = ?")
            .bind(bot_id)
            .bind(group_id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}

#[cfg(test)]
mod tests;
