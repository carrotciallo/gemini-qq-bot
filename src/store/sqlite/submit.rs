//! MCP submit channel persistence: armed turns and turn submissions for SqliteStore.
//! Moved out of sqlite.rs verbatim (std-split-sqlite); logic unchanged.

use super::*;

// ------------------------------------------------------------ MCP submit

impl SqliteStore {
    /// Arm a turn for `submit_reply`. One live arm per group; re-arming
    /// replaces the previous row and returns it, so the caller can audit an
    /// overwritten arm whose submissions were never consumed.
    pub async fn arm_turn(
        &self,
        group_id: &str,
        turn_token: &str,
        batch_id: &str,
        armed_at: i64,
        expires_at: i64,
    ) -> BotResult<Option<ArmedTurn>> {
        let previous = self.armed_turn(group_id).await?;
        sqlx::query(
            "INSERT INTO armed_turns (group_id, turn_token, batch_id, armed_at, expires_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(group_id) DO UPDATE SET
                turn_token = excluded.turn_token,
                batch_id = excluded.batch_id,
                armed_at = excluded.armed_at,
                expires_at = excluded.expires_at,
                confirmed_event_ids = '[]'",
        )
        .bind(group_id)
        .bind(turn_token)
        .bind(batch_id)
        .bind(armed_at)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(previous)
    }

    /// Publish a confirmed generation and its exact input coverage atomically.
    /// Every supplied row must still be claimed by this bot and this group.
    pub async fn arm_confirmed_turn(
        &self,
        bot_id: &str,
        arm: &ArmedTurn,
        event_ids: &[i64],
    ) -> BotResult<bool> {
        let ids = serde_json::json!(event_ids).to_string();
        let result = sqlx::query(
            "INSERT INTO armed_turns (group_id, turn_token, batch_id, armed_at, expires_at, confirmed_event_ids)
             SELECT ?1, ?2, ?3, ?4, ?5, ?6
             WHERE json_array_length(?6) > 0 AND
                (SELECT COUNT(*) FROM pending_events WHERE bot_id = ?7 AND group_id = ?1
                 AND batch_id IS NOT NULL AND id IN (SELECT value FROM json_each(?6))) = json_array_length(?6)
             ON CONFLICT(group_id) DO UPDATE SET turn_token = excluded.turn_token,
                batch_id = excluded.batch_id, armed_at = excluded.armed_at,
                expires_at = excluded.expires_at, confirmed_event_ids = excluded.confirmed_event_ids",
        )
        .bind(&arm.group_id).bind(&arm.turn_token).bind(&arm.batch_id)
        .bind(arm.armed_at).bind(arm.expires_at).bind(ids).bind(bot_id)
        .execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    /// The group's live arm, if any.
    pub async fn armed_turn(&self, group_id: &str) -> BotResult<Option<ArmedTurn>> {
        let row = sqlx::query(
            "SELECT group_id, turn_token, batch_id, armed_at, expires_at
             FROM armed_turns WHERE group_id = ?",
        )
        .bind(group_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| ArmedTurn {
            group_id: r.get("group_id"),
            turn_token: r.get("turn_token"),
            batch_id: r.get("batch_id"),
            armed_at: r.get("armed_at"),
            expires_at: r.get("expires_at"),
        }))
    }

    /// Record one call; returns its per-token sequence and persisted acceptance.
    /// Revalidate the live input token inside the same write statement: requests
    /// whose initial MCP check raced an insert are durably recorded as rejected.
    pub async fn record_submission(
        &self,
        group_id: &str,
        turn_token: &str,
        payload: &str,
        verdict: &str,
        reason: &str,
        now: i64,
    ) -> BotResult<(i64, bool)> {
        let row = sqlx::query(
            "WITH checked AS (
                SELECT NOT (?4 = 'accepted' AND NOT EXISTS (
                    SELECT 1 FROM armed_turns WHERE group_id = ?1
                    AND turn_token = ?2 AND expires_at >= ?6
                )) AS ok
             )
             INSERT INTO turn_submissions
                (group_id, turn_token, seq, payload, verdict, reason, created_at)
             SELECT ?1, ?2,
                (SELECT COALESCE(MAX(seq), 0) + 1 FROM turn_submissions WHERE turn_token = ?2),
                ?3, CASE WHEN ok THEN ?4 ELSE 'rejected' END,
                CASE WHEN ok THEN ?5 ELSE 'input generation changed or submission window closed' END,
                ?6 FROM checked
             RETURNING seq, verdict",
        )
        .bind(group_id)
        .bind(turn_token)
        .bind(payload)
        .bind(verdict)
        .bind(reason)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Ok((
            row.get("seq"),
            row.get::<String, _>("verdict") == "accepted",
        ))
    }

    /// Every recorded call under a token, in call order.
    pub async fn submissions_for(&self, turn_token: &str) -> BotResult<Vec<TurnSubmission>> {
        let rows = sqlx::query(
            "SELECT seq, payload, verdict, reason, created_at
             FROM turn_submissions WHERE turn_token = ? ORDER BY seq ASC, id ASC",
        )
        .bind(turn_token)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| TurnSubmission {
                seq: r.get("seq"),
                payload: r.get("payload"),
                verdict: r.get("verdict"),
                reason: r.get("reason"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    /// Drop a group's arm once its turn is over. Token-checked, so a stale
    /// disarm can never kill a newer turn's arm.
    pub async fn disarm_turn(&self, group_id: &str, turn_token: &str) -> BotResult<()> {
        sqlx::query("DELETE FROM armed_turns WHERE group_id = ? AND turn_token = ?")
            .bind(group_id)
            .bind(turn_token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
