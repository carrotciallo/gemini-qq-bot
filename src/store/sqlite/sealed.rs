//! Sealed-topic persistence ([seal-1]): the operator's per-group list of
//! dead topics. Kept deliberately tiny — a handful of rows per group,
//! read whole at every enforcement point.

use super::*;

impl SqliteStore {
    /// All sealed topics for one group, oldest first.
    pub async fn list_sealed_topics(
        &self,
        bot_id: &str,
        group_id: &str,
    ) -> BotResult<Vec<SealedTopic>> {
        let rows = sqlx::query(
            "SELECT id, bot_id, group_id, keywords, slot, note, created_at
             FROM sealed_topics WHERE bot_id = ? AND group_id = ? ORDER BY id",
        )
        .bind(bot_id)
        .bind(group_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(map_sealed_topic).collect())
    }

    /// Add one sealed topic; returns its row id.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_sealed_topic(
        &self,
        bot_id: &str,
        group_id: &str,
        keywords: &[String],
        slot: &str,
        note: &str,
        created_by: &str,
        now: i64,
    ) -> BotResult<i64> {
        let keywords_json = serde_json::to_string(keywords).unwrap_or_else(|_| "[]".into());
        let result = sqlx::query(
            "INSERT INTO sealed_topics (bot_id, group_id, keywords, slot, note, created_at, created_by)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(bot_id)
        .bind(group_id)
        .bind(keywords_json)
        .bind(slot)
        .bind(note)
        .bind(now)
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    /// Remove one sealed topic of this group; true when a row went away.
    pub async fn remove_sealed_topic(
        &self,
        bot_id: &str,
        group_id: &str,
        id: i64,
    ) -> BotResult<bool> {
        let result =
            sqlx::query("DELETE FROM sealed_topics WHERE bot_id = ? AND group_id = ? AND id = ?")
                .bind(bot_id)
                .bind(group_id)
                .bind(id)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }
}

fn map_sealed_topic(r: sqlx::sqlite::SqliteRow) -> SealedTopic {
    use sqlx::Row;
    let keywords_raw: String = r.get("keywords");
    SealedTopic {
        id: r.get("id"),
        bot_id: r.get("bot_id"),
        group_id: r.get("group_id"),
        keywords: serde_json::from_str(&keywords_raw).unwrap_or_default(),
        slot: r.get("slot"),
        note: r.get("note"),
        created_at: r.get("created_at"),
    }
}
