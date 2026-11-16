//! Migration runner.
//!
//! Migrations are embedded so the container image never needs the source tree at
//! runtime. SQLx tracks applied versions in `_sqlx_migrations`.

use std::path::Path;

use sqlx::SqlitePool;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use crate::error::BotResult;

/// Embedded migrations from `migrations/`.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Open (creating if needed) the SQLite database with the operational pragmas the
/// design requires: WAL, foreign keys, a busy timeout and normal sync.
pub async fn open_pool(path: &Path) -> BotResult<SqlitePool> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(std::time::Duration::from_secs(10));

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    Ok(pool)
}

/// Run all pending migrations.
pub async fn run(pool: &SqlitePool) -> BotResult<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(|e| crate::error::BotError::Message(format!("数据库迁移失败: {e}")))?;
    Ok(())
}

/// Open and migrate in one step.
pub async fn open_and_migrate(path: &Path) -> BotResult<SqlitePool> {
    let pool = open_pool(path).await?;
    run(&pool).await?;
    Ok(pool)
}

/// Convenience for tests: an in-memory database is not usable here because SQLx
/// pools hand out different connections, so tests use a temp file instead.
pub async fn open_temp(dir: &Path) -> BotResult<SqlitePool> {
    open_and_migrate(&dir.join("bot.sqlite3")).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn through(version: i64) -> Migrator {
        Migrator {
            migrations: std::borrow::Cow::Owned(
                MIGRATOR
                    .iter()
                    .filter(|m| m.version <= version)
                    .cloned()
                    .collect(),
            ),
            ..Migrator::DEFAULT
        }
    }

    async fn populated_upgrade(version: i64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.sqlite3");
        let pool = open_pool(&path).await.unwrap();
        let old = through(version);
        old.run(&pool).await.unwrap();
        sqlx::raw_sql(
            "INSERT INTO groups(bot_id,group_id,created_at,updated_at) VALUES('b','g',1,2);
             INSERT INTO group_state(bot_id,group_id,topic_state,updated_at) VALUES('b','g','old topic',2);
             INSERT INTO memories(memory_ref,bot_id,group_id,content,kind,confidence,created_at,updated_at,archived_at)
             VALUES('active','b','g','retained active','fact',0.7,10,20,NULL),('retired','b','g','retained retired','fact',0.6,11,21,22);
             INSERT INTO armed_turns(group_id,turn_token,batch_id,armed_at,expires_at) VALUES('g','fixture-token','old-batch',10,100);
             INSERT INTO turn_submissions(id,group_id,turn_token,seq,payload,verdict,created_at)
             VALUES(41,'g','fixture-token',1,'{\"should_reply\":false}','accepted',12);
             INSERT INTO pending_events(id,bot_id,group_id,message_id,sender_id,received_at,normalized_payload)
             VALUES(51,'b','g','human-message','human',30,'{}');"
        ).execute(&pool).await.unwrap();
        if version >= 23 {
            sqlx::query("UPDATE memories SET slot='hobby',tags='[\"kept\"]',expires_at=999 WHERE memory_ref='active'").execute(&pool).await.unwrap();
        }
        if version >= 25 {
            sqlx::query("INSERT INTO image_jobs(id,bot_id,group_id,requester_id,bucket,prompt,state,created_at,updated_at) VALUES('image-old','b','g','human','request','retained prompt','queued',31,32)")
                .execute(&pool).await.unwrap();
        }
        let checksums: Vec<(i64, Vec<u8>)> =
            sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&pool)
                .await
                .unwrap();
        through(27).run(&pool).await.unwrap();
        let memories: Vec<(String,String,String,Option<i64>,i64)> = sqlx::query_as("SELECT memory_ref,content,state,archived_at,state_changed_at FROM memories ORDER BY memory_ref").fetch_all(&pool).await.unwrap();
        assert_eq!(
            memories,
            vec![
                (
                    "active".into(),
                    "retained active".into(),
                    "active".into(),
                    None,
                    20
                ),
                (
                    "retired".into(),
                    "retained retired".into(),
                    "archived".into(),
                    Some(22),
                    21
                )
            ]
        );
        let active: (String, String, Option<i64>) =
            sqlx::query_as("SELECT slot,tags,expires_at FROM memories WHERE memory_ref='active'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            active,
            if version >= 23 {
                ("hobby".into(), "[\"kept\"]".into(), Some(999))
            } else {
                ("".into(), "[]".into(), None)
            }
        );
        let submission: (i64, String, String) =
            sqlx::query_as("SELECT id,payload,verdict FROM turn_submissions")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            submission,
            (41, "{\"should_reply\":false}".into(), "accepted".into())
        );
        let caps: (i64,i64,i64) = sqlx::query_as("SELECT self_daily_cap,request_daily_cap,request_cooldown_minutes FROM imagegen_settings").fetch_one(&pool).await.unwrap();
        assert_eq!(caps, (15, 5, 10));
        if version >= 25 {
            let image: (String, String, String, i64, i64) =
                sqlx::query_as("SELECT id,prompt,state,created_at,updated_at FROM image_jobs")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                image,
                (
                    "image-old".into(),
                    "retained prompt".into(),
                    "queued".into(),
                    31,
                    32
                )
            );
        }
        sqlx::query("INSERT INTO sealed_topics(id,bot_id,group_id,slot,created_at) VALUES(61,'b','g','sealed fixture',40)").execute(&pool).await.unwrap();
        // Preserve the requested historical checkpoints, then exercise current
        // additive migrations too, with actual SQLx version/checksum metadata.
        run(&pool).await.unwrap();
        let retained_checksums: Vec<(i64, Vec<u8>)> = sqlx::query_as(
            "SELECT version,checksum FROM _sqlx_migrations WHERE version<=? ORDER BY version",
        )
        .bind(version)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(retained_checksums, checksums);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT source FROM pending_events WHERE id=51")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "human"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT confirmed_event_ids FROM armed_turns WHERE group_id='g'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "[]"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT slot FROM sealed_topics WHERE id=61")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "sealed fixture"
        );
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(&pool)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "ok"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count as usize, MIGRATOR.iter().count());
        assert!(
            matches!(
                old.run(&pool).await,
                Err(sqlx::migrate::MigrateError::VersionMissing(_))
            ),
            "an older binary must not silently accept unknown migrations"
        );
        if version == 17 {
            assert!(
                sqlx::query("SELECT resident_enabled,resident_fallback_enabled FROM groups")
                    .fetch_all(&pool)
                    .await
                    .is_err(),
                "removed columns make a blind code downgrade unsafe"
            );
        }
        pool.close().await;
        let reopened = open_and_migrate(&path).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memories")
                .fetch_one(&reopened)
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations")
                .fetch_one(&reopened)
                .await
                .unwrap(),
            count
        );
    }

    #[tokio::test]
    async fn populated_17_to_27_and_current_preserves_data_and_refuses_blind_downgrade() {
        populated_upgrade(17).await;
    }

    #[tokio::test]
    async fn populated_22_to_27_and_current_preserves_data_and_refuses_blind_downgrade() {
        populated_upgrade(22).await;
    }

    #[tokio::test]
    async fn populated_25_to_27_and_current_preserves_data_and_refuses_blind_downgrade() {
        populated_upgrade(25).await;
    }

    #[tokio::test]
    async fn migrations_apply_cleanly() {
        let dir = tempfile::tempdir().expect("临时目录");
        let pool = open_temp(dir.path()).await.expect("迁移应当成功");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE '_sqlx%'",
        )
        .fetch_one(&pool)
        .await
        .expect("查询表数量");
        assert!(count >= 11, "期望至少 11 张业务表，实际 {count}");
    }

    #[tokio::test]
    async fn migrations_are_idempotent() {
        let dir = tempfile::tempdir().expect("临时目录");
        let pool = open_temp(dir.path()).await.expect("首次迁移");
        run(&pool).await.expect("二次迁移应当幂等");
    }

    #[tokio::test]
    async fn foreign_keys_are_enforced() {
        let dir = tempfile::tempdir().expect("临时目录");
        let pool = open_temp(dir.path()).await.expect("迁移应当成功");
        let err = sqlx::query(
            "INSERT INTO group_state (bot_id, group_id, updated_at) VALUES ('b', 'g', 0)",
        )
        .execute(&pool)
        .await;
        assert!(err.is_err(), "缺失父行时外键应当拒绝插入");
    }
}
