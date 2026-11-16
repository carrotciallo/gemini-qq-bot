use super::*;

#[tokio::test]
async fn spontaneous_source_creation_is_claimed_atomic_and_cancellation_keeps_humans() {
    let dir = tempfile::tempdir().unwrap();
    let pool = crate::store::migrations::open_temp(dir.path())
        .await
        .unwrap();
    let store = SqliteStore::new(pool);
    store.ensure_group("b", "g", 1).await.unwrap();
    store
        .insert_pending("b", "g", "human", "u", 1, "{}", "[]")
        .await
        .unwrap();
    let human = store.pending_for_group("b", "g").await.unwrap()[0].id;
    store.claim_batch(&[human], "mixed").await.unwrap();
    let synthetic = store
        .insert_spontaneous_pending("b", "g", "opening", 1, "{}", "mixed")
        .await
        .unwrap()
        .unwrap();
    let row: (String, String) =
        sqlx::query_as("SELECT source, batch_id FROM pending_events WHERE id = ?")
            .bind(synthetic)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(row, ("spontaneous".into(), "mixed".into()));
    assert!(store.pending_for_group("b", "g").await.unwrap().is_empty());
    assert!(
        store
            .insert_spontaneous_pending("b", "g", "opening", 1, "{}", "duplicate")
            .await
            .unwrap()
            .is_none()
    );
    // Use a persistent trigger: the pool may choose another connection.
    sqlx::query("CREATE TRIGGER refuse_opening_all BEFORE INSERT ON pending_events WHEN NEW.message_id = 'fault' BEGIN SELECT RAISE(ABORT, 'test'); END")
        .execute(store.pool()).await.unwrap();
    assert!(
        store
            .insert_spontaneous_pending("b", "g", "fault", 1, "{}", "fault")
            .await
            .is_err()
    );
    let faults: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pending_events WHERE message_id = 'fault'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(faults, 0);
    // Even accidental loss of the claim cannot make a synthetic row claimable.
    sqlx::query("UPDATE pending_events SET batch_id = NULL WHERE id = ?")
        .bind(synthetic)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(
        store
            .claim_batch(&[synthetic], "replay")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.pending_total("b").await.unwrap(), 0);
    store
        .cancel_spontaneous_events(&[human, synthetic])
        .await
        .unwrap();
    store.release_batch("mixed").await.unwrap();
    assert_eq!(
        store.pending_for_group("b", "g").await.unwrap()[0].id,
        human
    );
}

#[tokio::test]
async fn spontaneous_source_upgrade_recovers_legacy_rows_without_prefix_false_positives() {
    let dir = tempfile::tempdir().unwrap();
    let pool = crate::store::migrations::open_pool(&dir.path().join("old.sqlite3"))
        .await
        .unwrap();
    for migration in crate::store::migrations::MIGRATOR
        .iter()
        .filter(|m| m.version < 28)
    {
        sqlx::raw_sql(&migration.sql).execute(&pool).await.unwrap();
    }
    let store = SqliteStore::new(pool);
    store.ensure_group("b", "g", 1).await.unwrap();
    for (id, sender, role, claimed) in [
        ("spontaneous-100-123456", "b", "bot", true),
        ("spontaneous-101-123456", "b", "bot", false),
        ("spontaneous-102-123456", "u", "member", true),
        ("normal-bot-message", "b", "bot", true),
    ] {
        let payload =
            serde_json::json!({"message_id": id, "sender_id": sender, "speaker_role": role});
        store
            .insert_pending("b", "g", id, sender, 1, &payload.to_string(), "[]")
            .await
            .unwrap();
        if claimed {
            sqlx::query("UPDATE pending_events SET batch_id = 'old' WHERE message_id = ?")
                .bind(id)
                .execute(store.pool())
                .await
                .unwrap();
        }
    }
    sqlx::raw_sql(include_str!(
        "../../../../migrations/0028_pending_source.sql"
    ))
    .execute(store.pool())
    .await
    .unwrap();
    store.release_orphaned_claims("b").await.unwrap();
    let rows = store.pending_for_group("b", "g").await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.message_id.as_str())
            .collect::<Vec<_>>(),
        ["spontaneous-102-123456", "normal-bot-message"]
    );
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
}
