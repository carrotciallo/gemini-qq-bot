//! One-shot migration of pre-v1 memory rows to schema v1 (`--migrate-memories-v1`).
//!
//! Existing rows carry no `slot`/`tags` and no expiry. This tool asks the aux
//! model (hy3) to label them in batches — slot, tags, ttl per row, content never
//! touched — and either reports what *would* change (the default dry-run) or
//! writes the labels back (`--apply`, after an automatic timestamped backup).
//!
//! Safety rails, in code rather than in the runbook:
//!
//! * The production database paths are refused outright, in both the host and
//!   container spellings. The intended target is always a copy.
//! * A dry-run performs no writes at all — not even a migration bookkeeping row
//!   beyond the schema migration the caller already applied by opening the DB.
//! * A garbled model batch fails only its own rows; they land in the report's
//!   failure list with the reason, and the run continues.
//! * Rows the model would already expire (short-ttl kinds whose computed expiry
//!   is in the past) are only *listed*: retiring them is the expire sweep's job
//!   on the first fold cycle after go-live, never this tool's.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;

use crate::memory::compaction::SummaryModel;
use crate::memory::schema::{MemoryV1Fields, Ttl, sanitize_slot_tags, tags_column};
use crate::store::{MemoryRecord, SqliteStore};

/// Rows per model call: large enough to amortise the round trip, small enough
/// that one garbled reply cannot fail the whole run.
pub const BATCH_SIZE: usize = 20;

/// The production database locations this tool must never touch.
pub const PRODUCTION_DB_PATHS: &[&str] = &[
    "/home/carrot/gemini-qq-bot/runtime/data/bot.sqlite3",
    "/data/bot.sqlite3",
];

/// True when `path` names a production database (after canonicalisation when
/// the file exists, so a symlink cannot sneak past the check).
pub fn is_production_path(path: &Path) -> bool {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    PRODUCTION_DB_PATHS
        .iter()
        .any(|p| Path::new(p) == canonical.as_path() || Path::new(p) == path)
}

/// One row the model could not label, with the reason.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MigrationFailure {
    pub memory_ref: String,
    pub reason: String,
}

/// The full outcome of one run; serialised to the `--out` JSON report.
#[derive(Debug, Clone, Serialize, Default)]
pub struct MigrationReport {
    pub applied: bool,
    pub total_rows: usize,
    pub active_rows: usize,
    /// Active rows that still had an empty slot and were sent to the model.
    pub candidates: usize,
    pub converted: usize,
    pub failed: Vec<MigrationFailure>,
    /// ttl chosen per kind, e.g. `{"topic": {"48h": 12}}`.
    pub ttl_by_kind: BTreeMap<String, BTreeMap<String, usize>>,
    /// Most frequent slots across converted rows (top 30).
    pub top_slots: Vec<(String, usize)>,
    /// Converted rows whose computed expiry is already past: the first expire
    /// sweep after go-live would retire exactly these.
    pub would_expire: usize,
    /// The backup written before an `--apply`, when one happened.
    pub backup_path: Option<String>,
}

/// One labelled row, ready to write back.
#[derive(Debug, Clone, PartialEq)]
struct Labelled {
    memory_ref: String,
    fields: MemoryV1Fields,
    expires_at: Option<i64>,
}

/// Render one batch prompt. Only aliases and row content leave Rust — never a
/// raw user id (subjects render as `uN` / `group` / `bot`).
fn render_batch_prompt(
    rows: &[MemoryRecord],
    aliases: &[(String, String)],
    bot_id: &str,
) -> String {
    let mut out = String::new();
    out.push_str(
        "你在给一个 QQ 群的长期记忆条目补结构化标签。逐条阅读下面的记忆，只输出 JSON：\n\
{\"labels\":[{\"ref\":\"mem_xxx\",\"slot\":\"<类目标签>\",\"tags\":[\"<≤16字，最多5个>\"],\"ttl\":\"permanent|30d|7d|48h\"}]}\n\
规则：不改原文、不新增事实；slot 是给相似记忆分组用的类目名（2–10 字的名词短语，如「高达模型」「骑行安排」「考试」「游戏战绩」），不是这条记忆的摘要——禁止把原文改写成 slot；本批里话题相近的条目必须共用同一个 slot，拿不准就用更宽泛的类目；ttl 按内容时效判断——闲聊话题 48h、已发生的单次事件 7d、可能过期的计划/状态 30d、稳定偏好与长期事实 permanent。\n\n",
    );
    for row in rows {
        let subject = match &row.subject_user_id {
            None => "group".to_string(),
            Some(id) if id == bot_id => "bot".to_string(),
            Some(id) => aliases
                .iter()
                .find(|(user_id, _)| user_id == id)
                .map(|(_, alias)| alias.clone())
                .unwrap_or_else(|| "group".to_string()),
        };
        out.push_str(&format!(
            "- {} [{}] ({}) {}\n",
            row.memory_ref, subject, row.kind, row.content
        ));
    }
    out
}

/// Parse one batch reply into labels for the rows that were asked about.
///
/// Strict per row: unknown refs, duplicates and unparseable rows are failures,
/// not guesses. Sanitisation of slot/tags follows the schema rules.
fn parse_labels(raw: &str, batch: &[MemoryRecord]) -> (Vec<Labelled>, Vec<MigrationFailure>) {
    let mut labelled = Vec::new();
    let mut failed = Vec::new();
    let body = crate::memory::extract::json_body(raw);
    let items = body
        .as_ref()
        .and_then(|b| b.get("labels"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let mut seen: Vec<String> = Vec::new();
    for item in &items {
        let Some(memory_ref) = item.get("ref").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(row) = batch.iter().find(|r| r.memory_ref == memory_ref) else {
            failed.push(MigrationFailure {
                memory_ref: memory_ref.to_string(),
                reason: "unknown_ref".into(),
            });
            continue;
        };
        if seen.iter().any(|r| r == memory_ref) {
            failed.push(MigrationFailure {
                memory_ref: memory_ref.to_string(),
                reason: "duplicate_ref".into(),
            });
            continue;
        }
        seen.push(memory_ref.to_string());
        let raw_slot = item
            .get("slot")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let raw_tags: Vec<String> = item
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let (slot, tags) = sanitize_slot_tags(&raw_slot, &raw_tags);
        if slot.is_empty() {
            failed.push(MigrationFailure {
                memory_ref: memory_ref.to_string(),
                reason: "empty_slot".into(),
            });
            continue;
        }
        let ttl = item
            .get("ttl")
            .and_then(|v| v.as_str())
            .and_then(Ttl::parse)
            .unwrap_or_else(|| Ttl::default_for_kind(&row.kind));
        let fields = MemoryV1Fields {
            slot,
            tags,
            ttl: Some(ttl),
            expires_override: None,
        };
        // The expiry clock starts at the row's own creation, not at migration
        // time: a topic created three days ago is already dead, and resurrecting
        // it for a fresh 48h would defeat the migration's purpose.
        let expires_at = ttl.expires_at(row.created_at);
        labelled.push(Labelled {
            memory_ref: memory_ref.to_string(),
            fields,
            expires_at,
        });
    }
    for row in batch {
        if !seen.iter().any(|r| r == &row.memory_ref) {
            failed.push(MigrationFailure {
                memory_ref: row.memory_ref.clone(),
                reason: "missing_from_reply".into(),
            });
        }
    }
    (labelled, failed)
}

/// Run the migration over every group in the store.
pub async fn run_migration(
    store: &SqliteStore,
    model: &dyn SummaryModel,
    apply: bool,
    now: i64,
) -> MigrationReport {
    let mut report = MigrationReport {
        applied: apply,
        ..MigrationReport::default()
    };
    let groups: Vec<(String, String)> = sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT bot_id, group_id FROM memories ORDER BY bot_id, group_id",
    )
    .fetch_all(store.pool())
    .await
    .unwrap_or_default();

    let mut pending_writes: Vec<(String, String, Labelled)> = Vec::new();
    let mut slot_counts: BTreeMap<String, usize> = BTreeMap::new();

    for (bot_id, group_id) in &groups {
        let rows = store
            .list_memories_page(bot_id, group_id, 100_000, 0)
            .await
            .unwrap_or_default();
        report.total_rows += rows.len();
        let members = store
            .list_members(bot_id, group_id)
            .await
            .unwrap_or_default();
        let aliases: Vec<(String, String)> = members
            .iter()
            .map(|m| (m.user_id.clone(), m.speaker_ref.clone()))
            .collect();
        let active: Vec<MemoryRecord> = rows.into_iter().filter(|r| !r.is_archived()).collect();
        report.active_rows += active.len();
        let candidates: Vec<MemoryRecord> =
            active.into_iter().filter(|r| r.slot.is_empty()).collect();
        report.candidates += candidates.len();

        for batch in candidates.chunks(BATCH_SIZE) {
            let prompt = render_batch_prompt(batch, &aliases, bot_id);
            let raw = match model.summarise(prompt).await {
                Ok(raw) => raw,
                Err(class) => {
                    for row in batch {
                        report.failed.push(MigrationFailure {
                            memory_ref: row.memory_ref.clone(),
                            reason: format!("model_call:{}", class.as_str()),
                        });
                    }
                    continue;
                }
            };
            let (labelled, mut failures) = parse_labels(&raw, batch);
            report.failed.append(&mut failures);
            for label in labelled {
                report.converted += 1;
                *slot_counts.entry(label.fields.slot.clone()).or_default() += 1;
                let ttl_str = label
                    .fields
                    .ttl
                    .map(|t| t.as_str())
                    .unwrap_or("permanent")
                    .to_string();
                let kind = batch
                    .iter()
                    .find(|r| r.memory_ref == label.memory_ref)
                    .map(|r| r.kind.clone())
                    .unwrap_or_default();
                *report
                    .ttl_by_kind
                    .entry(kind)
                    .or_default()
                    .entry(ttl_str)
                    .or_default() += 1;
                if matches!(label.expires_at, Some(at) if at <= now) {
                    report.would_expire += 1;
                }
                pending_writes.push((bot_id.clone(), group_id.clone(), label));
            }
        }
    }

    let mut top: Vec<(String, usize)> = slot_counts.into_iter().collect();
    top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    top.truncate(30);
    report.top_slots = top;

    if apply {
        for (bot_id, group_id, label) in &pending_writes {
            let _ = sqlx::query(
                "UPDATE memories SET slot = ?, tags = ?, expires_at = ?
                 WHERE bot_id = ? AND group_id = ? AND memory_ref = ? AND archived_at IS NULL",
            )
            .bind(&label.fields.slot)
            .bind(tags_column(&label.fields.tags))
            .bind(label.expires_at)
            .bind(bot_id)
            .bind(group_id)
            .bind(&label.memory_ref)
            .execute(store.pool())
            .await;
        }
    }
    report
}

/// The backup path an `--apply` run writes first: `<db>.bak-migrate-v1-<ts>`.
pub fn backup_path_for(db_path: &Path, now: i64) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(format!(".bak-migrate-v1-{now}"));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorClass;

    struct FakeModel {
        reply: String,
    }

    #[async_trait::async_trait]
    impl SummaryModel for FakeModel {
        async fn summarise(&self, _prompt: String) -> Result<String, ErrorClass> {
            Ok(self.reply.clone())
        }
    }

    struct BoomModel;

    #[async_trait::async_trait]
    impl SummaryModel for BoomModel {
        async fn summarise(&self, _prompt: String) -> Result<String, ErrorClass> {
            Err(ErrorClass::AuxFailed)
        }
    }

    async fn store_with_rows() -> (tempfile::TempDir, Arc<SqliteStore>) {
        let dir = tempfile::tempdir().expect("tmp");
        let pool = crate::store::migrations::open_temp(dir.path())
            .await
            .expect("迁移");
        let store = Arc::new(SqliteStore::new(pool));
        store.ensure_group("bot", "100", 0).await.expect("建群");
        for (content, kind) in [
            ("群里在聊高达新作", "topic"),
            ("他下个月搬去杭州", "fact"),
            ("面板写的常驻说明", "fact"),
        ] {
            let origin = if content.starts_with("面板") {
                crate::store::origin::PANEL
            } else {
                crate::store::origin::HY3
            };
            store
                .insert_memory_with_origin(
                    "bot", "100", None, content, kind, "[]", 0.6, origin, 100,
                )
                .await
                .expect("写行");
        }
        (dir, store)
    }

    fn reply_for(refs: &[&str]) -> String {
        let labels: Vec<serde_json::Value> = refs
            .iter()
            .map(|r| {
                serde_json::json!({
                    "ref": r, "slot": "话题标签", "tags": ["高达"], "ttl": "48h"
                })
            })
            .collect();
        serde_json::json!({ "labels": labels }).to_string()
    }

    #[tokio::test]
    async fn dry_run_writes_nothing_and_reports() {
        let (_dir, store) = store_with_rows().await;
        let rows = store.list_memories("bot", "100", 10).await.expect("读");
        let refs: Vec<&str> = rows.iter().map(|r| r.memory_ref.as_str()).collect();
        let model = FakeModel {
            reply: reply_for(&refs),
        };
        let report = run_migration(&store, &model, false, 1_000_000).await;
        assert_eq!(report.candidates, 3);
        assert_eq!(report.converted, 3);
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        // topic expires by kind default; the reply's 48h applies to all three.
        assert_eq!(report.would_expire, 3);
        let after = store.list_memories("bot", "100", 10).await.expect("读");
        assert!(after.iter().all(|r| r.slot.is_empty()), "dry-run 不许写库");
    }

    #[tokio::test]
    async fn apply_writes_labels_back() {
        let (_dir, store) = store_with_rows().await;
        let rows = store.list_memories("bot", "100", 10).await.expect("读");
        let refs: Vec<&str> = rows.iter().map(|r| r.memory_ref.as_str()).collect();
        let model = FakeModel {
            reply: reply_for(&refs),
        };
        let report = run_migration(&store, &model, true, 1_000_000).await;
        assert_eq!(report.converted, 3);
        let after = store.list_memories("bot", "100", 10).await.expect("读");
        assert!(after.iter().all(|r| r.slot == "话题标签"));
        assert!(
            after.iter().all(|r| r.expires_at == Some(100 + 48 * 3600)),
            "到期按行自己的创建时间算"
        );
    }

    #[tokio::test]
    async fn a_dead_model_fails_rows_without_writing() {
        let (_dir, store) = store_with_rows().await;
        let report = run_migration(&store, &BoomModel, true, 1_000_000).await;
        assert_eq!(report.converted, 0);
        assert_eq!(report.failed.len(), 3);
        assert!(report.failed[0].reason.starts_with("model_call:"));
    }

    #[test]
    fn production_paths_are_refused() {
        assert!(is_production_path(Path::new(
            "/home/carrot/gemini-qq-bot/runtime/data/bot.sqlite3"
        )));
        assert!(is_production_path(Path::new("/data/bot.sqlite3")));
        assert!(!is_production_path(Path::new("/tmp/copy.sqlite3")));
    }
}
