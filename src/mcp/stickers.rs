//! MCP stdio tools over her sticker library.
//!
//! Two tools, and the split between them is the whole design:
//!
//! * `sticker_search` is read-only. It looks for a sticker she could send, and it only
//!   ever sees rows she already kept. Unjudged cache rows are invisible here, so browsing
//!   never accidentally promotes anything.
//! * `sticker_keep` is the single write. It records *her* verdict on one cached sticker:
//!   `self` keeps it, `ignore` refuses it forever. This is the only path into her
//!   library, which is what keeps the recognition cache and her taste separate.
//!
//! The library is bot-wide, not per group: a person does not keep one sticker drawer per
//! chat window. The cache is bot-wide for the same reason - one sha256, one row.

use serde_json::{Value, json};

use crate::store::{KEPT_REFUSED, KEPT_YES, SqliteStore};

/// Tool names the model sees.
pub const SEARCH_TOOL: &str = "sticker_search";
pub const KEEP_TOOL: &str = "sticker_keep";

/// Default and maximum number of hits.
pub const DEFAULT_LIMIT: i64 = 5;
pub const MAX_LIMIT: i64 = 20;

/// Every kept sticker is scored: the library is small enough to scan in full, and a
/// hard cap keeps a pathological library from blowing up a single turn.
pub const SCAN_LIMIT: i64 = 500;

/// Tool definitions for `tools/list`.
pub fn tool_definitions() -> Value {
    json!([
        {
            "name": SEARCH_TOOL,
            "description": "在一堆表情里找一张适合当前场合的。返回文字描述和句柄。只包含她自己收下的表情。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "想表达的情绪、场合或画面关键词，例如 生气、被夸、无语"
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LIMIT,
                        "description": "最多返回几个"
                    }
                },
                "required": ["query"]
            }
        },
        {
            "name": KEEP_TOOL,
            "description": "决定一张表情要不要进你自己的表情库。收下就以后能发，ignore 就永远不再问你。只能用这批图片里给出的句柄。收下时带上你自己写的 description，以后靠它找这张。",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "hash": {
                        "type": "string",
                        "description": "表情句柄，来自图片描述里的 表情 #xxxxxxxx"
                    },
                    "verdict": {
                        "type": "string",
                        "enum": ["self", "ignore"],
                        "description": "self 收进库，ignore 以后不用"
                    },
                    "description": {
                        "type": "string",
                        "description": "收下时写：你给这张表情的一句短描述（30 字内），写清画的是谁、什么表情动作、图里的文字。以后检索就靠它。ignore 时不用填"
                    }
                },
                "required": ["hash", "verdict"]
            }
        }
    ])
}

/// Render one library row the way the model reads it.
fn render_row(row: &crate::store::StickerRecord) -> String {
    let short = &row.hash[..row.hash.len().min(8)];
    let mut out = format!("[表情 #{short}]");
    if !row.ocr.trim().is_empty() {
        out.push_str(&format!(" 文字「{}」", row.ocr.trim()));
    }
    // The description replaces the old 画面/适合 pair: one paragraph says what the
    // picture is and when to send it, so it is rendered whole instead of split.
    let description = if row.description.trim().is_empty() {
        row.scene.trim()
    } else {
        row.description.trim()
    };
    if !description.is_empty() {
        out.push_str(&format!(" | {description}"));
    }
    if !row.emotion.trim().is_empty() {
        out.push_str(&format!(" | 情绪 {}", row.emotion.trim()));
    }
    if row.is_self == "self" || row.is_self == "self_series" {
        out.push_str(" | 是她自己");
    }
    out
}

/// Score her kept stickers with the ONE shared implementation
/// ([`crate::memory::stickers::score_text`]) and keep only the matches that
/// clear the shared threshold, best first. Ties break on hash so the same
/// query is stable.
///
/// The threshold is the B1 fix: below it, the honest result is "no match",
/// never the least-bad rows — the contract promises her that an unsuccessful
/// search means the sticker is not there, and search must not contradict it.
///
/// `recent` (this group's recent sends, newest window last
/// [`crate::app::turn::stickers::RECENT_SEND_SCAN_LIMIT`], duplicates
/// kept) re-ranks but never hides: the threshold and the returned score
/// stay the true match score, while the sort key is the raw score after
/// the A6 frequency divisor ([`crate::memory::stickers::divide_by_usage`]
/// over the hash's count in `recent`), the same ranking the server-side
/// preselection applies. A repeat stays findable when it is the only
/// thing that fits; it just stops winning by default — and the more the
/// group has used it, the harder it has to win.
pub fn top_matches(
    query: &str,
    rows: Vec<crate::store::StickerRecord>,
    limit: i64,
    recent: &[String],
) -> Vec<(f64, crate::store::StickerRecord)> {
    let limit = limit.clamp(1, MAX_LIMIT) as usize;
    let mut scored: Vec<(f64, f64, crate::store::StickerRecord)> = rows
        .into_iter()
        .map(|row| {
            let raw = crate::memory::stickers::score_text(query, &row.text());
            let sends = recent.iter().filter(|hash| *hash == &row.hash).count() as i64;
            let sort_key = crate::memory::stickers::divide_by_usage(raw, sends);
            (raw, sort_key, row)
        })
        .filter(|(raw, _, _)| crate::memory::stickers::match_tier(*raw).is_some())
        .collect();
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.2.hash.cmp(&b.2.hash))
    });
    scored.truncate(limit);
    scored.into_iter().map(|(raw, _, row)| (raw, row)).collect()
}

/// Render one hit with its match tier, so she can tell a strong match from a
/// marginal one instead of treating every returned row as equally fitting.
fn render_hit(score: f64, row: &crate::store::StickerRecord) -> String {
    let tier = crate::memory::stickers::match_tier(score).unwrap_or("一般");
    format!("{}（匹配：{}）", render_row(row), tier)
}

/// Audit-safe form of a search keyword: truncated, with the characters that
/// would corrupt the dot-segmented action format flattened.
fn audit_keyword(query: &str) -> String {
    query
        .chars()
        .take(12)
        .map(|c| {
            if c.is_whitespace() || c == '.' || c == '#' {
                '_'
            } else {
                c
            }
        })
        .collect()
}

/// Answer one `sticker_search` call.
///
/// Every call leaves an audit row (`mcp.sticker_search.*`, with an `.empty.`
/// variant when nothing cleared the threshold), so search volume and the
/// no-result rate are measurable instead of anecdotal. `group_id` only
/// attributes the audit row; the library itself stays bot-wide.
pub async fn call_search(
    store: &SqliteStore,
    bot_id: &str,
    group_id: Option<&str>,
    args: &Value,
) -> Value {
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_string();
    if query.is_empty() {
        return tool_error("query 不能为空");
    }
    let limit = args
        .get("limit")
        .and_then(Value::as_i64)
        .unwrap_or(DEFAULT_LIMIT);
    let rows = match store.list_library(bot_id, SCAN_LIMIT).await {
        Ok(rows) => rows,
        Err(_) => return tool_error("读取表情库失败"),
    };
    let now = crate::now_unix();
    let keyword = audit_keyword(&query);
    if rows.is_empty() {
        let _ = store
            .audit(
                "agent",
                &format!("mcp.sticker_search.empty.q{keyword}"),
                Some(bot_id),
                group_id,
                now,
            )
            .await;
        return json!({
            "content": [{"type": "text", "text": "表情库还是空的，你还没收下任何表情。"}],
            "isError": false
        });
    }
    // What this group recently sent, for the shared frequency divisor in
    // `top_matches`. A lookup failure degrades to "nothing recent": search
    // must not fail a turn over a worse ranking.
    let recent: Vec<String> = match group_id {
        Some(group) => store
            .recent_sent_stickers(
                bot_id,
                group,
                crate::app::turn::stickers::RECENT_SEND_SCAN_LIMIT,
            )
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(hash, _, _)| hash)
            .collect(),
        None => Vec::new(),
    };
    let hits = top_matches(&query, rows, limit, &recent);
    if hits.is_empty() {
        let _ = store
            .audit(
                "agent",
                &format!("mcp.sticker_search.empty.q{keyword}"),
                Some(bot_id),
                group_id,
                now,
            )
            .await;
        return json!({
            "content": [{"type": "text", "text": crate::memory::stickers::NO_STICKERS}],
            "isError": false
        });
    }
    let _ = store
        .audit(
            "agent",
            &format!("mcp.sticker_search.q{keyword}.n{}", hits.len()),
            Some(bot_id),
            group_id,
            now,
        )
        .await;
    let lines: Vec<String> = hits
        .iter()
        .map(|(score, row)| render_hit(*score, row))
        .collect();
    json!({
        "content": [{"type": "text", "text": lines.join("\n")}],
        "isError": false
    })
}

/// Answer one `sticker_keep` call.
///
/// The hash must already exist in the cache. A hash she invented cannot create a row,
/// which means she cannot add anything she was never shown. A recorded
/// verdict leaves an audit row (`mcp.sticker_keep.self|ignore.h…`), so the
/// library's growth is attributable instead of inferred from row counts.
///
/// Since 2026-11-24 she is the one who saw the picture, so a `self` verdict
/// may carry her own caption in `description`. The caption is written to the
/// row (when it beats the stored one) and becomes its search text. It is
/// also the keep-time dedup input: a caption near-verbatim to a kept row's
/// means the shelf already has this joke, so the new row is refused as a
/// duplicate instead of shelved twice.
pub async fn call_keep(
    store: &SqliteStore,
    bot_id: &str,
    group_id: Option<&str>,
    args: &Value,
) -> Value {
    let hash = args
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .trim_start_matches('#')
        .to_lowercase();
    let verdict = args
        .get("verdict")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    let caption: String = args
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .chars()
        .take(60)
        .collect();
    if hash.is_empty() {
        return tool_error("hash 不能为空");
    }
    let kept = match verdict.as_str() {
        "self" => KEPT_YES,
        "ignore" => KEPT_REFUSED,
        _ => return tool_error("verdict 只能是 self 或 ignore"),
    };
    // The model sees an 8-char handle, so resolve a prefix to the single full hash it
    // unambiguously names. Anything ambiguous or unknown is refused rather than guessed.
    let row = match store.sticker_by_hash(bot_id, &hash).await {
        Ok(Some(row)) => row,
        Ok(None) => match store.find_sticker_by_prefix(bot_id, &hash).await {
            Ok(Some(full)) => match store.sticker_by_hash(bot_id, &full).await {
                Ok(Some(row)) => row,
                _ => return tool_error("读取表情库失败"),
            },
            Ok(None) => return tool_error("没有这张表情，只能用图片里给出的句柄"),
            Err(_) => return tool_error("读取表情库失败"),
        },
        Err(_) => return tool_error("读取表情库失败"),
    };
    let full = row.hash.clone();
    // Keep-time semantic dedup: her caption against the shelf she already
    // owns. A near-verbatim match means this joke is already shelved, so the
    // candidate is refused as a duplicate — the same decision the ingest
    // gate used to make with the tagger's text, moved to the moment a
    // caption first exists.
    if kept == KEPT_YES && !caption.is_empty() {
        let duplicate = store
            .sticker_kept_descriptions(bot_id, 500)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|(other, _)| other != &full)
            .map(|(other, description)| {
                (
                    other,
                    crate::memory::stickers::description_similarity(&description, &caption),
                )
            })
            .filter(|(_, score)| *score >= crate::memory::stickers::SEMANTIC_DEDUP_MIN_SCORE)
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        if let Some((existing, _)) = duplicate {
            let _ = store.set_sticker_kept(bot_id, &full, KEPT_REFUSED).await;
            let short = &full[..full.len().min(8)];
            let existing_short = &existing[..existing.len().min(8)];
            let _ = store
                .audit(
                    "agent",
                    &format!("mcp.sticker_keep.duplicate.h{short}.of{existing_short}"),
                    Some(bot_id),
                    group_id,
                    crate::now_unix(),
                )
                .await;
            return json!({
                "content": [{"type": "text", "text": format!("库里已经有一张很像的了（#{existing_short}），这张就没重复收。")}],
                "isError": false
            });
        }
    }
    let recorded = store.set_sticker_kept(bot_id, &full, kept).await;
    // Her caption becomes the row's search text when it beats what is stored
    // (a candidate she just saw carries none, so hers normally wins).
    if kept == KEPT_YES
        && matches!(recorded, Ok(true))
        && !caption.is_empty()
        && caption.chars().count() > row.description.chars().count()
    {
        let blob = crate::memory::stickers::sticker_search_blob(
            &row.ocr,
            &row.emotion,
            &caption,
            &row.scene,
            &row.intent,
            &row.usage_hint,
        );
        let _ = store
            .set_sticker_description(bot_id, &full, &caption, &blob)
            .await;
    }
    if matches!(recorded, Ok(true)) {
        let short = &full[..full.len().min(8)];
        let _ = store
            .audit(
                "agent",
                &format!(
                    "mcp.sticker_keep.{}.h{short}",
                    if kept == KEPT_YES { "self" } else { "ignore" }
                ),
                Some(bot_id),
                group_id,
                crate::now_unix(),
            )
            .await;
    }
    match recorded {
        Ok(true) if kept == KEPT_YES => json!({
            "content": [{"type": "text", "text": "已经收进你的表情库了，以后可以直接发。"}],
            "isError": false
        }),
        Ok(true) => json!({
            "content": [{"type": "text", "text": "行，这张以后不用了。"}],
            "isError": false
        }),
        Ok(false) => tool_error("没有这张表情"),
        Err(_) => tool_error("写入表情库失败"),
    }
}

fn tool_error(message: &str) -> Value {
    json!({
        "content": [{"type": "text", "text": message}],
        "isError": true
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(hash: &str, ocr: &str, usage: &str) -> crate::store::StickerRecord {
        crate::store::StickerRecord {
            hash: hash.to_string(),
            bot_id: "1".into(),
            group_id: "2".into(),
            file_path: String::new(),
            kind: "sticker".into(),
            is_self: "other".into(),
            ocr: ocr.into(),
            description: String::new(),
            scene: String::new(),
            emotion: String::new(),
            intent: String::new(),
            usage_hint: usage.into(),
            search_blob: String::new(),
            phash: None,
            last_seen_at: None,
            seen_count: 1,
            nsfw: false,
            enabled: true,
            kept: KEPT_YES,
            created_at: 0,
        }
    }

    async fn test_store(dir: &tempfile::TempDir) -> SqliteStore {
        let pool = crate::store::migrations::open_and_migrate(&dir.path().join("bot.sqlite3"))
            .await
            .expect("迁移");
        SqliteStore::new(pool)
    }

    async fn candidate(store: &SqliteStore, hash: &str, description: &str) {
        store
            .upsert_sticker(
                hash,
                "1",
                "100",
                "/tmp/x.jpg",
                "sticker",
                "other",
                "",
                description,
                "",
                description,
                None,
                false,
                true,
                1,
            )
            .await
            .expect("写候选行");
    }

    #[tokio::test]
    async fn keep_writes_her_caption_to_the_row() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = test_store(&dir).await;
        candidate(&store, "aaaa1111bbbb2222", "").await;
        let out = call_keep(
            &store,
            "1",
            Some("100"),
            &json!({"hash": "aaaa1111", "verdict": "self", "description": "白猫趴键盘上，一脸无奈"}),
        )
        .await;
        assert_eq!(out["isError"], false, "{out}");
        let row = store
            .sticker_by_hash("1", "aaaa1111bbbb2222")
            .await
            .expect("读")
            .expect("行还在");
        assert_eq!(row.kept, KEPT_YES);
        assert_eq!(row.description, "白猫趴键盘上，一脸无奈");
        assert!(row.search_blob.contains("白猫趴键盘上"));
    }

    #[tokio::test]
    async fn keep_with_a_near_verbatim_caption_is_refused_as_duplicate() {
        let dir = tempfile::tempdir().expect("tmp");
        let store = test_store(&dir).await;
        candidate(&store, "aaaa1111bbbb2222", "白猫趴键盘上，一脸无奈").await;
        store
            .set_sticker_kept("1", "aaaa1111bbbb2222", KEPT_YES)
            .await
            .expect("先收一张");
        candidate(&store, "cccc3333dddd4444", "").await;
        let out = call_keep(
            &store,
            "1",
            Some("100"),
            &json!({"hash": "cccc3333", "verdict": "self", "description": "白猫趴键盘上，一脸无奈"}),
        )
        .await;
        assert_eq!(out["isError"], false, "{out}");
        let text = out["content"][0]["text"].as_str().unwrap_or_default();
        assert!(text.contains("已经有一张很像的"), "{text}");
        let row = store
            .sticker_by_hash("1", "cccc3333dddd4444")
            .await
            .expect("读")
            .expect("行还在");
        assert_eq!(row.kept, KEPT_REFUSED);
    }

    #[test]
    fn ocr_hit_outranks_usage_hit() {
        let hits = top_matches(
            "你怎么这么菜",
            vec![
                row("bbb", "", "被人说你怎么这么菜时回怼"),
                row("aaa", "你怎么这么菜", ""),
            ],
            5,
            &[],
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].1.hash, "aaa");
        assert!(hits[0].0 > hits[1].0);
    }

    #[test]
    fn an_unrelated_query_matches_nothing() {
        // The B1 promise: search may honestly find nothing instead of
        // returning the least-bad rows for a query they do not fit.
        let hits = top_matches(
            "量子力学考试",
            vec![row("aaa", "不许摸鱼", "上班时"), row("bbb", "晚安", "睡前")],
            5,
            &[],
        );
        assert!(hits.is_empty(), "got {hits:?}");
    }

    #[test]
    fn rank_is_stable_on_ties() {
        let hits = top_matches(
            "相同",
            vec![row("zzz", "相同", ""), row("aaa", "相同", "")],
            5,
            &[],
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].1.hash, "aaa");
        assert_eq!(hits[1].1.hash, "zzz");
    }

    #[test]
    fn a_recently_sent_hit_ranks_below_an_equal_fresh_one() {
        // Identical rows: without the divisor the hash tiebreak puts "aaa"
        // first. With "aaa" in the group's recent sends, the fresh row
        // wins the sort — while both keep their true (equal) scores, so
        // the repeat is demoted, never hidden or mislabelled.
        let hits = top_matches(
            "相同",
            vec![row("zzz", "相同", ""), row("aaa", "相同", "")],
            5,
            &["aaa".to_string()],
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].1.hash, "zzz");
        assert_eq!(hits[1].1.hash, "aaa");
        assert_eq!(hits[0].0, hits[1].0);
    }

    #[test]
    fn more_frequent_recent_sends_rank_lower() {
        // Equal matches: "aaa" went out three times inside the window,
        // "zzz" once. The old flat quarter-weight damping tied them and
        // the hash tiebreak put "aaa" first; the frequency divisor must
        // rank the less-used one higher.
        let recent: Vec<String> = ["aaa", "aaa", "aaa", "zzz"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let hits = top_matches(
            "相同",
            vec![row("aaa", "相同", ""), row("zzz", "相同", "")],
            5,
            &recent,
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].1.hash, "zzz");
        assert_eq!(hits[1].1.hash, "aaa");
    }

    #[test]
    fn render_shows_short_hash_tier_and_skips_empty_fields() {
        let r = row("a1b2c3d4e5f6", "不许摸鱼", "");
        let line = render_hit(2.0, &r);
        assert!(line.contains("[表情 #a1b2c3d4]"));
        assert!(line.contains("不许摸鱼"));
        assert!(line.contains("很贴"), "got {line}");
        assert!(!line.contains("适合"));
    }

    #[test]
    fn empty_query_matches_nothing() {
        assert!(top_matches("", vec![row("h", "有字", "有用途")], 5, &[]).is_empty());
    }

    #[test]
    fn audit_keyword_is_truncated_and_dot_safe() {
        assert_eq!(audit_keyword("无语 了.真的"), "无语_了_真的");
        assert_eq!(
            audit_keyword("一二三四五六七八九十甲乙丙").chars().count(),
            12
        );
    }
}
