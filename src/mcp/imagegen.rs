//! MCP tool `generate_image`: register a picture for the background
//! executor to draw.
//!
//! Drawing takes minutes and a turn has ninety seconds, so the tool never
//! draws — it validates, charges the quota bucket, and leaves a `queued`
//! job in `image_jobs`. The executor (app side) picks jobs up, runs agy,
//! and delivers the picture through the outbox. Registration is the moment
//! of commitment: the job counts against its bucket from here, whatever
//! happens later, which is what keeps a failing generator from becoming an
//! infinite retry loop against the daily cap.
//!
//! Two buckets, two rules (2026-11-11, operator-set):
//! * `request` — someone in this batch explicitly asked for a picture.
//!   Charged per person per group per day, with a per-person cooldown. The
//!   requester is named by `speaker_ref` and verified twice: the ref must
//!   resolve in this group, and that person must actually have spoken in
//!   the armed batch — a model inventing a requester to spend someone
//!   else's quota is refused. The owner is exempt from both cap and
//!   cooldown (the job is still recorded, marked by requester).
//! * `self` — she decided to draw on her own. Charged per group per day.
//!
//! Turn arming mirrors `submit_reply`: the call must carry the live turn's
//! token, so a call from a dead turn can never register work.

use serde_json::{Value, json};

use crate::mcp::Scope;
use crate::store::SqliteStore;

/// Tool name the model sees.
pub const TOOL_NAME: &str = "generate_image";

/// Prompt and caption caps, in characters. A picture prompt longer than
/// this is a paragraph, not a prompt; the caption rides as one chat line.
pub const MAX_PROMPT_CHARS: usize = 800;
pub const MAX_CAPTION_CHARS: usize = 100;

/// The tool's JSON schema, advertised on `tools/list`.
pub fn tool_definition() -> Value {
    json!({
        "name": TOOL_NAME,
        "description": "登记一张要画的图。画图要几分钟，登记成功只是排上队，图之后由系统发到群里；\
                        本轮回复里提一句在画就行，不要说已经画好了。mode 二选一：本批里有人明确\
                        要你画图时用 request，并把开口要图的人的 speaker_ref 填到 requester_ref；\
                        没人要求、你自己想画时用 self。画你自己时 self_portrait 填 true，prompt 只写\
                        场景和动作，你的外貌由系统按设定补齐，不要自己编。登记有每日额度，满了或\
                        冷却中会被拒绝，被拒绝就自然收手，不要换说法重试。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "turn_token": {
                    "type": "string",
                    "description": "本轮口令，照抄契约卡上给的值。"
                },
                "prompt": {
                    "type": "string",
                    "description": "要画什么：主体、场景、氛围，写具体。最多 800 字。"
                },
                "mode": {
                    "type": "string",
                    "enum": ["request", "self"],
                    "description": "request 是有人明确要图，self 是你自己想画。"
                },
                "requester_ref": {
                    "type": "string",
                    "description": "mode=request 时填开口要图那个人的 speaker_ref（本批出现过的 u 开头编号）。"
                },
                "self_portrait": {
                    "type": "boolean",
                    "description": "画的是你自己时填 true。"
                },
                "caption": {
                    "type": "string",
                    "description": "可选：图发出去时顺带的一句短文案，最多 100 字。"
                }
            },
            "required": ["turn_token", "prompt", "mode"]
        }
    })
}

fn tool_error(message: &str) -> Value {
    json!({
        "content": [{"type": "text", "text": message}],
        "isError": true
    })
}

fn tool_answer(text: String) -> Value {
    json!({
        "content": [{"type": "text", "text": text}],
        "isError": false
    })
}

fn arg_str<'a>(args: &'a Value, key: &str) -> &'a str {
    args.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// Handle one `generate_image` call: arm check, gate checks, registration.
pub async fn call_generate(store: &SqliteStore, scope: &Scope, args: &Value) -> Value {
    let now = crate::now_unix();
    let token = arg_str(args, "turn_token").to_string();

    // Arm check, mirroring submit_reply: the call must belong to the live turn.
    let arm = match store.armed_turn(&scope.group_id).await {
        Ok(Some(arm)) if arm.turn_token == token => {
            if now > arm.expires_at {
                return tool_error("本轮已过期，登记窗口关闭");
            }
            arm
        }
        Ok(Some(_)) => return tool_error("turn_token 与本轮不符"),
        Ok(None) => return tool_error("本轮未武装或已结束"),
        Err(_) => return tool_error("暂时无法核对本轮口令，请重试"),
    };

    let prompt = arg_str(args, "prompt").trim().to_string();
    if prompt.is_empty() {
        return tool_error("prompt 不能为空：写清要画什么");
    }
    if prompt.chars().count() > MAX_PROMPT_CHARS {
        return tool_error("prompt 太长了，压到 800 字以内");
    }
    let mode = arg_str(args, "mode");
    if mode != "request" && mode != "self" {
        return tool_error("mode 只能是 request 或 self");
    }
    let self_portrait = args
        .get("self_portrait")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let caption = arg_str(args, "caption").trim().to_string();
    if caption.chars().count() > MAX_CAPTION_CHARS {
        return tool_error("caption 太长了，压到 100 字以内");
    }
    let caption = if caption.is_empty() {
        None
    } else if let Some(reason) = crate::agent::contract::reply_content_rejection(&caption) {
        // The picture is still wanted. Never force another generation/charge
        // just because its optional caption is unsafe; log a reason, not text.
        let _ = store
            .audit(
                "agent",
                &format!("imagegen.caption.dropped.{reason}"),
                Some(&scope.bot_id),
                Some(&scope.group_id),
                now,
            )
            .await;
        None
    } else {
        Some(caption.as_str())
    };

    // A group that is off or paused does not draw, whichever bucket it is.
    match store.get_group(&scope.bot_id, &scope.group_id).await {
        Ok(Some(group)) if group.enabled && !group.paused => {}
        Ok(_) => return tool_error("这个群现在不画图"),
        Err(_) => return tool_error("暂时无法确认群状态，请重试"),
    }

    // Resolve the requester; owner exemption and charging happen together
    // inside the registration transaction, not in a racy read/check/write gate.
    let requester_id: String = if mode == "request" {
        let requester_ref = arg_str(args, "requester_ref").trim();
        if requester_ref.is_empty() {
            return tool_error("mode=request 要填 requester_ref：开口要图那个人的 speaker_ref");
        }
        let member = match store
            .member_by_ref(&scope.bot_id, &scope.group_id, requester_ref)
            .await
        {
            Ok(Some(member)) => member,
            Ok(None) => return tool_error("requester_ref 在本群找不到这个人"),
            Err(_) => return tool_error("暂时无法核对要图的人，请重试"),
        };
        match store
            .armed_sender_ids(&scope.bot_id, &scope.group_id, &token)
            .await
        {
            Ok(senders) if senders.iter().any(|s| s == &member.user_id) => {}
            Ok(_) => {
                return tool_error("requester_ref 不是本批开口的人：只能给本轮明确要图的人登记");
            }
            Err(_) => return tool_error("暂时无法核对本批发言人，请重试"),
        }
        member.user_id
    } else {
        "bot".to_string()
    };

    match store
        .register_image_job(
            &scope.bot_id,
            &scope.group_id,
            &token,
            &requester_id,
            mode,
            &prompt,
            self_portrait,
            caption,
            now,
        )
        .await
    {
        Ok(Ok(_job_id)) => {
            let _ = store
                .audit(
                    "agent",
                    "imagegen.queued",
                    Some(&scope.bot_id),
                    Some(&scope.group_id),
                    now,
                )
                .await;
            tool_answer(
                "已登记，图会在几分钟后发到群里。本轮回复里提一句你正在画就行，不要说已经画好了。"
                    .to_string(),
            )
        }
        Ok(Err("daily_cap")) if mode == "self" => {
            tool_error("你今天自己要画的图已到上限，明天再画吧")
        }
        Ok(Err("daily_cap")) => tool_error("他今天要的图已到上限，明天再画吧"),
        Ok(Err("cooldown")) => tool_error("刚给他画过一张，歇一会再要下一张"),
        Ok(Err("window_closed")) => tool_error("本轮口令已失效，登记窗口关闭"),
        Ok(Err("group_disabled")) => tool_error("这个群现在不画图"),
        Ok(Err(_)) => tool_error("登记参数无效"),
        Err(_) => tool_error("登记失败或数据库忙，请重试"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::migrations;

    fn now() -> i64 {
        crate::now_unix()
    }

    async fn store() -> (tempfile::TempDir, SqliteStore) {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("bot.sqlite3");
        let pool = migrations::open_and_migrate(&path).await.expect("迁移");
        let store = SqliteStore::new(pool);
        store.ensure_group("1", "100", now()).await.expect("建群");
        store
            .set_group_flags("1", "100", Some(true), Some(false), None, now())
            .await
            .expect("启用群");
        (dir, store)
    }

    async fn store_armed() -> (tempfile::TempDir, SqliteStore) {
        let (dir, store) = store().await;
        store
            .arm_turn("100", "tok", "batch1", now(), now() + 3600)
            .await
            .expect("武装");
        // User 42 is this group's u1 and spoke in this batch, as a real
        // turn would have it.
        let r = store
            .ensure_member("1", "100", "42", now())
            .await
            .expect("成员");
        assert_eq!(r, "u1");
        store
            .insert_pending("1", "100", "m1", "42", now(), "{}", "[]")
            .await
            .expect("pending");
        let rows = store
            .pending_for_group("1", "100")
            .await
            .expect("读 pending");
        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        store.claim_batch(&ids, "batch1").await.expect("claim");
        (dir, store)
    }

    fn scope() -> Scope {
        Scope {
            bot_id: "1".to_string(),
            group_id: "100".to_string(),
        }
    }

    fn is_error(v: &Value) -> bool {
        v.get("isError").and_then(Value::as_bool).unwrap_or(false)
    }

    fn text(v: &Value) -> String {
        v.pointer("/content/0/text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }

    #[tokio::test]
    async fn self_job_registers_without_a_requester() {
        let (_dir, store) = store_armed().await;
        let args = json!({"turn_token": "tok", "prompt": "星空下的车站", "mode": "self"});
        let out = call_generate(&store, &scope(), &args).await;
        assert!(!is_error(&out), "{}", text(&out));
        let jobs = store.list_image_jobs("1", "100", 0, 10).await.expect("读");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].bucket, "self");
        assert_eq!(jobs[0].requester_id, "bot");
        assert_eq!(jobs[0].state, "queued");
    }

    #[tokio::test]
    async fn request_job_charges_the_verified_requester() {
        let (_dir, store) = store_armed().await;
        let args = json!({"turn_token": "tok", "prompt": "一只猫", "mode": "request", "requester_ref": "u1"});
        let out = call_generate(&store, &scope(), &args).await;
        assert!(!is_error(&out), "{}", text(&out));
        let jobs = store.list_image_jobs("1", "100", 0, 10).await.expect("读");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].requester_id, "42");
    }

    #[tokio::test]
    async fn requester_who_did_not_speak_in_this_batch_is_refused() {
        let (_dir, store) = store_armed().await;
        // A real member of the group, but silent in this batch.
        let r = store
            .ensure_member("1", "100", "77", now())
            .await
            .expect("成员");
        assert_eq!(r, "u2");
        let args = json!({"turn_token": "tok", "prompt": "一只猫", "mode": "request", "requester_ref": "u2"});
        let out = call_generate(&store, &scope(), &args).await;
        assert!(is_error(&out));
        assert!(text(&out).contains("本批"), "{}", text(&out));
    }

    #[tokio::test]
    async fn request_cooldown_bites_on_the_immediate_second_call() {
        let (_dir, store) = store_armed().await;
        let args = json!({"turn_token": "tok", "prompt": "一只猫", "mode": "request", "requester_ref": "u1"});
        // First request lands; the same person immediately again hits the cooldown.
        assert!(!is_error(&call_generate(&store, &scope(), &args).await));
        let out = call_generate(&store, &scope(), &args).await;
        assert!(is_error(&out));
        assert!(text(&out).contains("歇"), "{}", text(&out));
    }

    #[tokio::test]
    async fn request_cap_bites_on_sixth_call_with_cooldown_expired() {
        let (_dir, store) = store_armed().await;
        let base = 1_794_204_000; // Fixed Shanghai afternoon, never near a day boundary.
        store
            .arm_turn("100", "tok", "batch1", base, base + 7200)
            .await
            .unwrap();
        for index in 0..6 {
            let result = store
                .register_image_job(
                    "1",
                    "100",
                    "tok",
                    "42",
                    "request",
                    "offline ledger",
                    false,
                    None,
                    base + index * 601,
                )
                .await
                .unwrap();
            if index < 5 {
                assert!(result.is_ok(), "request {index}: {result:?}");
            } else {
                assert_eq!(result, Err("daily_cap"), "not a cooldown refusal");
            }
        }
        let jobs = store.list_image_jobs("1", "100", 0, 20).await.unwrap();
        assert_eq!(
            jobs.len(),
            5,
            "a denied sixth call must not charge another row"
        );
    }

    #[tokio::test]
    async fn owner_is_exempt_from_cap_and_cooldown() {
        let (_dir, store) = store_armed().await;
        store.set_imagegen_owner("42", now()).await.expect("owner");
        let args = json!({"turn_token": "tok", "prompt": "一只猫", "mode": "request", "requester_ref": "u1"});
        for _ in 0..7 {
            let out = call_generate(&store, &scope(), &args).await;
            assert!(!is_error(&out), "{}", text(&out));
        }
    }

    #[tokio::test]
    async fn self_cap_bites_and_reset_gives_it_back() {
        let (_dir, store) = store_armed().await;
        store
            .update_imagegen_settings(
                &crate::store::ImagegenSettings {
                    self_daily_cap: 1,
                    ..Default::default()
                },
                now(),
            )
            .await
            .expect("改额度");
        let args = json!({"turn_token": "tok", "prompt": "星空", "mode": "self"});
        assert!(!is_error(&call_generate(&store, &scope(), &args).await));
        assert!(is_error(&call_generate(&store, &scope(), &args).await));
        store
            .imagegen_reset("1", "100", now() + 10)
            .await
            .expect("重置");
        // The arm predates the reset call's clock; use fresh args with the
        // same token — reset only moves the counting watermark.
        let out = call_generate(&store, &scope(), &args).await;
        assert!(!is_error(&out), "{}", text(&out));
    }

    async fn concurrent_registrations(store: &SqliteStore, mode: &str, n: usize) -> usize {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(n));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..n {
            let store = store.clone();
            let barrier = barrier.clone();
            let args = json!({"turn_token":"tok", "prompt":"local ledger only", "mode":mode, "requester_ref":"u1"});
            tasks.spawn(async move {
                barrier.wait().await;
                !is_error(&call_generate(&store, &scope(), &args).await)
            });
        }
        let mut accepted = 0;
        while let Some(result) = tasks.join_next().await {
            accepted += usize::from(result.expect("task"));
        }
        accepted
    }

    #[tokio::test]
    async fn atomic_quota_last_self_slot_has_only_one_winner() {
        let (_dir, store) = store_armed().await;
        for _ in 0..14 {
            store
                .insert_image_job("1", "100", "bot", "self", "seed", false, None, now())
                .await
                .expect("seed");
        }
        assert_eq!(concurrent_registrations(&store, "self", 16).await, 1);
        assert_eq!(
            store
                .count_image_jobs(
                    "1",
                    "100",
                    "self",
                    None,
                    crate::store::shanghai_day_start(now())
                )
                .await
                .expect("count"),
            15
        );
    }

    #[tokio::test]
    async fn atomic_quota_same_person_concurrency_obeys_cooldown() {
        let (_dir, store) = store_armed().await;
        assert_eq!(concurrent_registrations(&store, "request", 16).await, 1);
        assert_eq!(
            store
                .list_image_jobs("1", "100", 0, 100)
                .await
                .expect("jobs")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn atomic_quota_default_daily_caps_are_fifteen_and_five() {
        let (_dir, store) = store_armed().await;
        store
            .update_imagegen_settings(
                &crate::store::ImagegenSettings {
                    request_cooldown_minutes: 0,
                    ..Default::default()
                },
                now(),
            )
            .await
            .expect("settings");
        for (mode, cap) in [("self", 15), ("request", 5)] {
            let args =
                json!({"turn_token":"tok", "prompt":"ledger", "mode":mode, "requester_ref":"u1"});
            for _ in 0..cap {
                assert!(!is_error(&call_generate(&store, &scope(), &args).await));
            }
            assert!(is_error(&call_generate(&store, &scope(), &args).await));
        }
        assert_eq!(
            store
                .list_image_jobs("1", "100", 0, 100)
                .await
                .expect("jobs")
                .len(),
            20
        );
    }

    #[tokio::test]
    async fn atomic_quota_owner_concurrency_is_exempt_but_self_is_not() {
        let (_dir, store) = store_armed().await;
        store.set_imagegen_owner("42", now()).await.expect("owner");
        store
            .update_imagegen_settings(
                &crate::store::ImagegenSettings {
                    self_daily_cap: 0,
                    request_daily_cap: 0,
                    ..Default::default()
                },
                now(),
            )
            .await
            .expect("settings");
        assert_eq!(concurrent_registrations(&store, "request", 16).await, 16);
        assert_eq!(concurrent_registrations(&store, "self", 2).await, 0);
    }

    #[tokio::test]
    async fn atomic_quota_counts_exact_shanghai_midnight() {
        let (_dir, store) = store_armed().await;
        let day = crate::store::shanghai_day_start(now());
        store
            .update_imagegen_settings(
                &crate::store::ImagegenSettings {
                    self_daily_cap: 1,
                    ..Default::default()
                },
                now(),
            )
            .await
            .expect("settings");
        store
            .insert_image_job("1", "100", "bot", "self", "midnight", false, None, day)
            .await
            .expect("seed");
        store
            .insert_image_job("1", "100", "42", "request", "midnight", false, None, day)
            .await
            .expect("seed");
        assert_eq!(
            store
                .count_image_jobs("1", "100", "self", None, day)
                .await
                .expect("count"),
            1
        );
        assert_eq!(
            store
                .image_requester_counts("1", "100", day)
                .await
                .expect("counts"),
            vec![("42".into(), 1)]
        );
        let args = json!({"turn_token":"tok", "prompt":"ledger", "mode":"self"});
        assert!(is_error(&call_generate(&store, &scope(), &args).await));
    }

    #[tokio::test]
    async fn atomic_quota_reset_excludes_exact_watermark_not_later_jobs() {
        let (_dir, store) = store_armed().await;
        let reset = now() - 2;
        for at in [reset - 1, reset, reset + 1] {
            store
                .insert_image_job("1", "100", "bot", "self", "seed", false, None, at)
                .await
                .expect("seed");
            store
                .insert_image_job("1", "100", "42", "request", "seed", false, None, at)
                .await
                .expect("seed");
        }
        store
            .imagegen_reset("1", "100", reset)
            .await
            .expect("reset");
        let day = crate::store::shanghai_day_start(reset);
        assert_eq!(
            store
                .count_image_jobs("1", "100", "self", None, day)
                .await
                .expect("count"),
            1
        );
        assert_eq!(
            store
                .image_requester_counts("1", "100", day)
                .await
                .expect("counts"),
            vec![("42".into(), 1)]
        );
    }

    #[tokio::test]
    async fn unarmed_or_wrong_token_is_refused() {
        let (_dir, store) = store().await;
        let args = json!({"turn_token": "tok", "prompt": "星空", "mode": "self"});
        assert!(is_error(&call_generate(&store, &scope(), &args).await));
        let (_dir2, store2) = store_armed().await;
        let args = json!({"turn_token": "nope", "prompt": "星空", "mode": "self"});
        assert!(is_error(&call_generate(&store2, &scope(), &args).await));
    }
}
