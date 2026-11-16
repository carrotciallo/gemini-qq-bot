//! 状态机与纯函数的测试（从 spontaneous.rs 拆出，沿用本仓 tests.rs 惯例）。

use super::*;

use super::*;

fn cfg() -> SpontaneousConfig {
    SpontaneousConfig::default()
}

#[test]
fn quiet_hours_cover_one_to_eight() {
    let c = cfg();
    assert!(in_quiet_hours(1, &c));
    assert!(in_quiet_hours(7, &c));
    assert!(!in_quiet_hours(8, &c));
    assert!(!in_quiet_hours(0, &c));
    assert!(!in_quiet_hours(23, &c));
}

#[test]
fn quiet_hours_wrap_midnight() {
    let mut c = cfg();
    c.quiet_start_hour = 23;
    c.quiet_end_hour = 6;
    assert!(in_quiet_hours(23, &c));
    assert!(in_quiet_hours(5, &c));
    assert!(!in_quiet_hours(6, &c));
    assert!(!in_quiet_hours(22, &c));
}

#[test]
fn quiet_end_is_the_most_recent_daily_end() {
    let mut c = cfg(); // default quiet window 1..8
    // 2026-11-10 14:00 CST: today's 08:00 end is the latest.
    assert_eq!(last_quiet_end(1_794_290_400, &c), 1_794_268_800);
    // Wrap window 23..8, inside it at 2026-11-11 05:00: yesterday's end.
    c.quiet_start_hour = 23;
    c.quiet_end_hour = 8;
    assert_eq!(last_quiet_end(1_794_344_400, &c), 1_794_268_800);
    // Just after the end, 2026-11-11 08:30: today's end.
    assert_eq!(last_quiet_end(1_794_357_000, &c), 1_794_355_200);
    // No window configured: no epoch.
    c.quiet_start_hour = 8;
    assert_eq!(last_quiet_end(1_794_290_400, &c), 0);
}

#[test]
fn local_day_uses_shanghai_offset() {
    // 2026-11-09 16:30 UTC is already 2026-11-10 00:30 in Shanghai.
    let now = 1_794_241_800; // 2026-11-09T16:30:00Z
    assert_eq!(local_day(now), "2026-11-10");
    assert_eq!(local_hour(now), 0);
}

#[test]
fn silence_threshold_stays_in_range_and_is_stable() {
    let c = cfg();
    for day in ["2026-11-09", "2026-11-10", "2026-12-01"] {
        let t = daily_silence_threshold("900000001", day, &c);
        assert!((45..=120).contains(&t), "{t}");
        assert_eq!(t, daily_silence_threshold("900000001", day, &c));
    }
}

#[test]
fn angle_reply_parsing_is_strict() {
    assert_eq!(
        parse_angle_reply("前言 {\"topic\":\"新番\",\"angle\":\"从画风聊起\"} 后语"),
        Some(("新番".to_string(), "从画风聊起".to_string()))
    );
    assert_eq!(parse_angle_reply("{\"topic\":\"\"}"), None);
    assert_eq!(parse_angle_reply("没有 json"), None);
    // A raw QQ number in the topic is an identifier leak: rejected.
    assert_eq!(
        parse_angle_reply("{\"topic\":\"问问 123456789 在不在\",\"angle\":\"\"}"),
        None
    );
}

#[test]
fn angle_reply_reversed_braces_are_rejected() {
    assert_eq!(parse_angle_reply("}x{"), None);
    assert_eq!(parse_angle_reply("}{"), None);
}

#[test]
fn angle_reply_missing_braces_are_rejected() {
    for raw in ["", "{", "}", "{\"topic\":\"新番\"", "\"topic\":\"新番\"}"] {
        assert_eq!(parse_angle_reply(raw), None);
    }
}

#[test]
fn angle_reply_multibyte_noise_is_rejected_without_panicking() {
    for raw in ["后语}噪声{前言", "🦀}中文{✅", "前言{不是JSON}后语"] {
        assert_eq!(parse_angle_reply(raw), None);
    }
}

#[test]
fn angle_reply_valid_json_survives_unicode_surroundings() {
    assert_eq!(
        parse_angle_reply("🦀 前言 {\"topic\":\"新番\",\"angle\":\"聊画风\"} 后语 ✅"),
        Some(("新番".into(), "聊画风".into()))
    );
}

#[test]
fn cooldown_roundtrip_and_matching() {
    let mut state = SpontaneousState::default();
    state.cool("mem:r1", 1_000, false);
    assert!(state.in_cooldown("mem:r1", 999));
    assert!(!state.in_cooldown("mem:r1", 1_000));
    assert!(!state.in_cooldown("mem:r2", 999));
    state.cool("mem:r1", 500, true);
    assert_eq!(state.cooldown[0].until, 1_000, "cooldown only extends");
    assert!(state.cooldown[0].ignored);
    let json = serde_json::to_string(&state).expect("ser");
    let back: SpontaneousState = serde_json::from_str(&json).expect("de");
    assert_eq!(back.cooldown.len(), 1);
}

#[test]
fn garbled_state_blob_reads_as_fresh() {
    let state: SpontaneousState = serde_json::from_str("{not json").unwrap_or_default();
    assert_eq!(state.day_count, 0);
    assert!(state.pending.is_none());
}

// ---- state machine against a real store, fake speaker and model ----

/// 2026-11-09 14:00 in Shanghai: outside quiet hours.
const BASE: i64 = 1_794_204_000;

struct FakeModel;

#[async_trait::async_trait]
impl SummaryModel for FakeModel {
    async fn summarise(&self, _prompt: String) -> Result<String, crate::error::ErrorClass> {
        Ok("{\"topic\":\"周末去哪骑车\",\"angle\":\"从天气聊起\"}".to_string())
    }
}

#[derive(Default)]
struct FakeSpeaker {
    calls: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl OpeningSpeaker for FakeSpeaker {
    async fn speak(&self, group_id: &str, picked: &PickedTopic, _now: i64) -> Option<String> {
        self.calls
            .lock()
            .expect("锁")
            .push(format!("{group_id}:{}", picked.brief.topic));
        Some("sp".into())
    }
}

async fn fixture(
    cfg: SpontaneousConfig,
) -> (
    tempfile::TempDir,
    Arc<SqliteStore>,
    Arc<FakeSpeaker>,
    SpontaneousEngine,
    GroupRecord,
) {
    let dir = tempfile::tempdir().expect("tmp");
    let pool = crate::store::migrations::open_temp(dir.path())
        .await
        .expect("迁移");
    let store = Arc::new(SqliteStore::new(pool));
    store.ensure_group("bot", "100", BASE).await.expect("建组");
    store
        .set_group_flags("bot", "100", Some(true), None, None, BASE)
        .await
        .expect("启用");
    let group = store
        .get_group("bot", "100")
        .await
        .expect("读组")
        .expect("组在");
    let speaker = Arc::new(FakeSpeaker::default());
    let engine = SpontaneousEngine::new(
        Arc::clone(&store),
        Arc::new(FakeModel),
        speaker.clone() as Arc<dyn OpeningSpeaker>,
        Arc::new(RunningTurns::new()),
        "bot".to_string(),
        cfg,
        60,
    );
    (dir, store, speaker, engine, group)
}

async fn read_state(store: &SqliteStore) -> SpontaneousState {
    let raw = store
        .get_spontaneous_json("bot", "100")
        .await
        .expect("读状态");
    serde_json::from_str(&raw).expect("状态可解析")
}

async fn seed_pending(store: &SqliteStore, pending: PendingOpening) {
    let mut state = SpontaneousState {
        day: local_day(BASE),
        ..Default::default()
    };
    state.pending = Some(pending);
    store
        .put_spontaneous_json(
            "bot",
            "100",
            &serde_json::to_string(&state).expect("序列化"),
            BASE,
        )
        .await
        .expect("写状态");
}

async fn human_says(store: &SqliteStore, at: i64, id: &str) {
    store
        .insert_group_message(
            "bot",
            "100",
            id,
            "42",
            Some("甲"),
            "member",
            "在吗",
            false,
            None,
            None,
            at,
        )
        .await
        .expect("写群日志");
}

#[tokio::test]
async fn silence_fires_once_and_records_pending() {
    let (_d, store, speaker, engine, group) = fixture(cfg()).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    store
        .insert_memory_with_origin(
            "bot",
            "100",
            None,
            "群里聊过高达模型取向",
            "fact",
            "[1]",
            0.6,
            "hy3",
            BASE - 3 * 3_600,
        )
        .await
        .expect("写记忆");
    engine.tick_group(&group, BASE).await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
    let state = read_state(&store).await;
    let pending = state.pending.expect("已挂起");
    assert_eq!(pending.enqueued_at, BASE);
    assert_eq!(pending.batch_id.as_deref(), Some("sp"));
    assert!(pending.fired_at.is_none());
    assert_eq!(state.last_at, BASE);
    // A second tick while the opening is unresolved fires nothing.
    engine.tick_group(&group, BASE + 60).await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
}

#[tokio::test]
async fn disabled_engine_never_fires() {
    let mut c = cfg();
    c.enabled = false;
    let (_d, store, speaker, engine, group) = fixture(c).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    engine.tick_group(&group, BASE).await;
    assert!(speaker.calls.lock().expect("锁").is_empty());
    assert_eq!(
        store
            .get_spontaneous_json("bot", "100")
            .await
            .expect("读状态"),
        "{}"
    );
}

#[tokio::test]
async fn unanswered_opening_resolves_declined_after_the_window() {
    let (_d, store, _speaker, engine, group) = fixture(cfg()).await;
    seed_pending(
        &store,
        PendingOpening {
            topic: "周末去哪骑车".to_string(),
            key: "angle:abc".to_string(),
            enqueued_at: BASE - DECLINE_WINDOW - 1,
            batch_id: Some("sp".into()),
            fired_at: None,
        },
    )
    .await;
    engine.tick_group(&group, BASE).await;
    let state = read_state(&store).await;
    assert!(state.pending.is_none());
    assert_eq!(state.attempts, 1);
    assert_eq!(state.cooldown.len(), 1);
    assert_eq!(state.cooldown[0].until, BASE + SEVEN_DAYS);
    assert!(!state.cooldown[0].ignored);
}

#[tokio::test]
async fn sent_opening_then_a_human_reply_resolves_answered() {
    let (_d, store, _speaker, engine, group) = fixture(cfg()).await;
    let enqueued = BASE - 600;
    seed_pending(
        &store,
        PendingOpening {
            topic: "周末去哪骑车".to_string(),
            key: "angle:abc".to_string(),
            enqueued_at: enqueued,
            batch_id: Some("sp".into()),
            fired_at: None,
        },
    )
    .await;
    // Ground truth: the outbox shows she sent at enqueued+60.
    sqlx::query(
        "INSERT INTO outbox (id, bot_id, group_id, batch_id, text, state, message_id, created_at, updated_at)
         VALUES ('o1', 'bot', '100', 'sp', '走啊', 'sent', '9001', ?, ?)",
    )
    .bind(enqueued + 60)
    .bind(enqueued + 60)
    .execute(&store.pool)
    .await
    .expect("写 outbox");
    engine.tick_group(&group, BASE).await;
    let state = read_state(&store).await;
    let pending = state.pending.clone().expect("已发出待回复");
    assert_eq!(pending.fired_at, Some(enqueued + 60));
    assert_eq!(state.day_count, 1);
    // A human answers inside the window.
    human_says(&store, enqueued + 300, "m9").await;
    engine.tick_group(&group, enqueued + 360).await;
    let state = read_state(&store).await;
    assert!(state.pending.is_none());
    let actions: Vec<(String,)> = sqlx::query_as("SELECT action FROM audit_events ORDER BY id")
        .fetch_all(&store.pool)
        .await
        .expect("读审计");
    let actions: Vec<&str> = actions.iter().map(|(a,)| a.as_str()).collect();
    assert!(actions.contains(&"spontaneous.fired.g100"), "{actions:?}");
    assert!(
        actions.contains(&"spontaneous.answered.g100"),
        "{actions:?}"
    );
}

async fn attributed_opening_fixture(
    batch: Option<&str>,
    kind: &str,
    include_identity: bool,
) -> (
    tempfile::TempDir,
    Arc<SqliteStore>,
    SpontaneousEngine,
    SpontaneousState,
) {
    let (dir, store, _speaker, engine, _group) = fixture(cfg()).await;
    let mut pending = serde_json::json!({"topic":"周末骑车", "key":"angle:r19", "enqueued_at":BASE, "fired_at":null});
    if include_identity {
        pending["batch_id"] = serde_json::json!("sp-exact");
    }
    let state: SpontaneousState =
        serde_json::from_value(serde_json::json!({"day":local_day(BASE), "pending":pending}))
            .unwrap();
    if let Some(batch) = batch {
        sqlx::query("INSERT INTO outbox (id,bot_id,group_id,batch_id,text,kind,state,created_at,updated_at) VALUES ('r19','bot','100',?,'content',?,'sent',?,?)")
            .bind(batch).bind(kind).bind(BASE).bind(BASE+1).execute(&store.pool).await.unwrap();
    }
    (dir, store, engine, state)
}

#[tokio::test]
async fn attributed_opening_ordinary_reply_is_not_success() {
    let (_dir, _store, engine, mut state) =
        attributed_opening_fixture(Some("ordinary"), "text", true).await;
    assert!(!engine.resolve_pending("100", &mut state, BASE + 2).await);
    assert_eq!(state.day_count, 0);
    assert_eq!(state.pending.unwrap().fired_at, None);
}

#[tokio::test]
async fn attributed_opening_image_delivery_is_not_success() {
    let (_dir, _store, engine, mut state) =
        attributed_opening_fixture(Some("image-job"), "image", true).await;
    assert!(!engine.resolve_pending("100", &mut state, BASE + 2).await);
    assert_eq!(state.day_count, 0);
    assert_eq!(state.pending.unwrap().fired_at, None);
}

#[tokio::test]
async fn attributed_opening_exact_sent_batch_counts_once_across_reload() {
    let (_dir, store, engine, mut state) =
        attributed_opening_fixture(Some("sp-exact"), "text", true).await;
    assert!(engine.resolve_pending("100", &mut state, BASE + 2).await);
    assert_eq!(state.day_count, 1);
    seed_state(&store, state).await;
    let mut restored = read_state(&store).await;
    assert!(!engine.resolve_pending("100", &mut restored, BASE + 3).await);
    assert_eq!(restored.day_count, 1);
    assert_eq!(restored.pending.unwrap().fired_at, Some(BASE + 1));
}

#[tokio::test]
async fn attributed_opening_legacy_identity_is_not_guessed_from_nearby_send() {
    let (_dir, _store, engine, mut state) =
        attributed_opening_fixture(Some("ordinary"), "text", false).await;
    engine.resolve_pending("100", &mut state, BASE + 2).await;
    assert_eq!(
        state.day_count, 0,
        "legacy pending must never borrow another delivery"
    );
    assert!(state.pending.is_none());
    assert_eq!(state.attempts, 1);
}

#[test]
fn recent_topic_matching_uses_content_not_wording() {
    let mut state = SpontaneousState::default();
    state.recent_topics.push(RecentTopic {
        topic: "每周五晚上一起打本".to_string(),
        at: BASE,
    });
    // Same wording and a light paraphrase both count as used; an
    // unrelated topic does not, and the window closes after 14 days.
    assert!(state.topic_recently_used("每周五晚上一起打本", BASE + 60));
    assert!(state.topic_recently_used("每周五晚上一起打本吧", BASE + 60));
    assert!(!state.topic_recently_used("高达模型拼装进度", BASE + 60));
    assert!(!state.topic_recently_used("每周五晚上一起打本", BASE + FOURTEEN_DAYS));
    let json = serde_json::to_string(&state).expect("ser");
    let back: SpontaneousState = serde_json::from_str(&json).expect("de");
    assert_eq!(back.recent_topics.len(), 1);
    assert_eq!(back.recent_topics[0].topic, "每周五晚上一起打本");
}

async fn seed_state(store: &SqliteStore, state: SpontaneousState) {
    store
        .put_spontaneous_json(
            "bot",
            "100",
            &serde_json::to_string(&state).expect("序列化"),
            BASE,
        )
        .await
        .expect("写状态");
}

async fn seed_memory(store: &SqliteStore, content: &str) {
    store
        .insert_memory_with_origin(
            "bot",
            "100",
            None,
            content,
            "fact",
            "[1]",
            0.6,
            "hy3",
            BASE - 3 * 3_600,
        )
        .await
        .expect("写记忆");
}

#[tokio::test]
async fn reworded_recent_topic_does_not_refire() {
    let (_d, store, speaker, engine, group) = fixture(cfg()).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    // Every source can only offer the just-asked topic: the memory IS the
    // topic and the fake angle model regenerates it verbatim.
    seed_memory(&store, "周末去哪骑车").await;
    let mut state = SpontaneousState {
        day: local_day(BASE),
        ..Default::default()
    };
    state.recent_topics.push(RecentTopic {
        topic: "周末去哪骑车".to_string(),
        at: BASE - 3_600,
    });
    seed_state(&store, state).await;
    engine.tick_group(&group, BASE).await;
    assert!(speaker.calls.lock().expect("锁").is_empty());
}

#[tokio::test]
async fn fresh_topic_still_fires_with_recent_history_present() {
    let (_d, store, speaker, engine, group) = fixture(cfg()).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    seed_memory(&store, "上周有人过生日吃了蛋糕").await;
    let mut state = SpontaneousState {
        day: local_day(BASE),
        ..Default::default()
    };
    state.recent_topics.push(RecentTopic {
        topic: "高达模型拼装进度".to_string(),
        at: BASE - 3_600,
    });
    seed_state(&store, state).await;
    engine.tick_group(&group, BASE).await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
    // The fired topic is recorded for the next round's checks.
    let state = read_state(&store).await;
    assert_eq!(state.recent_topics.len(), 2);
}

#[tokio::test]
async fn openings_keep_a_minimum_gap_across_the_day_rollover() {
    let (_d, store, speaker, engine, group) = fixture(cfg()).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    seed_memory(&store, "群里聊过高达模型取向").await;
    // Yesterday's state: the last opening was 79 minutes ago — the exact
    // 2026-11-11/12 spacing that slipped past the per-day cap at midnight.
    let mut state = SpontaneousState {
        day: local_day(BASE - 86_400),
        ..Default::default()
    };
    state.last_at = BASE - 79 * 60;
    seed_state(&store, state).await;
    engine.tick_group(&group, BASE).await;
    assert!(speaker.calls.lock().expect("锁").is_empty());
    // Once the floor has passed, an opening may fire again.
    engine
        .tick_group(&group, BASE - 79 * 60 + MIN_OPENING_INTERVAL + 60)
        .await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
}

struct CaptureModel {
    prompts: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl SummaryModel for CaptureModel {
    async fn summarise(&self, prompt: String) -> Result<String, crate::error::ErrorClass> {
        self.prompts.lock().expect("锁").push(prompt);
        Ok("{\"topic\":\"周末去哪骑车\",\"angle\":\"从天气聊起\"}".to_string())
    }
}

#[tokio::test]
async fn angle_prompt_lists_recent_topics() {
    let (_d, store, _speaker, _engine, _group) = fixture(cfg()).await;
    seed_memory(&store, "群里聊过高达模型取向").await;
    let model = Arc::new(CaptureModel {
        prompts: std::sync::Mutex::new(Vec::new()),
    });
    let engine = SpontaneousEngine::new(
        Arc::clone(&store),
        model.clone(),
        Arc::new(FakeSpeaker::default()) as Arc<dyn OpeningSpeaker>,
        Arc::new(RunningTurns::new()),
        "bot".to_string(),
        cfg(),
        60,
    );
    let mut state = SpontaneousState::default();
    state.recent_topics.push(RecentTopic {
        topic: "肥鱼的新110盘片到了没".to_string(),
        at: BASE - 3_600,
    });
    let picked = engine
        .pick_new_angle("100", &state, &[])
        .await
        .expect("能选出话题");
    assert_eq!(picked.brief.topic, "周末去哪骑车");
    let prompts = model.prompts.lock().expect("锁");
    assert_eq!(prompts.len(), 1);
    assert!(
        prompts[0].contains("最近已经开过这些话题"),
        "{}",
        prompts[0]
    );
    assert!(
        prompts[0].contains("肥鱼的新110盘片到了没"),
        "{}",
        prompts[0]
    );
}

#[tokio::test]
async fn sent_opening_without_a_reply_resolves_ignored_with_long_cooldown() {
    let (_d, store, _speaker, engine, group) = fixture(cfg()).await;
    let fired = BASE - ANSWER_WINDOW - 1;
    seed_pending(
        &store,
        PendingOpening {
            topic: "周末去哪骑车".to_string(),
            key: "angle:abc".to_string(),
            enqueued_at: fired - 60,
            batch_id: Some("sp".into()),
            fired_at: Some(fired),
        },
    )
    .await;
    engine.tick_group(&group, BASE).await;
    let state = read_state(&store).await;
    assert!(state.pending.is_none());
    assert_eq!(state.cooldown.len(), 1);
    assert!(state.cooldown[0].ignored);
    assert_eq!(state.cooldown[0].until, fired + FOURTEEN_DAYS);
}

/// [spont-pace] Only normal/lively groups are opened at: a quiet-paced
/// group stays silent however long the silence, while the same fixture
/// at lively fires.
#[tokio::test]
async fn quiet_paced_group_is_never_opened_at() {
    let (_d, store, speaker, engine, _group) = fixture(cfg()).await;
    human_says(&store, BASE - 3 * 3_600, "m1").await;
    seed_memory(&store, "群里聊过高达模型取向").await;
    store
        .set_group_ambient_pace("bot", "100", "quiet", BASE)
        .await
        .expect("设档位");
    let quiet_group = store
        .get_group("bot", "100")
        .await
        .expect("读组")
        .expect("组在");
    engine.tick_group(&quiet_group, BASE).await;
    assert!(speaker.calls.lock().expect("锁").is_empty());
    store
        .set_group_ambient_pace("bot", "100", "lively", BASE)
        .await
        .expect("改档位");
    let lively_group = store
        .get_group("bot", "100")
        .await
        .expect("读组")
        .expect("组在");
    engine.tick_group(&lively_group, BASE).await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
}

/// [spont-stagger] The overnight quiet stretch does not count as
/// silence: a group whose last human spoke at 23:30 must not open at
/// 08:30 (thirty minutes after the window ends), only once its own
/// threshold of post-quiet silence has accumulated.
#[tokio::test]
async fn quiet_hours_do_not_manufacture_morning_eligibility() {
    let mut c = cfg();
    c.quiet_start_hour = 23;
    c.quiet_end_hour = 8;
    let (_d, store, speaker, engine, group) = fixture(c).await;
    // 2026-11-10 23:30 CST: the evening's last human message.
    human_says(&store, 1_794_324_600, "m1").await;
    seed_memory(&store, "群里聊过高达模型取向").await;
    // 2026-11-11 08:30 CST: nine hours since the message, but only
    // thirty minutes since the quiet window ended — no opening.
    engine.tick_group(&group, 1_794_357_000).await;
    assert!(speaker.calls.lock().expect("锁").is_empty());
    // 2026-11-11 10:30 CST: 150 minutes of post-quiet silence, past
    // every threshold in [45, 120] — the opening fires.
    engine.tick_group(&group, 1_794_364_200).await;
    assert_eq!(speaker.calls.lock().expect("锁").len(), 1);
}

/// [seal-4] Loop picking skips sealed topics — by keyword in the text
/// and by slot — and falls through to the next live loop.
#[test]
fn pickable_loop_skips_sealed_topics() {
    use crate::memory::compaction::OpenLoop;
    let lp = |text: &str, slot: &str| OpenLoop {
        text: text.to_string(),
        due: None,
        alias: String::new(),
        slot: slot.to_string(),
    };
    let loops = vec![
        lp("等水泥干透", ""),
        lp("拼单买豆", "水泥梗"),
        lp("周五去观鸟", "观鸟"),
    ];
    let sealed = [crate::store::SealedTopic {
        keywords: vec!["水泥".to_string()],
        slot: "水泥梗".to_string(),
        ..Default::default()
    }];
    let state = SpontaneousState::default();
    let picked = first_pickable_loop(&loops, &state, 1_000, &sealed).expect("应有可挑的");
    assert_eq!(picked.text, "周五去观鸟");
    assert!(first_pickable_loop(&loops[..2], &state, 1_000, &sealed).is_none());
    // Without the seal, the first loop is pickable as before.
    let picked = first_pickable_loop(&loops, &state, 1_000, &[]).expect("无封存时照常");
    assert_eq!(picked.text, "等水泥干透");
}
