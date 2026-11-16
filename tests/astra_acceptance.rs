//! R31 离线组合回放：真实 store / 真实 executor / 真实 resident（假 PTY）／
//! 真实 outbox → 回环 OneBot，覆盖 REPORT K5 的七个场景。
//!
//! 纪律（PLAN R31）：
//! * fake 只承担「外部世界」，不重写被测算法：`run_batch`、`barrier::evaluate`、
//!   `step_stream::parse_steps`、`Outbox`、`call_submit`、`call_generate` 全是生产实现。
//! * 假 PTY 用 shell 脚本扮演 agy 的 TUI（横幅 + 就绪标记 + 回显粘贴），步骤库由测试
//!   侧按真实 protobuf 结构写入，走 `DbStepReader` 的正规读取路径。
//! * 注入重复/乱序/超时：重复提交、插入卡与提交竞争、发送结果 unknown。
//! * 不发真实群消息、不调模型、不烧生图额度。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;

use gemini_qq_bot::agent::prompt::DEFAULT_PERSONA;
use gemini_qq_bot::app::preflight::{ImageDescriber, ImageTag};
use gemini_qq_bot::app::resident::ResidentManager;
use gemini_qq_bot::app::turn::{ResidentLimits, RunningTurns, TurnExecutor, shared_turn_limit};
use gemini_qq_bot::config::OutputChannel;
use gemini_qq_bot::health::{HealthRegistry, HealthSnapshot};
use gemini_qq_bot::media::download::{DownloadedImage, ImageFetcher, MediaError};
use gemini_qq_bot::memory::retrieval::MemoryContextBuilder;
use gemini_qq_bot::onebot::connection::Connection;
use gemini_qq_bot::onebot::normalize::{IncomingMessage, Segment, SpeakerRole};
use gemini_qq_bot::pipeline::outbox::Outbox;
use gemini_qq_bot::pipeline::scheduler::BatchJob;
use gemini_qq_bot::process::mihomo::{Mihomo, MihomoConfig};
use gemini_qq_bot::state::MemoryApplier;
use gemini_qq_bot::store::migrations;
use gemini_qq_bot::store::{OutboxRecord, OutboxState, SqliteStore};

const BOT: &str = "1";
const GROUP: &str = "100";
const OWNER: &str = "9";
const MEMBER: &str = "555";

// ---------------------------------------------------------------- 外部世界假件

struct FixtureFetcher {
    bytes: Vec<u8>,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl ImageFetcher for FixtureFetcher {
    async fn fetch(&self, _url: &str, _max_bytes: u64) -> Result<DownloadedImage, MediaError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(DownloadedImage {
            bytes: self.bytes.clone(),
            content_type: "image/jpeg".into(),
        })
    }
}

struct SilentDescriber;

#[async_trait::async_trait]
impl ImageDescriber for SilentDescriber {
    async fn describe(&self, _bot_id: &str, _group_id: &str, images: &[String]) -> Vec<ImageTag> {
        images
            .iter()
            .enumerate()
            .map(|(index, _)| ImageTag {
                index,
                kind: "picture".into(),
                ocr: String::new(),
                description: "一张图".into(),
                emotion: String::new(),
                self_ref: "other".into(),
                nsfw: false,
            })
            .collect()
    }
}

/// 这个 socket 怎么坏。
#[derive(Clone, Copy, PartialEq)]
enum BotFace {
    /// 正常回 echo。
    Ok,
    /// 收到请求但永不回包 → 客户端看到 `SendOutcome::Unknown`（真实的「断流」）。
    Silent,
    /// 明确拒绝（retcode 非 0）。
    Rejecting,
}

/// 只用来等 websocket 握手完成的 sink。
struct ReadySink(Arc<tokio::sync::Notify>);

#[async_trait::async_trait]
impl gemini_qq_bot::onebot::connection::FrameSink for ReadySink {
    async fn on_event(&self, _: serde_json::Value) {}
    async fn on_connected(&self) {
        self.0.notify_one();
    }
}

/// 回环 OneBot：收帧、记录、按 face 决定怎么回。
struct OneBotLink {
    url: String,
    frames: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>,
}

async fn start_onebot(face: BotFace) -> OneBotLink {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let frames = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&frames);
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut socket) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        let mut next_id: u64 = 1000;
        while let Some(Ok(message)) = socket.next().await {
            if !message.is_text() {
                continue;
            }
            let Ok(value) =
                serde_json::from_str::<serde_json::Value>(message.to_text().unwrap_or_default())
            else {
                continue;
            };
            let action = value
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let echo = value.get("echo").cloned().unwrap_or(json!(null));
            sink.lock().await.push(value);
            if face == BotFace::Silent {
                continue;
            }
            if face == BotFace::Rejecting {
                let reply = json!({"status":"failed","retcode":100,"data":null,"echo":echo});
                let _ = socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        reply.to_string().into(),
                    ))
                    .await;
                continue;
            }
            if action == "send_group_msg" || action == "send_poke" {
                next_id += 1;
                let reply = json!({
                    "status": "ok",
                    "retcode": 0,
                    "data": {"message_id": next_id},
                    "echo": echo
                });
                let _ = socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        reply.to_string().into(),
                    ))
                    .await;
            }
        }
    });
    OneBotLink {
        url: format!("ws://{address}"),
        frames,
    }
}

// ---------------------------------------------------------------- 假 PTY

/// 假 agy 的 TUI：横幅 + 就绪标记，然后把每一行粘贴回显进 inbox。
///
/// 每读完一行都重印就绪标记：真实 TUI 答完一轮会回到 `? for shortcuts`，
/// 屏幕空闲是 `try_resident_insert` 敢往里插话的前提。
fn fake_agy_script(inbox: &Path) -> String {
    format!(
        "#!/bin/sh\n\
         printf 'Antigravity CLI test\\r\\n'\n\
         printf '? for shortcuts\\r\\n'\n\
         while IFS= read -r line; do\n\
         \x20 clean=$(printf '%s' \"$line\" | sed -e 's/\\x1b\\[200~//g' -e 's/\\x1b\\[201~//g')\n\
         \x20 printf '%s\\n' \"$clean\" >> '{inbox}'\n\
         \x20 printf '? for shortcuts\\n'\n\
         done\n",
        inbox = inbox.display()
    )
}

fn write_exec(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).expect("写脚本");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&path).expect("meta").permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).expect("chmod");
    }
    path
}

// ---------------------------------------------------------------- 步骤库编码器
//
// 与 src/agent/step_stream/tests.rs 同构：手写极小 protobuf，让 parse_steps 走
// 真实解码路径，而不是被 fake 短路。

fn varint_bytes(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

fn tag_ld(field: u32) -> Vec<u8> {
    varint_bytes((u64::from(field) << 3) | 2)
}

fn tag_vint(field: u32) -> Vec<u8> {
    varint_bytes(u64::from(field) << 3)
}

fn ld(field: u32, body: &[u8]) -> Vec<u8> {
    let mut out = tag_ld(field);
    out.extend(varint_bytes(body.len() as u64));
    out.extend_from_slice(body);
    out
}

fn vint(field: u32, v: u64) -> Vec<u8> {
    let mut out = tag_vint(field);
    out.extend(varint_bytes(v));
    out
}

fn cat(parts: &[Vec<u8>]) -> Vec<u8> {
    parts.iter().flat_map(|p| p.iter().copied()).collect()
}

fn ts_meta(sec: u64, nanos: u64) -> Vec<u8> {
    let stamp = cat(&[vint(1, sec), vint(2, nanos)]);
    ld(5, &ld(1, &stamp))
}

fn user_payload(text: &str, sec: u64) -> Vec<u8> {
    cat(&[ts_meta(sec, 500_000_000), ld(19, &ld(2, text.as_bytes()))])
}

fn model_text_payload(text: &str, sec: u64) -> Vec<u8> {
    cat(&[ts_meta(sec, 0), ld(20, &ld(1, text.as_bytes()))])
}

const STEP_USER: i64 = 14;
const STEP_MODEL: i64 = 15;

/// 可写的步骤库：测试侧扮演「agy 自己写下转录」。
///
/// 恒懒创建：`DeliveryFloor::accepts` 在会话尚未绑定 DB 时只接受「粘贴前不存在的
/// 文件」，所以这个库必须在交付地板快照之后才落盘（`try_resident_insert` 路径下
/// 地板在粘贴前一刻才快照，因此只允许在 push 时才首次创建）。
struct StepDb {
    path: PathBuf,
    pool: tokio::sync::Mutex<Option<sqlx::SqlitePool>>,
    next_idx: std::sync::atomic::AtomicI64,
}

impl StepDb {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            pool: tokio::sync::Mutex::new(None),
            next_idx: std::sync::atomic::AtomicI64::new(0),
        }
    }

    async fn pool(&self) -> sqlx::SqlitePool {
        let mut slot = self.pool.lock().await;
        if let Some(pool) = slot.as_ref() {
            return pool.clone();
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).expect("步骤库目录");
        }
        let opts = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&self.path)
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .expect("步骤库");
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS steps (idx INTEGER PRIMARY KEY, step_type INTEGER NOT NULL DEFAULT 0, step_payload BLOB)",
        )
        .execute(&pool)
        .await
        .expect("建表");
        *slot = Some(pool.clone());
        pool
    }

    async fn push(&self, kind: &str, text: &str, sec: u64) -> i64 {
        let idx = self.next_idx.fetch_add(1, Ordering::SeqCst);
        let pool = self.pool().await;
        let (step_type, payload) = if kind == "user" {
            (STEP_USER, user_payload(text, sec))
        } else {
            (STEP_MODEL, model_text_payload(text, sec))
        };
        sqlx::query("INSERT INTO steps (idx, step_type, step_payload) VALUES (?, ?, ?)")
            .bind(idx)
            .bind(step_type)
            .bind(payload)
            .execute(&pool)
            .await
            .expect("写步骤");
        idx
    }
}

// ---------------------------------------------------------------- Lab

struct Lab {
    dir: tempfile::TempDir,
    store: Arc<SqliteStore>,
    executor: Arc<TurnExecutor>,
    manager: Arc<ResidentManager>,
    frames: Arc<tokio::sync::Mutex<Vec<serde_json::Value>>>,
    inbox: PathBuf,
    step_db: tokio::sync::RwLock<StepDb>,
    step_db_seq: std::sync::atomic::AtomicU32,
}

impl Lab {
    async fn new(channel: OutputChannel, face: BotFace) -> Self {
        Self::with_limits(
            channel,
            face,
            ResidentLimits {
                // 离线回放不需要真的等 5 秒静默，也不需要 15 秒确认窗口。
                quiet_s: 0.3,
                deliver_timeout_s: 8,
                poll_ms: 100,
                ..ResidentLimits::default()
            },
        )
        .await
    }

    async fn with_limits(channel: OutputChannel, face: BotFace, limits: ResidentLimits) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bot.sqlite3");
        let pool = migrations::open_and_migrate(&path).await.expect("迁移");
        let store = Arc::new(SqliteStore::new(pool));
        store.ensure_group(BOT, GROUP, 1).await.expect("建组");
        store
            .set_group_flags(BOT, GROUP, Some(true), Some(false), Some(true), 1)
            .await
            .expect("开组");
        store
            .ensure_member(BOT, GROUP, MEMBER, 1)
            .await
            .expect("成员");
        store
            .ensure_member(BOT, GROUP, OWNER, 1)
            .await
            .expect("主人");

        let inbox = dir.path().join("inbox.txt");
        std::fs::write(&inbox, b"").expect("inbox");
        let bin = write_exec(dir.path(), "agy", &fake_agy_script(&inbox));
        let link = start_onebot(face).await;
        let (supervisor, connection) = Connection::new(link.url.clone(), None);
        let connected = Arc::new(tokio::sync::Notify::new());
        let client = tokio::spawn(supervisor.run(Arc::new(ReadySink(Arc::clone(&connected)))));
        tokio::time::timeout(Duration::from_secs(5), connected.notified())
            .await
            .expect("onebot 连接");
        let _ = &client;

        let manager = Arc::new(ResidentManager::new(
            Arc::clone(&store),
            BOT.into(),
            bin,
            dir.path().to_path_buf(),
            "test-model".into(),
        ));

        let step_db_name = "00000000-0000-4000-8000-000000000002".to_string();
        let conversations = dir.path().join(".gemini/antigravity-cli/conversations");
        let step_db = StepDb::new(&conversations.join(format!("{step_db_name}.db")));

        let executor = Arc::new(TurnExecutor {
            no_submission_retry_groups: Vec::new(),
            webhook: None,
            store: Arc::clone(&store),
            connection,
            outbox: Outbox::new(Arc::clone(&store)),
            memory: MemoryApplier::new(Arc::clone(&store)),
            mihomo: Arc::new(Mihomo::new(
                MihomoConfig {
                    bin: PathBuf::new(),
                    config_path: PathBuf::new(),
                    api_port: 0,
                    proxy_port: 0,
                    api_secret: None,
                },
                false,
            )),
            health: Arc::new(HealthRegistry::new(HealthSnapshot::default())),
            bot_id: BOT.into(),
            owner_qq_id: OWNER.into(),
            model: "gemini-3.8-flash".into(),
            timeout: Duration::from_secs(30),
            output_channel: channel,
            conversation_turn_limit: shared_turn_limit(None),
            context_builder: MemoryContextBuilder::new(1200),
            persona_default: DEFAULT_PERSONA.to_string(),
            media_fetcher: Arc::new(FixtureFetcher {
                bytes: Vec::new(),
                calls: AtomicUsize::new(0),
            }),
            media_max_bytes: 1_000_000,
            media_max_pixels: 1_000_000,
            preflight: None,
            image_describer: Some(Arc::new(SilentDescriber)),
            hourly_call_budget: 0,
            sticker_dir: dir.path().join("stickers"),
            imagegen_dir: dir.path().join("imagegen"),
            agy_projects_dir: dir.path().join("projects"),
            secrets: vec!["synthetic-credential-value".into()],
            running_turns: Arc::new(RunningTurns::new()),
            quote_enabled: true,
            digest: None,
            resident: Arc::clone(&manager),
            resident_limits: limits,
            pacing: None,
            previous_batch: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        });

        Self {
            dir,
            store,
            executor,
            manager,
            frames: link.frames,
            inbox,
            step_db: tokio::sync::RwLock::new(step_db),
            step_db_seq: std::sync::atomic::AtomicU32::new(2),
        }
    }

    fn root(&self) -> PathBuf {
        self.dir.path().to_path_buf()
    }

    /// 会话目录（resident 通过 `conversations_dir(home)` 定位步骤库）。
    fn conversations(&self) -> PathBuf {
        self.root().join(".gemini/antigravity-cli/conversations")
    }

    async fn known_db(&self) -> PathBuf {
        self.step_db.read().await.path.clone()
    }

    /// 会话退役后生产会清掉 conversation id，下一轮是全新对话：agy 会新建
    /// 一个步骤库文件。测试侧同步换一个新的步骤库（旧文件留在目录里当诱饵），
    /// 否则交付地板只认「粘贴前不存在的文件」，重生轮次的回显永远确认不了。
    async fn fresh_conversation(&self) {
        let seq = self.step_db_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let name = format!("00000000-0000-4000-8000-{seq:012}");
        let path = self.conversations().join(format!("{name}.db"));
        *self.step_db.write().await = StepDb::new(&path);
    }

    async fn enqueue(&self, message_id: &str, sender: &str, text: &str) -> i64 {
        let event = incoming(message_id, sender, text);
        let payload = serde_json::to_string(&event).expect("payload");
        let wrote = self
            .store
            .insert_pending(
                BOT,
                GROUP,
                message_id,
                sender,
                gemini_qq_bot::now_unix(),
                &payload,
                "[]",
            )
            .await
            .expect("入队");
        assert!(wrote, "待处理消息应写入");
        let pending = self
            .store
            .pending_for_group(BOT, GROUP)
            .await
            .expect("待处理");
        pending.last().map(|p| p.id).expect("刚写入的行")
    }

    async fn claim(&self, ids: &[i64], batch: &str) -> BatchJob {
        let claimed = self.store.claim_batch(ids, batch).await.expect("claim");
        assert_eq!(claimed.len(), ids.len(), "claim 必须拿到全部行");
        BatchJob {
            group_id: GROUP.into(),
            batch_id: batch.into(),
            event_ids: claimed,
            privileged: false,
            chars: 10,
            images: 0,
            flooded: false,
            flood_background: None,
            retry: false,
            spontaneous: None,
        }
    }

    async fn inbox_lines(&self) -> Vec<String> {
        std::fs::read_to_string(&self.inbox)
            .unwrap_or_default()
            .lines()
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect()
    }

    async fn wait_inbox(&self, want: usize) -> Vec<String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        loop {
            let lines = self.inbox_lines().await;
            if lines.len() >= want || tokio::time::Instant::now() > deadline {
                return lines;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn all_outbox(&self) -> Vec<OutboxRecord> {
        let mut rows = Vec::new();
        for state in [
            OutboxState::Pending,
            OutboxState::Sending,
            OutboxState::Sent,
            OutboxState::Unknown,
        ] {
            rows.extend(self.store.outbox_batch(state, 100).await.expect("outbox"));
        }
        rows
    }

    async fn audits(&self) -> Vec<String> {
        self.store
            .recent_audit(500)
            .await
            .expect("audit")
            .into_iter()
            .map(|a| a.action)
            .collect()
    }

    async fn sent_texts(&self) -> Vec<String> {
        self.frames
            .lock()
            .await
            .iter()
            .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
            .filter_map(|f| {
                f.get("params")
                    .and_then(|p| p.get("message"))
                    .and_then(|m| m.as_array())
                    .map(|segs| {
                        segs.iter()
                            .filter_map(|s| s.get("data").and_then(|d| d.get("text")))
                            .filter_map(|t| t.as_str())
                            .collect::<Vec<_>>()
                            .join("")
                    })
            })
            .collect()
    }

    /// 等 inbox 稳定（连续两次同样行数）后返回全文，代表假 PTY 真正收到的那段粘贴。
    async fn stable_inbox(&self, at_least: usize) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut last = Vec::new();
        loop {
            let lines = self.inbox_lines().await;
            if lines.len() >= at_least && lines == last {
                return lines.join("\n");
            }
            if tokio::time::Instant::now() > deadline {
                return lines.join("\n");
            }
            last = lines;
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    }

    /// 测试侧拌演 agy：把「刚粘贴的那段话」当作 user 步写进真转录。
    async fn push_user_echo(&self, delivered: &str) -> i64 {
        let now = gemini_qq_bot::now_unix() as u64;
        self.step_db.read().await.push("user", delivered, now).await
    }

    async fn push_model_text(&self, text: &str) -> i64 {
        let now = gemini_qq_bot::now_unix() as u64;
        self.step_db.read().await.push("model", text, now).await
    }

    async fn arm(&self) -> Option<String> {
        self.store
            .armed_turn(GROUP)
            .await
            .expect("arm")
            .map(|a| a.turn_token)
    }

    /// 按生产 OpeningSpeaker 的方式造一个自发批次（合成事件＋主题卡）。
    async fn enqueue_spontaneous(&self, topic: &str, angle: &str, seq: u32) -> BatchJob {
        let now = gemini_qq_bot::now_unix();
        let text = format!("想聊聊：{topic}");
        let message_id = format!("spontaneous-{now}-{seq:06}");
        let message = IncomingMessage {
            bot_id: BOT.into(),
            group_id: GROUP.into(),
            message_id: message_id.clone(),
            sender_id: BOT.into(),
            speaker_ref: "bot".into(),
            speaker_role: SpeakerRole::Bot,
            display_name: String::new(),
            known_alias: None,
            received_at: now,
            segments: vec![Segment::Text { text: text.clone() }],
            text,
            mentions_bot: false,
            quote_of: None,
        };
        let payload = serde_json::to_string(&message).expect("payload");
        let batch_id = format!("sp-{BOT}-{GROUP}-{message_id}");
        let row_id = self
            .store
            .insert_spontaneous_pending(BOT, GROUP, &message_id, now, &payload, &batch_id)
            .await
            .expect("插入自发事件")
            .expect("应插入");
        BatchJob {
            group_id: GROUP.into(),
            batch_id,
            event_ids: vec![row_id],
            privileged: false,
            chars: 10,
            images: 0,
            flooded: false,
            flood_background: None,
            retry: false,
            spontaneous: Some(gemini_qq_bot::pipeline::scheduler::SpontaneousBrief {
                topic: topic.into(),
                angle: angle.into(),
            }),
        }
    }

    /// 生图任务行数（全群）。
    async fn image_job_count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM image_jobs")
            .fetch_one(self.store.pool())
            .await
            .expect("数生图任务")
    }
}

// ---------------------------------------------------------------- 测试驾驶

use gemini_qq_bot::app::turn::TurnResult;

impl Lab {
    fn scope(&self) -> gemini_qq_bot::mcp::Scope {
        gemini_qq_bot::mcp::Scope {
            bot_id: BOT.into(),
            group_id: GROUP.into(),
        }
    }

    /// 扮演模型调用 submit_reply（走生产 handler 本体，令牌取当前武装）。
    async fn submit(&self, args: serde_json::Value) -> serde_json::Value {
        let token = self.arm().await.expect("本轮应已武装");
        self.submit_with(&token, args).await
    }

    /// 用指定令牌调用 submit_reply（同秒新旧代次对照用）。
    async fn submit_with(&self, token: &str, mut args: serde_json::Value) -> serde_json::Value {
        args["turn_token"] = json!(token);
        gemini_qq_bot::mcp::submit::call_submit(&self.store, &self.scope(), &args).await
    }

    /// 等下一次粘贴（有界）：inbox 行数越过 cursor 且稳定后返回新增行文本并推进
    /// cursor；超时返回 None 且不推进。
    async fn try_next_paste(&self, cursor: &mut usize, wait: Duration) -> Option<String> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let lines = self.inbox_lines().await;
            if lines.len() > *cursor {
                tokio::time::sleep(Duration::from_millis(150)).await;
                let again = self.inbox_lines().await;
                if again.len() == lines.len() {
                    let text = again[*cursor..].join("\n");
                    *cursor = again.len();
                    return Some(text);
                }
                continue;
            }
            if tokio::time::Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    }

    /// 等下一次粘贴：超时即失败并摊开现场。
    async fn next_paste(&self, cursor: &mut usize) -> String {
        match self.try_next_paste(cursor, Duration::from_secs(25)).await {
            Some(text) => text,
            None => panic!("等粘贴超时（cursor={}）: {}", *cursor, self.dump().await),
        }
    }

    /// 等武装换代：arm 的令牌变成与 old 不同为止。
    async fn wait_arm_change(&self, old: &str) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(token) = self.arm().await
                && token != old
            {
                return token;
            }
            assert!(
                tokio::time::Instant::now() <= deadline,
                "等武装换代超时: {}",
                self.dump().await
            );
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    }

    /// 起一轮：把 run_batch 放到后台，返回句柄。
    fn spawn_turn(&self, job: BatchJob) -> tokio::task::JoinHandle<TurnResult> {
        let exec = Arc::clone(&self.executor);
        tokio::spawn(async move { exec.run_batch(&job).await })
    }

    /// 失败时把审计与 outbox 摊开看（退避延期审计会刷屏，滤掉）。
    async fn dump(&self) -> String {
        let audits: Vec<String> = self
            .audits()
            .await
            .into_iter()
            .filter(|a| !a.contains("deferred_backoff"))
            .collect();
        let outbox = self.all_outbox().await;
        let arm = self.arm().await;
        let pending = self
            .store
            .pending_for_group(BOT, GROUP)
            .await
            .map(|p| p.len())
            .unwrap_or(usize::MAX);
        format!(
            "arm={arm:?} pending={pending}\naudits={audits:?}\noutbox={}",
            outbox
                .iter()
                .map(|r| format!("{}:{}", r.state, r.text))
                .collect::<Vec<_>>()
                .join(" | ")
        )
    }
}

/// 无提交驾驶：不断回显新粘贴并补一个 model 步，直到轮次结束。
async fn drive_unsubmitted(
    lab: &Lab,
    handle: &mut tokio::task::JoinHandle<TurnResult>,
    cursor: &mut usize,
) -> TurnResult {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match tokio::time::timeout(Duration::from_millis(400), &mut *handle).await {
            Ok(res) => return res.expect("join"),
            Err(_) => {
                assert!(
                    tokio::time::Instant::now() <= deadline,
                    "无提交轮次应在一分钟内收尾: {}",
                    lab.dump().await
                );
                let lines = lab.inbox_lines().await;
                if lines.len() > *cursor {
                    let pasted = lab.next_paste(cursor).await;
                    lab.push_user_echo(&pasted).await;
                    lab.push_model_text("还在想").await;
                }
            }
        }
    }
}

/// 退避感知地领走唯一一条待处理消息并起轮：退避期内重领只会延期，睡 1 秒
/// 重领，直到粘贴出现。返回轮次句柄与粘贴文本。
async fn spawn_through_backoff(
    lab: &Lab,
    cursor: &mut usize,
    batch_prefix: &str,
) -> (tokio::task::JoinHandle<TurnResult>, String) {
    // 先把 cursor 同步到当前 inbox 末尾：上一轮的重试粘贴等残留行不许冒充
    // 本轮的粘贴（调用前提是没有轮次在跑）。
    *cursor = lab.inbox_lines().await.len();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let pending = lab.store.pending_for_group(BOT, GROUP).await.unwrap();
        assert_eq!(pending.len(), 1, "应恰有一条待处理: {}", lab.dump().await);
        let job = lab
            .claim(&[pending[0].id], &format!("{batch_prefix}-{attempt}"))
            .await;
        let mut handle = lab.spawn_turn(job);
        let pasted = tokio::select! {
            res = &mut handle => {
                assert_eq!(
                    res.expect("join"),
                    TurnResult::DeferredProxy,
                    "退避期内只应延期: {}",
                    lab.dump().await
                );
                None
            }
            pasted = lab.try_next_paste(cursor, Duration::from_secs(30)) => pasted,
        };
        match pasted {
            Some(pasted) => return (handle, pasted),
            None => {
                assert!(
                    tokio::time::Instant::now() <= deadline,
                    "退避迟迟不结束: {}",
                    lab.dump().await
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// K5-1：原批次＋插入＋同秒提交＋无提交重生。
///
/// 第一幕：插入确认后武装换代，同秒内旧令牌提交必须被拒、新令牌只确认
/// 一次，原批与插入消息都被消费、不重复回答。第二幕：模型始终不提交，
/// 轮次按无提交收尾、会话退役，消息退回待处理。第三幕：同一消息重生后
/// 只被回答一次。
#[tokio::test]
async fn s1_insert_same_second_submit_and_no_submit_rebirth() {
    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let mut cursor = 0usize;

    // —— 第一幕 ——
    let m1 = lab.enqueue("s1-m1", MEMBER, "帮我看看这周的课表").await;
    let job = lab.claim(&[m1], "s1-b1").await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let token_old = lab.arm().await.expect("第一代武装");

    lab.enqueue("s1-m2", MEMBER, "顺便把周四的实验课也标出来")
        .await;
    // 等插入粘贴出现（途中若先来 nudge 粘贴，一并回显后继续等）。
    let insert_text = loop {
        let pasted = lab.next_paste(&mut cursor).await;
        lab.push_user_echo(&pasted).await;
        if pasted.contains("本轮新增消息") {
            break pasted;
        }
    };
    assert!(insert_text.contains("周四的实验课"), "插入内容应含新消息");
    let token_new = lab.wait_arm_change(&token_old).await;

    let stale = lab
        .submit_with(
            &token_old,
            json!({"should_reply": true, "reply_text": "旧代次回复"}),
        )
        .await;
    assert_eq!(stale["isError"], true, "旧代次提交必须被拒: {stale}");
    let fresh = lab
        .submit_with(
            &token_new,
            json!({"should_reply": true, "reply_text": "课表整理好了，周四实验课也标出来了"}),
        )
        .await;
    assert_eq!(fresh["isError"], false, "新代次提交必须被收: {fresh}");
    lab.push_model_text("已提交").await;
    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("轮次应结束")
        .expect("join");
    assert_eq!(result, TurnResult::Replied, "{}", lab.dump().await);
    let sent = lab.sent_texts().await;
    assert_eq!(sent.len(), 1, "只应发出一条回复: {sent:?}");
    assert!(
        sent[0].contains("周四实验课"),
        "回复应覆盖插入消息: {sent:?}"
    );
    assert!(
        lab.store
            .pending_for_group(BOT, GROUP)
            .await
            .unwrap()
            .is_empty(),
        "原批与插入都应被消费"
    );

    // —— 第二幕 ——
    let m3 = lab.enqueue("s1-m3", MEMBER, "图书馆今天几点关门").await;
    let job2 = lab.claim(&[m3], "s1-b2").await;
    let mut handle2 = lab.spawn_turn(job2);
    let pasted2 = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted2).await;
    lab.push_model_text("我先想想").await;
    let result2 = drive_unsubmitted(&lab, &mut handle2, &mut cursor).await;
    assert_eq!(result2, TurnResult::BadOutput, "{}", lab.dump().await);
    let audits = lab.audits().await;
    assert!(
        audits.iter().any(|a| a.contains("no_submission")),
        "应留无提交审计: {audits:?}"
    );
    assert_eq!(
        lab.sent_texts().await.len(),
        1,
        "无提交轮次不许发出任何东西"
    );
    let pending = lab.store.pending_for_group(BOT, GROUP).await.unwrap();
    assert_eq!(pending.len(), 1, "消息应退回待处理、不丢: {pending:?}");
    assert_eq!(pending[0].id, m3);

    // —— 第三幕：退避期内重领只会延期，退避过后重生、只答一次 ——
    // 退役已清掉 conversation id：下一轮是全新对话，步骤库换新文件。
    lab.fresh_conversation().await;
    let (mut handle3, pasted3) = spawn_through_backoff(&lab, &mut cursor, "s1-b3").await;
    lab.push_user_echo(&pasted3).await;
    let resp = lab
        .submit(json!({"should_reply": true, "reply_text": "图书馆今天十点关门"}))
        .await;
    assert_eq!(resp["isError"], false, "重生后提交应被收: {resp}");
    lab.push_model_text("已提交").await;
    let result3 = match tokio::time::timeout(Duration::from_secs(30), &mut handle3).await {
        Ok(res) => res.expect("join"),
        Err(_) => panic!("重生轮次超时: {}", lab.dump().await),
    };
    assert_eq!(result3, TurnResult::Replied, "{}", lab.dump().await);
    let sent = lab.sent_texts().await;
    assert_eq!(sent.len(), 2, "两条问题各答一次、不重复: {sent:?}");
    assert!(sent[1].contains("十点关门"));
    assert!(
        lab.store
            .pending_for_group(BOT, GROUP)
            .await
            .unwrap()
            .is_empty(),
        "重生后消息应被消费"
    );
}

fn incoming(message_id: &str, sender: &str, text: &str) -> IncomingMessage {
    IncomingMessage {
        bot_id: BOT.into(),
        group_id: GROUP.into(),
        message_id: message_id.into(),
        sender_id: sender.into(),
        speaker_ref: if sender == OWNER { "u1" } else { "u2" }.into(),
        speaker_role: if sender == OWNER {
            SpeakerRole::Owner
        } else {
            SpeakerRole::Member
        },
        display_name: String::new(),
        known_alias: None,
        received_at: gemini_qq_bot::now_unix(),
        segments: vec![Segment::Text { text: text.into() }],
        text: text.into(),
        mentions_bot: false,
        quote_of: None,
    }
}

/// 数已发出的某种动作帧。
async fn frame_count(lab: &Lab, action: &str) -> usize {
    lab.frames
        .lock()
        .await
        .iter()
        .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some(action))
        .count()
}

/// 数已发出的 send_group_msg 帧（发送尝试次数）。
async fn send_frame_count(lab: &Lab) -> usize {
    lab.frames
        .lock()
        .await
        .iter()
        .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
        .count()
}

/// 当前会话的 PTY 进程 pid（从 session.start 审计里取最新一条）。
async fn session_pid(lab: &Lab) -> u32 {
    lab.audits()
        .await
        .iter()
        .find_map(|a| {
            a.rsplit(".pid")
                .next()
                .filter(|_| a.contains("resident.session.start."))
                .and_then(|p| p.parse::<u32>().ok())
        })
        .expect("应有 session.start 审计")
}

async fn db_rows(path: &Path) -> Vec<(i64, i64)> {
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .read_only(true);
    let Ok(pool) = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
    else {
        return Vec::new();
    };
    sqlx::query_as("SELECT idx, step_type FROM steps ORDER BY idx")
        .fetch_all(&pool)
        .await
        .unwrap_or_default()
}

async fn steps_overview(lab: &Lab) -> String {
    let mut out = Vec::new();
    let dir = lab.conversations();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("db") {
                out.push(format!(
                    "{}: {:?}",
                    path.file_name().unwrap().to_string_lossy(),
                    db_rows(&path).await
                ));
            }
        }
    }
    out.join(" ")
}

/// K5-7：发送结果 unknown、重启恢复。
///
/// OneBot 收到请求但永不回包：这一行只能落 unknown、不许算 sent、更不许
/// 盲目重发。随后模拟进程重启：崩溃瞬间停在 sending 的行被 reconcile 收
/// 成 unknown，原 unknown 行保持原样，整个恢复过程不再多发一帧。
#[tokio::test]
async fn s7_send_unknown_and_restart_recovery() {
    let lab = Lab::new(OutputChannel::Mcp, BotFace::Silent).await;
    let mut cursor = 0usize;
    let m1 = lab.enqueue("s7-m1", MEMBER, "帮我订明天的高铁票").await;
    let job = lab.claim(&[m1], "s7-b1").await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let resp = lab
        .submit(json!({"should_reply": true, "reply_text": "好的，明天的高铁票订上了"}))
        .await;
    assert_eq!(resp["isError"], false, "提交应被接受: {resp}");
    lab.push_model_text("已提交").await;
    let _ = tokio::time::timeout(Duration::from_secs(60), handle)
        .await
        .expect("轮次应结束")
        .expect("join");
    assert_eq!(send_frame_count(&lab).await, 1, "只应尝试发送一次");
    // 注：frames 记录的是「发出的帧」，Silent 面孔下帧已出门但永不回执——
    // 是否送达只认 outbox 状态，不认帧记录。
    let rows = lab.all_outbox().await;
    assert_eq!(rows.len(), 1, "{}", lab.dump().await);
    assert_eq!(
        rows[0].state,
        "unknown",
        "应落 unknown: {}",
        lab.dump().await
    );

    // 崩溃瞬间停在 sending 的一行 + 以新 Outbox 实例模拟重启恢复。
    let now = gemini_qq_bot::now_unix();
    let stuck = lab
        .executor
        .outbox
        .enqueue(BOT, GROUP, "s7-crash", "崩溃时正在发的半条", now)
        .await
        .expect("建行");
    lab.executor.outbox.begin(&stuck, now).await.expect("begin");
    let restarted = Outbox::new(Arc::clone(&lab.store));
    let fixed = restarted.reconcile(now + 1).await.expect("reconcile");
    assert_eq!(fixed, 1, "应恰好收掉那行 sending");
    let rows = lab.all_outbox().await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(|r| r.state == "unknown"),
        "两行都应是 unknown: {}",
        lab.dump().await
    );
    assert_eq!(send_frame_count(&lab).await, 1, "恢复过程不许重发");
}

/// K5-2：主链超时/进程退出/PTY 无就绪。
///
/// 第一幕粘贴落了屏但转录永远不前进（TUI 没接住输入）：交付不确认、
/// 会话丢弃、消息退回待处理、不发任何工具自述。第二幕交付确认后 PTY
/// 进程被杀：轮次失败、消息再次退回。第三幕重生后只接一次、答一次。
#[tokio::test]
async fn s2_delivery_unconfirmed_and_process_death() {
    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let mut cursor = 0usize;

    // —— 第一幕 ——
    let m1 = lab.enqueue("s2-m1", MEMBER, "周末去哪儿玩比较好").await;
    let job = lab.claim(&[m1], "s2-b1").await;
    let handle = lab.spawn_turn(job);
    let _pasted = lab.next_paste(&mut cursor).await;
    let result = tokio::time::timeout(Duration::from_secs(60), handle)
        .await
        .expect("应在交付超时内收尾")
        .expect("join");
    assert_eq!(result, TurnResult::AgentFailed, "{}", lab.dump().await);
    let audits = lab.audits().await;
    assert!(
        audits.iter().any(|a| a.contains("deliver_unconfirmed")),
        "应留交付不确认审计: {audits:?}"
    );
    assert!(
        lab.sent_texts().await.is_empty(),
        "不许发出工具自述或任何文本"
    );
    assert!(lab.all_outbox().await.is_empty(), "不许留下 outbox 行");
    assert_eq!(
        lab.store.pending_for_group(BOT, GROUP).await.unwrap().len(),
        1,
        "消息不许丢"
    );

    // —— 第二幕：交付确认后直接杀掉 PTY 进程本体（close_all 是生命周期
    // 闩锁、会拒绝之后所有新会话，不能拿它冒充进程退出） ——
    lab.fresh_conversation().await;
    let (mut handle2, pasted2) = spawn_through_backoff(&lab, &mut cursor, "s2-b2").await;
    lab.push_user_echo(&pasted2).await;
    let pid = session_pid(&lab).await;
    let killed = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .expect("kill 应执行");
    assert!(killed.success(), "kill {pid} 应成功");
    let result2 = tokio::time::timeout(Duration::from_secs(60), &mut handle2)
        .await
        .expect("进程死后轮次应收尾")
        .expect("join");
    assert_eq!(result2, TurnResult::AgentFailed, "{}", lab.dump().await);
    assert!(
        lab.sent_texts().await.is_empty(),
        "进程死后不许发出任何东西"
    );
    assert_eq!(
        lab.store.pending_for_group(BOT, GROUP).await.unwrap().len(),
        1,
        "消息应再次退回"
    );

    // —— 第三幕 ——
    // 注意：崩溃路径（resident_fail）不清理 conversation id，重生会话会续用
    // 同一对话——步骤库不换文件（与无提交退役的 retire_resident 清 id 不同）。
    let (mut handle3, pasted3) = spawn_through_backoff(&lab, &mut cursor, "s2-b3").await;
    lab.push_user_echo(&pasted3).await;
    let resp = lab
        .submit(json!({"should_reply": true, "reply_text": "周末可以去爬山，天气不错"}))
        .await;
    assert_eq!(resp["isError"], false, "重生后提交应被收: {resp}");
    lab.push_model_text("已提交").await;
    let result3 = match tokio::time::timeout(Duration::from_secs(30), &mut handle3).await {
        Ok(res) => res.expect("join"),
        Err(_) => panic!("重生轮次超时: {}", lab.dump().await),
    };
    assert_eq!(result3, TurnResult::Replied, "{}", lab.dump().await);
    let sent = lab.sent_texts().await;
    assert_eq!(sent.len(), 1, "只答一次: {sent:?}");
    assert!(sent[0].contains("爬山"));
    assert!(
        lab.store
            .pending_for_group(BOT, GROUP)
            .await
            .unwrap()
            .is_empty(),
        "消息应被消费"
    );
}

/// K5-3：自发失败及其间的正常人类回复。
///
/// 自发轮次无论是被模型明确拒绝（should_reply=false）还是彻底无提交
/// 失败，都不许开口、不许戳人、不许生图，合成事件收尾即取消、不回池。
/// 其后一条正常人类消息照常被回答，且不被记成自发开场成功。
#[tokio::test]
async fn s3_spontaneous_failure_stays_silent_and_human_reply_not_misattributed() {
    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let mut cursor = 0usize;

    // —— 第一幕：自发开场被明确拒绝 ——
    let job = lab
        .enqueue_spontaneous("新出的游戏《星尘远征》", "聊聊它的开放世界", 1)
        .await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let resp = lab.submit(json!({"should_reply": false})).await;
    assert_eq!(resp["isError"], false, "拒绝开场的提交应被收: {resp}");
    lab.push_model_text("不开场").await;
    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("轮次应结束")
        .expect("join");
    assert_eq!(result, TurnResult::Silent, "{}", lab.dump().await);
    assert!(lab.sent_texts().await.is_empty(), "自发被拒不许开口");
    assert_eq!(frame_count(&lab, "send_poke").await, 0, "不许戳人");
    assert_eq!(lab.image_job_count().await, 0, "不许生图");
    assert!(
        lab.store
            .pending_for_group(BOT, GROUP)
            .await
            .unwrap()
            .is_empty(),
        "合成事件应被取消、不回池"
    );

    // —— 第二幕：自发轮次彻底无提交、失败收尾 ——
    let job2 = lab
        .enqueue_spontaneous("周末的同人展", "问问谁打算去", 2)
        .await;
    let mut handle2 = lab.spawn_turn(job2);
    let pasted2 = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted2).await;
    lab.push_model_text("想想怎么开场").await;
    let result2 = drive_unsubmitted(&lab, &mut handle2, &mut cursor).await;
    assert_eq!(result2, TurnResult::BadOutput, "{}", lab.dump().await);
    assert!(lab.sent_texts().await.is_empty(), "自发失败不许开口");
    assert_eq!(frame_count(&lab, "send_poke").await, 0, "不许戳人");
    assert_eq!(lab.image_job_count().await, 0, "不许生图");
    assert!(
        lab.store
            .pending_for_group(BOT, GROUP)
            .await
            .unwrap()
            .is_empty(),
        "失败的合成事件同样不许回池"
    );
    let spontaneous_audits = lab
        .audits()
        .await
        .iter()
        .filter(|a| a.contains("spontaneous"))
        .count();

    // —— 第三幕：正常人类回复，不冒充开场成功 ——
    lab.fresh_conversation().await;
    let _m = lab.enqueue("s3-m1", MEMBER, "今晚吃什么好").await;
    let (handle3, pasted3) = spawn_through_backoff(&lab, &mut cursor, "s3-b1").await;
    lab.push_user_echo(&pasted3).await;
    let resp = lab
        .submit(json!({"should_reply": true, "reply_text": "吃火锅吧，天冷"}))
        .await;
    assert_eq!(resp["isError"], false, "正常提交应被收: {resp}");
    lab.push_model_text("已提交").await;
    let result3 = tokio::time::timeout(Duration::from_secs(30), handle3)
        .await
        .expect("轮次应结束")
        .expect("join");
    assert_eq!(result3, TurnResult::Replied, "{}", lab.dump().await);
    let sent = lab.sent_texts().await;
    assert_eq!(sent.len(), 1, "正常回复只发一条: {sent:?}");
    assert!(sent[0].contains("火锅"));
    let spontaneous_after = lab
        .audits()
        .await
        .iter()
        .filter(|a| a.contains("spontaneous"))
        .count();
    assert_eq!(
        spontaneous_after, spontaneous_audits,
        "人类回复不许新增自发归因审计"
    );
}

/// K5-6：封存话题、退休记忆、跨群猜 ref。
///
/// 写入层：话题封存后，同 slot 或命中关键词的 add 都被 MemoryApplier
/// 拒为 RejectedSealed；检索输入层：list_memories 经 retain_unsealed
/// 过滤后封存行不再出现（load_memory_inputs 的同一组合）。退休层：
/// retire 后对同一 ref 的 update 拒为 RejectedArchived、内容不被改写。
/// 跨群层：同 slot 在未封存的 200 群可独立写入；拿别群的 ref 来 update
/// 两边都拒为 RejectedOutOfScope。搜索层：已退休记忆不被搜出。
#[tokio::test]
async fn s6_sealed_retired_and_cross_group_memory_isolation() {
    use gemini_qq_bot::agent::contract::{MemoryOp, MemoryOpKind};
    use gemini_qq_bot::state::memory::ApplyOutcome;

    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let store = Arc::clone(&lab.store);
    let applier = gemini_qq_bot::state::MemoryApplier::new(Arc::clone(&store));
    let now = gemini_qq_bot::now_unix();
    let no_ref = |_: &str| -> Option<String> { None };

    let add_op = |content: &str, slot: &str| MemoryOp {
        op: MemoryOpKind::Add,
        subject_ref: None,
        content: content.into(),
        kind: "fact".into(),
        source_refs: Vec::new(),
        confidence: 1.0,
        memory_ref: None,
        slot: Some(slot.into()),
        tags: Vec::new(),
        ttl: None,
    };
    let update_op = |target: &str, content: &str| MemoryOp {
        op: MemoryOpKind::Update,
        subject_ref: None,
        content: content.into(),
        kind: "fact".into(),
        source_refs: Vec::new(),
        confidence: 1.0,
        memory_ref: Some(target.into()),
        slot: None,
        tags: Vec::new(),
        ttl: None,
    };

    // 基线：一条正常记忆写入本群。
    let out = applier
        .apply(
            BOT,
            GROUP,
            &add_op("阿橘是一只橘猫，住在小明家", "宠物"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("基线写入");
    let ApplyOutcome::Added(ref1) = out else {
        panic!("基线应写入成功: {out:?}")
    };

    // 封存这个话题（slot＋关键词双通道）。
    store
        .add_sealed_topic(
            BOT,
            GROUP,
            &["橘猫".to_string()],
            "宠物",
            "验收封存",
            OWNER,
            now,
        )
        .await
        .expect("封存");

    // 写入层：同 slot 被拒、换 slot 但内容命中关键词也被拒。
    let out = applier
        .apply(
            BOT,
            GROUP,
            &add_op("阿橘最近胖了两斤", "宠物"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("封存后写入（slot）");
    assert_eq!(out, ApplyOutcome::RejectedSealed, "同 slot 应被封存拒绝");
    let out = applier
        .apply(
            BOT,
            GROUP,
            &add_op("橘猫又把花瓶推下去了", "日常"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("封存后写入（关键词）");
    assert_eq!(out, ApplyOutcome::RejectedSealed, "命中关键词应被封存拒绝");

    // 检索输入层：封存行不进入 load_memory_inputs 的组合结果。
    let sealed = store
        .list_sealed_topics(BOT, GROUP)
        .await
        .expect("读封存表");
    let mut rows = store.list_memories(BOT, GROUP, 500).await.expect("列记忆");
    assert_eq!(rows.len(), 1, "封存不删行，行还在库里");
    gemini_qq_bot::memory::sealed::retain_unsealed(&mut rows, &sealed);
    assert!(rows.is_empty(), "封存行不许进入检索/摘要输入: {rows:?}");

    // 退休：retire 本身不受封存阻挡；其后 update 必须被拒、内容不被改写。
    let retire_op = MemoryOp {
        op: MemoryOpKind::Retire,
        ..update_op(&ref1, "")
    };
    let out = applier
        .apply(BOT, GROUP, &retire_op, no_ref, no_ref, now)
        .await
        .expect("退休");
    assert_eq!(out, ApplyOutcome::Retired(ref1.clone()));
    let out = applier
        .apply(
            BOT,
            GROUP,
            &update_op(&ref1, "阿橘复活了"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("退休后更新");
    assert_eq!(
        out,
        ApplyOutcome::RejectedArchived(ref1.clone()),
        "退休行不许被更新复活"
    );
    let row = store
        .memory_by_ref(BOT, GROUP, &ref1)
        .await
        .expect("按 ref 读")
        .expect("行应还在");
    assert!(row.is_archived(), "行应保持退休状态");
    assert!(
        row.content.contains("住在小明家"),
        "内容不许被改写: {}",
        row.content
    );

    // 跨群：同 slot/内容在未封存的 200 群独立写入成功。
    store.ensure_group(BOT, "200", 1).await.expect("建 200 群");
    let out = applier
        .apply(
            BOT,
            "200",
            &add_op("阿橘是一只橘猫，住在小明家", "宠物"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("200 群写入");
    let ApplyOutcome::Added(ref2) = out else {
        panic!("200 群未封存、应写入成功: {out:?}")
    };
    assert_ne!(ref1, ref2, "两群的 ref 应不同");

    // 跨群猜 ref：两边互猜都被拒，且两边内容纹丝不动。
    let out = applier
        .apply(
            BOT,
            "200",
            &update_op(&ref1, "串写成 200 的了"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("200 猜 100 的 ref");
    assert_eq!(out, ApplyOutcome::RejectedOutOfScope);
    let out = applier
        .apply(
            BOT,
            GROUP,
            &update_op(&ref2, "串写成 100 的了"),
            no_ref,
            no_ref,
            now,
        )
        .await
        .expect("100 猜 200 的 ref");
    assert_eq!(out, ApplyOutcome::RejectedOutOfScope);
    let row2 = store
        .memory_by_ref(BOT, "200", &ref2)
        .await
        .expect("读 200 行")
        .expect("200 行应还在");
    assert!(!row2.is_archived());
    assert!(
        row2.content.contains("住在小明家"),
        "200 行不许被串改: {}",
        row2.content
    );

    // 搜索层：本群已退休的记忆不被搜出；200 群自己的行只归 200 群搜到。
    let scope100 = gemini_qq_bot::mcp::Scope {
        bot_id: BOT.into(),
        group_id: GROUP.into(),
    };
    let res = gemini_qq_bot::mcp::memory::call_memory_search(
        &store,
        &scope100,
        &json!({"query": "橘猫"}),
    )
    .await;
    assert!(
        !res.to_string().contains("阿橘"),
        "已退休记忆不许被搜出: {res}"
    );
    let scope200 = gemini_qq_bot::mcp::Scope {
        bot_id: BOT.into(),
        group_id: "200".into(),
    };
    let res = gemini_qq_bot::mcp::memory::call_memory_search(
        &store,
        &scope200,
        &json!({"query": "橘猫"}),
    )
    .await;
    assert!(
        res.to_string().contains("阿橘"),
        "200 群应搜到自己的行: {res}"
    );
}

/// K5-5：八段文字＋引用＋表情＋戳一戳混合提交；分段绕过与跨批引用。
///
/// 正路：一段带本批 quote_ref、共八段文字、一个菜单内表情、一个
/// poke_ref 一次提交——引用必须落在本批消息上、八段按序送达、表情
/// 帧的字节就是菜单里那张的文件、戳恰好一次。绕路一：字符串化段被
/// 结构校验当场拒；把自述拆成两段提交，自述永不出现在发出的帧里。
/// 绕路二：引用本批不存在的 m9，任何帧都不许带上引用段。
#[tokio::test]
async fn s5_mixed_parts_quote_sticker_poke_and_bypass_attempts() {
    use base64::Engine as _;

    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let store = Arc::clone(&lab.store);
    let now = gemini_qq_bot::now_unix();

    // —— 播种表情库：一张「开心」表情，库里 kept+enabled 才进菜单 ——
    let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    // 用 image 本体编码一张 4x4 JPEG：发送路径会解码重编码，
    // 字节必须能被同一套编解码器读回，手填的 base64 串容易抄坏。
    let mut img = image::RgbImage::new(4, 4);
    for px in img.pixels_mut() {
        *px = image::Rgb([255, 200, 40]);
    }
    let mut sticker_jpeg = Vec::new();
    image::DynamicImage::ImageRgb8(img)
        .write_to(
            &mut std::io::Cursor::new(&mut sticker_jpeg),
            image::ImageFormat::Jpeg,
        )
        .expect("编码表情 JPEG");
    let sticker_bytes: &[u8] = &sticker_jpeg;
    let sticker_dir = lab.root().join("stickers");
    std::fs::create_dir_all(&sticker_dir).expect("建表情目录");
    std::fs::write(sticker_dir.join(format!("{hash}.jpg")), sticker_bytes).expect("写表情文件");
    store
        .upsert_sticker(
            hash,
            BOT,
            GROUP,
            &format!("stickers/{hash}.jpg"),
            "sticker",
            "other",
            "今天好开心啊，哈哈哈，开心大笑",
            "笑得很开心的猫",
            "开心",
            "开心 哈哈哈 大笑",
            None,
            false,
            true,
            now,
        )
        .await
        .expect("登记表情");
    store
        .set_sticker_kept(BOT, hash, 1)
        .await
        .expect("收入表情库");

    // —— 正路：两条消息一批，八段＋引用＋表情＋戳 ——
    let m1 = lab
        .enqueue("s5-m1", MEMBER, "今天好开心啊，哈哈哈，帮我把这句翻成英文")
        .await;
    let m2 = lab.enqueue("s5-m2", OWNER, "翻完顺手戳一下小明").await;
    let job = lab.claim(&[m1, m2], "s5-a").await;
    let handle = lab.spawn_turn(job);
    let mut cursor = 0usize;
    let pasted = lab.next_paste(&mut cursor).await;
    assert!(
        pasted.contains("#e3b0c442"),
        "表情候选应带着短编号进提示词: {pasted}"
    );
    lab.push_user_echo(&pasted).await;
    let resp = lab
        .submit(json!({
            "should_reply": true,
            "reply_parts": [
                {"text": "翻译好了：", "quote_ref": "m1", "quote_echo": "好开心"},
                "I am so happy today, hahaha",
                "这句语气很轻松",
                "也可以说 I feel great today",
                "两种都自然",
                "看场合挑一个就行",
                "哈哈哈那段保留了原样",
                "下次还能帮你翻"
            ],
            "sticker_ops": [{"op": "send", "hash": "e3b0c442"}],
            "poke_ref": "u2"
        }))
        .await;
    assert!(
        resp.get("isError").and_then(|v| v.as_bool()) != Some(true),
        "混合提交应被接受: {resp}"
    );
    lab.push_model_text("翻译和表情都发完了").await;
    let result = handle.await.expect("正路轮次任务");
    assert!(
        matches!(result, TurnResult::Replied),
        "正路应回复成功: {result:?}\n{}",
        lab.dump().await
    );

    let frames = lab.frames.lock().await.clone();
    let sends: Vec<&serde_json::Value> = frames
        .iter()
        .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
        .collect();
    assert_eq!(
        sends.len(),
        9,
        "八段文字＋一张表情 = 9 帧: {sends:?}\n{}",
        lab.dump().await
    );
    // 第一段带引用，引用必须落在本批的 s5-m1 上。
    let first = sends[0]
        .get("params")
        .and_then(|p| p.get("message"))
        .and_then(|m| m.as_array())
        .expect("第一帧应有消息段");
    let reply_seg = first
        .iter()
        .find(|s| s.get("type").and_then(|t| t.as_str()) == Some("reply"))
        .expect("第一帧应带引用段");
    assert_eq!(
        reply_seg
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|i| i.as_str()),
        Some("s5-m1"),
        "引用必须落在本批消息上"
    );
    // 八段按序、内容完整。
    let texts: Vec<String> = sends
        .iter()
        .filter_map(|f| {
            f.get("params")
                .and_then(|p| p.get("message"))
                .and_then(|m| m.as_array())
                .and_then(|segs| {
                    segs.iter().find_map(|s| {
                        (s.get("type").and_then(|t| t.as_str()) == Some("text")).then(|| {
                            s.get("data")
                                .and_then(|d| d.get("text"))
                                .and_then(|t| t.as_str())
                                .unwrap_or_default()
                                .to_string()
                        })
                    })
                })
        })
        .collect();
    assert_eq!(
        texts,
        vec![
            "翻译好了：",
            "I am so happy today, hahaha",
            "这句语气很轻松",
            "也可以说 I feel great today",
            "两种都自然",
            "看场合挑一个就行",
            "哈哈哈那段保留了原样",
            "下次还能帮你翻"
        ],
        "八段应按序完整送达"
    );
    // 表情帧的字节就是菜单里那张。
    let _ = sticker_bytes;
    let sticker_frame = sends
        .iter()
        .find(|f| {
            f.get("params")
                .and_then(|p| p.get("message"))
                .and_then(|m| m.as_array())
                .map(|segs| {
                    segs.iter()
                        .any(|s| s.get("type").and_then(|t| t.as_str()) == Some("image"))
                })
                .unwrap_or(false)
        })
        .expect("应有一帧表情图片");
    let file = sticker_frame
        .get("params")
        .and_then(|p| p.get("message"))
        .and_then(|m| m.as_array())
        .and_then(|segs| {
            segs.iter().find_map(|s| {
                s.get("data")
                    .and_then(|d| d.get("file"))
                    .and_then(|f| f.as_str())
                    .map(str::to_string)
            })
        })
        .expect("表情帧应带文件");
    // 发送路径解码重编码，字节不必相等；但必须能解回同一张 4x4 图。
    let payload = file
        .strip_prefix("base64://")
        .expect("表情帧应是 base64 载荷");
    let sent_bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .expect("表情帧载荷应可解码");
    let img = image::load_from_memory(&sent_bytes).expect("表情帧字节应是有效图片");
    assert_eq!(
        (img.width(), img.height()),
        (4, 4),
        "发出的应是菜单里那张 4x4 表情"
    );
    // 戳恰好一次、戳的是小明。
    let pokes: Vec<&serde_json::Value> = frames
        .iter()
        .filter(|f| {
            f.get("action")
                .and_then(|a| a.as_str())
                .map(|a| a.contains("poke"))
                .unwrap_or(false)
        })
        .collect();
    assert_eq!(pokes.len(), 1, "一轮最多戳一次: {pokes:?}");
    assert_eq!(
        pokes[0]
            .get("params")
            .and_then(|p| p.get("user_id"))
            .and_then(|u| u.as_str()),
        Some(MEMBER),
        "戳的对象应是小明"
    );
    let audits = lab.audits().await;
    assert!(
        audits
            .iter()
            .any(|a| a.starts_with("sticker.offer.g100.") && a.contains("e3b0c442")),
        "offer 审计应记录这张表情: {audits:?}"
    );

    // —— 绕路一：字符串化段——要么被拒，要么被归一化成正常段，
    // 原始 JSON 串永远不许作为正文发出去 ——
    let m3 = lab.enqueue("s5-m3", MEMBER, "再帮我看看这句").await;
    let job = lab.claim(&[m3], "s5-b").await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let frames_before = frame_count(&lab, "send_group_msg").await;
    let resp = lab
        .submit(json!({
            "should_reply": true,
            "reply_parts": ["{\"text\":\"字符串化的段\"}"]
        }))
        .await;
    if resp.get("isError").and_then(|v| v.as_bool()) == Some(true) {
        // 响亮拒绝也是合格结局：补一份干净的收尾。
        let resp = lab
            .submit(json!({"should_reply": true, "reply_text": "正常收尾"}))
            .await;
        assert!(
            resp.get("isError").and_then(|v| v.as_bool()) != Some(true),
            "干净重交应被接受: {resp}"
        );
        lab.push_model_text("看完了").await;
        let result = handle.await.expect("绕路一（拒绝支）轮次任务");
        assert!(
            matches!(result, TurnResult::Replied),
            "拒绝支应回复: {result:?}"
        );
    } else {
        // 被归一化接受：发出的必须是段内正文，不是原始 JSON 串。
        lab.push_model_text("看完了").await;
        let result = handle.await.expect("绕路一（归一化支）轮次任务");
        assert!(
            matches!(result, TurnResult::Replied),
            "归一化支应回复: {result:?}"
        );
        let frames = lab.frames.lock().await.clone();
        let new_texts: Vec<String> = frames
            .iter()
            .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
            .skip(frames_before)
            .filter_map(|f| {
                f.get("params")
                    .and_then(|pp| pp.get("message"))
                    .and_then(|m| m.as_array())
                    .and_then(|segs| {
                        segs.iter().find_map(|sg| {
                            (sg.get("type").and_then(|t| t.as_str()) == Some("text")).then(|| {
                                sg.get("data")
                                    .and_then(|d| d.get("text"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or_default()
                                    .to_string()
                            })
                        })
                    })
            })
            .collect();
        assert_eq!(
            new_texts,
            vec!["字符串化的段"],
            "字符串化段只能以归一化后的正文发出，原始 JSON 串不许上链"
        );
    }

    // —— 绕路二：自述拆成两段提交，自述永不出现在发出的帧里 ——
    let m4 = lab.enqueue("s5-m4", MEMBER, "还有一句也帮我看看").await;
    let job = lab.claim(&[m4], "s5-c").await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let frames_before = frame_count(&lab, "send_group_msg").await;
    let resp = lab
        .submit(json!({
            "should_reply": true,
            "reply_parts": ["回复已提", "交完毕。", "这一段是正常内容"]
        }))
        .await;
    let split_rejected = resp.get("isError").and_then(|v| v.as_bool()) == Some(true);
    if split_rejected {
        // 整份在提交口被拒：补一份干净的收尾。
        let resp = lab
            .submit(json!({"should_reply": true, "reply_text": "这一段是正常内容"}))
            .await;
        assert!(
            resp.get("isError").and_then(|v| v.as_bool()) != Some(true),
            "干净重交应被接受: {resp}"
        );
        lab.push_model_text("看完了").await;
        let result = handle.await.expect("绕路二（提交口拒绝支）轮次任务");
        assert!(
            matches!(result, TurnResult::Replied),
            "拒绝支应回复: {result:?}"
        );
    } else {
        // 提交口放行、提交内容在落盘校验被判自述：整轮按静默收尾，
        // 一段都不许发——拆段不能把自述洗成可发送内容。
        lab.push_model_text("看完了").await;
        let result = handle.await.expect("绕路二（落盘静默支）轮次任务");
        assert!(
            matches!(result, TurnResult::Silent),
            "拆段自述应整轮静默: {result:?}\n{}",
            lab.dump().await
        );
        let audits = lab.audits().await;
        assert!(
            audits
                .iter()
                .any(|a| a.starts_with("agent.rejections.g100.")),
            "落盘校验应留下拒绝记录: {audits:?}"
        );
    }
    let frames = lab.frames.lock().await.clone();
    let new_texts: Vec<String> = frames
        .iter()
        .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
        .skip(frames_before)
        .filter_map(|f| {
            f.get("params")
                .and_then(|pp| pp.get("message"))
                .and_then(|m| m.as_array())
                .and_then(|segs| {
                    segs.iter().find_map(|sg| {
                        (sg.get("type").and_then(|t| t.as_str()) == Some("text")).then(|| {
                            sg.get("data")
                                .and_then(|d| d.get("text"))
                                .and_then(|t| t.as_str())
                                .unwrap_or_default()
                                .to_string()
                        })
                    })
                })
        })
        .collect();
    if split_rejected {
        assert_eq!(
            new_texts,
            vec!["这一段是正常内容"],
            "拒绝支只有干净重交的内容能发出"
        );
    } else {
        assert!(
            new_texts.is_empty(),
            "静默支不许发出任何一段（包括被拆开的自述）: {new_texts:?}"
        );
    }

    // —— 绕路二：引用本批不存在的 m9 ——
    let m5 = lab.enqueue("s5-m5", MEMBER, "最后一句也看看").await;
    let job = lab.claim(&[m5], "s5-d").await;
    let handle = lab.spawn_turn(job);
    let pasted = lab.next_paste(&mut cursor).await;
    lab.push_user_echo(&pasted).await;
    let frames_before = frame_count(&lab, "send_group_msg").await;
    let resp = lab
        .submit(json!({
            "should_reply": true,
            "reply_parts": [{"text": "引用一个不存在的消息", "quote_ref": "m9"}]
        }))
        .await;
    if resp.get("isError").and_then(|v| v.as_bool()) != Some(true) {
        lab.push_model_text("看完了").await;
        let _ = handle.await.expect("绕路二轮次任务");
    } else {
        let resp = lab
            .submit(json!({"should_reply": true, "reply_text": "不引用了，直接说"}))
            .await;
        assert!(
            resp.get("isError").and_then(|v| v.as_bool()) != Some(true),
            "去掉坏引用后应被接受: {resp}"
        );
        lab.push_model_text("看完了").await;
        let result = handle.await.expect("绕路二轮次任务");
        assert!(
            matches!(result, TurnResult::Replied),
            "绕路二应回复: {result:?}"
        );
    }
    let frames = lab.frames.lock().await.clone();
    let new_frames: Vec<&serde_json::Value> = frames
        .iter()
        .filter(|f| f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg"))
        .skip(frames_before)
        .collect();
    for f in &new_frames {
        let segs = f
            .get("params")
            .and_then(|p| p.get("message"))
            .and_then(|m| m.as_array())
            .expect("帧应有消息段");
        assert!(
            segs.iter()
                .all(|s| s.get("type").and_then(|t| t.as_str()) != Some("reply")),
            "不存在的引用不许出现在任何帧里: {f}"
        );
    }
}

/// K5-4：群 A 生图失败、群 B 成功/无产物——跨群隔离与回退纪律。
///
/// 执行器走真实 ImagegenExecutor：web 后端钉在拒连端口（每次必失败、
/// 落一次 web_failed 审计），agy 后端是按模式文件行事的假脚本
/// （ok=把种子图 cp 到任务目标、其他=什么都不写）。断言：每个任务
/// 只回退一次、只登记一份额度；A 的失败只给 A 发失败说明；B 的图
/// 只进 B；B 无产物时不许拿上一张旧图重发；caption 在交付口再过
/// 一遍同一道过滤（密钥被掩码、自述被整段丢弃、图照发）。
#[tokio::test]
async fn s4_imagegen_cross_group_isolation_fallback_and_caption_filter() {
    use base64::Engine as _;

    let lab = Lab::new(OutputChannel::Mcp, BotFace::Ok).await;
    let store = Arc::clone(&lab.store);
    let now = gemini_qq_bot::now_unix();

    // —— 第二个群同样启用 ——
    store.ensure_group(BOT, "200", 1).await.expect("建 200 群");
    store
        .set_group_flags(BOT, "200", Some(true), Some(false), Some(true), 1)
        .await
        .expect("启用 200 群");

    // —— 假 agy：读 $HOME/agy-mode，ok 时把 $HOME/seed.jpg 放到提示词指定的目标 ——
    let root = lab.root();
    let agy_home = root.join("agy-home");
    let out_dir = root.join("imagegen-out");
    std::fs::create_dir_all(&agy_home).expect("建 agy home");
    std::fs::create_dir_all(&out_dir).expect("建生图输出目录");
    let mut img = image::RgbImage::new(6, 6);
    for px in img.pixels_mut() {
        *px = image::Rgb([40, 120, 255]);
    }
    let mut seed = Vec::new();
    image::DynamicImage::ImageRgb8(img)
        .write_to(
            &mut std::io::Cursor::new(&mut seed),
            image::ImageFormat::Jpeg,
        )
        .expect("编码种子图");
    std::fs::write(agy_home.join("seed.jpg"), &seed).expect("写种子图");
    let bin = root.join("fake-agy.sh");
    std::fs::write(
        &bin,
        "#!/bin/sh\n\
         mode=$(cat \"$HOME/agy-mode\" 2>/dev/null)\n\
         target=$(printf '%s' \"$2\" | tr ' ' '\\n' | grep '\\.jpg$' | tail -n 1)\n\
         if [ \"$mode\" = \"ok\" ] && [ -n \"$target\" ]; then\n\
         \x20 cp \"$HOME/seed.jpg\" \"$target\"\n\
         fi\n\
         echo done\n",
    )
    .expect("写假 agy");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("加可执行位");
    }
    let set_mode = |mode: &str| {
        std::fs::write(agy_home.join("agy-mode"), mode).expect("写模式文件");
    };

    let executor = gemini_qq_bot::app::imagegen::ImagegenExecutor::new(
        Arc::clone(&store),
        lab.executor.connection.clone(),
        BOT.into(),
        out_dir,
        bin,
        agy_home.clone(),
        root.join("persona"),
        "web".into(),
        "http://127.0.0.1:1".into(),
        Some("test-token".to_string()),
        vec!["synthetic-credential-value".into()],
    );

    // —— 登记助手：先武装该群窗口，再走 register_image_job 的额度事务 ——
    let register = |group: &'static str,
                    token: &'static str,
                    requester: &'static str,
                    prompt: &'static str,
                    caption: Option<&'static str>| {
        let store = Arc::clone(&store);
        async move {
            let now = gemini_qq_bot::now_unix();
            store
                .arm_turn(group, token, "s4-batch", now, now + 600)
                .await
                .expect("武装窗口");
            store
                .register_image_job(
                    BOT, group, token, requester, "request", prompt, false, caption, now,
                )
                .await
                .expect("登记任务")
                .expect("额度应放行")
        }
    };
    let frames_of = |frames: &[serde_json::Value], group: &str| -> Vec<serde_json::Value> {
        frames
            .iter()
            .filter(|f| {
                f.get("action").and_then(|a| a.as_str()) == Some("send_group_msg")
                    && f.get("params")
                        .and_then(|p| p.get("group_id"))
                        .and_then(|g| g.as_str())
                        == Some(group)
            })
            .cloned()
            .collect()
    };
    let frame_texts = |frames: &[serde_json::Value]| -> Vec<String> {
        frames
            .iter()
            .filter_map(|f| {
                f.get("params")
                    .and_then(|p| p.get("message"))
                    .and_then(|m| m.as_array())
                    .and_then(|segs| {
                        segs.iter().find_map(|s| {
                            (s.get("type").and_then(|t| t.as_str()) == Some("text")).then(|| {
                                s.get("data")
                                    .and_then(|d| d.get("text"))
                                    .and_then(|t| t.as_str())
                                    .unwrap_or_default()
                                    .to_string()
                            })
                        })
                    })
            })
            .collect()
    };
    let frame_images = |frames: &[serde_json::Value]| -> Vec<(u32, u32)> {
        frames
            .iter()
            .filter_map(|f| {
                f.get("params")
                    .and_then(|p| p.get("message"))
                    .and_then(|m| m.as_array())
                    .and_then(|segs| {
                        segs.iter().find_map(|s| {
                            if s.get("type").and_then(|t| t.as_str()) != Some("image") {
                                return None;
                            }
                            let file = s.get("data")?.get("file")?.as_str()?;
                            let payload = file.strip_prefix("base64://")?;
                            let bytes = base64::engine::general_purpose::STANDARD
                                .decode(payload)
                                .ok()?;
                            let img = image::load_from_memory(&bytes).ok()?;
                            Some((img.width(), img.height()))
                        })
                    })
            })
            .collect()
    };
    let web_failed = |audits: &[String]| -> usize {
        audits
            .iter()
            .filter(|a| a.starts_with("imagegen.web_failed."))
            .count()
    };

    // —— 群 A：web 失败 → 回退 agy → 仍无产物 ——
    set_mode("none");
    let job_a = register(GROUP, "s4-token-a", MEMBER, "画一只橘猫", None).await;
    assert!(executor.tick().await, "A 任务应被领取");
    let frames = lab.frames.lock().await.clone();
    let a_frames = frames_of(&frames, GROUP);
    assert_eq!(
        frame_texts(&a_frames),
        vec!["这张没画出来，等会儿再试试吧"],
        "A 只应收到一句失败说明: {a_frames:?}"
    );
    assert!(frame_images(&a_frames).is_empty(), "A 不许收到任何图");
    let job = store
        .get_image_job(&job_a)
        .await
        .expect("读 A 任务")
        .expect("A 任务应存在");
    assert_eq!(job.state, "failed", "A 任务应落 failed");
    let audits = lab.audits().await;
    assert_eq!(web_failed(&audits), 1, "A 只应回退一次: {audits:?}");
    assert_eq!(
        store
            .count_image_jobs(BOT, GROUP, "request", Some(MEMBER), 0)
            .await
            .expect("数 A 额度"),
        1,
        "A 的额度只登记一份（两次后端尝试不重复计费）"
    );

    // —— 群 B 之一：成功；caption 里的密钥必须在交付口被掩码 ——
    set_mode("ok");
    let job_b1 = register(
        "200",
        "s4-token-b",
        MEMBER,
        "画一艘帆船",
        Some("看这张图，密钥是 synthetic-credential-value，别外传"),
    )
    .await;
    let a_frames_before = frames_of(&lab.frames.lock().await.clone(), GROUP).len();
    assert!(executor.tick().await, "B1 任务应被领取");
    let frames = lab.frames.lock().await.clone();
    let b_frames = frames_of(&frames, "200");
    assert_eq!(
        frame_images(&b_frames),
        vec![(6, 6)],
        "B 应收到那张 6x6 的图: {b_frames:?}"
    );
    let b_texts = frame_texts(&b_frames);
    assert_eq!(b_texts.len(), 1, "B 应有一条 caption: {b_frames:?}");
    assert!(
        b_texts[0].contains("看这张图") && !b_texts[0].contains("synthetic-credential-value"),
        "caption 应保留正文、掩掉密钥: {b_texts:?}"
    );
    assert_eq!(
        frames_of(&frames, GROUP).len(),
        a_frames_before,
        "B 出图时 A 不许收到任何新帧"
    );
    let audits = lab.audits().await;
    assert_eq!(web_failed(&audits), 2, "B1 也只回退一次: {audits:?}");
    assert_eq!(
        store
            .count_image_jobs(BOT, "200", "request", Some(MEMBER), 0)
            .await
            .expect("数 B 额度"),
        1,
        "B 的额度只登记一份"
    );
    let job = store
        .get_image_job(&job_b1)
        .await
        .expect("读 B1 任务")
        .expect("B1 应存在");
    assert_ne!(job.state, "failed", "B1 不应是失败态");

    // —— 群 B 之二：caption 是提交自述——图照发，caption 整段丢弃 ——
    let _job_b2 = register(
        "200",
        "s4-token-b2",
        OWNER,
        "画一座灯塔",
        Some("回复已提交完毕。"),
    )
    .await;
    assert!(executor.tick().await, "B2 任务应被领取");
    let frames = lab.frames.lock().await.clone();
    let b_frames = frames_of(&frames, "200");
    assert_eq!(
        frame_images(&b_frames).len(),
        2,
        "B 第二张图应送达（旧图不重发、新图恰一张）"
    );
    for text in frame_texts(&b_frames) {
        assert!(
            !text.contains("提交"),
            "自述 caption 不许借图说出口: {text}"
        );
    }
    let audits = lab.audits().await;
    assert!(
        audits
            .iter()
            .any(|a| a.starts_with("imagegen.caption.dropped")),
        "自述 caption 应被交付口丢弃并留审计: {audits:?}"
    );

    // —— 群 B 之三：agy 退出正常但无产物——只发失败说明，旧图不重发 ——
    set_mode("none");
    let job_b3 = register("200", "s4-token-b3", "777", "画一朵云", None).await;
    assert!(executor.tick().await, "B3 任务应被领取");
    let frames = lab.frames.lock().await.clone();
    let b_frames = frames_of(&frames, "200");
    assert_eq!(
        frame_images(&b_frames).len(),
        2,
        "无产物时不许拿旧图顶替或重发"
    );
    assert!(
        frame_texts(&b_frames)
            .iter()
            .filter(|t| t.as_str() == "这张没画出来，等会儿再试试吧")
            .count()
            == 1,
        "B3 应收到一句失败说明: {:?}",
        frame_texts(&b_frames)
    );
    let job = store
        .get_image_job(&job_b3)
        .await
        .expect("读 B3 任务")
        .expect("B3 应存在");
    assert_eq!(job.state, "failed");
    let audits = lab.audits().await;
    assert_eq!(web_failed(&audits), 4, "四个任务各回退恰一次: {audits:?}");
    assert_eq!(
        store
            .count_image_jobs(BOT, "200", "request", None, 0)
            .await
            .expect("数 B 总额度"),
        3,
        "B 三个任务各登记一份"
    );
}
