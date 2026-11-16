//! Background executor for `generate_image` jobs.
//!
//! The MCP tool only registers; this loop is what draws. One job at a
//! time. Two backends: the Gemini web worker (a headful Chrome on the
//! host driving gemini.google.com, pinned to 3.8 Flash High — the
//! primary since 2026-11-12) and, as its fallback as well as the
//! standalone alternative, a separate agy binary (the resident's
//! pinned 1.5.2 hangs inside the image subagent — 2026-11-11 smoke) with
//! its own agy home (the resident's home bans subagents via tool-guard
//! hooks, and image generation is one). The picture lands as a JPEG in the
//! session's brain directory; the prompt tells the agent to `cp` it to the
//! job's target path verbatim — never to convert formats, because the
//! container has no image tools and a conversion request sends the agent
//! hunting for some for ten minutes (same smoke).
//!
//! Delivery reuses the outbox primitives (begin/classify/finish), so an
//! ambiguous send becomes `unknown` and is never auto-resent, exactly like
//! a turn's reply. Only the frame building is local: the rows this
//! executor enqueues are a caption text and one plain image.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::onebot::connection::ConnectionHandle;
use crate::onebot::normalize::SpeakerRole;
use crate::pipeline::outbox::{DeliveryResult, Outbox, classify};
use crate::store::{ImageJob, SqliteStore};

/// Hard cap for one generation, process spawn to exit. Sized from live
/// timings, not the plan's estimate: a trivial prompt took ~100 s in the
/// afternoon smoke but 388 s on the first production evening (image
/// service latency swings widely), and the 360 s first cut killed a job
/// that was ~30 s from landing.
pub const GENERATION_TIMEOUT: Duration = Duration::from_secs(600);

/// After a timeout kill, one grace window for a late write to the exact
/// job target. Shared brain artifacts are not eligible.
pub const HARVEST_GRACE: Duration = Duration::from_secs(45);
/// Idle pause between empty polls.
pub const POLL_INTERVAL: Duration = Duration::from_secs(10);
/// A generated picture larger than this is not a picture we send.
pub const MAX_IMAGE_BYTES: usize = 10 * 1024 * 1024;
const MAX_IMAGE_PIXELS: u64 = 40_000_000;
const MAX_IMAGE_EDGE: u32 = 16_384;
const MAX_WEB_RESPONSE_BYTES: usize = 64 * 1024;
// A cancelled caller cannot cancel spawn_blocking. Keep the permit inside
// the blocking task so concurrent/cancelled decodes cannot multiply memory.
static IMAGE_DECODE_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// The one failure line a group ever sees; the detail stays in the audit.
pub const FAILURE_NOTE: &str = "这张没画出来，等会儿再试试吧";

/// The failure line when the image model's quota is gone: say so,
/// instead of implying a retry in a few minutes would fare better.
pub const QUOTA_NOTE: &str = "生图额度到上限了，等恢复了再喊我画吧";

/// Failure note text by error class.
pub fn failure_note(class: &str) -> &'static str {
    if class == "quota" {
        QUOTA_NOTE
    } else {
        FAILURE_NOTE
    }
}

/// How often this invocation's stdout/stderr are checked for a quota
/// rejection. A rejection hidden by the parent CLI in a shared transcript
/// cannot safely fail this job early without a verified session binding.
pub const QUOTA_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Whether a session transcript carries the image model's quota
/// rejection (a 429 from cloudcode-pa: reason `QUOTA_EXHAUSTED`,
/// "You have exhausted your capacity on this model").
pub fn transcript_shows_quota_exhaustion(text: &str) -> bool {
    text.contains("QUOTA_EXHAUSTED") || text.contains("exhausted your capacity")
}

/// Inspect only stdout/stderr files created for this invocation. Shared
/// brain transcripts have no proven job ownership, regardless of their mtime.
fn quota_exhaustion_seen(stdout: &Path, stderr: &Path) -> bool {
    [stdout, stderr].into_iter().any(|path| {
        std::fs::read_to_string(path)
            .map(|text| transcript_shows_quota_exhaustion(&text))
            .unwrap_or(false)
    })
}

/// Who hears about a failed generation: a member who asked for a picture
/// is waiting on it, so request jobs get the failure note; a self-
/// initiated picture nobody asked for fails silently (user ruling,
/// 2026-11-11) — the audit row is the only trace.
pub fn failure_note_warranted(bucket: &str) -> bool {
    bucket == "request"
}

pub struct ImagegenExecutor {
    store: Arc<SqliteStore>,
    outbox: Outbox,
    connection: ConnectionHandle,
    bot_id: String,
    out_dir: PathBuf,
    bin: PathBuf,
    home: PathBuf,
    persona_dir: PathBuf,
    backend: String,
    web_url: String,
    web_token: Option<String>,
    /// Where the web worker drops finished pictures: a sibling of
    /// `out_dir` inside the same data volume (the worker writes to the
    /// host side of the mount).
    web_dir: PathBuf,
    http: reqwest::Client,
    secrets: Vec<String>,
}

/// Client timeout for one web generation. The worker's own ceiling is
/// a 240 s in-page wait plus load/download overheads; this leaves room
/// on top without approaching the agy path's 600 s budget.
pub const WEB_TIMEOUT: Duration = Duration::from_secs(360);

/// What one `/generate` request observed.
///
/// Timings and the worker's own queue counters are diagnostics for the
/// question "is the JP egress node the bottleneck?" — nothing here changes
/// which node is used. Deliberately absent: the worker URL, the bearer
/// token, the prompt and any raw worker text. A field the worker does not
/// answer with stays `None` rather than being guessed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebObservation {
    /// Wall time from request send to response body/artifact collected.
    pub elapsed_ms: i64,
    /// Worker-reported queue wait before rendering started.
    pub queue_ms: Option<i64>,
    /// Worker-reported in-page render wait.
    pub render_ms: Option<i64>,
    /// Bounded local error class; `None` means the picture is on disk.
    pub error: Option<String>,
}

impl WebObservation {
    /// One audit suffix carrying every field, so a reader never has to join
    /// several lines to see what one request cost. `q` and `r` print `-`
    /// when the worker gave no answer — the difference between "zero" and
    /// "not reported" has to stay visible.
    fn audit_tag(&self, group_id: &str) -> String {
        fn slot(value: Option<i64>) -> String {
            value.map_or_else(|| "-".to_string(), |v| v.to_string())
        }
        format!(
            ".g{group_id}.ms{}.q{}.r{}",
            self.elapsed_ms,
            slot(self.queue_ms),
            slot(self.render_ms)
        )
    }
}

/// Read a non-negative integer the worker may have reported.
fn worker_millis(value: &serde_json::Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|v| v.as_i64()).filter(|v| *v >= 0)
}

impl ImagegenExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<SqliteStore>,
        connection: ConnectionHandle,
        bot_id: String,
        out_dir: PathBuf,
        bin: PathBuf,
        home: PathBuf,
        persona_dir: PathBuf,
        backend: String,
        web_url: String,
        web_token: Option<String>,
        secrets: Vec<String>,
    ) -> Self {
        let web_dir = out_dir
            .parent()
            .map(|p| p.join("imagegen-web"))
            .unwrap_or_else(|| out_dir.clone());
        let http = reqwest::Client::builder()
            .timeout(WEB_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            outbox: Outbox::new(Arc::clone(&store)),
            store,
            connection,
            bot_id,
            out_dir,
            bin,
            home,
            persona_dir,
            backend,
            web_url,
            web_token,
            web_dir,
            http,
            secrets,
        }
    }

    /// Poll-and-draw forever. A claimed job runs to completion before the
    /// next poll, which is what makes the executor single-flight.
    pub async fn run(self: Arc<Self>) {
        loop {
            match self.tick().await {
                true => {}
                false => tokio::time::sleep(POLL_INTERVAL).await,
            }
        }
    }

    /// Run one queued job if there is one. Returns whether work was done.
    pub async fn tick(&self) -> bool {
        // Between jobs, a `running` row is an orphan (see
        // `reclaim_running_image_jobs`): hand it back to the queue
        // before claiming, so a job stranded by a shutdown is drawn
        // after all instead of sitting `running` forever.
        match self
            .store
            .reclaim_running_image_jobs(crate::now_unix())
            .await
        {
            Ok(stale) => {
                for job in &stale {
                    self.audit(job, "imagegen.reclaimed").await;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "imagegen reclaim failed");
            }
        }
        let job = match self.store.claim_next_image_job(crate::now_unix()).await {
            Ok(Some(job)) => job,
            _ => return false,
        };
        self.run_job(&job).await
    }

    async fn group_active(&self, group_id: &str) -> bool {
        matches!(self.store.get_group(&self.bot_id, group_id).await,
            Ok(Some(group)) if group.enabled && !group.paused)
    }

    async fn cancel_inactive(&self, job: &ImageJob) -> bool {
        let persisted = self
            .store
            .cancel_image_job(&job.id, crate::now_unix())
            .await
            .is_ok();
        self.audit(job, "imagegen.cancelled.inactive").await;
        persisted
    }

    // False backs off the poller after a persistence failure. A generated job
    // retains its bound artifact, so retrying the outbox never spends generation.
    async fn run_job(&self, job: &ImageJob) -> bool {
        if !self.group_active(&job.group_id).await {
            return self.cancel_inactive(job).await;
        }
        let now = crate::now_unix();
        let target = self.out_dir.join(format!("{}.jpg", job.id));
        let outcome = if let Some(path) = job.file_path.as_deref() {
            if Path::new(path) == target && try_collect(&target).await.is_some() {
                GenOutcome::Ready { harvested: false }
            } else {
                GenOutcome::Failed {
                    class: "artifact_missing",
                }
            }
        } else if self.backend == "web" {
            let observation = self.run_web(job).await;
            match observation.error.clone() {
                None => {
                    self.audit(
                        job,
                        &format!("imagegen.web.ok{}", observation.audit_tag(&job.group_id)),
                    )
                    .await;
                    GenOutcome::Ready { harvested: false }
                }
                Some(class) => {
                    if !self.group_active(&job.group_id).await {
                        return self.cancel_inactive(job).await;
                    }
                    // Any web failure — auth, UI drift, timeout — falls
                    // back to the agy path once, so a broken web worker
                    // degrades the bot to yesterday's behaviour instead
                    // of breaking image generation outright.
                    self.audit(
                        job,
                        &format!(
                            "imagegen.web_failed.{class}{}",
                            observation.audit_tag(&job.group_id)
                        ),
                    )
                    .await;
                    run_generation(
                        &self.bin,
                        &self.home,
                        &self.out_dir,
                        job,
                        &build_prompt(job, &self.persona_dir, &self.out_dir),
                    )
                    .await
                }
            }
        } else {
            run_generation(
                &self.bin,
                &self.home,
                &self.out_dir,
                job,
                &build_prompt(job, &self.persona_dir, &self.out_dir),
            )
            .await
        };
        if !self.group_active(&job.group_id).await {
            return self.cancel_inactive(job).await;
        }
        match outcome {
            GenOutcome::Ready { harvested } => {
                let file_name = format!("{}.jpg", job.id);
                let file_path = target.display().to_string();
                // Persist artifact ownership before attempting the transactional
                // outbox. `generated` is not sent or even durably queued yet.
                if self
                    .store
                    .finish_image_job(&job.id, "generated", Some(&file_path), None, now)
                    .await
                    .is_err()
                {
                    return false;
                }
                if harvested {
                    self.audit(job, "imagegen.harvested").await;
                }
                // Delayed jobs may predate MCP screening. Drop only an unsafe
                // caption; the image and its single ledger charge stay intact.
                let caption = job.caption.as_deref().and_then(|text| {
                    if crate::agent::contract::reply_content_rejection(text).is_some() {
                        return None;
                    }
                    let masked = crate::agent::segments::filter_secrets(text, &self.secrets);
                    if masked.trim().is_empty()
                        || crate::agent::contract::reply_content_rejection(&masked).is_some()
                    {
                        None
                    } else {
                        Some(masked)
                    }
                });
                if job.caption.is_some() && caption.is_none() {
                    self.audit(job, "imagegen.caption.dropped").await;
                }
                if !self.group_active(&job.group_id).await {
                    return self.cancel_inactive(job).await;
                }
                let ids = match self
                    .outbox
                    .enqueue_generated_image(job, caption.as_deref(), &file_name, &file_path, now)
                    .await
                {
                    Ok(ids) => ids,
                    Err(_) => {
                        self.audit(job, "imagegen.outbox_failed").await;
                        return false;
                    }
                };
                self.audit(job, "imagegen.queued").await;
                let delivery = self.deliver_rows(&job.group_id, &ids).await;
                self.audit(
                    job,
                    if delivery.all_sent() {
                        "imagegen.sent"
                    } else {
                        "imagegen.delivery_incomplete"
                    },
                )
                .await;
            }
            GenOutcome::Failed { class } => {
                let _ = self
                    .store
                    .finish_image_job(&job.id, "failed", None, Some(class), now)
                    .await;
                self.audit(job, &format!("imagegen.failed.{class}")).await;
                if !self.group_active(&job.group_id).await {
                    return self.cancel_inactive(job).await;
                }
                if failure_note_warranted(&job.bucket)
                    && let Ok(id) = self
                        .outbox
                        .enqueue(
                            &self.bot_id,
                            &job.group_id,
                            &job.id,
                            failure_note(class),
                            now,
                        )
                        .await
                {
                    self.deliver_rows(&job.group_id, &[id]).await;
                }
            }
        }
        true
    }

    /// One generation through the web worker. On success the finished
    /// JPEG has been copied from the worker's drop directory into
    /// `out_dir` under the job's file name, indistinguishable from an
    /// agy-produced file downstream. Errors are bounded local classes;
    /// worker text and transport details never enter the audit.
    async fn run_web(&self, job: &ImageJob) -> WebObservation {
        let started = std::time::Instant::now();
        let mut observation = WebObservation::default();
        let outcome = self.web_request(job, &mut observation).await;
        observation.elapsed_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        if let Err(class) = outcome {
            observation.error = Some(class);
        }
        observation
    }

    /// The request half of `run_web`. Kept separate so the timing wrapper has
    /// exactly one exit and cannot forget to record a class.
    async fn web_request(
        &self,
        job: &ImageJob,
        observation: &mut WebObservation,
    ) -> Result<(), String> {
        let mut body = serde_json::json!({
            "prompt": build_web_prompt(job),
            "out": job.id,
        });
        if job.self_portrait {
            // Resolved by the worker against the host side of the data
            // volume; the sheet is the same file the agy path reads.
            body["reference"] = serde_json::Value::String("persona/persona-sheet.png".to_string());
        }
        let mut req = self
            .http
            .post(format!("{}/generate", self.web_url))
            .timeout(WEB_TIMEOUT)
            .json(&body);
        if let Some(token) = &self.web_token {
            req = req.bearer_auth(token);
        }
        let mut resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                "timeout".to_string()
            } else {
                "network".to_string()
            }
        })?;
        let status = resp.status();
        if !status.is_success() {
            // 429 is the worker saying "no quota / slow down", not "the network
            // is bad". Lumping them together is exactly what would make an
            // egress-node decision look like a capacity problem (R28).
            return Err(if status.as_u16() == 429 {
                "rate_limited".to_string()
            } else {
                "http_status".to_string()
            });
        }
        if resp
            .content_length()
            .is_some_and(|n| n > MAX_WEB_RESPONSE_BYTES as u64)
        {
            return Err("response_size".into());
        }
        let mut response = Vec::new();
        while let Some(chunk) = resp.chunk().await.map_err(|_| "network".to_string())? {
            if chunk.len() > MAX_WEB_RESPONSE_BYTES - response.len() {
                return Err("response_size".into());
            }
            response.extend_from_slice(&chunk);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&response).map_err(|_| "internal".to_string())?;
        // Queue/render timings ride along even on a failure: a quota refusal
        // answered in 40 ms and a UI drift after a 200 s queue look identical
        // in the error class alone.
        observation.queue_ms = worker_millis(&value, "queue_ms");
        observation.render_ms = worker_millis(&value, "render_ms");
        if let Some(class) = web_result_class(&value) {
            return Err(class);
        }
        let src = self.web_dir.join(format!("{}.jpg", job.id));
        let bytes = read_verified_image(&src)
            .await
            .ok_or_else(|| "download".to_string())?;
        let target = self.out_dir.join(format!("{}.jpg", job.id));
        // Persist the validated snapshot, never reopen/copy an unchecked source.
        tokio::fs::write(&target, &bytes)
            .await
            .map_err(|_| "internal".to_string())?;
        Ok(())
    }

    async fn audit(&self, job: &ImageJob, action: &str) {
        let _ = self
            .store
            .audit(
                "system",
                action,
                Some(&self.bot_id),
                Some(&job.group_id),
                crate::now_unix(),
            )
            .await;
    }

    /// Deliver rows this executor enqueued, with the outbox's own state
    /// machine. Mirrors the turn delivery for the two shapes produced
    /// here (plain text, plain image) and logs sends into group history.
    async fn deliver_rows(&self, group_id: &str, ids: &[String]) -> ImageDelivery {
        let mut delivery = ImageDelivery::default();
        for id in ids {
            let now = crate::now_unix();
            let row = match self.store.get_outbox(id).await {
                Ok(Some(row)) => row,
                _ => {
                    let _ = self
                        .outbox
                        .finish(
                            id,
                            &DeliveryResult::Unknown {
                                detail: "row-missing".into(),
                            },
                            now,
                        )
                        .await;
                    delivery.unknown += 1;
                    continue;
                }
            };
            let frame = if row.kind == "image" {
                let name = row.text.as_str();
                let valid = !name.is_empty()
                    && !name.contains('/')
                    && !name.contains('\\')
                    && !name.contains("..");
                let bytes = if valid {
                    read_verified_image(&self.out_dir.join(name)).await
                } else {
                    None
                };
                match bytes.and_then(|b| crate::onebot::action::SendGroupMsg::image(group_id, &b)) {
                    Some(msg) => msg,
                    None => {
                        let _ = self
                            .outbox
                            .finish(
                                id,
                                &DeliveryResult::Unknown {
                                    detail: format!("image-bytes-missing:{name}"),
                                },
                                now,
                            )
                            .await;
                        delivery.unknown += 1;
                        continue;
                    }
                }
            } else {
                crate::onebot::action::SendGroupMsg::text(group_id, &row.text)
            };
            // Same sizing as the turn delivery: a ~1 MB picture upload
            // does not fit the 10 s text budget (first production send
            // landed `unknown`, 2026-11-11).
            let send_timeout = if row.kind == "image" {
                Duration::from_secs(45)
            } else {
                Duration::from_secs(10)
            };
            // Recheck immediately before each network attempt, including the
            // caption/image boundary. In-flight sends cannot be retracted.
            if !self.group_active(group_id).await {
                let _ = self.store.cancel_image_job(&row.batch_id, now).await;
                let _ = self
                    .outbox
                    .finish(
                        id,
                        &DeliveryResult::Rejected {
                            detail: "group_inactive".into(),
                        },
                        now,
                    )
                    .await;
                let _ = self
                    .store
                    .audit(
                        "system",
                        "imagegen.delivery_cancelled.inactive",
                        Some(&self.bot_id),
                        Some(group_id),
                        now,
                    )
                    .await;
                delivery.rejected += 1;
                continue;
            }
            if self.outbox.begin(id, now).await.is_err() {
                delivery.unknown += 1;
                continue;
            }
            let outcome = self
                .connection
                .call("send_group_msg", frame.params(), send_timeout)
                .await;
            let result = classify(outcome);
            let persisted = self
                .outbox
                .finish(id, &result, crate::now_unix())
                .await
                .is_ok();
            match (&result, persisted) {
                (DeliveryResult::Sent { .. }, true) => delivery.sent += 1,
                (DeliveryResult::Rejected { .. }, true) => delivery.rejected += 1,
                _ => delivery.unknown += 1,
            }
            if let DeliveryResult::Sent { message_id } = &result {
                let mid = message_id.clone().unwrap_or_else(|| format!("bot:{id}"));
                let text = if row.kind == "image" {
                    "[图片]".to_string()
                } else {
                    row.text.clone()
                };
                let _ = self
                    .store
                    .insert_group_message(
                        &self.bot_id,
                        group_id,
                        &mid,
                        &self.bot_id,
                        None,
                        SpeakerRole::Bot.as_str(),
                        &text,
                        row.kind == "image",
                        None,
                        None,
                        crate::now_unix(),
                    )
                    .await;
            }
        }
        delivery
    }
}

#[derive(Debug, Default)]
struct ImageDelivery {
    sent: usize,
    rejected: usize,
    unknown: usize,
}

impl ImageDelivery {
    fn all_sent(&self) -> bool {
        self.sent > 0 && self.rejected == 0 && self.unknown == 0
    }
}

/// What one generation attempt produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenOutcome {
    /// The target file is in place and valid. `harvested` marks that it
    /// came from the brain-directory fallback rather than the agent's `cp`.
    Ready {
        harvested: bool,
    },
    Failed {
        class: &'static str,
    },
}

/// Assemble the agy prompt for one job. The trailing instruction is load-
/// bearing (2026-11-11 smoke): copy the artifact verbatim, never convert.
pub fn build_prompt(job: &ImageJob, persona_dir: &Path, out_dir: &Path) -> String {
    let target = out_dir.join(format!("{}.jpg", job.id));
    let mut out = String::new();
    if job.self_portrait {
        out.push_str(&format!(
            "The subject of this picture is the character on the reference sheet at {} \
             and in the character card at {}. Read both first and match her appearance \
             exactly (blue-to-purple gradient hair, pink-left/gold-right heterochromia, \
             cat ears and big tail, star-themed navy-purple-white outfit). Scene: ",
            persona_dir.join("persona-sheet.png").display(),
            persona_dir.join("character-card.txt").display(),
        ));
    }
    out.push_str(&job.prompt);
    out.push_str(&format!(
        "\n\nUse your image generation capability to create this image. After it is \
         generated, copy the generated image file as-is with cp to {} — do not convert \
         formats and do not edit it. Then reply with a single word: done.",
        target.display()
    ));
    out
}

/// Assemble the web worker prompt for one job. A self portrait anchors
/// on the persona sheet, which travels with the request as the
/// reference image — the web model matches her from the picture itself,
/// so unlike the agy prompt there are no file paths to recite.
pub fn build_web_prompt(job: &ImageJob) -> String {
    let mut out = String::new();
    if job.self_portrait {
        out.push_str(
            "The subject of this picture is the character in the attached \
             reference image. Match her appearance exactly (blue-to-purple \
             gradient hair, pink-left/gold-right heterochromia, cat ears \
             and big tail, star-themed navy-purple-white outfit). Scene: ",
        );
    }
    out.push_str(&job.prompt);
    out
}

/// Interpret a web worker response: `None` when it reports success,
/// otherwise the failure class to audit (and to fall back on).
pub fn web_result_class(value: &serde_json::Value) -> Option<String> {
    if value.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        return None;
    }
    let class = value
        .get("error_class")
        .and_then(|v| v.as_str())
        .unwrap_or("internal");
    Some(
        match class {
            "auth" | "timeout" | "quota" | "network" | "download" | "ui_drift" | "internal" => {
                class
            }
            _ => "internal",
        }
        .to_string(),
    )
}

/// Decode only complete JPEG/PNG within byte, dimension and allocation caps.
/// Production callers run this CPU-bound check in the single blocking slot.
pub fn image_bytes_ok(bytes: &[u8]) -> bool {
    if bytes.is_empty() || bytes.len() > MAX_IMAGE_BYTES {
        return false;
    }
    let Ok(format) = image::guess_format(bytes) else {
        return false;
    };
    let complete = match format {
        image::ImageFormat::Jpeg => bytes.ends_with(&[0xff, 0xd9]),
        image::ImageFormat::Png => bytes.ends_with(b"\x00\x00\x00\x00IEND\xaeB\x60\x82"),
        _ => false,
    };
    if !complete {
        return false;
    }
    let reader = || image::ImageReader::with_format(std::io::Cursor::new(bytes), format);
    let Ok((width, height)) = reader().into_dimensions() else {
        return false;
    };
    if width == 0
        || height == 0
        || width.max(height) > MAX_IMAGE_EDGE
        || u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS
    {
        return false;
    }
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_IMAGE_EDGE);
    limits.max_image_height = Some(MAX_IMAGE_EDGE);
    limits.max_alloc = Some(256 * 1024 * 1024);
    let mut decoder = reader();
    decoder.limits(limits);
    decoder.decode().is_ok()
}

async fn read_verified_image(path: &Path) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    // Acquire before allocating bytes; retain through CPU work even on cancellation.
    let permit = IMAGE_DECODE_SLOT.acquire().await.ok()?;
    let file = tokio::fs::File::open(path).await.ok()?;
    let metadata = file.metadata().await.ok()?;
    if !metadata.is_file() || metadata.len() > MAX_IMAGE_BYTES as u64 {
        return None;
    }
    let mut bytes = Vec::new();
    file.take((MAX_IMAGE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .ok()?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        image_bytes_ok(&bytes).then_some(bytes)
    })
    .await
    .ok()?
}

/// Spawn the imagegen agy for one job and judge only its exact target.
/// Unbound brain artifacts are never recovered: freshness is not ownership.
pub async fn run_generation(
    bin: &Path,
    home: &Path,
    out_dir: &Path,
    job: &ImageJob,
    prompt: &str,
) -> GenOutcome {
    let target = out_dir.join(format!("{}.jpg", job.id));
    if let Err(error) = tokio::fs::remove_file(&target).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return GenOutcome::Failed { class: "no_output" };
    }
    let log_path = out_dir.join(format!("{}.log", job.id));
    let err_path = out_dir.join(format!("{}.log.err", job.id));
    let (out_file, err_file) = match (
        std::fs::File::create(&log_path),
        std::fs::File::create(&err_path),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return GenOutcome::Failed { class: "spawn" },
    };
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("-p")
        .arg(prompt)
        .env("HOME", home)
        .env("http_proxy", "http://127.0.0.1:7890")
        .env("https_proxy", "http://127.0.0.1:7890")
        .env("no_proxy", "localhost,127.0.0.1")
        .current_dir(home)
        .stdin(std::process::Stdio::null())
        .stdout(out_file)
        .stderr(err_file)
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(_) => return GenOutcome::Failed { class: "spawn" },
    };
    #[cfg(unix)]
    let process_group = child
        .id()
        .map(crate::process::group::OwnedProcessGroup::new);
    // Only this child's diagnostics can fail this attempt early. If the
    // parent CLI hides a rejection in an unbound transcript, wait for the
    // deadline instead of attributing another conversation's rejection.
    let deadline = std::time::Instant::now() + GENERATION_TIMEOUT;
    let mut timed_out = false;
    let mut terminal_error = None;
    loop {
        match tokio::time::timeout(QUOTA_CHECK_INTERVAL, child.wait()).await {
            Ok(Ok(_status)) => break,
            Ok(Err(_)) => {
                terminal_error = Some("agy_exit");
                break;
            }
            Err(_) => {
                if quota_exhaustion_seen(&log_path, &err_path) {
                    terminal_error = Some("quota");
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    timed_out = true;
                    break;
                }
            }
        }
    }
    // Stop every writer before inspecting the artifact, even if the wrapper
    // has already exited. The guard also handles cancellation at any await.
    #[cfg(unix)]
    if let Some(group) = &process_group {
        let (_, exit) = tokio::join!(group.stop(Duration::from_millis(500)), child.wait());
        if exit.is_err() && terminal_error.is_none() {
            terminal_error = Some("agy_exit");
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill().await;
    }
    if let Some(class) = terminal_error {
        return GenOutcome::Failed { class };
    }
    if let Some(harvested) = try_collect(&target).await {
        return GenOutcome::Ready { harvested };
    }
    if quota_exhaustion_seen(&log_path, &err_path) {
        return GenOutcome::Failed { class: "quota" };
    }
    if timed_out {
        // Only a late write to this job's explicit target is eligible.
        tokio::time::sleep(HARVEST_GRACE).await;
        if let Some(harvested) = try_collect(&target).await {
            return GenOutcome::Ready { harvested };
        }
        return GenOutcome::Failed { class: "timeout" };
    }
    GenOutcome::Failed { class: "no_output" }
}

/// Accept only the explicitly requested target, never neighbouring files.
/// The outcome retains its existing non-harvested representation.
async fn try_collect(target: &Path) -> Option<bool> {
    read_verified_image(target).await.map(|_| false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(self_portrait: bool) -> ImageJob {
        ImageJob {
            id: "job_test1".to_string(),
            bot_id: "1".to_string(),
            group_id: "100".to_string(),
            requester_id: "42".to_string(),
            bucket: "request".to_string(),
            prompt: "在星空下喝奶茶".to_string(),
            self_portrait,
            caption: None,
            state: "running".to_string(),
            file_path: None,
            error_class: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    async fn atomic_image_fixture() -> (ImagegenExecutor, ImageJob, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let pool = crate::store::migrations::open_and_migrate(&dir.path().join("bot.db"))
            .await
            .unwrap();
        let store = Arc::new(SqliteStore::new(pool));
        let now = crate::now_unix();
        store.ensure_group("1", "100", now).await.unwrap();
        store
            .set_group_flags("1", "100", Some(true), Some(false), None, now)
            .await
            .unwrap();
        store
            .insert_image_job(
                "1",
                "100",
                "bot",
                "self",
                "draw",
                false,
                Some("ordinary caption"),
                now,
            )
            .await
            .unwrap();
        let job = store.claim_next_image_job(now).await.unwrap().unwrap();
        let (_connection, handle) =
            crate::onebot::connection::Connection::new("ws://127.0.0.1:1", None);
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        let executor = ImagegenExecutor::new(
            store,
            handle,
            "1".into(),
            out,
            write_fake_agy(dir.path()),
            home,
            dir.path().join("persona"),
            "agy".into(),
            String::new(),
            None,
            Vec::new(),
        );
        (executor, job, dir)
    }

    #[tokio::test]
    async fn inactive_image_queued_job_never_starts_or_revives() {
        let (executor, job, dir) = atomic_image_fixture().await;
        let marker = dir.path().join("cli-called");
        std::fs::write(
            &executor.bin,
            format!("#!/bin/sh\ntouch '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        executor
            .store
            .finish_image_job(&job.id, "queued", None, None, crate::now_unix())
            .await
            .unwrap();
        executor
            .store
            .set_group_flags("1", "100", Some(false), None, None, crate::now_unix())
            .await
            .unwrap();
        assert!(executor.tick().await);
        assert!(!marker.exists(), "disabled group started a generation");
        assert_eq!(
            executor
                .store
                .get_image_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        executor
            .store
            .set_group_flags("1", "100", Some(true), Some(false), None, crate::now_unix())
            .await
            .unwrap();
        assert!(
            !executor.tick().await,
            "reenabling resurrected a cancelled job"
        );
        assert_eq!(
            executor
                .store
                .count_image_jobs("1", "100", "self", None, 0)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn inactive_image_web_failure_does_not_fallback_or_notify() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut executor, job, dir) = atomic_image_fixture().await;
        let marker = dir.path().join("fallback-called");
        std::fs::write(
            &executor.bin,
            format!("#!/bin/sh\ntouch '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        executor.backend = "web".into();
        executor.web_url = format!("http://{}", listener.local_addr().unwrap());
        let store = executor.store.clone();
        let worker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = socket.read(&mut buf).await.unwrap();
            store
                .set_group_flags("1", "100", None, Some(true), None, crate::now_unix())
                .await
                .unwrap();
            let body = r#"{"ok":false,"error_class":"timeout"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let mut requested = job.clone();
        requested.bucket = "request".into();
        executor.run_job(&requested).await;
        worker.await.unwrap();
        assert!(!marker.exists(), "paused group invoked fallback");
        assert_eq!(
            executor
                .store
                .get_image_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
            .fetch_one(executor.store.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0, "paused group got a failure note or image");
    }

    #[tokio::test]
    async fn inactive_image_delivery_rechecks_after_enqueue() {
        let (executor, job, _dir) = atomic_image_fixture().await;
        let id = executor
            .outbox
            .enqueue("1", "100", &job.id, "queued caption", crate::now_unix())
            .await
            .unwrap();
        executor
            .store
            .set_group_flags("1", "100", None, Some(true), None, crate::now_unix())
            .await
            .unwrap();
        executor.deliver_rows("100", &[id.clone()]).await;
        let row = executor.store.get_outbox(&id).await.unwrap().unwrap();
        assert_eq!(row.state, "unknown");
        let cancelled: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'imagegen.delivery_cancelled.inactive'")
            .fetch_one(executor.store.pool()).await.unwrap();
        assert_eq!(
            cancelled, 1,
            "inactive delivery was not explicitly cancelled before sending"
        );
        executor
            .store
            .set_group_flags("1", "100", None, Some(false), None, crate::now_unix())
            .await
            .unwrap();
        assert!(executor.outbox.deliverable(10).await.unwrap().is_empty());
    }

    async fn atomic_image_fault(executor: &ImagegenExecutor) {
        sqlx::query("CREATE TRIGGER fail_second BEFORE INSERT ON outbox WHEN NEW.part_index = 1 BEGIN SELECT RAISE(ABORT, 'fixture-second-part'); END")
            .execute(executor.store.pool()).await.unwrap();
    }

    #[tokio::test]
    async fn atomic_image_enqueue_failure_is_not_success() {
        let (executor, job, _dir) = atomic_image_fixture().await;
        atomic_image_fault(&executor).await;
        executor.run_job(&job).await;
        let stored = executor
            .store
            .get_image_job(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            stored.state, "succeeded",
            "generation cannot claim an unpersisted delivery"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
            .fetch_one(executor.store.pool())
            .await
            .unwrap();
        assert_eq!(rows, 0);
        assert!(executor.out_dir.join(format!("{}.jpg", job.id)).exists());
    }

    #[tokio::test]
    async fn atomic_image_unknown_is_never_a_sent_audit() {
        let (executor, job, _dir) = atomic_image_fixture().await;
        executor.run_job(&job).await;
        let sent: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_events WHERE action = 'imagegen.sent'")
                .fetch_one(executor.store.pool())
                .await
                .unwrap();
        assert_eq!(sent, 0, "disconnected delivery was audited sent");
        let unknown = executor
            .store
            .outbox_batch(crate::store::OutboxState::Unknown, 10)
            .await
            .unwrap();
        assert_eq!(unknown.len(), 2);
        assert!(executor.outbox.deliverable(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn atomic_image_restart_reuses_generated_artifact_without_resend() {
        let (mut executor, job, dir) = atomic_image_fixture().await;
        atomic_image_fault(&executor).await;
        executor.run_job(&job).await;
        sqlx::query("DROP TRIGGER fail_second")
            .execute(executor.store.pool())
            .await
            .unwrap();
        // A second generation would fail: recovery must use the DB-owned artifact.
        std::fs::write(&executor.bin, "#!/bin/sh\nexit 99\n").unwrap();
        executor.store.pool().close().await;
        let pool = crate::store::migrations::open_and_migrate(&dir.path().join("bot.db"))
            .await
            .unwrap();
        executor.store = Arc::new(SqliteStore::new(pool));
        executor.outbox = Outbox::new(executor.store.clone());
        assert!(
            executor.tick().await,
            "generated job was lost across restart"
        );
        let rows = executor
            .store
            .outbox_batch(crate::store::OutboxState::Unknown, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.iter().filter(|row| row.kind == "image").count(), 1);
        assert_eq!(
            executor
                .store
                .get_image_job(&job.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            "succeeded"
        );
        assert!(!executor.tick().await, "terminal delivery was replayed");
        assert_eq!(
            executor
                .store
                .count_image_jobs("1", "100", "self", None, 0)
                .await
                .unwrap(),
            1
        );
    }

    struct CaptionSink(Arc<tokio::sync::Notify>);

    #[async_trait::async_trait]
    impl crate::onebot::connection::FrameSink for CaptionSink {
        async fn on_event(&self, _: serde_json::Value) {}
        async fn on_connected(&self) {
            self.0.notify_one();
        }
    }

    // Uses only a loopback fake OneBot and a fake CLI; no real messages or quota.
    async fn caption_delivery_fixture(
        caption: &str,
        via_mcp: bool,
    ) -> (Vec<String>, Vec<serde_json::Value>, Option<String>, i64) {
        use futures_util::{SinkExt, StreamExt};
        let dir = tempfile::tempdir().expect("tmp");
        let pool = crate::store::migrations::open_and_migrate(&dir.path().join("bot.db"))
            .await
            .expect("db");
        let store = Arc::new(SqliteStore::new(pool));
        let now = crate::now_unix();
        store.ensure_group("1", "100", now).await.expect("group");
        store
            .set_group_flags("1", "100", Some(true), Some(false), None, now)
            .await
            .expect("enable");
        if via_mcp {
            store
                .arm_turn("100", "caption-test", "batch", now, now + 3600)
                .await
                .expect("arm");
            let result = crate::mcp::imagegen::call_generate(&store, &crate::mcp::Scope {bot_id:"1".into(), group_id:"100".into()},
                &serde_json::json!({"turn_token":"caption-test", "mode":"self", "prompt":"画星空", "caption":caption})).await;
            assert_eq!(result["isError"], false);
        } else {
            // A previously queued row must be screened without MCP registration.
            store
                .insert_image_job(
                    "1",
                    "100",
                    "bot",
                    "self",
                    "画星空",
                    false,
                    Some(caption),
                    now,
                )
                .await
                .expect("job");
        }
        let job = store
            .claim_next_image_job(now)
            .await
            .expect("claim")
            .expect("job");
        let stored_caption = job.caption.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("loopback");
        let address = listener.local_addr().expect("address");
        let frames = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received = frames.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut socket = tokio_tungstenite::accept_async(stream).await.expect("ws");
            while let Some(Ok(message)) = socket.next().await {
                if !message.is_text() {
                    continue;
                }
                let value: serde_json::Value =
                    serde_json::from_str(message.to_text().expect("text")).expect("json");
                if value["action"] == "send_group_msg" {
                    received.lock().await.push(value.clone());
                }
                let response = serde_json::json!({"status":"ok", "retcode":0, "data":{"message_id":123}, "echo":value["echo"]});
                if socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        response.to_string().into(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        let (supervisor, connection) =
            crate::onebot::connection::Connection::new(format!("ws://{address}"), None);
        let ready = Arc::new(tokio::sync::Notify::new());
        let client = tokio::spawn(supervisor.run(Arc::new(CaptionSink(ready.clone()))));
        tokio::time::timeout(Duration::from_secs(3), ready.notified())
            .await
            .expect("connected");
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        let executor = ImagegenExecutor::new(
            store.clone(),
            connection,
            "1".into(),
            out,
            write_fake_agy(dir.path()),
            home,
            dir.path().join("persona"),
            "agy".into(),
            String::new(),
            None,
            vec!["synthetic-caption-secret-123456".into()],
        );
        let execution = tokio::time::timeout(Duration::from_secs(10), executor.run_job(&job)).await;
        client.abort();
        server.abort();
        let _ = client.await;
        let _ = server.await;
        execution.expect("offline execution");
        let rows = store
            .outbox_batch(crate::store::OutboxState::Sent, 10)
            .await
            .expect("outbox");
        assert_eq!(rows.iter().filter(|r| r.kind == "image").count(), 1);
        assert_eq!(
            store
                .get_image_job(&job.id)
                .await
                .expect("job")
                .expect("row")
                .state,
            "succeeded"
        );
        let texts = rows
            .into_iter()
            .filter(|r| r.kind == "text")
            .map(|r| r.text)
            .collect();
        let sent = frames.lock().await.clone();
        let charged = store
            .count_image_jobs("1", "100", "self", None, now)
            .await
            .expect("count");
        (texts, sent, stored_caption, charged)
    }

    #[tokio::test]
    async fn caption_screen_blocks_machinery_before_registration_and_queued_delivery() {
        for caption in ["回复已提交✅", "should_reply: true", "你好[CQ:at,qq=all]"] {
            for via_mcp in [true, false] {
                let (texts, frames, stored, charged) =
                    caption_delivery_fixture(caption, via_mcp).await;
                assert!(texts.is_empty(), "unsafe caption reached outbox: {caption}");
                assert_eq!(frames.len(), 1, "only the image may be sent");
                if via_mcp {
                    assert_eq!(stored, None);
                }
                assert_eq!(charged, 1);
            }
        }
    }

    #[tokio::test]
    async fn caption_screen_masks_named_and_known_synthetic_secrets() {
        let secret = "synthetic-caption-secret-123456";
        let caption = format!("api_key={secret}；这串也是{secret}");
        let (texts, frames, _, charged) = caption_delivery_fixture(&caption, false).await;
        assert_eq!(texts.len(), 1);
        assert!(!texts[0].contains(secret));
        assert!(
            !serde_json::to_string(&frames)
                .expect("wire")
                .contains(secret)
        );
        assert_eq!(charged, 1);
    }

    #[tokio::test]
    async fn caption_screen_preserves_ordinary_caption_without_extra_charge() {
        let caption = "今晚的星空，送给大家";
        for via_mcp in [true, false] {
            let (texts, frames, _, charged) = caption_delivery_fixture(caption, via_mcp).await;
            assert_eq!(texts, vec![caption]);
            assert_eq!(frames.len(), 2);
            assert!(
                serde_json::to_string(&frames)
                    .expect("wire")
                    .contains(caption)
            );
            assert_eq!(charged, 1);
        }
    }

    #[test]
    fn self_portrait_prompt_anchors_persona_and_demands_verbatim_copy() {
        let p = build_prompt(
            &job(true),
            Path::new("/data/persona"),
            Path::new("/data/imagegen"),
        );
        assert!(p.contains("/data/persona/persona-sheet.png"));
        assert!(p.contains("/data/persona/character-card.txt"));
        assert!(p.contains("在星空下喝奶茶"));
        assert!(p.contains("/data/imagegen/job_test1.jpg"));
        assert!(p.contains("do not convert"), "{p}");
    }

    #[test]
    fn plain_prompt_has_no_persona_anchor() {
        let p = build_prompt(
            &job(false),
            Path::new("/data/persona"),
            Path::new("/data/imagegen"),
        );
        assert!(!p.contains("persona-sheet"));
        assert!(p.contains("在星空下喝奶茶"));
    }

    fn encoded_test_image(format: image::ImageFormat) -> Vec<u8> {
        let mut out = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 2)
            .write_to(&mut out, format)
            .unwrap();
        out.into_inner()
    }

    #[test]
    fn verified_artifact_rejects_magic_without_a_decodable_image() {
        for bytes in [&b"\xff\xd8\xfffake"[..], &b"\x89PNGfake"[..]] {
            assert!(!image_bytes_ok(bytes), "magic alone accepted");
        }
        for format in [image::ImageFormat::Png, image::ImageFormat::Jpeg] {
            assert!(image_bytes_ok(&encoded_test_image(format)));
        }
    }

    #[tokio::test]
    async fn verified_artifact_rejects_excess_bytes_and_pixel_bombs() {
        let mut jpeg = encoded_test_image(image::ImageFormat::Jpeg);
        let sof = jpeg.windows(2).position(|s| s == [0xff, 0xc0]).unwrap();
        jpeg[sof + 5..sof + 9].fill(0xff);
        assert!(!image_bytes_ok(&jpeg), "huge decoded dimensions accepted");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.jpg");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len((MAX_IMAGE_BYTES + 1) as u64).unwrap();
        assert_eq!(try_collect(&path).await, None);
    }

    #[tokio::test]
    async fn verified_artifact_rejects_partial_files_until_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.jpg");
        let png = encoded_test_image(image::ImageFormat::Png);
        std::fs::write(&path, &png[..png.len() / 2]).unwrap();
        assert_eq!(
            try_collect(&path).await,
            None,
            "half-written artifact accepted"
        );
        std::fs::write(&path, png).unwrap();
        assert_eq!(try_collect(&path).await, Some(false));
    }

    #[tokio::test]
    async fn verified_artifact_http_500_ok_body_falls_back_exactly_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut executor, job, dir) = atomic_image_fixture().await;
        let marker = dir.path().join("fallback-count");
        std::fs::write(
            &executor.bin,
            format!("#!/bin/sh\nprintf x >> '{}'\nexit 1\n", marker.display()),
        )
        .unwrap();
        std::fs::create_dir_all(&executor.web_dir).unwrap();
        std::fs::write(
            executor.web_dir.join(format!("{}.jpg", job.id)),
            encoded_test_image(image::ImageFormat::Jpeg),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        executor.backend = "web".into();
        executor.web_url = format!("http://{}", listener.local_addr().unwrap());
        let worker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 8192];
            let _ = socket.read(&mut buf).await.unwrap();
            let body = r#"{"ok":true}"#;
            socket.write_all(format!("HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
        });
        executor.run_job(&job).await;
        worker.await.unwrap();
        assert_eq!(
            std::fs::read(&marker).unwrap_or_default(),
            b"x",
            "fallback count must be exactly one"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox")
            .fetch_one(executor.store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "500 response must not enqueue its artifact");
    }

    #[test]
    fn verified_artifact_unknown_worker_error_is_not_audited_verbatim() {
        assert_eq!(
            web_result_class(
                &serde_json::json!({"ok":false,"error_class":"untrusted-secret-shaped-detail"})
            ),
            Some("internal".into())
        );
        assert_eq!(
            web_result_class(&serde_json::json!({"ok":false,"error_class":"auth"})),
            Some("auth".into())
        );
    }

    #[tokio::test]
    async fn verified_artifact_web_response_limits_and_exact_snapshot() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut executor, job, _dir) = atomic_image_fixture().await;
        std::fs::create_dir_all(&executor.web_dir).unwrap();
        let original = encoded_test_image(image::ImageFormat::Png);
        std::fs::write(executor.web_dir.join(format!("{}.jpg", job.id)), &original).unwrap();
        for (body, chunked, expected) in [
            (
                vec![b' '; MAX_WEB_RESPONSE_BYTES + 1],
                false,
                Some("response_size"),
            ),
            (
                vec![b' '; MAX_WEB_RESPONSE_BYTES + 1],
                true,
                Some("response_size"),
            ),
            (br#"{"ok":true}"#.to_vec(), false, None),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            executor.web_url = format!("http://{}", listener.local_addr().unwrap());
            let worker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 8192];
                let _ = socket.read(&mut request).await.unwrap();
                let response = if chunked {
                    format!(
                        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                        body.len(),
                        String::from_utf8(body).unwrap()
                    )
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        String::from_utf8(body).unwrap()
                    )
                };
                // The size gate may close before the fixture finishes writing.
                let _ = socket.write_all(response.as_bytes()).await;
            });
            let result = executor.run_web(&job).await;
            worker.await.unwrap();
            assert_eq!(result.error.as_deref(), expected);
        }
        let copied = std::fs::read(executor.out_dir.join(format!("{}.jpg", job.id))).unwrap();
        assert_eq!(
            copied, original,
            "copy must preserve the validated bytes without conversion"
        );
        // Delivery revalidates its own snapshot, so replacing a queued file with
        // invalid bytes cannot exploit the earlier validation.
        std::fs::write(
            executor.out_dir.join(format!("{}.jpg", job.id)),
            b"\xff\xd8\xffbad",
        )
        .unwrap();
        let ids = executor
            .outbox
            .enqueue_image(
                "1",
                "100",
                &job.id,
                None,
                &format!("{}.jpg", job.id),
                crate::now_unix(),
            )
            .await
            .unwrap();
        let result = executor.deliver_rows("100", &ids).await;
        assert_eq!(result.unknown, 1);
        assert_eq!(result.sent, 0);
    }

    #[test]
    fn image_gate_accepts_jpeg_and_png_only() {
        assert!(image_bytes_ok(&encoded_test_image(
            image::ImageFormat::Jpeg
        )));
        assert!(image_bytes_ok(&encoded_test_image(image::ImageFormat::Png)));
        assert!(!image_bytes_ok(&[]));
        assert!(!image_bytes_ok(b"GIF89a...."));
        assert!(!image_bytes_ok(&vec![0xFF; MAX_IMAGE_BYTES + 1]));
    }

    #[tokio::test]
    async fn collection_requires_the_exact_target_even_with_equal_mtimes() {
        let dir = tempfile::tempdir().expect("tmp");
        let target = dir.path().join("current.jpg");
        let neighbour = dir.path().join("previous.jpg");
        std::fs::write(&neighbour, encoded_test_image(image::ImageFormat::Png)).expect("old");
        assert_eq!(try_collect(&target).await, None);
        let jpeg = encoded_test_image(image::ImageFormat::Jpeg);
        std::fs::write(&target, &jpeg).expect("new");
        let fixed = std::time::UNIX_EPOCH + Duration::from_secs(1);
        for path in [&target, &neighbour] {
            std::fs::File::options()
                .write(true)
                .open(path)
                .expect("open")
                .set_modified(fixed)
                .expect("mtime");
        }
        assert_eq!(try_collect(&target).await, Some(false));
        assert_eq!(std::fs::read(&target).expect("target"), jpeg);
    }

    #[test]
    fn failure_note_only_for_requested_pictures() {
        assert!(failure_note_warranted("request"));
        assert!(!failure_note_warranted("self"));
    }

    #[test]
    fn quota_signature_matches_the_real_429_body() {
        // The shape the image subagent's transcript carried on the first
        // production night: a cloudcode-pa 429 naming the reason.
        let body = r#"image failed to generate: 429 Too Many Requests, "reason": "QUOTA_EXHAUSTED", "message": "You have exhausted your capacity on this model.""#;
        assert!(transcript_shows_quota_exhaustion(body));
        assert!(!transcript_shows_quota_exhaustion(
            "Created the following subagent"
        ));
        assert!(!transcript_shows_quota_exhaustion(""));
    }

    #[test]
    fn quota_scan_only_reads_current_invocation_logs() {
        let dir = tempfile::tempdir().expect("tmp");
        let stdout = dir.path().join("stdout");
        let stderr = dir.path().join("stderr");
        std::fs::write(dir.path().join("old-transcript"), "QUOTA_EXHAUSTED").expect("old");
        assert!(!quota_exhaustion_seen(&stdout, &stderr));
        for path in [&stdout, &stderr] {
            std::fs::write(path, "QUOTA_EXHAUSTED").expect("write");
            assert!(quota_exhaustion_seen(&stdout, &stderr));
            std::fs::write(path, "").expect("truncate");
        }
        assert!(!quota_exhaustion_seen(&stdout, &stderr));
    }

    #[tokio::test]
    async fn image_fixture_has_an_independent_decoder_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let bin = write_fake_agy(dir.path());
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        let job = job(false);
        let prompt = build_prompt(&job, Path::new("/unused"), &out);
        assert_eq!(
            run_generation(&bin, &home, &out, &job, &prompt).await,
            GenOutcome::Ready { harvested: false }
        );
        // Do not use image_bytes_ok as the oracle for the fake producer.
        let bytes = std::fs::read(out.join(format!("{}.jpg", job.id))).unwrap();
        let decoded =
            image::load_from_memory_with_format(&bytes, image::ImageFormat::Jpeg).unwrap();
        assert_eq!((decoded.width(), decoded.height()), (2, 2));
    }

    #[tokio::test]
    async fn collection_ownership_ignores_old_equal_and_new_neighbour_mtimes() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("job.jpg");
        let neighbour = dir.path().join("foreign.jpg");
        let jpeg = encoded_test_image(image::ImageFormat::Jpeg);
        std::fs::write(&neighbour, &jpeg).unwrap();
        for seconds in [1, 100, 200] {
            std::fs::File::options()
                .write(true)
                .open(&neighbour)
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(seconds))
                .unwrap();
            assert_eq!(
                try_collect(&target).await,
                None,
                "neighbour time {seconds} cannot establish ownership"
            );
        }
        std::fs::write(&target, &jpeg).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&target)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(100))
            .unwrap();
        assert_eq!(try_collect(&target).await, Some(false));
        assert_eq!(std::fs::read(&target).unwrap(), jpeg);
    }

    #[test]
    fn quota_failures_get_the_quota_note() {
        assert_eq!(failure_note("quota"), QUOTA_NOTE);
        assert_eq!(failure_note("timeout"), FAILURE_NOTE);
        assert_eq!(failure_note("no_output"), FAILURE_NOTE);
    }

    /// A fake agy that behaves: it lifts the target path out of its prompt
    /// argument and writes a minimal JPEG there.
    fn write_fake_agy(dir: &Path) -> PathBuf {
        let path = dir.join("fake-agy.sh");
        let fixture = dir.join("fixture.jpg");
        std::fs::write(&fixture, encoded_test_image(image::ImageFormat::Jpeg)).unwrap();
        std::fs::write(
            &path,
            format!("#!/bin/sh\ntarget=$(printf '%s' \"$2\" | grep -o '/[^ ]*\\.jpg' | head -n 1)\ncp '{}' \"$target\"\n", fixture.display()),
        )
        .expect("写脚本");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        path
    }

    #[tokio::test]
    async fn fake_agy_round_produces_a_ready_target() {
        let dir = tempfile::tempdir().expect("tmp");
        let bin = write_fake_agy(dir.path());
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        let j = job(false);
        let prompt = build_prompt(&j, Path::new("/data/persona"), &out);
        let outcome = run_generation(&bin, &home, &out, &j, &prompt).await;
        assert_eq!(outcome, GenOutcome::Ready { harvested: false });
        assert!(image_bytes_ok(
            &std::fs::read(out.join("job_test1.jpg")).expect("产物")
        ));
    }

    /// A fake agy whose own stderr reports the quota rejection
    /// and then hangs: the executor must fail fast as `quota`, not sit
    /// out the generation timeout.
    #[tokio::test]
    async fn quota_rejection_fails_fast() {
        let dir = tempfile::tempdir().expect("tmp");
        let bin = dir.path().join("quota-agy.sh");
        std::fs::write(
            &bin,
            "#!/bin/sh\n\
             printf '%s' 'error 429 QUOTA_EXHAUSTED exhausted your capacity' >&2\n\
             exec sleep 60\n",
        )
        .expect("写脚本");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        let j = job(false);
        let t0 = std::time::Instant::now();
        let outcome = run_generation(&bin, &home, &out, &j, "prompt").await;
        assert_eq!(outcome, GenOutcome::Failed { class: "quota" });
        assert!(
            t0.elapsed() < Duration::from_secs(45),
            "快速失败没生效: {:?}",
            t0.elapsed()
        );
    }

    /// A slow-but-successful agy crosses several wait slices before it
    /// exits; the sliced wait must still reap it and collect the target.
    #[tokio::test]
    async fn slow_agy_across_wait_slices_still_succeeds() {
        let dir = tempfile::tempdir().expect("tmp");
        let bin = dir.path().join("slow-agy.sh");
        let fixture = dir.path().join("slow-fixture.jpg");
        std::fs::write(&fixture, encoded_test_image(image::ImageFormat::Jpeg)).unwrap();
        std::fs::write(
            &bin,
            format!("#!/bin/sh\nsleep 12\ntarget=$(printf '%s' \"$2\" | grep -o '/[^ ]*\\.jpg' | head -n 1)\ncp '{}' \"$target\"\n", fixture.display()),
        )
        .expect("写脚本");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        let j = job(false);
        let prompt = build_prompt(&j, Path::new("/data/persona"), &out);
        let outcome = run_generation(&bin, &home, &out, &j, &prompt).await;
        assert_eq!(outcome, GenOutcome::Ready { harvested: false });
    }

    #[tokio::test]
    async fn silent_agy_is_a_no_output_failure() {
        let dir = tempfile::tempdir().expect("tmp");
        let bin = dir.path().join("mute-agy.sh");
        std::fs::write(&bin, "#!/bin/sh\nexit 0\n").expect("写脚本");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        let j = job(false);
        let outcome = run_generation(&bin, &home, &out, &j, "prompt").await;
        assert_eq!(outcome, GenOutcome::Failed { class: "no_output" });
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn lifecycle_image_leader_exit_does_not_leave_a_writer() {
        use crate::process::test_support::*;
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        let bin = dir.path().join("agy");
        let child_path = dir.path().join("child.pid");
        executable(
            &bin,
            &format!(
                "#!/bin/sh\nsh -c 'trap \"\" HUP TERM; echo $$ > {}; exec sleep 60' &\nwhile [ ! -s {} ]; do sleep 0.01; done\nexit 0\n",
                child_path.display(),
                child_path.display()
            ),
        );
        let result = run_generation(&bin, &home, &out, &job(false), "prompt").await;
        let pid = pid_file(&child_path).await;
        let gone = stopped(pid).await;
        cleanup(pid);
        assert_eq!(result, GenOutcome::Failed { class: "no_output" });
        assert!(gone, "image leader exit leaked a live descendant");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn lifecycle_image_cancellation_kills_the_whole_job() {
        use crate::process::test_support::*;
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        let bin = dir.path().join("agy");
        let child_path = dir.path().join("child.pid");
        executable(
            &bin,
            &format!(
                "#!/bin/sh\nsh -c 'trap \"\" HUP TERM; echo $$ > {}; exec sleep 60' &\nwait\n",
                child_path.display()
            ),
        );
        let task =
            tokio::spawn(
                async move { run_generation(&bin, &home, &out, &job(false), "prompt").await },
            );
        let pid = pid_file(&child_path).await;
        task.abort();
        assert!(matches!(task.await, Err(err) if err.is_cancelled()));
        let gone = stopped(pid).await;
        cleanup(pid);
        assert!(gone, "cancelled image attempt leaked a live descendant");
    }

    async fn unbound_generation_fixture(script: &str, conversations: &[&str]) -> GenOutcome {
        let dir = tempfile::tempdir().expect("tmp");
        let home = dir.path().join("home");
        let out = dir.path().join("out");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&out).expect("out");
        for name in conversations {
            let conv = home.join(".gemini/antigravity-cli/brain").join(name);
            let logs = conv.join(".system_generated/logs");
            std::fs::create_dir_all(&logs).expect("logs");
            std::fs::write(conv.join("previous.jpg"), b"\xff\xd8\xffold").expect("image");
            std::fs::write(logs.join("transcript.jsonl"), "QUOTA_EXHAUSTED").expect("transcript");
        }
        let bin = dir.path().join("unbound-agy.sh");
        std::fs::write(&bin, script).expect("script");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        run_generation(&bin, &home, &out, &job(false), "prompt").await
    }

    #[tokio::test]
    async fn unbound_previous_group_image_is_never_delivered() {
        assert_eq!(
            unbound_generation_fixture("#!/bin/sh\nexit 0\n", &["previous-group"]).await,
            GenOutcome::Failed { class: "no_output" }
        );
    }

    #[tokio::test]
    async fn unbound_concurrent_conversations_are_never_guessed() {
        assert_eq!(
            unbound_generation_fixture("#!/bin/sh\nexit 0\n", &["group-a", "group-b"]).await,
            GenOutcome::Failed { class: "no_output" }
        );
    }

    #[tokio::test]
    async fn unbound_old_quota_does_not_abort_current_job() {
        assert_eq!(
            unbound_generation_fixture("#!/bin/sh\nexec sleep 6\n", &["previous-group"]).await,
            GenOutcome::Failed { class: "no_output" }
        );
    }

    #[tokio::test]
    async fn unbound_no_image_remains_no_output() {
        assert_eq!(
            unbound_generation_fixture("#!/bin/sh\nexit 0\n", &[]).await,
            GenOutcome::Failed { class: "no_output" }
        );
    }

    #[tokio::test]
    async fn unbound_current_process_quota_is_still_detected() {
        assert_eq!(
            unbound_generation_fixture("#!/bin/sh\necho QUOTA_EXHAUSTED >&2\nexec sleep 6\n", &[])
                .await,
            GenOutcome::Failed { class: "quota" }
        );
    }

    #[test]
    fn web_prompt_plain_is_the_job_prompt() {
        assert_eq!(build_web_prompt(&job(false)), "在星空下喝奶茶");
    }

    #[test]
    fn web_prompt_self_portrait_anchors_on_the_attached_sheet() {
        let p = build_web_prompt(&job(true));
        assert!(p.contains("attached"), "{p}");
        assert!(p.contains("heterochromia"), "{p}");
        assert!(p.contains("在星空下喝奶茶"), "{p}");
        assert!(!p.contains("cp "), "no shell instructions: {p}");
    }

    /// 成功路径要把时延和 worker 自报的队列/渲染耗时一起记下来，缺失的字段
    /// 必须显式留 `-`——「零」和「没上报」不是同一件事。
    #[tokio::test]
    async fn web_observation_records_latency_and_missing_queue_fields() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut executor, job, _dir) = atomic_image_fixture().await;
        std::fs::create_dir_all(&executor.web_dir).unwrap();
        std::fs::write(
            executor.web_dir.join(format!("{}.jpg", job.id)),
            encoded_test_image(image::ImageFormat::Jpeg),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        executor.web_url = format!("http://{}", listener.local_addr().unwrap());
        let worker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let body = r#"{"ok":true,"queue_ms":1200,"render_ms":43000}"#;
            let _ = socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await;
        });
        let observation = executor.run_web(&job).await;
        worker.await.unwrap();
        assert_eq!(observation.error, None);
        assert_eq!(observation.queue_ms, Some(1200));
        assert_eq!(observation.render_ms, Some(43000));
        assert!(observation.elapsed_ms >= 0);
        let tag = observation.audit_tag("100");
        assert!(tag.contains(".ms"), "{tag}");
        assert!(tag.contains(".q1200"), "队列耗时必须进审计：{tag}");
        assert!(tag.contains(".r43000"), "{tag}");

        // 同一端点，worker 只回 ok：缺失字段留空而不是补 0。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        executor.web_url = format!("http://{}", listener.local_addr().unwrap());
        let worker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let body = r#"{"ok":true}"#;
            let _ = socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .as_bytes(),
                )
                .await;
        });
        let observation = executor.run_web(&job).await;
        worker.await.unwrap();
        assert_eq!(observation.queue_ms, None);
        assert_eq!(observation.render_ms, None);
        assert!(
            observation.audit_tag("100").contains(".q-.r-"),
            "缺失字段必须是 `-`，不能是 0"
        );
    }

    /// 503 与 429 必须分开：把额度不足当成网络差，正是「换节点解决不了却
    /// 狂换节点」的成因。
    #[tokio::test]
    async fn web_observation_separates_rate_limit_from_server_failure() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut executor, job, _dir) = atomic_image_fixture().await;
        for (status, expected) in [
            ("429 Too Many Requests", Some("rate_limited")),
            ("503 Service Unavailable", Some("http_status")),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            executor.web_url = format!("http://{}", listener.local_addr().unwrap());
            let worker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 8192];
                let _ = socket.read(&mut request).await.unwrap();
                let _ = socket
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                        )
                        .as_bytes(),
                    )
                    .await;
            });
            let observation = executor.run_web(&job).await;
            worker.await.unwrap();
            assert_eq!(observation.error.as_deref(), expected, "{status}");
            assert!(observation.elapsed_ms >= 0, "失败也要有时延");
        }
    }

    #[test]
    fn web_result_class_reads_ok_and_error_shapes() {
        assert_eq!(web_result_class(&serde_json::json!({"ok": true})), None);
        assert_eq!(
            web_result_class(&serde_json::json!({"ok": false, "error_class": "auth"})),
            Some("auth".to_string())
        );
        assert_eq!(
            web_result_class(&serde_json::json!({"ok": false})),
            Some("internal".to_string())
        );
        assert_eq!(
            web_result_class(&serde_json::json!({})),
            Some("internal".to_string())
        );
    }
}
