//! Spontaneous-topic engine (schema v1, [mem-v1-4]).
//!
//! After a long silence the bot may open a topic of her own — never on a
//! timer alone: a 60-second ticker re-checks every gate each round (config
//! switch, group enable/pause, pace whitelist, quiet hours, daily
//! caps, hourly call budget, a turn already in flight, humans already
//! waiting). An opening is a *synthetic batch through the ordinary turn
//! pipeline*: one bot-authored pending event carrying a topic card in the
//! prompt. The agent may still decline with `should_reply=false`; nothing
//! here sends a message itself.
//!
//! Outcomes are read back from the ground the pipeline already writes —
//! the outbox (she actually sent) and the group log (a human answered) —
//! by the same ticker, so a crash between enqueue and send cannot strand
//! the state machine: a pending opening with no send inside the decline
//! window simply resolves as declined.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::TurnExecutor;
use super::turn::RunningTurns;
use crate::config::SpontaneousConfig;
use crate::memory::compaction::{
    SummaryModel, leaks_identifier, parse_structured_summary, summary_display_text,
};
use crate::memory::extract::is_near_duplicate;
use crate::onebot::normalize::{IncomingMessage, Segment, SpeakerRole};
use crate::pipeline::scheduler::{BatchJob, Scheduler, SpontaneousBrief};
use crate::store::{AmbientPace, GroupRecord, SqliteStore};

const SEVEN_DAYS: i64 = 7 * 86_400;
const FOURTEEN_DAYS: i64 = 14 * 86_400;
/// A human reply within this window after the opening counts as answered.
const ANSWER_WINDOW: i64 = 1_800;
/// An enqueued opening with no outbox send by now counts as declined.
const DECLINE_WINDOW: i64 = 900;
/// Open loops due within this horizon (or overdue, or undated) are pickable.
const LOOP_HORIZON: i64 = SEVEN_DAYS;
/// Floor between two openings in one group, independent of the per-day
/// silence roll: the day counter resets at local midnight, and on
/// 2026-11-11/12 two openings landed 79 minutes apart across that reset,
/// each legal under the per-day rules alone.
const MIN_OPENING_INTERVAL: i64 = 4 * 3_600;
/// Local clock for quiet hours and day boundaries: the deployment runs on
/// Asia/Shanghai (UTC+8, no DST), the same fixed offset the prompt clock uses.
const LOCAL_OFFSET: i64 = 8 * 3_600;

/// One topic key's cooling-off entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownEntry {
    pub key: String,
    pub until: i64,
    /// True when the opening was sent and nobody answered: the longer
    /// 14-day cooldown applies and the flag says why.
    #[serde(default)]
    pub ignored: bool,
}

/// One enqueued opening's topic text, kept for content-level repeat
/// detection. The key cooldowns cannot see a paraphrase: the angle source
/// mints a fresh key from each generated wording, so on 2026-11-11 the
/// same open loop was re-asked twice in one night under two different
/// keys. Recent topic texts are compared by bigram similarity instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecentTopic {
    pub topic: String,
    pub at: i64,
}

/// An opening that has been enqueued and is waiting for its outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingOpening {
    pub topic: String,
    pub key: String,
    pub enqueued_at: i64,
    /// Exact accepted synthetic batch. Missing on legacy state: never infer it.
    #[serde(default)]
    pub batch_id: Option<String>,
    /// Set once the outbox shows she actually sent something.
    #[serde(default)]
    pub fired_at: Option<i64>,
}

/// The engine's per-group state, stored as one JSON blob in
/// `group_state.spontaneous_json`. A garbled blob parses as a fresh state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SpontaneousState {
    /// When the last opening was enqueued (epoch seconds).
    pub last_at: i64,
    /// Local day (`YYYY-MM-DD`) the counters below belong to.
    pub day: String,
    /// Openings that actually sent today.
    pub day_count: u32,
    /// Declined attempts today. The attempts gate is `day_count + attempts`.
    pub attempts: u32,
    pub cooldown: Vec<CooldownEntry>,
    pub pending: Option<PendingOpening>,
    /// Topics of openings enqueued in the last 14 days, newest last.
    #[serde(default)]
    pub recent_topics: Vec<RecentTopic>,
}

impl SpontaneousState {
    fn in_cooldown(&self, key: &str, now: i64) -> bool {
        self.cooldown.iter().any(|e| e.key == key && e.until > now)
    }

    /// True when `topic` says substantially the same thing as an opening
    /// enqueued inside the 14-day window, however it is worded.
    fn topic_recently_used(&self, topic: &str, now: i64) -> bool {
        self.recent_topics
            .iter()
            .any(|r| now - r.at < FOURTEEN_DAYS && is_near_duplicate(topic, &r.topic))
    }

    fn cool(&mut self, key: &str, until: i64, ignored: bool) {
        if let Some(entry) = self.cooldown.iter_mut().find(|e| e.key == key) {
            entry.until = entry.until.max(until);
            entry.ignored = entry.ignored || ignored;
        } else {
            self.cooldown.push(CooldownEntry {
                key: key.to_string(),
                until,
                ignored,
            });
        }
    }
}

/// A picked topic plus its cooldown key.
struct PickedTopic {
    key: String,
    brief: SpontaneousBrief,
}

/// How an opening reaches the group. The production implementation is
/// [`PipelineSpeaker`]; tests substitute a recorder. The seam exists so the
/// state machine can be tested without a live turn executor — the same
/// reason the compactor talks to a [`SummaryModel`] trait.
#[async_trait::async_trait]
pub trait OpeningSpeaker: Send + Sync + 'static {
    /// Return the accepted batch identity, or None if the lane refused it.
    async fn speak(&self, group_id: &str, picked: &PickedTopic, now: i64) -> Option<String>;
}

/// The engine. Owns no loop of its own; [`super::App`] ticks it.
pub struct SpontaneousEngine {
    store: Arc<SqliteStore>,
    model: Arc<dyn SummaryModel>,
    speaker: Arc<dyn OpeningSpeaker>,
    running_turns: Arc<RunningTurns>,
    bot_id: String,
    cfg: SpontaneousConfig,
    hourly_call_budget: u32,
}

impl SpontaneousEngine {
    pub fn new(
        store: Arc<SqliteStore>,
        model: Arc<dyn SummaryModel>,
        speaker: Arc<dyn OpeningSpeaker>,
        running_turns: Arc<RunningTurns>,
        bot_id: String,
        cfg: SpontaneousConfig,
        hourly_call_budget: u32,
    ) -> Self {
        Self {
            store,
            model,
            speaker,
            running_turns,
            bot_id,
            cfg,
            hourly_call_budget,
        }
    }

    /// One ticker round for one group: resolve a pending opening's outcome,
    /// then maybe fire a new one. Every early return is a gate doing its job.
    pub async fn tick_group(&self, group: &GroupRecord, now: i64) {
        if !self.cfg.enabled {
            return;
        }
        let group_id = group.group_id.as_str();
        let raw = self
            .store
            .get_spontaneous_json(&self.bot_id, group_id)
            .await
            .unwrap_or_else(|_| "{}".to_string());
        let mut state: SpontaneousState = serde_json::from_str(&raw).unwrap_or_default();
        let mut dirty = false;

        // Day rollover first, so a resolution below lands on today's counters.
        let today = local_day(now);
        if state.day != today {
            state.day = today;
            state.day_count = 0;
            state.attempts = 0;
            dirty = true;
        }

        if state.pending.is_some() {
            dirty |= self.resolve_pending(group_id, &mut state, now).await;
        }

        if state.pending.is_none() {
            dirty |= self.maybe_fire(group, &mut state, now).await;
        }

        if dirty {
            state.cooldown.retain(|e| e.until > now - 86_400);
            state.recent_topics.retain(|r| now - r.at < FOURTEEN_DAYS);
            if state.recent_topics.len() > 10 {
                let excess = state.recent_topics.len() - 10;
                state.recent_topics.drain(..excess);
            }
            if let Ok(json) = serde_json::to_string(&state) {
                let _ = self
                    .store
                    .put_spontaneous_json(&self.bot_id, group_id, &json, now)
                    .await;
            }
        }
    }

    /// Advance a pending opening from ground truth (outbox + group log).
    /// Returns whether the state changed.
    async fn resolve_pending(
        &self,
        group_id: &str,
        state: &mut SpontaneousState,
        now: i64,
    ) -> bool {
        let Some(pending) = state.pending.clone() else {
            return false;
        };
        match pending.fired_at {
            None => {
                let Some(batch_id) = pending.batch_id.as_deref().filter(|id| !id.is_empty()) else {
                    state.pending = None;
                    state.attempts += 1;
                    state.cool(&pending.key, now + SEVEN_DAYS, false);
                    self.audit("spontaneous.declined.unattributed", group_id, now)
                        .await;
                    return true;
                };
                let Ok(sent) = self
                    .store
                    .first_batch_sent_at(&self.bot_id, group_id, batch_id, now)
                    .await
                else {
                    // A storage failure is not evidence of decline or successful delivery.
                    return false;
                };
                if let Some(sent_at) = sent {
                    state.pending = Some(PendingOpening {
                        fired_at: Some(sent_at),
                        ..pending.clone()
                    });
                    state.day_count += 1;
                    state.cool(&pending.key, sent_at + SEVEN_DAYS, false);
                    self.audit("spontaneous.fired", group_id, now).await;
                    return true;
                }
                if now - pending.enqueued_at > DECLINE_WINDOW {
                    state.pending = None;
                    state.attempts += 1;
                    state.cool(&pending.key, now + SEVEN_DAYS, false);
                    self.audit("spontaneous.declined", group_id, now).await;
                    return true;
                }
                false
            }
            Some(fired_at) => {
                let humans = self
                    .store
                    .count_group_messages_after(&self.bot_id, group_id, fired_at as f64)
                    .await
                    .unwrap_or(0);
                if humans > 0 && now - fired_at <= ANSWER_WINDOW + 120 {
                    state.pending = None;
                    self.audit("spontaneous.answered", group_id, now).await;
                    return true;
                }
                if now - fired_at > ANSWER_WINDOW {
                    state.pending = None;
                    state.cool(&pending.key, fired_at + FOURTEEN_DAYS, true);
                    self.audit("spontaneous.ignored", group_id, now).await;
                    return true;
                }
                false
            }
        }
    }

    /// All gates, topic picking and the synthetic-batch enqueue. Returns
    /// whether the state changed (an opening was enqueued).
    async fn maybe_fire(
        &self,
        group: &GroupRecord,
        state: &mut SpontaneousState,
        now: i64,
    ) -> bool {
        let group_id = group.group_id.as_str();
        if !group.enabled || group.paused {
            return false;
        }
        // Only normal and lively groups get spontaneous openings
        // (operator ruling 2026-11-12): quiet and addressed-only groups
        // are never opened at. Replying when spoken to is unaffected —
        // this gate is only about her starting a topic unprompted.
        if !matches!(group.pace(), AmbientPace::Normal | AmbientPace::Lively) {
            return false;
        }
        if in_quiet_hours(local_hour(now), &self.cfg) {
            return false;
        }
        if state.day_count >= self.cfg.daily_cap
            || state.day_count + state.attempts >= self.cfg.max_attempts
        {
            return false;
        }
        if self.running_turns.contains(group_id) {
            return false;
        }
        if self
            .store
            .pending_count(&self.bot_id, group_id)
            .await
            .unwrap_or(0)
            > 0
        {
            return false;
        }
        if self.hourly_call_budget > 0 {
            let calls = self
                .store
                .count_agent_calls_since(&self.bot_id, group_id, now - 3_600)
                .await
                .unwrap_or(0);
            if calls >= i64::from(self.hourly_call_budget) {
                return false;
            }
        }
        let Some(last_human) = self
            .store
            .last_human_message_at(&self.bot_id, group_id)
            .await
            .unwrap_or(None)
        else {
            return false;
        };
        let silence_needed = daily_silence_threshold(group_id, &state.day, &self.cfg) * 60;
        // Silence is measured from the end of the quiet window when that
        // is later than the last human message: the quiet stretch itself
        // must not count, or every group crosses its threshold the minute
        // quiet hours end and all open at once (2026-11-12).
        let silence_epoch = last_human.max(last_quiet_end(now, &self.cfg));
        if now - silence_epoch < silence_needed
            || now - state.last_at < silence_needed.max(MIN_OPENING_INTERVAL)
        {
            return false;
        }

        let Some(picked) = self.pick_topic(group_id, state, now).await else {
            return false;
        };
        // A cooled-down key voids the whole attempt: no re-pick, no audit —
        // nothing changed, and the next tick rolls the dice again.
        if state.in_cooldown(&picked.key, now) {
            return false;
        }
        // Same void for a topic that merely rewords a recent opening: the
        // angle source's key is a hash of its own wording, so the key
        // check above can never catch it (2026-11-12 incident).
        if state.topic_recently_used(&picked.brief.topic, now) {
            return false;
        }
        let Some(batch_id) = self.speaker.speak(group_id, &picked, now).await else {
            return false;
        };
        state.last_at = now;
        state.recent_topics.push(RecentTopic {
            topic: picked.brief.topic.clone(),
            at: now,
        });
        state.pending = Some(PendingOpening {
            topic: picked.brief.topic.clone(),
            key: picked.key,
            enqueued_at: now,
            batch_id: Some(batch_id),
            fired_at: None,
        });
        true
    }

    /// Weighted topic picking: open loop 40% / fresh angle 40% / memory
    /// callback 20%, each source falling through to the next when empty.
    async fn pick_topic(
        &self,
        group_id: &str,
        state: &SpontaneousState,
        now: i64,
    ) -> Option<PickedTopic> {
        // Sealed topics ([seal-4]): no source may re-seed a dead topic
        // into the chat. Read once per pick; a failed read fails open.
        let sealed = self
            .store
            .list_sealed_topics(&self.bot_id, group_id)
            .await
            .unwrap_or_default();
        let roll = hash64(&format!(
            "{group_id}|{}|{}|{}",
            state.day,
            state.attempts,
            now / 600
        )) % 100;
        let order: [u8; 3] = if roll < 40 {
            [0, 1, 2]
        } else if roll < 80 {
            [1, 0, 2]
        } else {
            [2, 1, 0]
        };
        for source in order {
            let picked = match source {
                0 => self.pick_open_loop(group_id, state, now, &sealed).await,
                1 => self.pick_new_angle(group_id, state, &sealed).await,
                _ => self.pick_memory(group_id, state, now, &sealed).await,
            };
            if picked.is_some() {
                return picked;
            }
        }
        None
    }

    async fn pick_open_loop(
        &self,
        group_id: &str,
        state: &SpontaneousState,
        now: i64,
        sealed: &[crate::store::SealedTopic],
    ) -> Option<PickedTopic> {
        let summary = self
            .store
            .get_summary(&self.bot_id, group_id)
            .await
            .ok()??;
        let parsed = parse_structured_summary(&summary.summary_text)?;
        let loop_ = first_pickable_loop(&parsed.open_loops, state, now, sealed)?;
        Some(PickedTopic {
            key: format!("loop:{:016x}", hash64(&loop_.text)),
            brief: SpontaneousBrief {
                topic: loop_.text.clone(),
                angle: "这是之前群里聊起过、还没下文的事，自然地问一句后续怎么样了，别像在催进度。"
                    .to_string(),
            },
        })
    }

    async fn pick_new_angle(
        &self,
        group_id: &str,
        state: &SpontaneousState,
        sealed: &[crate::store::SealedTopic],
    ) -> Option<PickedTopic> {
        let mut material = String::new();
        if let Ok(Some(summary)) = self.store.get_summary(&self.bot_id, group_id).await {
            let display = summary_display_text(&summary.summary_text);
            if !display.trim().is_empty() {
                material.push_str("群画像与近况：\n");
                material.push_str(display.trim());
                material.push('\n');
            }
        }
        if let Ok(mut memories) = self
            .store
            .list_active_memories(&self.bot_id, group_id, 5)
            .await
        {
            crate::memory::sealed::retain_unsealed(&mut memories, sealed);
            for memory in memories {
                material.push_str(&format!("- {}\n", memory.content.trim()));
            }
        }
        if material.trim().is_empty() {
            return None;
        }
        // The generator otherwise converges on the most concrete open
        // loop every time: tell it what was already asked (2026-11-12).
        let mut recent_block = String::new();
        if !state.recent_topics.is_empty() {
            recent_block.push_str("最近已经开过这些话题，不要重复，也不要换个说法再问一遍：\n");
            for recent in state.recent_topics.iter().rev().take(5) {
                recent_block.push_str(&format!("- {}\n", recent.topic));
            }
        }
        let prompt = format!(
            "你在为一个 QQ 群想一个可以自然挑起聊天的新话题。只能依据下面的素材，不许编造群里没发生过的事，不许包含任何 QQ 号、链接或 @。\n素材：\n{material}{recent_block}\n只输出一行 JSON：{{\"topic\":\"话题，不超过 40 字\",\"angle\":\"切入角度，不超过 60 字\"}}"
        );
        let raw = self.model.summarise(prompt).await.ok()?;
        let (topic, angle) = parse_angle_reply(&raw)?;
        // The summary text feeding the material can still carry a legacy
        // sealed topic; the generated topic itself is checked before it
        // may fire ([seal-4]).
        if crate::memory::sealed::hit(sealed, "", &topic).is_some() {
            return None;
        }
        Some(PickedTopic {
            key: format!("angle:{:016x}", hash64(&topic)),
            brief: SpontaneousBrief { topic, angle },
        })
    }

    async fn pick_memory(
        &self,
        group_id: &str,
        state: &SpontaneousState,
        now: i64,
        sealed: &[crate::store::SealedTopic],
    ) -> Option<PickedTopic> {
        let memories = self
            .store
            .list_active_memories(&self.bot_id, group_id, 50)
            .await
            .ok()?;
        let live: Vec<_> = memories
            .iter()
            .filter(|m| {
                !m.is_expired(now)
                    && crate::memory::sealed::hit(sealed, &m.slot, &m.content).is_none()
            })
            .collect();
        if live.is_empty() {
            return None;
        }
        let idx = (hash64(&format!("{group_id}|{}|{}|mem", state.day, state.attempts))
            % live.len() as u64) as usize;
        let memory = live[idx];
        let topic: String = memory.content.trim().chars().take(80).collect();
        if topic.is_empty() {
            return None;
        }
        Some(PickedTopic {
            key: format!("mem:{}", memory.memory_ref),
            brief: SpontaneousBrief {
                topic,
                angle: "这是你记得的一件旧事，找个自然的由头提起，别像在背档案。".to_string(),
            },
        })
    }

    async fn audit(&self, action: &str, group_id: &str, now: i64) {
        let _ = self
            .store
            .audit(
                "system",
                &format!("{action}.g{group_id}"),
                Some(&self.bot_id),
                Some(group_id),
                now,
            )
            .await;
    }
}

/// Production [`OpeningSpeaker`]: the synthetic batch through the ordinary
/// pipeline (see the method body for the mechanics).
pub struct PipelineSpeaker {
    store: Arc<SqliteStore>,
    scheduler: Arc<Scheduler>,
    executor: Arc<TurnExecutor>,
    bot_id: String,
}

impl PipelineSpeaker {
    pub fn new(
        store: Arc<SqliteStore>,
        scheduler: Arc<Scheduler>,
        executor: Arc<TurnExecutor>,
        bot_id: String,
    ) -> Self {
        Self {
            store,
            scheduler,
            executor,
            bot_id,
        }
    }
}

#[async_trait::async_trait]
impl OpeningSpeaker for PipelineSpeaker {
    /// Enqueue the synthetic batch through the ordinary pipeline: one
    /// bot-authored pending event, claimed on the spot, handed to the
    /// scheduler with the topic card attached. Returns its exact batch id;
    /// on refusal the synthetic event is removed so it can never surface
    /// later as an ordinary batch.
    async fn speak(&self, group_id: &str, picked: &PickedTopic, now: i64) -> Option<String> {
        let text = format!("想聊聊：{}", picked.brief.topic);
        let message_id = format!(
            "spontaneous-{}-{:06}",
            now,
            hash64(&format!("{group_id}|{now}")) % 1_000_000
        );
        let message = IncomingMessage {
            bot_id: self.bot_id.clone(),
            group_id: group_id.to_string(),
            message_id: message_id.clone(),
            sender_id: self.bot_id.clone(),
            speaker_ref: "bot".to_string(),
            speaker_role: SpeakerRole::Bot,
            display_name: String::new(),
            known_alias: None,
            received_at: now,
            segments: vec![Segment::Text { text: text.clone() }],
            text: text.clone(),
            mentions_bot: false,
            quote_of: None,
        };
        let Ok(payload) = serde_json::to_string(&message) else {
            return None;
        };
        let batch_id = format!("sp-{}-{group_id}-{message_id}", self.bot_id);
        let Ok(Some(row_id)) = self
            .store
            .insert_spontaneous_pending(
                &self.bot_id,
                group_id,
                &message_id,
                now,
                &payload,
                &batch_id,
            )
            .await
        else {
            return None;
        };
        let job = BatchJob {
            group_id: group_id.to_string(),
            batch_id: batch_id.clone(),
            event_ids: vec![row_id],
            privileged: false,
            chars: text.chars().count(),
            images: 0,
            flooded: false,
            flood_background: None,
            retry: false,
            spontaneous: Some(picked.brief.clone()),
        };
        let accepted = self
            .scheduler
            .enqueue(Arc::clone(&self.executor), job)
            .await;
        if !accepted {
            let _ = self.store.cancel_spontaneous_events(&[row_id]).await;
        }
        accepted.then_some(batch_id)
    }
}

/// Extract and validate the angle model's `{"topic","angle"}` reply.
/// Anything malformed, oversized or carrying a raw identifier is a miss.
/// The first summary loop a spontaneous opening may use: due within the
/// horizon, not covered by a recent opening, and not about a sealed
/// topic ([seal-4]). Pure so the pick rules stay testable without an
/// engine.
fn first_pickable_loop<'a>(
    loops: &'a [crate::memory::compaction::OpenLoop],
    state: &SpontaneousState,
    now: i64,
    sealed: &[crate::store::SealedTopic],
) -> Option<&'a crate::memory::compaction::OpenLoop> {
    loops.iter().find(|l| {
        l.due.map_or(true, |due| due <= now + LOOP_HORIZON)
            && !state.topic_recently_used(&l.text, now)
            && crate::memory::sealed::hit(sealed, &l.slot, &l.text).is_none()
    })
}

fn parse_angle_reply(raw: &str) -> Option<(String, String)> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    let value: serde_json::Value = serde_json::from_str(raw.get(start..=end)?).ok()?;
    let topic = value.get("topic")?.as_str()?.trim().to_string();
    let angle = value
        .get("angle")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if topic.is_empty()
        || topic.chars().count() > 60
        || angle.chars().count() > 80
        || leaks_identifier(&topic)
        || leaks_identifier(&angle)
    {
        return None;
    }
    Some((topic, angle))
}

/// Local hour of day (Asia/Shanghai fixed offset), 0..=23.
fn local_hour(now: i64) -> u32 {
    (((now + LOCAL_OFFSET).rem_euclid(86_400)) / 3_600) as u32
}

/// Local calendar day (Asia/Shanghai fixed offset), `YYYY-MM-DD`.
fn local_day(now: i64) -> String {
    let days = (now + LOCAL_OFFSET).div_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Quiet window membership; wraps midnight when start > end.
fn in_quiet_hours(hour: u32, cfg: &SpontaneousConfig) -> bool {
    let (start, end) = (cfg.quiet_start_hour % 24, cfg.quiet_end_hour % 24);
    if start == end {
        return false;
    }
    if start < end {
        hour >= start && hour < end
    } else {
        hour >= start || hour < end
    }
}

/// The most recent moment the quiet window ended, at or before `now`,
/// on the local clock; 0 when no quiet window is configured. Only the
/// window's end hour matters here, wrap or not: the end recurs daily.
fn last_quiet_end(now: i64, cfg: &SpontaneousConfig) -> i64 {
    let (start, end) = (cfg.quiet_start_hour % 24, cfg.quiet_end_hour % 24);
    if start == end {
        return 0;
    }
    // Local midnight today as a unix instant.
    let day_start = (now + LOCAL_OFFSET).div_euclid(86_400) * 86_400 - LOCAL_OFFSET;
    let mut candidate = day_start + i64::from(end) * 3_600;
    if candidate > now {
        candidate -= 86_400;
    }
    candidate
}

/// The day's silence threshold in minutes: deterministic per group and
/// day inside [min, max], so the opening time is not a fixed daily beat.
fn daily_silence_threshold(group_id: &str, day: &str, cfg: &SpontaneousConfig) -> i64 {
    let (min, max) = (
        cfg.silence_min_minutes,
        cfg.silence_max_minutes.max(cfg.silence_min_minutes),
    );
    if max == min {
        return min;
    }
    let span = (max - min + 1) as u64;
    min + (hash64(&format!("{group_id}|{day}|silence")) % span) as i64
}

/// FNV-1a 64: a stable hash for per-day deterministic picks (the std
/// hasher's random state would re-roll every restart).
fn hash64(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Days-from-civil inverse (Howard Hinnant's algorithm), for `local_day`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests;
