//! Memory compaction: turn a group's reviewed memories into a derived summary.
//!
//! Boundaries that shape this module:
//!
//! * Input is **only** the current group's reviewed `memories` rows. Raw pending
//!   events, chat logs and other groups' data are never read.
//! * The outgoing payload contains no QQ UIN, `group_id`, `message_id`, Agy session
//!   id or other group's content. Subjects appear as group-local aliases.
//! * The model returns a structured summary (schema v2: profile, member facts,
//!   active topics, open loops); Rust validates the JSON envelope, the field caps
//!   and the alias/revision references, and stores the canonical JSON. A bad or
//!   timed out response records an error class and **keeps the previous summary**.
//!   Readers that need prose render it back with [`summary_display_text`].
//! * Scheduling: trigger at >50 memories or >8,000 characters, 24 h cooldown per
//!   group, global concurrency 1, at most 10 runs per day including manual ones.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::ErrorClass;
use crate::memory::extract::json_span;
use crate::store::{CompactionJob, JobState, MemoryRecord, SqliteStore};

/// Trigger thresholds.
#[derive(Debug, Clone, Copy)]
pub struct CompactionThresholds {
    /// Compact when the group holds more than this many memories.
    pub max_memories: i64,
    /// …or more than this many characters across all memory content.
    pub max_chars: i64,
    /// Per-group cooldown between successful runs.
    pub cooldown_seconds: i64,
    /// Global daily cap, manual runs included.
    pub daily_limit: i64,
}

impl Default for CompactionThresholds {
    fn default() -> Self {
        Self {
            max_memories: 50,
            max_chars: 8_000,
            cooldown_seconds: 24 * 60 * 60,
            daily_limit: 10,
        }
    }
}

/// Bounds on the generated summary.
pub const MIN_SUMMARY_CHARS: usize = 20;
pub const MAX_SUMMARY_CHARS: usize = 1_200;

/// One closed memory claim, as a structural tombstone for the prompt.
///
/// Built from the `state` columns (superseded / retired rows), never from
/// content matching: the compactor is told these (alias, slot) claims are
/// closed, and the validator refuses an open loop that re-declares one.
#[derive(Debug, Clone, PartialEq)]
pub struct Tombstone {
    /// Group-local alias of the subject ("群" when the row had no subject).
    pub alias: String,
    /// The closed row's slot (its structural topic key).
    pub slot: String,
    /// The closed row's content, one line, so the model can tell the closed
    /// claim apart from a live one on the same slot.
    pub content: String,
}

/// One compaction request, already scoped to a single group.
#[derive(Debug, Clone)]
pub struct CompactionRequest {
    pub bot_id: String,
    pub group_id: String,
    /// Memories to summarise; already restricted to this group.
    pub memories: Vec<MemoryRecord>,
    /// Current memory revision, recorded as `source_revision`.
    pub source_revision: i64,
    /// Alias mapping used in the prompt so no raw ids leave Rust.
    pub aliases: Vec<(String, String)>,
    /// Closed claims for this group (superseded / retired rows).
    pub tombstones: Vec<Tombstone>,
    /// Topics the operator has sealed for this group ([seal-3]).
    pub sealed_topics: Vec<crate::store::SealedTopic>,
}

/// Why a compaction did not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Below both thresholds.
    BelowThreshold,
    /// Inside the 24 h cooldown.
    Cooldown,
    /// Daily cap reached.
    DailyLimit,
    /// Another group is compacting (global concurrency 1).
    Busy,
    /// No memories at all.
    NoMemories,
    /// The store refused the job.
    Rejected,
}

impl SkipReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SkipReason::BelowThreshold => "below_threshold",
            SkipReason::Cooldown => "cooldown",
            SkipReason::DailyLimit => "daily_limit",
            SkipReason::Busy => "busy",
            SkipReason::NoMemories => "no_memories",
            SkipReason::Rejected => "rejected",
        }
    }
}

/// Result of asking for a compaction.
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionDecision {
    /// A job was created and is queued.
    Queued(CompactionJob),
    /// Not now, with the reason recorded.
    Skipped(SkipReason),
}

/// The model's response, after validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidatedSummary {
    pub summary_text: String,
    pub source_revision: i64,
}

/// Validation failure of a compactor response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryError {
    /// Not JSON, or not the expected object.
    Malformed,
    /// Too short to be a useful summary.
    TooShort,
    /// Exceeded the character budget.
    TooLong,
    /// Referenced a revision other than the one sent.
    BadRevision,
    /// Mentioned something that looks like a raw identifier.
    LeakedIdentifier,
    /// An open loop re-declared a closed (alias, slot) claim.
    Tombstoned,
    /// Some section of the summary is about a sealed topic ([seal-3]).
    Sealed,
}

impl SummaryError {
    pub fn as_str(self) -> &'static str {
        match self {
            SummaryError::Malformed => "malformed",
            SummaryError::TooShort => "too_short",
            SummaryError::TooLong => "too_long",
            SummaryError::BadRevision => "bad_revision",
            SummaryError::LeakedIdentifier => "leaked_identifier",
            SummaryError::Tombstoned => "tombstoned",
            SummaryError::Sealed => "sealed",
        }
    }

    /// Map onto the stable error classes stored on the job row.
    pub fn error_class(self) -> ErrorClass {
        match self {
            SummaryError::Malformed | SummaryError::BadRevision => ErrorClass::AuxBadOutput,
            SummaryError::TooShort
            | SummaryError::TooLong
            | SummaryError::LeakedIdentifier
            | SummaryError::Tombstoned
            | SummaryError::Sealed => ErrorClass::AuxBadOutput,
        }
    }
}

/// Validate the compactor's JSON response.
///
/// `expected_revision` is the revision that was sent; a mismatch means the model
/// answered for a different snapshot and the result must not overwrite the summary.
pub fn validate_summary(
    raw: &str,
    expected_revision: i64,
) -> Result<ValidatedSummary, SummaryError> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| SummaryError::Malformed)?;
    let obj = value.as_object().ok_or(SummaryError::Malformed)?;

    let text = obj
        .get("summary")
        .or_else(|| obj.get("summary_text"))
        .and_then(|v| v.as_str())
        .ok_or(SummaryError::Malformed)?
        .trim()
        .to_string();
    if text.is_empty() {
        return Err(SummaryError::Malformed);
    }

    let revision = match obj.get("source_revision") {
        None => expected_revision,
        Some(v) => v.as_i64().ok_or(SummaryError::Malformed)?,
    };
    if revision != expected_revision {
        return Err(SummaryError::BadRevision);
    }

    let chars = text.chars().count();
    if chars < MIN_SUMMARY_CHARS {
        return Err(SummaryError::TooShort);
    }
    if chars > MAX_SUMMARY_CHARS {
        return Err(SummaryError::TooLong);
    }
    if leaks_identifier(&text) {
        return Err(SummaryError::LeakedIdentifier);
    }
    Ok(ValidatedSummary {
        summary_text: text,
        source_revision: revision,
    })
}

/// Conservative detector for raw identifiers or cross-boundary references that must
/// never appear in a derived summary: bare long digit runs (QQ numbers, message ids),
/// explicit id keys, and CQ codes.
pub fn leaks_identifier(text: &str) -> bool {
    if text.contains("[CQ:") || text.contains("group_id") || text.contains("message_id") {
        return true;
    }
    // A run of 5+ digits is never a legitimate part of a summary sentence.
    let mut run = 0usize;
    for ch in text.chars() {
        if ch.is_ascii_digit() {
            run += 1;
            if run >= 5 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

/// Render the compactor prompt. Contains only aliased memory content of one group.
///
/// The answer is a schema v1 structured summary (see [`StructuredSummary`]): the
/// free-text summary it replaces could not be consumed by anything but a human
/// reader, while the structured form feeds the spontaneous-topic picker and the
/// seed brief renderer.
pub fn render_prompt(request: &CompactionRequest) -> String {
    let mut out = String::with_capacity(2_048);
    out.push_str(
        "你在为一个聊天群维护一份结构化记忆摘要。下面只给出该群已审核的记忆条目，\
每条带一个群内别名。请把它们压缩成一份 JSON 结构化摘要。\n\n要求：\n",
    );
    out.push_str("1. 只使用给出的信息，不得推测、不得编造新的成员或事实。\n");
    out.push_str(
        "2. 不得出现任何数字编号、身份证号、聊天记录原文或外部链接；正文里不要出现条目键。\n",
    );
    out.push_str("3. 不得提及别的群、别的会话或系统配置。\n");
    out.push_str(
        "4. 只输出这一个 JSON 对象，不要 Markdown：\
{\"v\":2,\"group_profile\":\"<群画像，≤200字>\",\
\"members\":[{\"alias\":\"u1\",\"facts\":[\"<要点，≤60字，最多3条>\"]}],\
\"active_topics\":[{\"topic\":\"<话题>\",\"heat\":0.0到1.0,\"since\":<起始的大致时间戳，拿不准填0>}],\
\"open_loops\":[{\"text\":\"<未完结的约定/悬念，≤80字>\",\"due\":<截止时间戳或null>,\"alias\":\"<来源条目的别名，无则空字符串>\",\"slot\":\"<来源条目的主题，无则空字符串>\"}],\
\"source_revision\":N}\n",
    );
    out.push_str("5. members 最多 8 人（只列有实质要点的），active_topics 最多 5 条，open_loops 最多 5 条；没有的段落给空数组。\n");
    out.push_str("6. open_loops 每条都要从来源条目原样抄下 alias 与 slot（条目行方括号里的就是）；来源条目没有主题就把 slot 留空。若下方给出已结案清单，清单里的（别名，主题）不得再出现在 open_loops 里，也不得在任何段落里写成现行事实。\n");
    out.push_str("7. 标为 [bot] 的条目是这个机器人本人的记忆：它在群里的自称与别称指的都是它自己，members 里它只能以 alias \"bot\" 出现一次，不许把它的别称另立为成员，也不许把它的事记到别人名下。条目里出现的主人、管理员要按来源分辨：[bot] 条目里的主人是机器人本人的主人；其他条目里指代不明的，不要当断言照抄——写出能确定的具体人，确定不了就略去这一句。\n\n");
    out.push_str(&format!(
        "source_revision = {}\n\n",
        request.source_revision
    ));
    out.push_str("记忆条目：\n");
    for (index, memory) in request.memories.iter().enumerate() {
        let subject = memory
            .subject_user_id
            .as_ref()
            .and_then(|uid| {
                request
                    .aliases
                    .iter()
                    .find(|(user_id, _)| user_id == uid)
                    .map(|(_, alias)| alias.clone())
            })
            .unwrap_or_else(|| "群".to_string());
        let slot = if memory.slot.trim().is_empty() {
            String::new()
        } else {
            format!(" [{}]", memory.slot.trim())
        };
        out.push_str(&format!(
            "{}. [{}] ({}){} {}\n",
            index + 1,
            subject,
            memory.kind,
            slot,
            memory.content.trim()
        ));
    }
    if !request.tombstones.is_empty() {
        out.push_str("\n已结案清单（下列断言已被更正或作废，不得再写成现行事实或未完结事项）：\n");
        // The validator sees every tombstone; the prompt shows only the
        // freshest 50 (rows arrive newest-first) to bound its size.
        for stone in request.tombstones.iter().take(50) {
            if stone.slot.is_empty() {
                out.push_str(&format!("- [{}] {}\n", stone.alias, stone.content.trim()));
            } else {
                out.push_str(&format!(
                    "- [{}] [{}] {}\n",
                    stone.alias,
                    stone.slot,
                    stone.content.trim()
                ));
            }
        }
    }
    if !request.sealed_topics.is_empty() {
        out.push_str("\n封存话题（管理员已明确封存，摘要的任何段落都不得出现这些话题的内容）：\n");
        for topic in &request.sealed_topics {
            let mut line = String::from("- ");
            if !topic.keywords.is_empty() {
                line.push_str(&format!("关键词：{}", topic.keywords.join("、")));
            }
            if !topic.slot.is_empty() {
                if !topic.keywords.is_empty() {
                    line.push_str("；");
                }
                line.push_str(&format!("主题槽：{}", topic.slot));
            }
            line.push('\n');
            out.push_str(&line);
        }
    }
    out
}

/// One member's distilled facts inside a [`StructuredSummary`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberFacts {
    pub alias: String,
    pub facts: Vec<String>,
}

/// One live topic inside a [`StructuredSummary`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveTopic {
    pub topic: String,
    /// 0..=1, how hot the topic currently is.
    pub heat: f64,
    /// Epoch seconds when the topic started (0 when unknown).
    pub since: i64,
}

/// One dangling promise or open question inside a [`StructuredSummary`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenLoop {
    pub text: String,
    /// Epoch deadline, when the loop has one.
    pub due: Option<i64>,
    /// Alias of the member the loop belongs to, copied from the source
    /// memory line (schema v2; empty when untagged or group-wide).
    #[serde(default)]
    pub alias: String,
    /// Slot of the source memory the loop was distilled from (schema v2).
    /// Together with `alias` this is the structural key the tombstone
    /// check matches on.
    #[serde(default)]
    pub slot: String,
}

/// A group's structured summary, schema v1: what compaction stores in
/// `group_memory_summaries.summary_text` as canonical JSON. Readers that need
/// prose go through [`summary_display_text`]; the spontaneous-topic picker reads
/// this structure directly. Pre-v1 rows hold free text, which every reader
/// treats as an opaque profile paragraph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuredSummary {
    pub v: u32,
    pub group_profile: String,
    #[serde(default)]
    pub members: Vec<MemberFacts>,
    #[serde(default)]
    pub active_topics: Vec<ActiveTopic>,
    #[serde(default)]
    pub open_loops: Vec<OpenLoop>,
    pub source_revision: i64,
}

/// Field caps for a structured summary (schema v1).
pub const MAX_PROFILE_CHARS: usize = 200;
pub const MAX_SUMMARY_MEMBERS: usize = 8;
pub const MAX_MEMBER_FACT_CHARS: usize = 60;
pub const MAX_MEMBER_FACTS: usize = 3;
pub const MAX_SUMMARY_TOPICS: usize = 5;
pub const MAX_OPEN_LOOPS: usize = 5;
pub const MAX_OPEN_LOOP_CHARS: usize = 80;

/// Parse a stored summary back into its structured form; `None` for a legacy
/// free-text row (or a garbled one, which readers treat the same way).
pub fn parse_structured_summary(stored: &str) -> Option<StructuredSummary> {
    let parsed: StructuredSummary = serde_json::from_str(stored).ok()?;
    (parsed.v == 1 || parsed.v == 2).then_some(parsed)
}

/// Render a stored summary as prompt prose: structured rows become profile +
/// member/topic/loop lines, legacy rows pass through untouched.
pub fn summary_display_text(stored: &str) -> String {
    let Some(summary) = parse_structured_summary(stored) else {
        return stored.to_string();
    };
    let mut out = summary.group_profile.clone();
    for member in &summary.members {
        if member.facts.is_empty() {
            continue;
        }
        out.push_str(&format!("\n{}：{}", member.alias, member.facts.join("；")));
    }
    if !summary.active_topics.is_empty() {
        let topics: Vec<&str> = summary
            .active_topics
            .iter()
            .map(|t| t.topic.as_str())
            .collect();
        out.push_str(&format!("\n近期话题：{}", topics.join("、")));
    }
    if !summary.open_loops.is_empty() {
        let loops: Vec<&str> = summary.open_loops.iter().map(|l| l.text.as_str()).collect();
        out.push_str(&format!("\n未完结：{}", loops.join("；")));
    }
    out
}

/// Validate a structured-summary response (schema v1 or v2) and canonicalise
/// it for storage as v2.
///
/// Strict by design (the fold philosophy: bad output keeps the old summary):
/// unknown aliases, over-cap fields and identifier leaks all refuse the whole
/// payload.
pub fn validate_summary_v2(
    raw: &str,
    expected_revision: i64,
    valid_aliases: &[String],
    tombstones: &[Tombstone],
    sealed: &[crate::store::SealedTopic],
) -> Result<ValidatedSummary, SummaryError> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| SummaryError::Malformed)?;
    let obj = value.as_object().ok_or(SummaryError::Malformed)?;
    if !matches!(obj.get("v").and_then(|v| v.as_i64()), Some(1) | Some(2)) {
        return Err(SummaryError::Malformed);
    }
    let revision = match obj.get("source_revision") {
        None => expected_revision,
        Some(v) => v.as_i64().ok_or(SummaryError::Malformed)?,
    };
    if revision != expected_revision {
        return Err(SummaryError::BadRevision);
    }
    let mut summary: StructuredSummary =
        serde_json::from_value(value).map_err(|_| SummaryError::Malformed)?;
    summary.source_revision = expected_revision;
    // Stored summaries are canonical schema v2 from memfix [mf-2] on.
    summary.v = 2;

    let profile = summary.group_profile.trim();
    if profile.is_empty() || profile.chars().count() > MAX_PROFILE_CHARS {
        return Err(SummaryError::TooLong);
    }
    if leaks_identifier(profile) {
        return Err(SummaryError::LeakedIdentifier);
    }
    // Sealed topics ([seal-3]): the operator's list binds every section,
    // not just loops — member facts were the tombstone check's blind
    // spot, and a sealed topic living on as a "fact" is the same
    // resurrection with a different label.
    if crate::memory::sealed::hit(sealed, "", profile).is_some() {
        return Err(SummaryError::Sealed);
    }
    if summary.members.len() > MAX_SUMMARY_MEMBERS
        || summary.active_topics.len() > MAX_SUMMARY_TOPICS
        || summary.open_loops.len() > MAX_OPEN_LOOPS
    {
        return Err(SummaryError::TooLong);
    }
    for member in &summary.members {
        if !valid_aliases.iter().any(|a| a == &member.alias) && member.alias != "bot" {
            return Err(SummaryError::Malformed);
        }
        if member.facts.len() > MAX_MEMBER_FACTS {
            return Err(SummaryError::TooLong);
        }
        for fact in &member.facts {
            let fact = fact.trim();
            if fact.is_empty() || fact.chars().count() > MAX_MEMBER_FACT_CHARS {
                return Err(SummaryError::TooLong);
            }
            if leaks_identifier(fact) {
                return Err(SummaryError::LeakedIdentifier);
            }
            if crate::memory::sealed::hit(sealed, "", fact).is_some() {
                return Err(SummaryError::Sealed);
            }
        }
    }
    for topic in &summary.active_topics {
        if topic.topic.trim().is_empty() || topic.topic.chars().count() > MAX_OPEN_LOOP_CHARS {
            return Err(SummaryError::TooLong);
        }
        if !topic.heat.is_finite() || !(0.0..=1.0).contains(&topic.heat) {
            return Err(SummaryError::Malformed);
        }
        if leaks_identifier(&topic.topic) {
            return Err(SummaryError::LeakedIdentifier);
        }
        if crate::memory::sealed::hit(sealed, "", &topic.topic).is_some() {
            return Err(SummaryError::Sealed);
        }
    }
    for open_loop in &summary.open_loops {
        let text = open_loop.text.trim();
        if text.is_empty() || text.chars().count() > MAX_OPEN_LOOP_CHARS {
            return Err(SummaryError::TooLong);
        }
        if leaks_identifier(text) {
            return Err(SummaryError::LeakedIdentifier);
        }
        if crate::memory::sealed::hit(sealed, open_loop.slot.trim(), text).is_some() {
            return Err(SummaryError::Sealed);
        }
        // Structural resurrection guard (memfix [mf-2], extended by
        // [opt-3]): a loop that tags itself with a closed (alias, slot)
        // pair is refused wholesale. A loop that names an alias but omits
        // the slot is refused too when that alias has any closed row —
        // requirement 6 already obliges the model to copy the source
        // entry's slot, and an untagged loop over a closed claim is how
        // a settled matter resurrects (2026-11-12 audit: the exemption
        // was on file while the summary's loop kept re-asking for nine
        // days). Deliberately no text matching: loop texts are short
        // imperatives and closed rows are statements, and their bigram
        // overlap bands for resurrections vs legitimate follow-ups
        // overlap, so no threshold separates them.
        if loop_resurrects_closed(open_loop, tombstones) {
            return Err(SummaryError::Tombstoned);
        }
    }
    let canonical = serde_json::to_string(&summary).map_err(|_| SummaryError::Malformed)?;
    Ok(ValidatedSummary {
        summary_text: canonical,
        source_revision: expected_revision,
    })
}

/// Whether one open loop re-declares a closed claim: it carries an alias
/// that has closed rows and either omits the slot or names one of the
/// closed slots. The validator's resurrection guard and the runner's
/// salvage share this one predicate so they can never disagree.
fn loop_resurrects_closed(open_loop: &OpenLoop, tombstones: &[Tombstone]) -> bool {
    let alias = open_loop.alias.trim();
    if alias.is_empty() {
        return false;
    }
    let slot = open_loop.slot.trim();
    let alias_stones: Vec<&Tombstone> = tombstones.iter().filter(|t| t.alias == alias).collect();
    !alias_stones.is_empty() && (slot.is_empty() || alias_stones.iter().any(|t| t.slot == slot))
}

/// Remove every open loop that re-declares a closed claim, returning the
/// dropped (alias, slot) pairs. The runner's salvage uses it: the rest of
/// the summary validated, so forgetting the resurrected loops (this
/// module's failure direction) beats discarding the whole compaction.
fn drop_tombstoned_loops(
    summary: &mut StructuredSummary,
    tombstones: &[Tombstone],
) -> Vec<(String, String)> {
    let mut dropped = Vec::new();
    summary.open_loops.retain(|open_loop| {
        if loop_resurrects_closed(open_loop, tombstones) {
            dropped.push((
                open_loop.alias.trim().to_string(),
                open_loop.slot.trim().to_string(),
            ));
            false
        } else {
            true
        }
    });
    dropped
}

/// Resolve tombstone rows to prompt entries: subjects reuse the request's
/// alias table, uncovered subjects continue the same `uN` sequence, the bot
/// maps to `bot`, and subject-less rows render as the whole group. Rows
/// without a slot are kept with an empty slot ([opt-3]): they cannot key
/// the pair check, but they belong on the closure list the model reads,
/// and they arm the validator's slot-less-loop rule for their alias.
fn tombstone_entries(
    rows: Vec<MemoryRecord>,
    aliases: &[(String, String)],
    bot_id: &str,
) -> Vec<Tombstone> {
    let mut table: Vec<(String, String)> = aliases.to_vec();
    let mut out = Vec::new();
    for row in rows {
        let alias = match &row.subject_user_id {
            None => "群".to_string(),
            Some(uid) if uid == bot_id => "bot".to_string(),
            Some(uid) => match table.iter().find(|(id, _)| id == uid) {
                Some((_, a)) => a.clone(),
                None => {
                    let mut next = 1usize;
                    let alias = loop {
                        let candidate = format!("u{next}");
                        next += 1;
                        if !table.iter().any(|(_, a)| a == &candidate) {
                            break candidate;
                        }
                    };
                    table.push((uid.clone(), alias.clone()));
                    alias
                }
            },
        };
        out.push(Tombstone {
            alias,
            slot: row.slot.trim().to_string(),
            content: row.content.clone(),
        });
    }
    out
}

/// Fill in aliases for memory subjects the caller's table does not cover.
///
/// The persistent speaker-alias table only holds operator-assigned nicknames
/// and is empty for most groups; without a mapping, the prompt renders every
/// subject as the whole group while the reply schema still invites member
/// entries, and the strict validator then rejects whatever alias the model
/// invents (production: every v2 compaction job failed `aux_bad_output`).
/// Uncovered subjects get stable `uN` aliases in first-appearance order —
/// the same scheme the fold prompt uses — and the bot itself maps to `bot`,
/// which the validator already accepts.
fn effective_aliases(
    provided: Vec<(String, String)>,
    memories: &[MemoryRecord],
    bot_id: &str,
) -> Vec<(String, String)> {
    let mut out = provided;
    let mut next = 1usize;
    for memory in memories {
        let Some(uid) = &memory.subject_user_id else {
            continue;
        };
        if out.iter().any(|(id, _)| id == uid) {
            continue;
        }
        let alias = if uid == bot_id {
            "bot".to_string()
        } else {
            loop {
                let candidate = format!("u{next}");
                next += 1;
                if !out.iter().any(|(_, a)| a == &candidate) {
                    break candidate;
                }
            }
        };
        out.push((uid.clone(), alias));
    }
    out
}

/// The compactor's model interface. The real implementation is the Aux adapter;
/// tests use a fake.
#[async_trait::async_trait]
pub trait SummaryModel: Send + Sync + 'static {
    /// Return the raw JSON string produced by the model.
    async fn summarise(&self, prompt: String) -> Result<String, ErrorClass>;
}

/// Scheduler that decides when a compaction may run.
pub struct CompactionScheduler {
    store: Arc<SqliteStore>,
    thresholds: CompactionThresholds,
    /// Guards the global "concurrency 1" rule.
    busy: Arc<tokio::sync::Mutex<()>>,
    /// Model label recorded on stored summaries. Defaults to the aux
    /// gateway model (the historical producer); the app overrides it with
    /// the agy model now that summaries run on the dedicated memory
    /// session (memfix [mf-2]).
    summary_model_label: String,
}

impl std::fmt::Debug for CompactionScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionScheduler")
            .field("thresholds", &self.thresholds)
            .finish_non_exhaustive()
    }
}

impl CompactionScheduler {
    pub fn new(store: Arc<SqliteStore>, thresholds: CompactionThresholds) -> Self {
        Self {
            store,
            thresholds,
            busy: Arc::new(tokio::sync::Mutex::new(())),
            summary_model_label: crate::config::DEFAULT_AGY_MODEL.to_string(),
        }
    }

    /// Override the model label recorded on stored summaries.
    pub fn with_summary_model_label(mut self, label: String) -> Self {
        self.summary_model_label = label;
        self
    }

    /// Whether the group currently exceeds a threshold.
    pub async fn above_threshold(&self, bot_id: &str, group_id: &str) -> bool {
        match self.store.memory_stats(bot_id, group_id).await {
            Ok((count, chars)) => {
                count > self.thresholds.max_memories || chars > self.thresholds.max_chars
            }
            Err(_) => false,
        }
    }

    /// Decide whether a compaction may be queued right now, and queue it if so.
    ///
    /// `requested_by` is `owner` for the QQ command path and `panel` for the admin
    /// page; manual runs count against the daily limit exactly like automatic ones.
    pub async fn maybe_queue(
        &self,
        bot_id: &str,
        group_id: &str,
        requested_by: crate::store::ActorKind,
        now: i64,
        force: bool,
    ) -> CompactionDecision {
        let (count, _chars) = match self.store.memory_stats(bot_id, group_id).await {
            Ok(stats) => stats,
            Err(_) => return CompactionDecision::Skipped(SkipReason::Rejected),
        };
        if count == 0 {
            return CompactionDecision::Skipped(SkipReason::NoMemories);
        }
        if !force && !self.above_threshold(bot_id, group_id).await {
            return CompactionDecision::Skipped(SkipReason::BelowThreshold);
        }
        // Per-group 24 h cooldown.
        match self.store.last_success_at(bot_id, group_id).await {
            Ok(Some(last)) if now - last < self.thresholds.cooldown_seconds && !force => {
                return CompactionDecision::Skipped(SkipReason::Cooldown);
            }
            Ok(_) => {}
            Err(_) => return CompactionDecision::Skipped(SkipReason::Rejected),
        }
        // Global daily cap, manual runs included.
        let day_start = now - (now % 86_400);
        match self.store.jobs_since(day_start).await {
            Ok(n) if n >= self.thresholds.daily_limit => {
                return CompactionDecision::Skipped(SkipReason::DailyLimit);
            }
            Ok(_) => {}
            Err(_) => return CompactionDecision::Skipped(SkipReason::Rejected),
        }
        let revision = match self.store.memory_revision(bot_id, group_id).await {
            Ok(r) => r,
            Err(_) => return CompactionDecision::Skipped(SkipReason::Rejected),
        };
        match self
            .store
            .create_job(bot_id, group_id, revision, requested_by.as_str(), now)
            .await
        {
            Ok(job) => CompactionDecision::Queued(job),
            Err(_) => CompactionDecision::Skipped(SkipReason::Rejected),
        }
    }

    /// Try to take the single global execution slot.
    pub fn try_lock(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        Arc::clone(&self.busy).try_lock_owned().ok()
    }

    /// Run one job end to end.
    ///
    /// On success the summary is written and the job marked `succeeded`; on any error
    /// the previous summary is left untouched and the job records the error class.
    pub async fn run_job(
        &self,
        job: &CompactionJob,
        model: Arc<dyn SummaryModel>,
        aliases: Vec<(String, String)>,
        now: i64,
    ) -> Result<ValidatedSummary, SummaryError> {
        let memories = match self
            .store
            .list_memories(&job.bot_id, &job.group_id, 500)
            .await
        {
            Ok(m) => m,
            Err(_) => {
                let _ = self
                    .store
                    .finish_job(&job.id, JobState::Failed, Some("store"), now)
                    .await;
                return Err(SummaryError::Malformed);
            }
        };
        let aliases = effective_aliases(aliases, &memories, &job.bot_id);
        // Fetch 200, not 50: the validator needs the full closure window
        // (one night of manual retirements alone can pass 50 rows), while
        // the prompt renders only the freshest 50 of them.
        let tombstone_rows = self
            .store
            .list_memory_tombstones(&job.bot_id, &job.group_id, 200)
            .await
            .unwrap_or_default();
        let tombstones = tombstone_entries(tombstone_rows, &aliases, &job.bot_id);
        let sealed_topics = self
            .store
            .list_sealed_topics(&job.bot_id, &job.group_id)
            .await
            .unwrap_or_default();
        let request = CompactionRequest {
            bot_id: job.bot_id.clone(),
            group_id: job.group_id.clone(),
            memories,
            source_revision: job.source_revision,
            aliases,
            tombstones,
            sealed_topics,
        };
        let valid_aliases: Vec<String> = request
            .aliases
            .iter()
            .map(|(_, alias)| alias.clone())
            .collect();
        let prompt = render_prompt(&request);

        let _ = self.store.mark_job_running(&job.id, now).await;
        let raw = match model.summarise(prompt).await {
            Ok(raw) => raw,
            Err(class) => {
                let _ = self
                    .store
                    .finish_job(&job.id, JobState::Failed, Some(class.as_str()), now)
                    .await;
                return Err(SummaryError::Malformed);
            }
        };
        // A v1-shaped answer goes through the strict structured validator; the
        // legacy free-text shape stays readable for one release so a mixed
        // fleet never hard-fails a job over format alone.
        //
        // The reply is sliced to its JSON object first (the shared
        // extract::json_span, same as consolidation and digest): the model
        // fences or wraps its JSON often enough that judging the wrapper
        // instead of the content failed every production job from
        // 2026-11-19 to 2026-11-22 while the objects inside were valid.
        let candidate = json_span(&raw).unwrap_or(raw.trim());
        let structured = serde_json::from_str::<serde_json::Value>(candidate)
            .ok()
            .and_then(|v| {
                v.as_object()
                    .map(|obj| obj.contains_key("group_profile") || obj.contains_key("members"))
            })
            .unwrap_or(false);
        let mut outcome = if structured {
            validate_summary_v2(
                candidate,
                job.source_revision,
                &valid_aliases,
                &request.tombstones,
                &request.sealed_topics,
            )
        } else {
            validate_summary(candidate, job.source_revision)
        };
        // Salvage a tombstone refusal (2026-11-24): the model keeps
        // re-declaring a group's long-closed loops, and one resurrected
        // loop used to sink an otherwise valid summary — one group sat
        // uncompacted for days on exactly this. Drop just the offending
        // loops and judge the rest; the validator itself stays strict,
        // and the drop is logged below with the pairs it removed.
        if structured && matches!(outcome, Err(SummaryError::Tombstoned)) {
            if let Ok(mut summary) = serde_json::from_str::<StructuredSummary>(candidate) {
                let dropped = drop_tombstoned_loops(&mut summary, &request.tombstones);
                if !dropped.is_empty() {
                    if let Ok(resliced) = serde_json::to_string(&summary) {
                        let retry = validate_summary_v2(
                            &resliced,
                            job.source_revision,
                            &valid_aliases,
                            &request.tombstones,
                            &request.sealed_topics,
                        );
                        if retry.is_ok() {
                            tracing::warn!(
                                group_id = job.group_id,
                                dropped = ?dropped,
                                "记忆压缩剔除已结案 loop 后通过校验"
                            );
                        }
                        outcome = retry;
                    }
                }
            }
        }
        match outcome {
            Ok(validated) => {
                let _ = self
                    .store
                    .put_summary(
                        &job.bot_id,
                        &job.group_id,
                        &validated.summary_text,
                        validated.source_revision,
                        &self.summary_model_label,
                        now,
                    )
                    .await;
                let _ = self
                    .store
                    .finish_job(&job.id, JobState::Succeeded, None, now)
                    .await;
                Ok(validated)
            }
            Err(e) => {
                // Record the specific validation failure, not just its
                // error class: every SummaryError maps to AuxBadOutput, so
                // the class alone cannot tell a tombstone refusal from a
                // truncated reply (2026-11-11: a production job failed
                // undiagnosably for exactly this reason). The received
                // text's shape goes to the log for the same reason: the
                // session DB only keeps the model's final text, so when
                // the client settled early this excerpt is the only
                // surviving evidence of what was actually validated.
                // The full text is logged too (2026-11-23): head/tail
                // excerpts alone left the 2026-11-19..22 failure streak
                // impossible to pin to the failing field.
                tracing::warn!(
                    group_id = job.group_id,
                    error = e.as_str(),
                    raw_len = raw.len(),
                    raw_head = raw.chars().take(80).collect::<String>(),
                    raw_tail = raw
                        .chars()
                        .skip(raw.chars().count().saturating_sub(40))
                        .collect::<String>(),
                    raw_full = raw.as_str(),
                    "记忆压缩回复未过校验"
                );
                let _ = self
                    .store
                    .finish_job(&job.id, JobState::Failed, Some(e.as_str()), now)
                    .await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> CompactionRequest {
        CompactionRequest {
            bot_id: "1".into(),
            group_id: "100".into(),
            memories: vec![
                MemoryRecord {
                    memory_ref: "mem_a".into(),
                    bot_id: "1".into(),
                    group_id: "100".into(),
                    subject_user_id: Some("555".into()),
                    content: "喜欢手冲咖啡".into(),
                    kind: "preference".into(),
                    source_message_ids: "[]".into(),
                    confidence: 0.9,
                    revision: 1,
                    origin: "agy".into(),
                    archived_at: None,
                    created_at: 10,
                    updated_at: 10,
                    slot: String::new(),
                    tags: Vec::new(),
                    expires_at: None,
                    state: "active".to_string(),
                    superseded_by: None,
                    state_changed_at: None,
                },
                MemoryRecord {
                    memory_ref: "mem_b".into(),
                    bot_id: "1".into(),
                    group_id: "100".into(),
                    subject_user_id: None,
                    content: "群里有三个人常聊茶叶".into(),
                    kind: "topic".into(),
                    source_message_ids: "[]".into(),
                    confidence: 0.7,
                    revision: 1,
                    origin: "agy".into(),
                    archived_at: None,
                    created_at: 10,
                    updated_at: 10,
                    slot: String::new(),
                    tags: Vec::new(),
                    expires_at: None,
                    state: "active".to_string(),
                    superseded_by: None,
                    state_changed_at: None,
                },
            ],
            source_revision: 7,
            aliases: vec![("555".into(), "u1".into())],
            tombstones: vec![],
            sealed_topics: vec![],
        }
    }

    fn bare_record(subject: Option<&str>, content: &str) -> MemoryRecord {
        MemoryRecord {
            memory_ref: "mem_x".into(),
            bot_id: "1".into(),
            group_id: "100".into(),
            subject_user_id: subject.map(|v| v.to_string()),
            content: content.into(),
            kind: "fact".into(),
            source_message_ids: "[]".into(),
            confidence: 0.8,
            revision: 1,
            origin: "hy3".into(),
            archived_at: None,
            created_at: 10,
            updated_at: 10,
            slot: String::new(),
            tags: Vec::new(),
            expires_at: None,
            state: "active".to_string(),
            superseded_by: None,
            state_changed_at: None,
        }
    }

    #[test]
    fn effective_aliases_fill_uncovered_subjects_in_first_appearance_order() {
        let memories = vec![
            bare_record(Some("555"), "甲的事实"),
            bare_record(Some("777"), "乙的事实"),
            bare_record(Some("555"), "甲的另一条"),
            bare_record(None, "全群事实"),
            bare_record(Some("1"), "机器人自己的事实"),
        ];
        let out = effective_aliases(vec![("555".into(), "阿猫".into())], &memories, "1");
        assert_eq!(
            out,
            vec![
                ("555".to_string(), "阿猫".to_string()),
                ("777".to_string(), "u1".to_string()),
                ("1".to_string(), "bot".to_string()),
            ]
        );
        // With an empty provided table every subject is derived.
        let out = effective_aliases(Vec::new(), &memories, "1");
        assert_eq!(out[0], ("555".to_string(), "u1".to_string()));
        assert_eq!(out[1], ("777".to_string(), "u2".to_string()));
        assert_eq!(out[2], ("1".to_string(), "bot".to_string()));
    }

    #[test]
    fn derived_aliases_make_member_entries_validatable() {
        // Regression for the production failure: with no speaker aliases at
        // all, a reply whose members use the derived uN aliases must pass.
        let memories = vec![bare_record(Some("555"), "喜欢手冲咖啡")];
        let aliases = effective_aliases(Vec::new(), &memories, "1");
        let valid: Vec<String> = aliases.iter().map(|(_, a)| a.clone()).collect();
        let raw = r#"{"v":1,"group_profile":"咖啡爱好者小群","members":[{"alias":"u1","facts":["喜欢手冲咖啡"]}],"active_topics":[],"open_loops":[],"source_revision":7}"#;
        assert!(validate_summary_v2(raw, 7, &valid, &[], &[]).is_ok());
    }

    #[test]
    fn fenced_summary_validates_after_slicing() {
        // Production replies arrive fenced (2026-11-19..22: jobs failed
        // `malformed` on the wrapper alone). run_job slices the reply
        // with json_span before validating; pin that the sliced forms of
        // a valid reply pass the strict validator.
        let inner = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u1","facts":["爱喝手冲咖啡"]}],"active_topics":[],"open_loops":[],"source_revision":7}"#;
        for raw in [format!("```json\n{inner}\n```"), format!("{inner}\n```")] {
            let candidate = json_span(&raw).expect("应切出对象");
            assert!(
                validate_summary_v2(candidate, 7, &["u1".to_string()], &[], &[]).is_ok(),
                "fenced reply should validate after slicing: {raw}"
            );
        }
    }

    #[test]
    fn salvage_drops_only_tombstoned_loops_and_revalidates() {
        // the production stall: a valid summary whose open loops re-declare
        // closed claims (one by exact slot, one by omitting it). The
        // strict validator refuses the whole reply; the runner's salvage
        // drops exactly those loops and the remainder must pass.
        let stones = [Tombstone {
            alias: "群".to_string(),
            slot: "刷本计划".to_string(),
            content: "每周六刷金狮子（已取消）".to_string(),
        }];
        let raw = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u1","facts":["爱喝手冲咖啡"]}],"active_topics":[],"open_loops":[{"text":"等50级带人刷本","due":null,"alias":"群","slot":"刷本计划"},{"text":"那件事后来怎样了","due":null,"alias":"群","slot":""},{"text":"周五拼单买豆子","due":null,"alias":"u1","slot":"拼单"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(raw, 7, &["u1".to_string()], &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
        let mut summary: StructuredSummary = serde_json::from_str(raw).expect("应可解析");
        let dropped = drop_tombstoned_loops(&mut summary, &stones);
        assert_eq!(
            dropped,
            vec![
                ("群".to_string(), "刷本计划".to_string()),
                ("群".to_string(), "".to_string()),
            ]
        );
        assert_eq!(summary.open_loops.len(), 1);
        assert_eq!(summary.open_loops[0].slot, "拼单");
        let resliced = serde_json::to_string(&summary).expect("应可序列化");
        assert!(validate_summary_v2(&resliced, 7, &["u1".to_string()], &stones, &[]).is_ok());
    }

    #[test]
    fn thresholds_match_the_design() {
        let t = CompactionThresholds::default();
        assert_eq!(t.max_memories, 50);
        assert_eq!(t.max_chars, 8_000);
        assert_eq!(t.cooldown_seconds, 86_400);
        assert_eq!(t.daily_limit, 10);
    }

    #[test]
    fn prompt_uses_aliases_and_no_raw_ids() {
        let prompt = render_prompt(&request());
        assert!(prompt.contains("[u1] (preference) 喜欢手冲咖啡"));
        assert!(prompt.contains("[群] (topic)"));
        // The raw user id must not appear.
        assert!(!prompt.contains("555"));
        assert!(!prompt.contains("mem_a"));
        assert!(prompt.contains("source_revision = 7"));
        assert!(!leaks_identifier(&prompt) || prompt.contains("source_revision"));
    }

    /// [opt-2] The prompt must carry the identity discipline the 2026-11-12
    /// audit found missing: [bot] is the robot itself (its nicknames are
    /// not another member), and 主人/管理员 must be resolved per source.
    #[test]
    fn prompt_carries_bot_identity_and_authority_rules() {
        let prompt = render_prompt(&request());
        assert!(prompt.contains("自称与别称指的都是它自己"), "{prompt}");
        assert!(prompt.contains("只能以 alias \"bot\" 出现一次"), "{prompt}");
        assert!(
            prompt.contains("[bot] 条目里的主人是机器人本人的主人"),
            "{prompt}"
        );
    }

    #[test]
    fn structured_summary_validates_and_canonicalises() {
        let raw = r#"{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u1","facts":["爱喝手冲咖啡"]}],"active_topics":[{"topic":"咖啡","heat":0.8,"since":0}],"open_loops":[{"text":"周五一起打本","due":null}],"source_revision":7}"#;
        let v = validate_summary_v2(raw, 7, &["u1".to_string()], &[], &[]).expect("应通过");
        let parsed = parse_structured_summary(&v.summary_text).expect("存入的应是结构化 JSON");
        assert_eq!(parsed.members[0].alias, "u1");
        assert_eq!(parsed.open_loops[0].text, "周五一起打本");
        let display = summary_display_text(&v.summary_text);
        assert!(display.contains("本群常聊咖啡"));
        assert!(display.contains("u1：爱喝手冲咖啡"));
        // Legacy rows render as themselves.
        assert_eq!(summary_display_text("旧的自由文本摘要"), "旧的自由文本摘要");
    }

    #[test]
    fn structured_summary_rejects_bad_shapes() {
        let aliases = ["u1".to_string()];
        let unknown_alias = r#"{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u9","facts":["x"]}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(unknown_alias, 7, &aliases, &[], &[]).unwrap_err(),
            SummaryError::Malformed
        );
        let bad_heat = r#"{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","active_topics":[{"topic":"咖啡","heat":1.5,"since":0}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(bad_heat, 7, &aliases, &[], &[]).unwrap_err(),
            SummaryError::Malformed
        );
        let long_fact = format!(
            r#"{{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{{"alias":"u1","facts":["{}"]}}],"source_revision":7}}"#,
            "长".repeat(MAX_MEMBER_FACT_CHARS + 1)
        );
        assert_eq!(
            validate_summary_v2(&long_fact, 7, &aliases, &[], &[]).unwrap_err(),
            SummaryError::TooLong
        );
        let wrong_rev =
            r#"{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","source_revision":6}"#;
        assert_eq!(
            validate_summary_v2(wrong_rev, 7, &aliases, &[], &[]).unwrap_err(),
            SummaryError::BadRevision
        );
    }

    #[test]
    fn tombstoned_loop_is_rejected_but_live_loop_passes() {
        let aliases = ["u1".to_string()];
        let stones = [Tombstone {
            alias: "u1".to_string(),
            slot: "检讨".to_string(),
            content: "欠十遍检讨".to_string(),
        }];
        // A loop tagging itself with the closed (alias, slot) pair is refused.
        let dead = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"还欠十遍检讨","due":null,"alias":"u1","slot":"检讨"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(dead, 7, &aliases, &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
        // The guard is structural only: the same words untagged, or tagged
        // to a live slot, remain the model's own judgement call.
        let untagged = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"还欠十遍检讨","due":null}],"source_revision":7}"#;
        assert!(validate_summary_v2(untagged, 7, &aliases, &stones, &[]).is_ok());
        let live = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u1","slot":"咖啡"}],"source_revision":7}"#;
        assert!(validate_summary_v2(live, 7, &aliases, &stones, &[]).is_ok());
        // Canonical form is v2 even for a v1-shaped reply.
        let v1 = r#"{"v":1,"group_profile":"本群常聊咖啡与手办，气氛温和。","source_revision":7}"#;
        let v = validate_summary_v2(v1, 7, &aliases, &[], &[]).expect("应通过");
        assert_eq!(
            parse_structured_summary(&v.summary_text).expect("结构化").v,
            2
        );
    }

    /// [opt-3] A loop that names an alias but omits the slot can no longer
    /// slip past the guard when that alias has closed rows: the slot is
    /// the model's to copy, and leaving it off is how a settled claim
    /// resurrects. An alias with nothing closed keeps the old tolerance.
    #[test]
    fn slotless_loop_over_closed_alias_is_rejected() {
        let aliases = ["u1".to_string(), "u2".to_string()];
        let stones = [Tombstone {
            alias: "u1".to_string(),
            slot: "检讨".to_string(),
            content: "欠十遍检讨".to_string(),
        }];
        let slotless = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"还欠十遍检讨","due":null,"alias":"u1"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(slotless, 7, &aliases, &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
        let clean = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u2"}],"source_revision":7}"#;
        assert!(validate_summary_v2(clean, 7, &aliases, &stones, &[]).is_ok());
    }

    /// [R29] The owner kept the conservative tombstone policy: a loop may be
    /// refused for naming a closed alias whose slot it did not copy, even
    /// though the pair itself is not the one that was closed. This pins the
    /// refusal so a future "narrowing" cannot quietly turn the guard into a
    /// pair-only check and let a settled loop stream back in.
    #[test]
    fn tombstone_policy_keeps_refusing_sibling_slots() {
        let aliases = ["u1".to_string()];
        let stones = [Tombstone {
            alias: "u1".to_string(),
            slot: "检讨".to_string(),
            content: "欠十遍检讨".to_string(),
        }];
        // A DIFFERENT slot on the same alias: tolerated, the pair is not closed.
        let other_slot = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u1","slot":"咖啡"}],"source_revision":7}"#;
        assert!(validate_summary_v2(other_slot, 7, &aliases, &stones, &[]).is_ok());
        // The closed pair itself: refused, and this is the one case the
        // narrowing option would also have refused.
        let closed_pair = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"还欠十遍检讨","due":null,"alias":"u1","slot":"检讨"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(closed_pair, 7, &aliases, &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
        // Tagged with the alias but no slot: refused while the alias has any
        // closed row — the conservative half the owner chose to keep.
        let slotless = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u1"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(slotless, 7, &aliases, &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
    }

    /// [R29] Activation is per group: another group's alias table and empty
    /// tombstone list mean the same words are the model's own judgement call
    /// there. The guard must never leak a closure across a group boundary.
    #[test]
    fn tombstone_guard_does_not_leak_across_groups() {
        let other_group_aliases = ["u1".to_string()];
        let loop_text = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"还欠十遍检讨","due":null,"alias":"u1","slot":"检讨"}],"source_revision":7}"#;
        // Same alias, same slot, no closure in this group: allowed.
        assert!(validate_summary_v2(loop_text, 7, &other_group_aliases, &[], &[]).is_ok());
        // Retired rows are group-scoped in storage; an alias resolved from a
        // DIFFERENT group cannot carry a closure into this one because
        // `tombstone_entries` only ever sees this group's rows.
        let row = bare_record(Some("555"), "欠十遍检讨");
        let entries = tombstone_entries(vec![row], &[("555".to_string(), "u1".to_string())], "1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].alias, "u1");
        assert_eq!(entries[0].slot, "");
    }

    /// [R29] The guard keys on alias AND slot together; a closure held by one
    /// alias must not refuse another alias that happens to reuse the same slot
    /// name. Pinned because the conservative "any closure for the alias" rule
    /// reads like a blanket refusal and a careless edit could make it one.
    #[test]
    fn tombstone_guard_matches_the_right_alias_only() {
        let aliases = ["u1".to_string(), "u2".to_string()];
        let stones = [
            Tombstone {
                alias: "u1".to_string(),
                slot: "检讨".to_string(),
                content: "欠十遍检讨".to_string(),
            },
            Tombstone {
                alias: "u2".to_string(),
                slot: "拼单".to_string(),
                content: "拼单已取消".to_string(),
            },
        ];
        // u2 reusing u1's slot name is not closed for u2.
        let cross = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u2","slot":"检讨"}],"source_revision":7}"#;
        assert!(validate_summary_v2(cross, 7, &aliases, &stones, &[]).is_ok());
        // u2's own closed pair is refused.
        let own = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"周五拼单买豆","due":null,"alias":"u2","slot":"拼单"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(own, 7, &aliases, &stones, &[]).unwrap_err(),
            SummaryError::Tombstoned
        );
    }

    /// [seal-3] A sealed topic is refused in every summary section —
    /// member facts included, which the tombstone guard never covered —
    /// while a clean summary passes with the same list in force. The
    /// prompt must name the sealed list, or the model would keep
    /// producing refused summaries blind.
    #[test]
    fn sealed_topic_is_rejected_in_any_summary_section() {
        let aliases = ["u1".to_string()];
        let sealed = [crate::store::SealedTopic {
            keywords: vec!["水泥".to_string()],
            ..Default::default()
        }];
        let in_fact = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u1","facts":["鱼缸被灌了水泥"]}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(in_fact, 7, &aliases, &[], &sealed).unwrap_err(),
            SummaryError::Sealed
        );
        let in_loop = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","open_loops":[{"text":"等水泥干透","due":null,"alias":"u1"}],"source_revision":7}"#;
        assert_eq!(
            validate_summary_v2(in_loop, 7, &aliases, &[], &sealed).unwrap_err(),
            SummaryError::Sealed
        );
        let clean = r#"{"v":2,"group_profile":"本群常聊咖啡与手办，气氛温和。","members":[{"alias":"u1","facts":["喜欢手冲咖啡"]}],"source_revision":7}"#;
        assert!(validate_summary_v2(clean, 7, &aliases, &[], &sealed).is_ok());

        let mut req = request();
        req.sealed_topics = vec![crate::store::SealedTopic {
            keywords: vec!["水泥".to_string()],
            slot: "水泥梗".to_string(),
            ..Default::default()
        }];
        let prompt = render_prompt(&req);
        assert!(prompt.contains("封存话题"), "{prompt}");
        assert!(prompt.contains("水泥"), "{prompt}");
    }

    /// [opt-3] Slot-less closed rows used to vanish from the tombstone
    /// list entirely; they are kept now (empty slot) so the closure list
    /// the model reads is complete.
    #[test]
    fn tombstone_entries_keep_slotless_rows() {
        let row = bare_record(Some("555"), "欠十遍检讨");
        let entries = tombstone_entries(vec![row], &[("555".to_string(), "u1".to_string())], "1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].alias, "u1");
        assert_eq!(entries[0].slot, "");
        assert_eq!(entries[0].content, "欠十遍检讨");
    }

    #[test]
    fn prompt_lists_tombstones_and_slot_tags() {
        let mut req = request();
        req.memories[0].slot = "咖啡偏好".to_string();
        req.tombstones = vec![Tombstone {
            alias: "u1".into(),
            slot: "检讨".into(),
            content: "检讨已被管理员免除".into(),
        }];
        let prompt = render_prompt(&req);
        assert!(prompt.contains("[u1] (preference) [咖啡偏好] 喜欢手冲咖啡"));
        assert!(prompt.contains("已结案清单"));
        assert!(prompt.contains("- [u1] [检讨] 检讨已被管理员免除"));
        assert!(prompt.contains("\"v\":2"));
    }

    #[test]
    fn tombstone_entries_reuse_and_extend_aliases() {
        let mut dead = bare_record(Some("777"), "旧断言");
        dead.slot = "检讨".into();
        let mut slotless = bare_record(Some("777"), "无主题旧行");
        slotless.slot = String::new();
        let entries = tombstone_entries(
            vec![dead, slotless],
            &[("555".to_string(), "u1".to_string())],
            "1",
        );
        // The uncovered subject continues the uN sequence. Since [opt-3]
        // the slotless row is kept too (empty slot): it cannot key the
        // pair check, but it belongs on the closure list and arms the
        // slot-less-loop rule for its alias.
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].alias, "u2");
        assert_eq!(entries[0].slot, "检讨");
        assert_eq!(entries[1].alias, "u2");
        assert_eq!(entries[1].slot, "");
    }

    #[test]
    fn valid_summary_is_accepted() {
        let raw = r#"{"summary":"本群常聊咖啡与茶叶，成员偏好温和的日常话题，没有争议性讨论。","source_revision":7}"#;
        let v = validate_summary(raw, 7).expect("应通过");
        assert_eq!(v.source_revision, 7);
        assert!(v.summary_text.starts_with("本群常聊"));
    }

    #[test]
    fn validate_summary_rejects_each_error_class() {
        let long = format!(
            r#"{{"summary":"{}","source_revision":7}}"#,
            "很长".repeat(MAX_SUMMARY_CHARS)
        );
        let cases = [
            (
                r#"{"summary":"本群常聊咖啡与茶叶，成员偏好温和的日常话题，没有争议性讨论。","source_revision":6}"#,
                SummaryError::BadRevision,
            ),
            (
                r#"{"summary":"太短","source_revision":7}"#,
                SummaryError::TooShort,
            ),
            (long.as_str(), SummaryError::TooLong),
            (
                r#"{"summary":"成员 123456789 喜欢咖啡并且经常在群里分享新的豆子和冲煮技巧。","source_revision":7}"#,
                SummaryError::LeakedIdentifier,
            ),
            (
                r#"{"summary":"成员喜欢咖啡并且经常分享[CQ:at,qq=all]这类标记，需要清理掉。","source_revision":7}"#,
                SummaryError::LeakedIdentifier,
            ),
            ("not json", SummaryError::Malformed),
            (r#"{"nothing":1}"#, SummaryError::Malformed),
            (r#"["a"]"#, SummaryError::Malformed),
        ];
        for (raw, err) in cases {
            assert_eq!(validate_summary(raw, 7).unwrap_err(), err, "{raw}");
        }
    }

    #[test]
    fn identifier_detection_allows_normal_numbers() {
        assert!(!leaks_identifier("群里有 3 个人常聊咖啡"));
        assert!(!leaks_identifier("每天聊 20 分钟"));
        assert!(leaks_identifier("编号 12345"));
    }

    #[test]
    fn error_classes_are_stable_strings() {
        for err in [
            SummaryError::Malformed,
            SummaryError::TooShort,
            SummaryError::TooLong,
            SummaryError::BadRevision,
            SummaryError::LeakedIdentifier,
        ] {
            assert_eq!(err.error_class(), ErrorClass::AuxBadOutput);
            assert!(!err.as_str().is_empty());
        }
    }
}
