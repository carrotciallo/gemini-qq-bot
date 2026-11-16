//! Persistence layer: typed records plus the SQLite implementation.

pub mod migrations;
pub mod sqlite;

pub use sqlite::SqliteStore;

use serde::{Deserialize, Serialize};

/// A group's configuration and Agy session pointer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRecord {
    pub bot_id: String,
    pub group_id: String,
    pub enabled: bool,
    pub paused: bool,
    pub agy_conversation_id: Option<String>,
    /// Successful Agy turns since the last conversation reset.
    #[serde(default)]
    pub agy_turn_count: i64,
    /// Whether a running resident turn may absorb a newly claimed batch.
    #[serde(default)]
    pub resident_insert_enabled: bool,
    /// How eagerly the bot joins chatter not addressed to her:
    /// addressed_only | quiet | normal | lively. Panel-set through the
    /// admin API; read it through [`GroupRecord::pace`], which maps
    /// unknown values to normal.
    #[serde(default = "default_ambient_pace")]
    pub ambient_pace: String,
    pub prompt_version: i64,
    pub memory_revision: i64,
    /// Display name fetched from OneBot at discovery time.
    #[serde(default)]
    pub name: Option<String>,
    /// Member count fetched together with the name.
    #[serde(default)]
    pub member_count: Option<i64>,
    /// When the bot was removed from the group (`group_decrease`), if
    /// ever. The row and its data are kept; the marker only tells the
    /// console the group is gone.
    #[serde(default)]
    pub departed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

fn default_ambient_pace() -> String {
    "normal".to_string()
}

/// Reply-eagerness tier for ambient chatter (messages not addressed to
/// the bot). Addressed messages always form turns; this tier only
/// governs how readily ambient chatter does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmbientPace {
    AddressedOnly,
    Quiet,
    Normal,
    Lively,
}

impl AmbientPace {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "addressed_only" => Some(Self::AddressedOnly),
            "quiet" => Some(Self::Quiet),
            "normal" => Some(Self::Normal),
            "lively" => Some(Self::Lively),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AddressedOnly => "addressed_only",
            Self::Quiet => "quiet",
            Self::Normal => "normal",
            Self::Lively => "lively",
        }
    }
}

impl GroupRecord {
    /// The group's ambient pace tier; unknown stored values read as normal.
    pub fn pace(&self) -> AmbientPace {
        AmbientPace::parse(&self.ambient_pace).unwrap_or(AmbientPace::Normal)
    }
}

/// A pending (not yet delivered to the agent) event row.
#[derive(Debug, Clone)]
pub struct PendingEventRow {
    pub id: i64,
    pub bot_id: String,
    pub group_id: String,
    pub message_id: String,
    pub sender_id: String,
    pub received_at: i64,
    pub normalized_payload: String,
    pub image_refs: String,
    pub batch_id: Option<String>,
}

/// A member's stable per-group alias.
#[derive(Debug, Clone)]
pub struct MemberRecord {
    pub bot_id: String,
    pub group_id: String,
    pub user_id: String,
    pub speaker_ref: String,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
}

/// Mood / topic state for a group.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GroupStateRecord {
    pub mood_json: Option<String>,
    pub topic_state: Option<String>,
    pub updated_at: i64,
}

/// Which writer produced a memory row. Stored verbatim in `memories.origin`.
pub mod origin {
    /// The per-turn agent contract (`memory_ops`).
    pub const AGY: &str = "agy";
    /// An operator using the admin panel.
    pub const PANEL: &str = "panel";
    /// The fold-time extraction pass in `crate::memory::extract`.
    pub const HY3: &str = "hy3";

    /// Audit actor for a row written by `origin`. Extraction is recorded under its
    /// own actor name so the audit trail distinguishes it from the turn contract.
    pub fn audit_actor(origin: &str) -> &str {
        match origin {
            HY3 => "hy3_extract",
            other => other,
        }
    }
}

/// A reviewed memory fact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRecord {
    pub memory_ref: String,
    pub bot_id: String,
    pub group_id: String,
    pub subject_user_id: Option<String>,
    pub content: String,
    pub kind: String,
    pub source_message_ids: String,
    pub confidence: f64,
    pub revision: i64,
    /// Writer that produced the row: `agy`, `panel` or `hy3`.
    pub origin: String,
    /// `Some(ts)` means retired: excluded from retrieval and never updated.
    pub archived_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Schema v1 predicate label (`高达模型取向`); empty when the writer gave none.
    #[serde(default)]
    pub slot: String,
    /// Schema v1 clustering labels; empty when the writer gave none.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Schema v1 expiry (epoch seconds); `None` is permanent. An expired row is
    /// retired by the expire sweep and never surfaces in retrieval meanwhile.
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// Lifecycle state (migration 0026): `active`, `superseded`, `retired`,
    /// `expired` or `archived`. Structured data — never inferred from content.
    /// Invariant: `active` exactly when `archived_at` is `None`.
    #[serde(default = "default_memory_state")]
    pub state: String,
    /// The row that replaced this one, when `state` is `superseded`.
    #[serde(default)]
    pub superseded_by: Option<String>,
    /// When `state` last changed (epoch seconds).
    #[serde(default)]
    pub state_changed_at: Option<i64>,
}

fn default_memory_state() -> String {
    "active".to_string()
}

/// One operator-declared dead topic for a group (migration 0027, [seal-1]).
///
/// A sealed topic must not re-enter the group's derived context. An entry
/// hits when its `slot` equals the candidate's slot (only when the entry
/// names one) OR any of its `keywords` appears in the candidate text.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SealedTopic {
    pub id: i64,
    pub bot_id: String,
    pub group_id: String,
    pub keywords: Vec<String>,
    pub slot: String,
    pub note: String,
    pub created_at: i64,
}

impl MemoryRecord {
    /// True when the row is retired and must not surface or be written again.
    pub fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }

    /// True when the row's v1 expiry has passed at `now`.
    pub fn is_expired(&self, now: i64) -> bool {
        matches!(self.expires_at, Some(at) if at <= now)
    }
}

/// One row of the raw group log. Never deleted by the bot: archiving only flips a
/// flag, so a bad summary can always be recomputed from the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessageRecord {
    pub id: i64,
    pub bot_id: String,
    pub group_id: String,
    pub message_id: String,
    pub sender_id: String,
    pub display_name: Option<String>,
    pub speaker_role: String,
    pub text: String,
    pub has_image: bool,
    pub sticker_hash: Option<String>,
    /// QQ's sticker flag for the image: `Some("1")` is an animated sticker, `Some("0")`
    /// a normal picture, `None` for text-only or pre-migration rows.
    pub sub_type: Option<String>,
    /// JSON array of every tagged picture on this message, photos and screenshots
    /// included.
    ///
    /// Stickers also get a row in `stickers` because they are library material. A photo
    /// never enters the shelf, but its description must stay inspectable - that is where
    /// the `self` verdict for an ordinary picture lives.
    pub image_tags: Option<String>,
    pub created_at: i64,
    pub archived: bool,
}

/// A compacted window of the raw log, produced by the preflight Aux pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryChunkRecord {
    pub chunk_id: String,
    pub bot_id: String,
    pub group_id: String,
    pub seq: i64,
    pub from_msg_id: i64,
    pub to_msg_id: i64,
    pub summary: String,
    pub model_id: String,
    pub created_at: i64,
}

/// One sticker in the library. `hash` is the sha256 of the bytes, so a reposted
/// sticker hits the existing row instead of costing another tagging call.
///
/// This row is the RECOGNITION CACHE. `kept` decides whether it is also in her
/// library: 1 = she kept it and may send it, 0 = tagged but unjudged, -1 = she
/// refused it. Only `kept = 1` is retrievable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StickerRecord {
    pub hash: String,
    pub bot_id: String,
    /// Group where the image was first seen. Provenance only, not a scope.
    pub group_id: String,
    pub file_path: String,
    pub kind: String,
    pub is_self: String,
    pub ocr: String,
    /// One paragraph: the character, expression and action, any visible text, the art
    /// style, and optionally when to send it. What the tagger writes now; replaces
    /// `scene` + `intent` + `usage_hint`, which stay only for rows tagged before this
    /// field existed. Empty means the row predates it.
    pub description: String,
    /// Legacy: rows tagged before `description` kept their picture clause here.
    pub scene: String,
    /// JSON array of emotion words.
    pub emotion: String,
    /// Legacy: rows tagged before `description` kept their meaning clause here.
    pub intent: String,
    /// Legacy: rows tagged before `description` kept their occasion clause here.
    pub usage_hint: String,
    pub search_blob: String,
    /// 64-bit average hash of the decoded picture, or `None` when the bytes never
    /// decoded. Stored signed because SQLite integers are signed; compare through
    /// `stickers::phash_distance`, which reads the bits back as `u64`.
    pub phash: Option<i64>,
    /// When this picture was last seen again. `None` on rows that never repeated.
    pub last_seen_at: Option<i64>,
    /// How many times this picture has been posted. Starts at 1.
    pub seen_count: i64,
    pub nsfw: bool,
    pub enabled: bool,
    /// 1 kept by her, 0 unjudged, -1 refused. See [`KEPT_YES`] and friends.
    pub kept: i64,
    pub created_at: i64,
}

/// She kept this sticker; it is in her library and she may send it.
pub const KEPT_YES: i64 = 1;
/// Tagged but she has not judged it. Cached, not offerable.
pub const KEPT_UNJUDGED: i64 = 0;
/// She looked at it and said no. Never offer it again.
pub const KEPT_REFUSED: i64 = -1;

/// Derived summary of a group's reviewed memories.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryRecord {
    pub bot_id: String,
    pub group_id: String,
    pub summary_text: String,
    pub source_revision: i64,
    pub model_id: String,
    pub updated_at: i64,
}

/// A rolling handover note: what the outgoing conversation would tell its
/// successor so the rotation seam stays invisible (see `memory::handover`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverRecord {
    pub bot_id: String,
    pub group_id: String,
    pub note_text: String,
    /// Newest group_messages row id the note was built from.
    pub source_msg_id: i64,
    pub model_id: String,
    pub updated_at: i64,
}

/// Lifecycle of a memory compaction job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
        }
    }
}

/// Who asked for a compaction job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorKind {
    Owner,
    Panel,
    /// A background loop acting on its own schedule (auto compaction).
    System,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::Owner => "owner",
            ActorKind::Panel => "panel",
            ActorKind::System => "system",
        }
    }
}

/// A memory compaction job row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionJob {
    pub id: String,
    pub bot_id: String,
    pub group_id: String,
    pub source_revision: i64,
    pub state: String,
    pub attempt_count: i64,
    pub requested_by: String,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub error_class: Option<String>,
}

/// Lifecycle of an Agy call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallState {
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl CallState {
    pub fn as_str(self) -> &'static str {
        match self {
            CallState::Running => "running",
            CallState::Succeeded => "succeeded",
            CallState::Failed => "failed",
            CallState::Unknown => "unknown",
        }
    }
}

/// A record of one Agy invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCall {
    pub id: String,
    pub bot_id: String,
    pub group_id: String,
    pub batch_id: String,
    pub state: String,
    pub attempt_count: i64,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub error_class: Option<String>,
}

/// The step counters a finished resident round writes onto its call row.
///
/// `session_events` is the whole step stream of the resident session (the same
/// number migration 0012's `steps_count` has always held); `turn_delta` is how
/// many of those steps belong to the round that just finished. They are
/// different numbers on purpose — a resident call spans many batches, so the
/// cumulative count is not this round's size (K2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCounts {
    pub session_events: i64,
    pub turn_delta: i64,
    pub last_progress_idx: i64,
    pub insert_count: i64,
    pub draft_count: i64,
}

/// Per-group seed quarantine state (migration 0020). `last_seed_hash` is the
/// fingerprint of the seed brief the group's current resident session was
/// born with; `rejected_hash` is a seed an upstream content filter refused,
/// which must never be resubmitted verbatim; `tier` is the degradation tier
/// the next spawn assembles at; `clean_rounds` counts consecutive delivered
/// rounds towards earning the full seed back.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedQuarantine {
    pub bot_id: String,
    pub group_id: String,
    pub last_seed_hash: Option<String>,
    pub rejected_hash: Option<String>,
    pub tier: i64,
    pub clean_rounds: i64,
    pub updated_at: i64,
}

/// Outbox lifecycle. `Unknown` means the OneBot send result was ambiguous and the
/// message must never be retried automatically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxState {
    Pending,
    Sending,
    Sent,
    Unknown,
}

impl OutboxState {
    pub fn as_str(self) -> &'static str {
        match self {
            OutboxState::Pending => "pending",
            OutboxState::Sending => "sending",
            OutboxState::Sent => "sent",
            OutboxState::Unknown => "unknown",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(OutboxState::Pending),
            "sending" => Some(OutboxState::Sending),
            "sent" => Some(OutboxState::Sent),
            "unknown" => Some(OutboxState::Unknown),
            _ => None,
        }
    }
}

/// The one live `submit_reply` arm for a group (MCP output channel).
///
/// The executor writes this just before a turn starts; the MCP server checks
/// the token the model presents against it on every call. One row per group: a
/// group never has two turns live at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArmedTurn {
    pub group_id: String,
    pub turn_token: String,
    pub batch_id: String,
    pub armed_at: i64,
    pub expires_at: i64,
}

/// One recorded `submit_reply` call, in per-token call order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnSubmission {
    pub seq: i64,
    /// The raw arguments JSON the model presented.
    pub payload: String,
    /// `accepted` or `rejected`.
    pub verdict: String,
    /// Rejection reason; empty when accepted.
    pub reason: String,
    pub created_at: i64,
}

/// One outbound message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxRecord {
    pub id: String,
    pub bot_id: String,
    pub group_id: String,
    pub batch_id: String,
    pub text: String,
    /// `text`, `sticker` or `image`. A sticker row keeps its bytes out of the
    /// database; an `image` row does the same, with `text` holding the bare
    /// file name under the imagegen output directory.
    pub kind: String,
    /// Content hash when `kind == "sticker"`; the bytes live at
    /// `<sticker_dir>/<hash>.jpg`.
    pub sticker_hash: Option<String>,
    /// OneBot message id this part quotes (a `reply` segment is prepended at
    /// send time). Only ever set on a reply's first part.
    pub quote_message_id: Option<String>,
    pub state: String,
    pub message_id: Option<String>,
    pub part_index: i64,
    pub part_count: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Fields for one part of a multi-message reply.
pub struct OutboxPart<'a> {
    pub bot_id: &'a str,
    pub group_id: &'a str,
    pub batch_id: &'a str,
    pub text: &'a str,
    pub part_index: i64,
    pub part_count: i64,
    pub now: i64,
    /// Content hash when this part is a sticker instead of text.
    pub sticker_hash: Option<&'a str>,
    /// Generated-picture file name when this part is a plain image instead of
    /// text. The name is stored in the row's `text` column (the locator, like
    /// a sticker's hash) and resolved against the imagegen output directory
    /// at delivery time; it must be a bare file name, never a path.
    pub image_file: Option<&'a str>,
    /// OneBot message id this part quotes; see [`OutboxRecord::quote_message_id`].
    pub quote_message_id: Option<&'a str>,
}

/// One registered picture request (migration 0025). A job counts against
/// its bucket's daily cap from the moment it exists, whatever state it ends
/// in; the panel reset watermark is the only way to give quota back early.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageJob {
    pub id: String,
    pub bot_id: String,
    pub group_id: String,
    /// QQ id the picture is charged to, or `bot` for the self bucket.
    pub requester_id: String,
    /// `self` (she decided to draw) or `request` (a member explicitly asked).
    pub bucket: String,
    pub prompt: String,
    pub self_portrait: bool,
    pub caption: Option<String>,
    /// queued | running | succeeded | failed
    pub state: String,
    pub file_path: Option<String>,
    pub error_class: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The panel-editable image generation limits (single row, migration 0025).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImagegenSettings {
    pub self_daily_cap: i64,
    pub request_daily_cap: i64,
    pub request_cooldown_minutes: i64,
    pub updated_at: i64,
}

impl Default for ImagegenSettings {
    fn default() -> Self {
        Self {
            self_daily_cap: 15,
            request_daily_cap: 5,
            request_cooldown_minutes: 10,
            updated_at: 0,
        }
    }
}

/// Start of the current day in Asia/Shanghai (fixed UTC+8, no DST), as a
/// unix epoch. The imagegen buckets count by this day; compaction's UTC day
/// is a legacy exception, not a model to copy.
pub fn shanghai_day_start(now: i64) -> i64 {
    const OFFSET: i64 = 8 * 3600;
    ((now + OFFSET).div_euclid(86_400) * 86_400) - OFFSET
}

/// A low-cardinality audit entry. Only `owner` / `panel` actors are recorded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: i64,
    pub actor_kind: String,
    pub action: String,
    pub bot_id: Option<String>,
    pub group_id: Option<String>,
    pub created_at: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `origin` is written verbatim into `memories.origin`, so the constants are data.
    #[test]
    fn origin_constants_are_the_stored_slugs() {
        assert_eq!(origin::AGY, "agy");
        assert_eq!(origin::PANEL, "panel");
        assert_eq!(origin::HY3, "hy3");
    }

    #[test]
    fn only_extraction_gets_its_own_audit_actor() {
        assert_eq!(origin::audit_actor(origin::HY3), "hy3_extract");
        // The other writers keep their own name; inventing a suffix would make the
        // audit trail disagree with the row it describes.
        assert_eq!(origin::audit_actor(origin::AGY), "agy");
        assert_eq!(origin::audit_actor(origin::PANEL), "panel");
        assert_eq!(origin::audit_actor("unknown"), "unknown");
        assert_eq!(origin::audit_actor(""), "");
    }

    /// The three sticker verdicts are a closed vocabulary and only `KEPT_YES` is
    /// retrievable, so the numbers must not collide or drift.
    #[test]
    fn kept_values_are_distinct_and_ordered() {
        assert_eq!(KEPT_REFUSED, -1);
        assert_eq!(KEPT_UNJUDGED, 0);
        assert_eq!(KEPT_YES, 1);
        let all = [KEPT_REFUSED, KEPT_UNJUDGED, KEPT_YES];
        let mut sorted = all.to_vec();
        sorted.sort_unstable();
        assert_eq!(sorted, all.to_vec(), "取值必须严格递增");
        assert_eq!(all.iter().filter(|v| **v == KEPT_YES).count(), 1);
    }

    #[test]
    fn job_state_slugs_round_trip_through_serde() {
        for state in [
            JobState::Queued,
            JobState::Running,
            JobState::Succeeded,
            JobState::Failed,
        ] {
            let json = serde_json::to_string(&state).expect("序列化");
            assert_eq!(json, format!("\"{}\"", state.as_str()));
            let back: JobState = serde_json::from_str(&json).expect("反序列化");
            assert_eq!(back, state);
        }
    }

    #[test]
    fn call_state_slugs_are_lowercase_and_unique() {
        let mut slugs = vec![
            CallState::Running.as_str(),
            CallState::Succeeded.as_str(),
            CallState::Failed.as_str(),
            CallState::Unknown.as_str(),
        ];
        assert_eq!(
            slugs,
            vec!["running", "succeeded", "failed", "unknown"],
            "错误分类必须保持稳定"
        );
        slugs.sort_unstable();
        let before = slugs.len();
        slugs.dedup();
        assert_eq!(slugs.len(), before);
    }

    #[test]
    fn actor_kind_slugs_are_the_audit_actors() {
        assert_eq!(ActorKind::Owner.as_str(), "owner");
        assert_eq!(ActorKind::Panel.as_str(), "panel");
        assert_ne!(ActorKind::Owner.as_str(), ActorKind::Panel.as_str());
    }

    #[test]
    fn outbox_slugs_round_trip_and_unknown_parses_back_to_unknown() {
        for state in [
            OutboxState::Pending,
            OutboxState::Sending,
            OutboxState::Sent,
            OutboxState::Unknown,
        ] {
            assert_eq!(OutboxState::parse(state.as_str()), Some(state));
        }
        // `unknown` is the ambiguous-send state and must never be confused with
        // `pending`: the pipeline refuses to retry it automatically.
        assert_eq!(
            OutboxState::parse("unknown"),
            Some(OutboxState::Unknown),
            "send 结果不明不能被读成待重试"
        );
    }

    #[test]
    fn outbox_parse_refuses_anything_else() {
        for raw in [
            "",
            "PENDING",
            " pending",
            "pending ",
            "retry",
            "Unknown",
            "pending\n",
        ] {
            assert_eq!(OutboxState::parse(raw), None, "不应解析: {raw:?}");
        }
    }

    #[test]
    fn a_record_is_archived_only_after_the_flag_lands() {
        let mut memory = MemoryRecord {
            memory_ref: "mem_500_1".into(),
            bot_id: "1".into(),
            group_id: "500".into(),
            subject_user_id: None,
            content: "喜欢香菜".into(),
            kind: "preference".into(),
            source_message_ids: "[1]".into(),
            confidence: 0.6,
            revision: 1,
            origin: origin::AGY.into(),
            archived_at: None,
            created_at: 10,
            updated_at: 10,
            slot: String::new(),
            tags: Vec::new(),
            expires_at: None,
            state: "active".to_string(),
            superseded_by: None,
            state_changed_at: None,
        };
        assert!(!memory.is_archived());
        memory.archived_at = Some(0);
        // Even the epoch counts as archived: the flag is presence, not truthiness.
        assert!(memory.is_archived());
        memory.archived_at = Some(1_700_000_000);
        assert!(memory.is_archived());
    }
}
