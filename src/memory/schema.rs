//! Memory schema v1: the single JSON shape every model-facing memory exchange uses.
//!
//! Before v1 a memory's content was one free-text sentence and the extra structure
//! (what the fact is *about*, how long it should live) existed only inside that
//! sentence, so every consumer — extraction dedupe, consolidation, the panel — had
//! to re-parse prose. v1 keeps the sentence (humans and the agent still read it)
//! and adds three explicit fields beside it:
//!
//! * `slot` — a short predicate label (`高达模型取向`), the dedupe handle: two rows
//!   with the same subject, kind and slot are the same claim said twice.
//! * `tags` — up to five short labels, the clustering handle for subject-less rows.
//! * `ttl` — how long the claim may live; it is materialised into the row's
//!   `expires_at` at insert time and enforced by retrieval and the expire sweep.
//!
//! Two strictness levels, deliberately:
//!
//! * [`MemoryV1::validate`] is strict and is used where a bad shape must cost the
//!   whole payload (migration output, structured summaries).
//! * [`sanitize_slot_tags`] is lenient and is used on the write paths (extraction,
//!   agent contract): the v1 fields are optional metadata there, so a malformed
//!   slot must never cost an otherwise good memory — it is dropped to the default.

use serde::{Deserialize, Serialize};

use crate::agent::contract::{MAX_MEMORY_CONTENT_CHARS, MEMORY_KINDS};
use crate::store::MemoryRecord;

/// The only schema version this build reads and writes.
pub const SCHEMA_VERSION: u32 = 1;

/// A slot is a label, not a sentence: anything longer is content in disguise.
pub const MAX_SLOT_CHARS: usize = 24;
/// Tags are a clustering handle; a row that needs more is several rows.
pub const MAX_TAGS: usize = 5;
/// One tag's character ceiling.
pub const MAX_TAG_CHARS: usize = 16;

/// How long a memory may live, as exchanged in v1 JSON (`ttl` field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ttl {
    Permanent,
    #[serde(rename = "30d")]
    Days30,
    #[serde(rename = "7d")]
    Days7,
    #[serde(rename = "48h")]
    Hours48,
}

impl Ttl {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim() {
            "permanent" => Some(Self::Permanent),
            "30d" => Some(Self::Days30),
            "7d" => Some(Self::Days7),
            "48h" => Some(Self::Hours48),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permanent => "permanent",
            Self::Days30 => "30d",
            Self::Days7 => "7d",
            Self::Hours48 => "48h",
        }
    }

    /// Seconds after `base` at which a row with this ttl expires; `None` never does.
    pub fn expires_at(self, base: i64) -> Option<i64> {
        match self {
            Self::Permanent => None,
            Self::Days30 => Some(base.saturating_add(30 * 86_400)),
            Self::Days7 => Some(base.saturating_add(7 * 86_400)),
            Self::Hours48 => Some(base.saturating_add(48 * 3_600)),
        }
    }

    /// The ttl a kind gets when the writer did not name one: open-ended chatter
    /// (topics, events) decays; durable kinds live until merged or retired.
    pub fn default_for_kind(kind: &str) -> Self {
        match kind {
            "topic" => Self::Hours48,
            "event" => Self::Days7,
            _ => Self::Permanent,
        }
    }

    /// Rebuild the ttl a stored row effectively carries, from its expiry and the
    /// kind default. Only used when rendering a row back into v1 JSON.
    pub fn for_record(record: &MemoryRecord) -> Self {
        match record.expires_at {
            None => Self::Permanent,
            Some(_) => Self::default_for_kind(&record.kind),
        }
    }
}

/// The v1 fields a writer may attach to a memory write.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryV1Fields {
    pub slot: String,
    pub tags: Vec<String>,
    pub ttl: Option<Ttl>,
    /// A precomputed expiry that wins over `ttl` when set (`Some(None)` forces
    /// permanent). Consolidation uses it to carry the cluster's own expiry
    /// forward instead of restarting the clock.
    pub expires_override: Option<Option<i64>>,
}

impl MemoryV1Fields {
    /// Expiry this write materialises at `now`: the override first, then an
    /// explicit ttl, then the kind default.
    pub fn expires_at(&self, kind: &str, now: i64) -> Option<i64> {
        if let Some(overridden) = self.expires_override {
            return overridden;
        }
        self.ttl
            .unwrap_or_else(|| Ttl::default_for_kind(kind))
            .expires_at(now)
    }
}

/// Lenient cleanup of a writer-supplied slot/tag pair: trim, drop empties, dedupe
/// tags in order, and drop (never truncate, never reject) anything over a cap.
pub fn sanitize_slot_tags(slot: &str, tags: &[String]) -> (String, Vec<String>) {
    let slot = slot.trim();
    let slot = if slot.chars().count() > MAX_SLOT_CHARS {
        String::new()
    } else {
        slot.to_string()
    };
    let mut out: Vec<String> = Vec::new();
    for tag in tags {
        let tag = tag.trim();
        if tag.is_empty() || tag.chars().count() > MAX_TAG_CHARS {
            continue;
        }
        if !out.iter().any(|t| t == tag) {
            out.push(tag.to_string());
        }
        if out.len() >= MAX_TAGS {
            break;
        }
    }
    (slot, out)
}

/// Who a v1 memory is about. `alias` is the group-local speaker alias (`u3`),
/// the literal `group` for group-level rows, or `bot` for the assistant itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubjectV1 {
    #[serde(rename = "type")]
    pub subject_type: String,
    pub alias: String,
}

/// One memory in v1 JSON: what the model sees on injection and what the
/// migration tool emits. Field order is the render order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryV1 {
    pub v: u32,
    #[serde(rename = "ref")]
    pub memory_ref: String,
    pub kind: String,
    pub subject: SubjectV1,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub slot: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub confidence: f64,
    #[serde(default)]
    pub evidence: Vec<String>,
    pub first_seen: i64,
    pub last_seen: i64,
    pub ttl: String,
    pub status: String,
}

impl MemoryV1 {
    /// Render a stored row. `subject_alias` is the caller's alias-table answer for
    /// the row's subject (`None` renders as the group); a subject equal to the bot
    /// id renders as `bot`, matching the fold prompt's convention.
    pub fn from_record(record: &MemoryRecord, subject_alias: Option<&str>) -> Self {
        let (subject_type, alias) = match &record.subject_user_id {
            None => ("group", "group".to_string()),
            Some(id) if id == &record.bot_id => ("bot", "bot".to_string()),
            // A subject the caller cannot name an alias for renders as the group:
            // showing `user/group` would claim a person the reader cannot identify.
            Some(_) => match subject_alias {
                Some(alias) => ("user", alias.to_string()),
                None => ("group", "group".to_string()),
            },
        };
        let evidence: Vec<String> =
            serde_json::from_str(&record.source_message_ids).unwrap_or_default();
        Self {
            v: SCHEMA_VERSION,
            memory_ref: record.memory_ref.clone(),
            kind: record.kind.clone(),
            subject: SubjectV1 {
                subject_type: subject_type.to_string(),
                alias,
            },
            slot: record.slot.clone(),
            content: record.content.clone(),
            tags: record.tags.clone(),
            confidence: record.confidence,
            evidence,
            first_seen: record.created_at,
            last_seen: record.updated_at,
            ttl: Ttl::for_record(record).as_str().to_string(),
            status: if record.is_archived() {
                "archived".to_string()
            } else {
                "active".to_string()
            },
        }
    }

    /// Strict validation for payloads that arrive as v1 JSON (migration output).
    /// Returns the first violated rule as a stable machine-readable tag.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v != SCHEMA_VERSION {
            return Err("bad_version");
        }
        if !MEMORY_KINDS.contains(&self.kind.as_str()) {
            return Err("bad_kind");
        }
        if self.content.trim().is_empty() || self.content.chars().count() > MAX_MEMORY_CONTENT_CHARS
        {
            return Err("bad_content");
        }
        if self.slot.chars().count() > MAX_SLOT_CHARS {
            return Err("bad_slot");
        }
        if self.tags.len() > MAX_TAGS
            || self
                .tags
                .iter()
                .any(|t| t.trim().is_empty() || t.chars().count() > MAX_TAG_CHARS)
        {
            return Err("bad_tags");
        }
        if !self.confidence.is_finite() || !(0.0..=1.0).contains(&self.confidence) {
            return Err("bad_confidence");
        }
        if Ttl::parse(&self.ttl).is_none() {
            return Err("bad_ttl");
        }
        if !matches!(self.subject.subject_type.as_str(), "user" | "group" | "bot") {
            return Err("bad_subject");
        }
        Ok(())
    }
}

/// Parse the stored `tags` column (a JSON string array). A garbled column reads
/// as no tags: the row's content still stands on its own.
pub fn parse_tags_column(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// Serialise tags for the `tags` column.
pub fn tags_column(tags: &[String]) -> String {
    serde_json::to_string(tags).unwrap_or_else(|_| "[]".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> MemoryRecord {
        MemoryRecord {
            memory_ref: "mem_x".into(),
            bot_id: "bot1".into(),
            group_id: "100".into(),
            subject_user_id: Some("42".into()),
            content: "只认可成品和 MGSD".into(),
            kind: "preference".into(),
            source_message_ids: "[\"111\",\"222\"]".into(),
            confidence: 0.7,
            revision: 1,
            origin: "hy3".into(),
            archived_at: None,
            created_at: 100,
            updated_at: 200,
            slot: "高达模型取向".into(),
            tags: vec!["高达".into()],
            expires_at: None,
            state: "active".to_string(),
            superseded_by: None,
            state_changed_at: None,
        }
    }

    #[test]
    fn ttl_defaults_follow_kind() {
        assert_eq!(Ttl::default_for_kind("topic"), Ttl::Hours48);
        assert_eq!(Ttl::default_for_kind("event"), Ttl::Days7);
        assert_eq!(Ttl::default_for_kind("fact"), Ttl::Permanent);
        assert_eq!(Ttl::Hours48.expires_at(1_000), Some(1_000 + 48 * 3_600));
        assert_eq!(Ttl::Permanent.expires_at(1_000), None);
        assert_eq!(Ttl::parse("7d"), Some(Ttl::Days7));
        assert_eq!(Ttl::parse("forever"), None);
    }

    #[test]
    fn sanitize_drops_oversize_and_dedupes() {
        let (slot, tags) = sanitize_slot_tags(
            &"x".repeat(MAX_SLOT_CHARS + 1),
            &[
                " 高达 ".into(),
                "高达".into(),
                "".into(),
                "y".repeat(MAX_TAG_CHARS + 1),
                "模型".into(),
            ],
        );
        assert_eq!(slot, "");
        assert_eq!(tags, vec!["高达".to_string(), "模型".to_string()]);
    }

    #[test]
    fn from_record_renders_subject_kinds() {
        let mut row = record();
        let v1 = MemoryV1::from_record(&row, Some("u3"));
        assert_eq!(v1.subject.subject_type, "user");
        assert_eq!(v1.subject.alias, "u3");
        assert_eq!(v1.evidence, vec!["111".to_string(), "222".to_string()]);
        v1.validate().expect("渲染出的 v1 应合法");

        row.subject_user_id = None;
        let v1 = MemoryV1::from_record(&row, None);
        assert_eq!(v1.subject.subject_type, "group");

        row.subject_user_id = Some("bot1".into());
        let v1 = MemoryV1::from_record(&row, None);
        assert_eq!(v1.subject.subject_type, "bot");
        assert_eq!(v1.subject.alias, "bot");
    }

    #[test]
    fn validate_rejects_each_bad_field() {
        let mut v1 = MemoryV1::from_record(&record(), Some("u3"));
        v1.v = 2;
        assert_eq!(v1.validate(), Err("bad_version"));
        v1.v = 1;
        v1.kind = "mood".into();
        assert_eq!(v1.validate(), Err("bad_kind"));
        v1.kind = "fact".into();
        v1.slot = "x".repeat(MAX_SLOT_CHARS + 1);
        assert_eq!(v1.validate(), Err("bad_slot"));
        v1.slot = String::new();
        v1.tags = vec!["t".into(); MAX_TAGS + 1];
        assert_eq!(v1.validate(), Err("bad_tags"));
        v1.tags = Vec::new();
        v1.ttl = "forever".into();
        assert_eq!(v1.validate(), Err("bad_ttl"));
    }
}
