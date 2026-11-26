//! Shared vocabulary for the image path and the intake timing constants.
//!
//! This module used to hold the cheap gate that scored bursts and described
//! pictures through the Aux gateway. Both jobs are gone (2026-11-24): pacing
//! is structural now, and the agent looks at pictures herself. What remains
//! is the parts other modules still share — the per-batch picture budgets,
//! the [`ImageTag`] text record a library row renders as, QQ's kind flag
//! resolution, and the timing constants intake still reads.

/// How many ordinary pictures (not stickers) one turn may stage for the agent.
pub const MAX_IMAGES_PER_CALL: usize = 4;

/// Stickers have their own budget. They used to share [`MAX_IMAGES_PER_CALL`],
/// so two stickers in front of three photos silently dropped a photo.
pub const MAX_STICKERS_PER_CALL: usize = 4;

/// How many unarchived raw-log rows the gate sees as conversational context.
pub const PREFLIGHT_CONTEXT_MESSAGES: i64 = 20;

/// The shortest time a burst must exist before its score is allowed to release it.
///
/// Played back over the real group log, a pure score gate (floor 0) produced one Agy call
/// **more** than the old fixed window and cut one speaker mid-thought: a burst that scores
/// 0, like a lone `[图片]`, is released on the tick it arrives, so a message sent two
/// seconds later can no longer join it. A two-second floor already restores "same calls,
/// zero cuts" while still cutting the average wait from 4.9 s to 2.9 s — the old five
/// second window was paying three extra seconds for nothing.
pub const MIN_BURST_SECONDS: i64 = 2;

/// One picture's text record: what a library row knows about it, in the
/// shape the prompt renders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageTag {
    /// 1-based position in the call, so a partial answer can still be mapped back.
    pub index: usize,
    pub kind: String,
    pub ocr: String,
    /// One short caption (≤30 chars): subject, defining look, expression or
    /// action, art style if distinctive. Replaces the old `scene` + `intent` +
    /// `usage_hint` triple, which used to cost three separate answers.
    pub description: String,
    pub emotion: String,
    /// `self` / `self_series` / `other`.
    pub self_ref: String,
    pub nsfw: bool,
}

impl ImageTag {
    /// Render the tag as the one line the agent will see.
    ///
    /// Deliberately flat: the agent needs the content and the tone, not a schema. Empty
    /// fields are omitted so a bare photo does not turn into a row of blanks.
    pub fn render(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        let kind = if self.kind.is_empty() {
            "image"
        } else {
            self.kind.as_str()
        };
        parts.push(kind.to_string());
        if !self.ocr.trim().is_empty() {
            parts.push(format!("文字「{}」", self.ocr.trim()));
        }
        if !self.description.trim().is_empty() {
            parts.push(self.description.trim().to_string());
        }
        if !self.emotion.trim().is_empty() {
            parts.push(format!("情绪{}", self.emotion.trim()));
        }
        if self.self_ref == "self" || self.self_ref == "self_series" {
            parts.push(format!("主体{}", self.self_ref));
        }
        if self.nsfw {
            parts.push("敏感内容".to_string());
        }
        parts.join("｜")
    }
}

/// QQ's own sticker flag wins over the vision model's guess.
///
/// `sub_type` is what the sender actually picked in the client: `1` is an animated
/// sticker, `0` a normal picture. The tagger only gets to decide when the flag is
/// missing (older clients, or a segment that never had one) — trusting the model
/// instead loses 240px stickers that it calls photos.
pub fn resolve_kind(sub_type: Option<&str>, model_kind: &str) -> String {
    match sub_type {
        Some("1") => "sticker".to_string(),
        Some("0") => "photo".to_string(),
        Some(other) => format!("image:{other}"),
        None => model_kind.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qq_sub_type_decides_kind_over_the_model() {
        // 1 is a sticker, 0 a picture: the sender's choice beats the tagger's guess.
        assert_eq!(resolve_kind(Some("1"), "photo"), "sticker");
        assert_eq!(resolve_kind(Some("0"), "sticker"), "photo");
        // An unexpected flag is surfaced verbatim rather than silently normalised.
        assert_eq!(resolve_kind(Some("7"), "sticker"), "image:7");
        // No flag at all: the model is the only witness, so it keeps the call.
        assert_eq!(resolve_kind(None, "sticker"), "sticker");
        assert_eq!(resolve_kind(None, "screenshot"), "screenshot");
    }

    fn tag(kind: &str, self_ref: &str) -> ImageTag {
        ImageTag {
            index: 1,
            kind: kind.into(),
            ocr: String::new(),
            description: "一只白猫趴在键盘上".into(),
            emotion: String::new(),
            self_ref: self_ref.into(),
            nsfw: false,
        }
    }

    /// `self` is not a sticker-only field. A photo that happens to be her must say so,
    /// otherwise the agent reads a picture of herself as a stranger.
    #[test]
    fn self_is_reported_for_photos_as_well_as_stickers() {
        assert!(tag("photo", "self").render().contains("主体self"));
        assert!(tag("sticker", "self").render().contains("主体self"));
        assert!(
            tag("art", "self_series")
                .render()
                .contains("主体self_series")
        );
        // Someone else's picture must stay silent rather than claim a subject.
        assert!(!tag("photo", "other").render().contains("主体"));
    }

    /// A photo has no library handle: it is conversation material, so the rendered line
    /// must not invite `sticker_keep` on something that can never be kept.
    #[test]
    fn a_photo_line_carries_no_sticker_handle() {
        let line = tag("photo", "self").render();
        assert!(!line.contains('#'));
        assert!(line.starts_with("photo"));
    }
}
