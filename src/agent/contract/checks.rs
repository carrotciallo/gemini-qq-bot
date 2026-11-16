//! Per-section contract checks: sticker ops, nicknames, part quotes and memory ops.
//! Moved out of contract.rs verbatim (std-split-contract); logic unchanged.

use super::*;

pub(super) fn clamp_chars(text: String, max: usize, report: &mut SalvageReport) -> String {
    if text.chars().count() <= max {
        return text;
    }
    report.clamped += 1;
    text.chars().take(max).collect()
}

pub(super) fn validate_sticker_ops(
    raw: Vec<StickerOpRaw>,
    _scope: &ValidationScope,
    rejections: &mut Rejections,
) -> Vec<StickerOp> {
    let mut out: Vec<StickerOp> = Vec::new();
    let mut sends = 0usize;
    if raw.len() > MAX_STICKER_OPS {
        rejections.push(RejectReason::TooManyOps);
    }
    for op in raw.into_iter().take(MAX_STICKER_OPS) {
        let kind = match op.op.as_deref() {
            // Keep/ignore belong to the sticker_keep MCP tool, not this JSON.
            Some("keep") | Some("ignore") => {
                rejections.push(RejectReason::Forbidden);
                continue;
            }
            Some("send") => StickerOpKind::Send,
            // Anything else is forbidden: the model cannot delete or rename.
            Some(_) => {
                rejections.push(RejectReason::Forbidden);
                continue;
            }
            None => {
                rejections.push(RejectReason::Malformed);
                continue;
            }
        };
        // `send` may name something already in her library, which the caller
        // resolves against the database — validation only checks the shape here.
        let raw_hash = op.hash.unwrap_or_default().trim().to_ascii_lowercase();
        if !is_sticker_hash(&raw_hash) {
            rejections.push(RejectReason::Malformed);
            continue;
        }
        let hash = raw_hash;
        sends += 1;
        if sends > MAX_STICKER_SENDS {
            rejections.push(RejectReason::TooManyOps);
            continue;
        }
        // The same hash twice is a model slip, not two actions: the first wins.
        if out.iter().any(|existing| existing.hash == hash) {
            continue;
        }
        let reason = op
            .reason
            .unwrap_or_default()
            .trim()
            .chars()
            .filter(|ch| !ch.is_control())
            .take(MAX_STICKER_REASON_CHARS)
            .collect::<String>();
        out.push(StickerOp {
            op: kind,
            hash,
            reason,
        });
    }
    out
}

/// A sticker hash is `sha256` hex: up to 64 lowercase characters.
pub fn is_sticker_hash(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= STICKER_HASH_CHARS
        && raw
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(super) fn validate_nicknames(
    raw: Vec<NicknameRaw>,
    scope: &ValidationScope,
    rejections: &mut Rejections,
) -> Vec<NicknameProposal> {
    let mut out = Vec::new();
    for nick in raw.into_iter().take(8) {
        let Some(subject_ref) = nick.subject_ref.filter(|s| !s.is_empty()) else {
            rejections.push(RejectReason::UnknownRef);
            continue;
        };
        if !scope.speaker_refs.iter().any(|r| r == &subject_ref) {
            rejections.push(RejectReason::UnknownRef);
            continue;
        }
        // `sanitize_alias` already truncated to `MAX_ALIAS_CHARS` and trimmed, so the
        // only outcome left to check is emptiness. A "too long" branch here used to
        // sit after the truncation and could never fire.
        let alias = sanitize_alias(nick.alias.unwrap_or_default());
        if alias.is_empty() {
            continue;
        }
        out.push(NicknameProposal { subject_ref, alias });
    }
    out
}

/// Longest alias kept: a display name, not a description.
pub const MAX_ALIAS_CHARS: usize = 16;

/// Normalise a proposed alias: drop control characters, then truncate to
/// [`MAX_ALIAS_CHARS`] characters, then trim the ends.
pub(super) fn sanitize_alias(raw: String) -> String {
    raw.chars()
        .filter(|ch| !ch.is_control() && !matches!(ch, '\u{2028}' | '\u{2029}' | '\u{feff}'))
        .take(MAX_ALIAS_CHARS)
        .collect::<String>()
        .trim()
        .to_string()
}

/// Validate per-part quotes against the scope: the ref must have been
/// shown this turn (batch `m…` or history `h…`) and the echo must stay a
/// snippet, not a paragraph. Bad entries drop softly, like a bad top-level
/// `quote_ref`.
pub(super) fn validate_part_quotes(
    raw: Vec<(usize, crate::agent::segments::RawPartQuote)>,
    scope: &ValidationScope,
    rejections: &mut Rejections,
) -> Vec<PartQuote> {
    let mut out = Vec::new();
    for (part_index, quote) in raw {
        let trimmed = quote.quote_ref.trim().to_string();
        let known = scope.message_refs.iter().any(|r| r == &trimmed)
            || scope.history_refs.iter().any(|r| r == &trimmed);
        if !known {
            rejections.push(RejectReason::UnknownRef);
            continue;
        }
        if quote
            .quote_echo
            .as_deref()
            .is_some_and(|e| e.chars().count() > 24)
        {
            rejections.push(RejectReason::Malformed);
            continue;
        }
        out.push(PartQuote {
            part_index,
            quote_ref: trimmed,
            quote_echo: quote.quote_echo,
        });
    }
    out
}

impl AgentOutput {
    pub(super) fn silent() -> Self {
        Self {
            should_reply: false,
            reply_text: String::new(),
            reply_parts: Vec::new(),
            nicknames: Vec::new(),
            mood_update: None,
            topic_state: None,
            memory_ops: Vec::new(),
            sticker_ops: Vec::new(),
            quote_ref: None,
            part_quotes: Vec::new(),
            poke_ref: None,
        }
    }
}

pub(super) fn validate_op(
    op: MemoryOpRaw,
    scope: &ValidationScope,
    rejections: &mut Rejections,
) -> Option<MemoryOp> {
    let kind_op = match op.op.as_deref() {
        Some("add") => MemoryOpKind::Add,
        Some("update") => MemoryOpKind::Update,
        Some("retire") => MemoryOpKind::Retire,
        // Hard `delete` and anything else is forbidden: retirement is a state
        // transition (see `retire`); only the panel may actually delete.
        Some(_) => {
            rejections.push(RejectReason::Forbidden);
            return None;
        }
        None => {
            rejections.push(RejectReason::Malformed);
            return None;
        }
    };

    let content = op.content.unwrap_or_default().trim().to_string();
    if content.is_empty() {
        rejections.push(RejectReason::Malformed);
        return None;
    }
    if content.chars().count() > MAX_MEMORY_CONTENT_CHARS {
        rejections.push(RejectReason::TooLong);
        return None;
    }
    if contains_forbidden_markup(&content) {
        rejections.push(RejectReason::Forbidden);
        return None;
    }

    let kind = op.kind.unwrap_or_default().trim().to_string();
    if !MEMORY_KINDS.contains(&kind.as_str()) {
        rejections.push(RejectReason::BadKind);
        return None;
    }

    let confidence = match op.confidence {
        Some(c) if (0.0..=1.0).contains(&c) && c.is_finite() => c,
        Some(_) => {
            rejections.push(RejectReason::BadConfidence);
            return None;
        }
        // A missing confidence is treated as low confidence rather than trusted.
        None => 0.3,
    };

    let source_refs = op.source_refs.unwrap_or_default();
    if source_refs.is_empty() {
        rejections.push(RejectReason::NoSource);
        return None;
    }
    if source_refs.len() > MAX_SOURCE_REFS {
        rejections.push(RejectReason::TooLong);
        return None;
    }
    for r in &source_refs {
        if !scope.message_refs.iter().any(|m| m == r) {
            // A ref this batch never contained: either invented or cross-group.
            rejections.push(if looks_like_raw_id(r) {
                RejectReason::CrossGroup
            } else {
                RejectReason::UnknownRef
            });
            return None;
        }
    }

    let subject_ref = match op.subject_ref {
        None => None,
        Some(s) => {
            let s = s.trim().to_string();
            if s.is_empty() {
                None
            } else if !scope.speaker_refs.iter().any(|r| r == &s) {
                rejections.push(if looks_like_raw_id(&s) {
                    RejectReason::CrossGroup
                } else {
                    RejectReason::UnknownRef
                });
                return None;
            } else {
                Some(s)
            }
        }
    };

    let memory_ref = match kind_op {
        MemoryOpKind::Add => {
            // A caller-supplied ref for a new fact would let the model choose ids.
            if op.memory_ref.is_some() {
                rejections.push(RejectReason::Forbidden);
                return None;
            }
            None
        }
        MemoryOpKind::Update | MemoryOpKind::Retire => match op.memory_ref {
            None => {
                rejections.push(RejectReason::MissingTarget);
                return None;
            }
            Some(r) => match normalize_memory_ref(&r) {
                MemoryRefKind::Store(normalized) => Some(normalized),
                MemoryRefKind::RawId => {
                    rejections.push(RejectReason::CrossGroup);
                    return None;
                }
                MemoryRefKind::Unknown => {
                    rejections.push(RejectReason::UnknownRef);
                    return None;
                }
            },
        },
    };

    // A group-level add (no subject) whose text leans on an unnamed
    // authority — 主人 / 管理员 / 群主 said or decided something — files a
    // claim nobody can attribute later. 2026-11-12 audit: an unnamed
    // 「管理员」 exemption sat in one group's memory while the words were
    // actually the owner's, and the bot argued about whose it was for
    // nine days. Naming the person at write time is cheap; do it there.
    if matches!(kind_op, MemoryOpKind::Add)
        && subject_ref.is_none()
        && ["主人", "管理员", "群主"]
            .iter()
            .any(|w| content.contains(w))
    {
        rejections.push(RejectReason::DanglingAuthority);
        return None;
    }

    // Sealed topics ([seal-2]): the operator has declared this topic dead
    // for the group, so it may not be (re)written — neither as a fresh add
    // nor smuggled into an update's replacement text. The applier repeats
    // the check at the store door for callers that skip validation.
    if matches!(kind_op, MemoryOpKind::Add | MemoryOpKind::Update)
        && crate::memory::sealed::hit(
            &scope.sealed_topics,
            op.slot.as_deref().unwrap_or_default(),
            &content,
        )
        .is_some()
    {
        rejections.push(RejectReason::SealedTopic);
        return None;
    }

    // The v1 fields are optional metadata: sanitise them, never reject over them.
    let (slot, tags) = crate::memory::schema::sanitize_slot_tags(
        op.slot.as_deref().unwrap_or_default(),
        &op.tags.unwrap_or_default(),
    );
    let ttl = op
        .ttl
        .as_deref()
        .and_then(crate::memory::schema::Ttl::parse);
    Some(MemoryOp {
        op: kind_op,
        subject_ref,
        content,
        kind,
        source_refs,
        confidence,
        memory_ref,
        slot: if slot.is_empty() { None } else { Some(slot) },
        tags,
        ttl,
    })
}

/// Reject markup the bot must never relay: CQ codes, mass mentions, and `<@id>`
/// style directives that the OneBot implementation might interpret.
pub fn contains_forbidden_markup(text: &str) -> bool {
    const FORBIDDEN: [&str; 8] = [
        "[CQ:",
        "[cq:",
        "@全体成员",
        "@all",
        "@All",
        "@ALL",
        "<@",
        "[at:",
    ];
    FORBIDDEN.iter().any(|needle| text.contains(needle))
}

/// One outbound content policy, shared by strict validation, partial salvage,
/// plain-text rescue and delayed captions. Return only a reason code, never text.
pub fn reply_content_rejection(text: &str) -> Option<&'static str> {
    let normalized = normalize_reply_screen(text);
    let lower = normalized.to_ascii_lowercase();
    if contains_forbidden_markup(&lower) {
        return Some("forbidden");
    }
    const CONTRACT_MARKERS: &[&str] = &[
        "should_reply",
        "reply_parts",
        "reply_text",
        "memory_ops",
        "sticker_ops",
        "nicknames",
        "quote_ref",
        "quote_echo",
        "mood_update",
        "topic_update",
        "poke_ref",
        "turn_token",
        "call_mcp_tool",
    ];
    if CONTRACT_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return Some("contract_marker");
    }
    is_submission_acknowledgement(&normalized).then_some("submission_ack")
}

/// Check individual parts first, then minimal contiguous combinations. Already
/// rejected parts cannot cause neighboring ordinary prose to be discarded too.
/// Callers bound the list by MAX_REPLY_PARTS before this quadratic scan.
pub(crate) fn reply_part_rejections(parts: &[String]) -> Vec<Option<&'static str>> {
    let mut reasons: Vec<_> = parts
        .iter()
        .map(|part| reply_content_rejection(part))
        .collect();
    for width in 2..=parts.len() {
        for start in 0..=parts.len() - width {
            let end = start + width;
            let joined = parts[start..end].concat();
            // Include fragments and badges surrounding a pure acknowledgement,
            // even if a shorter subspan was already rejected. Unlike phrase
            // matching, the vocabulary check cannot absorb ordinary prose.
            if is_acknowledgement_by_composition(&joined) {
                reasons[start..end].fill(Some("submission_ack"));
                continue;
            }
            if reasons[start..end].iter().any(Option::is_some) {
                continue;
            }
            if let Some(reason) = reply_content_rejection(&joined) {
                reasons[start..end].fill(Some(reason));
            }
        }
    }
    reasons
}

fn normalize_reply_screen(text: &str) -> String {
    text.chars()
        .filter(|c| {
            !c.is_control()
                && !matches!(
                    c,
                    '\u{200b}'
                        | '\u{200c}'
                        | '\u{200d}'
                        | '\u{2060}'
                        | '\u{feff}'
                        | '\u{fe0e}'
                        | '\u{fe0f}'
                )
        })
        .collect()
}

/// The agent narrating its own submission machinery — "已提交本轮回复。",
/// "已记录本轮提交" — is not a chat reply. `本轮` (this turn) is contract
/// vocabulary that group chat never uses, so the screen keys on the turn
/// word paired with a submission verb, plus the bare acknowledgement forms
/// and the tool's own name. Ordinary chat that merely mentions handing
/// something in does not trip it.
///
/// The phrase shapes alone leaked twice more (2026-11-11
/// "本轮已成功提交回复。", 2026-11-13 "回复已提交完毕。"): the narrator
/// rewords itself around any list. So [`is_acknowledgement_by_composition`]
/// also screens what a narration is *made of*, not just how it is phrased.
pub fn is_submission_acknowledgement(text: &str) -> bool {
    let core = acknowledgement_core(text);
    let t = core.as_str();
    const BARE: [&str; 3] = ["已提交", "已记录", "提交完成"];
    if BARE.contains(&t) || t.contains("submit_reply") {
        return true;
    }
    if (t.contains("本轮回复") || t.contains("本轮提交"))
        && (t.contains("提交") || t.contains("记录") || t.contains("发送"))
    {
        return true;
    }
    is_acknowledgement_by_composition(t)
}

fn acknowledgement_core(text: &str) -> String {
    let normalized = normalize_reply_screen(text);
    // A finite decoration set, not arbitrary emoji stripping: ordinary prose
    // retains its words, while simple acknowledgement badges cannot hide it.
    let t = normalized.trim_matches(|c: char| {
        c.is_whitespace()
            || c.is_ascii_punctuation()
            || matches!(
                c,
                '。' | '！'
                    | '？'
                    | '～'
                    | '，'
                    | '、'
                    | '；'
                    | '：'
                    | '“'
                    | '”'
                    | '「'
                    | '」'
                    | '✅'
                    | '✔'
                    | '☑'
                    | '👍'
                    | '👌'
                    | '📨'
                    | '📤'
                    | '📩'
                    | '✉'
                    | '💬'
            )
    });
    t.to_owned()
}

/// Composition screen for submission narration: after dropping sentence
/// punctuation, a short text (at most 16 chars) built *only* from the
/// narration vocabulary, carrying at least one submission verb
/// (提交/发送/记录) and at least one object-or-completion marker
/// (回复/已/完毕/完成/成功), is the agent talking to the system in some
/// word order — never a chat reply. Real prose about handing something
/// in always carries words outside the vocabulary （作业/老师/一下…),
/// which is what keeps it deliverable.
fn is_acknowledgement_by_composition(text: &str) -> bool {
    const VOCABULARY: &str = "本轮回复提交发送记录已成功完毕完成更新";
    let normalized = acknowledgement_core(text);
    let core: String = normalized
        .chars()
        .filter(|c| {
            !matches!(
                c,
                '，' | '。' | '、' | '；' | '：' | '·' | '…' | '—' | ' ' | '　'
            )
        })
        .collect();
    let core = core.trim_end_matches(|c| matches!(c, '了' | '啦' | '哦' | '呢'));
    let length = core.chars().count();
    if length == 0 || length > 16 {
        return false;
    }
    if !core.chars().all(|c| VOCABULARY.contains(c)) {
        return false;
    }
    let has_verb = core.contains("提交") || core.contains("发送") || core.contains("记录");
    let has_marker = core.contains("回复")
        || core.contains("已")
        || core.contains("完毕")
        || core.contains("完成")
        || core.contains("成功");
    has_verb && has_marker
}

/// A raw QQ id (all digits, 5..=20 long) appearing where an alias belongs means the
/// model tried to reach across the boundary.
pub(super) fn looks_like_raw_id(candidate: &str) -> bool {
    candidate.len() >= 5 && candidate.len() <= 20 && candidate.bytes().all(|b| b.is_ascii_digit())
}

/// How an update `memory_ref` parsed.
enum MemoryRefKind {
    /// A store key (`mem_{hex}`), optionally prefixed with `mem:`.
    Store(String),
    /// Looks like a QQ number.
    RawId,
    /// Anything else (`u1`, `chunk:N`, `log:N`, `mem_other`, …).
    Unknown,
}

/// Strip an optional `mem:` prefix (old MCP labels) and accept only `mem_{hex}`.
pub(super) fn normalize_memory_ref(raw: &str) -> MemoryRefKind {
    let trimmed = raw.trim();
    let stripped = trimmed.strip_prefix("mem:").unwrap_or(trimmed);
    if looks_like_raw_id(stripped) {
        return MemoryRefKind::RawId;
    }
    let Some(rest) = stripped.strip_prefix("mem_") else {
        return MemoryRefKind::Unknown;
    };
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return MemoryRefKind::Unknown;
    }
    MemoryRefKind::Store(format!("mem_{}", rest.to_ascii_lowercase()))
}
