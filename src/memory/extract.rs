//! Shared reply-parsing helpers for the memory subsystem.
//!
//! This module used to run fold-time memory extraction: the fold request
//! asked the aux model for memory candidates alongside the chunk note, and
//! the same response was parsed into new `memories` rows. That pipeline was
//! retired in memfix [mf-3] (2026-11-11) when memory judgement moved to the
//! agent itself — new rows now come only from the agent's per-turn
//! `memory_ops` writes and the operator panel. What remains here is the
//! helper layer other memory modules already shared: JSON body slicing for
//! model replies, near-duplicate detection and subject normalisation (used
//! by consolidation), and the confidence ceiling constant.

use serde_json::Value;

use crate::memory::retrieval::{bigrams, overlap_score};

/// Hard ceiling on extracted confidence.
///
/// Extraction read a raw chat log, not a reviewed statement, so it never
/// claimed more certainty than the agent contract can: a candidate was
/// capped below what a deliberate per-turn write may assert. Consolidation
/// still applies the same ceiling to rows of extraction provenance.
pub const MAX_EXTRACTED_CONFIDENCE: f64 = 0.8;

/// Content bigram similarity at or above which a candidate counts as a duplicate.
/// 0.85 missed paraphrases of the same claim ("青蛙长文是希腊奶写的" versus
/// "希腊奶蛙写青蛙长文"). 0.55 still requires most of the bigrams to agree.
pub const NEAR_DUPLICATE_THRESHOLD: f64 = 0.55;

/// The JSON object inside a possibly fenced model reply.
///
/// Model replies across the memory subsystem share the fencing habit, so
/// the slicing lives in one place instead of being copied.
pub fn json_body(raw: &str) -> Option<Value> {
    let trimmed = raw.trim();
    let candidate = if trimmed.starts_with('{') {
        trimmed
    } else {
        let start = trimmed.find('{')?;
        let end = trimmed.rfind('}')?;
        if end <= start {
            return None;
        }
        &trimmed[start..=end]
    };
    serde_json::from_str(candidate).ok()
}

/// Normalise a subject for equality checks: case and spacing carry no meaning here.
pub fn normalize_subject(subject: &str) -> String {
    subject
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

/// True when `content` is the same fact as an existing memory's content.
pub fn is_near_duplicate(content: &str, existing: &str) -> bool {
    overlap_score(&bigrams(content), &bigrams(existing)) >= NEAR_DUPLICATE_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn near_duplicate_detection_uses_the_bigram_overlap() {
        assert!(is_near_duplicate(
            "每周五晚上一起打本",
            "每周五晚上一起打本"
        ));
        assert!(is_near_duplicate(
            "每周五晚上一起打本吧",
            "每周五晚上一起打本"
        ));
        assert!(!is_near_duplicate(
            "每周五晚上一起打本",
            "住在杭州喜欢喝咖啡"
        ));
    }

    #[test]
    fn subjects_are_normalised_before_comparison() {
        assert_eq!(normalize_subject(" U1 "), "u1");
        assert_eq!(normalize_subject("小林"), "小林");
    }
}
