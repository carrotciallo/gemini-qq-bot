//! Sealed-topic matching ([seal-1]): the pure half of the sealed-topic
//! mechanism. Storage lives in the store (`list_sealed_topics`); every
//! enforcement point — contract validation, the memory applier,
//! consolidation, compaction, retrieval, spontaneous picks — funnels
//! through [`hit`] so the semantics stay identical everywhere.
//!
//! Why keywords and not just slots: a production cement saga (the case
//! this feature exists for) spread its rows across the slots 水泥梗 /
//! 鱼缸 / 鱼缸服务器 / 水泥墩子 / (empty). Slot-only sealing would leak;
//! keyword-only sealing can over-hit ordinary talk, so entries carry
//! both and the operator chooses per topic.

use crate::store::{MemoryRecord, SealedTopic};

/// The first sealed entry hitting this (slot, text) pair, if any.
///
/// A hit is: the entry names a slot and it equals `slot`, OR any of the
/// entry's keywords appears in `content`. Empty keywords never hit, and
/// keyword matching is a plain substring test — for Chinese group text
/// there are no word boundaries to lean on.
pub fn hit<'a>(topics: &'a [SealedTopic], slot: &str, content: &str) -> Option<&'a SealedTopic> {
    let slot = slot.trim();
    topics.iter().find(|topic| {
        (!topic.slot.is_empty() && topic.slot == slot)
            || topic
                .keywords
                .iter()
                .any(|k| !k.is_empty() && content.contains(k.as_str()))
    })
}

/// Drop every row a sealed topic claims. Used by the read paths
/// (retrieval inputs) as a belt-and-braces filter for legacy rows that
/// predate write-side enforcement.
pub fn retain_unsealed(rows: &mut Vec<MemoryRecord>, topics: &[SealedTopic]) {
    if topics.is_empty() {
        return;
    }
    rows.retain(|row| hit(topics, &row.slot, &row.content).is_none());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(keywords: &[&str], slot: &str) -> SealedTopic {
        SealedTopic {
            keywords: keywords.iter().map(|s| s.to_string()).collect(),
            slot: slot.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn keyword_hit_needs_no_slot() {
        let topics = [topic(&["水泥"], "")];
        assert!(hit(&topics, "鱼缸", "鱼缸被灌了水泥").is_some());
        assert!(hit(&topics, "", "今天聊装修材料").is_none());
    }

    #[test]
    fn slot_hit_needs_no_keyword() {
        let topics = [topic(&[], "水泥梗")];
        assert!(hit(&topics, "水泥梗", "毫无关键词的一句").is_some());
        assert!(hit(&topics, "别的槽", "毫无关键词的一句").is_none());
    }

    #[test]
    fn empty_entries_never_hit() {
        let topics = [topic(&[], ""), topic(&[""], "")];
        assert!(hit(&topics, "", "任意文本").is_none());
        assert!(hit(&[], "水泥梗", "灌水泥").is_none());
    }

    fn record(slot: &str, content: &str) -> MemoryRecord {
        MemoryRecord {
            memory_ref: "mem_x".into(),
            bot_id: "1".into(),
            group_id: "100".into(),
            subject_user_id: None,
            content: content.into(),
            kind: "fact".into(),
            source_message_ids: "[]".into(),
            confidence: 0.5,
            revision: 1,
            origin: "hy3".into(),
            archived_at: None,
            created_at: 1,
            updated_at: 1,
            slot: slot.into(),
            tags: Vec::new(),
            expires_at: None,
            state: "active".into(),
            superseded_by: None,
            state_changed_at: None,
        }
    }

    #[test]
    fn retain_unsealed_drops_only_hits() {
        let topics = [topic(&["水泥"], "")];
        let mut rows = vec![
            record("鱼缸", "鱼缸被灌了水泥"),
            record("观鸟", "今天认了一只夜鹭"),
        ];
        retain_unsealed(&mut rows, &topics);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slot, "观鸟");
    }
}
