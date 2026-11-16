//! Sticker preselection for a turn: trigger queries, diverse picks and
//! the A6 anti-repeat ranking (hard cooldown + frequency divisor) over
//! the group's kept shelf. Moved out of turn.rs verbatim (std-split-turn);
//! logic unchanged by the move.

/// How many pre-matched library stickers one prompt may offer (A1′).
pub(crate) const MAX_STICKER_OFFERS: usize = 3;

/// Cap on the short queries one batch scores the library against (A1′).
pub(crate) const MAX_OFFER_QUERIES: usize = 32;

/// How much of the kept shelf one batch's preselection scans (A1′).
pub(crate) const OFFER_SCAN_LIMIT: i64 = 500;

/// How many of a group's recent sends feed the A6 anti-repeat ranking,
/// shared by the offer preselection and `sticker_search`: the newest
/// [`OFFER_COOLDOWN_SENDS`] hard-exclude a sticker from the menu, and the
/// whole window feeds the frequency divisor's send counts.
pub(crate) const RECENT_SEND_SCAN_LIMIT: i64 = 100;

/// How many of a group's most recent sends hard-exclude a sticker from
/// the offer menu (A6). Counted in sends, not minutes, so the exclusion
/// tracks how much of a sticker the group has actually seen: any sticker
/// can be sent at most once per `OFFER_COOLDOWN_SENDS + 1` sends in a
/// group, which caps a single sticker's share near 5% no matter how
/// broadly it matches.
pub(crate) const OFFER_COOLDOWN_SENDS: usize = 20;

/// Wording that implies an emotion outranks nothing at all (A2): each hit adds
/// its phrase list to the query set. These never select a sticker directly —
/// the phrases are scored like any other query, so a trigger can only widen what
/// the library is compared against, never overrule the scoring.
pub(crate) const EMOTION_TRIGGERS: &[(&[&str], &str)] = &[
    (&["哈哈哈", "笑死", "笑拉"], "大笑 得意 俏皮"),
    (&["绉号", "外号", "叫错"], "气鼓鼓 假装生气 质问"),
    (&["无语", "服了", "醉了"], "无语 翻白眼"),
    (&["真的假的", "离谱", "震惊"], "震惊 难以置信 求证"),
    (&["谢谢", "谢了"], "开心 感谢"),
    (&["困了", "想睡", "晚安"], "困怔 想睡"),
    (&["吃瓜", "围观", "热闹"], "吃瓜 看热闹"),
    (&["厉害", "牛", "强"], "得意 点赞 认可"),
    (&["呜呜", "抱抱", "难过"], "委屈 安慰"),
    (&["急了", "破防"], "得意 俏皮 调侃"),
];

/// Extra queries implied by a batch's wording (A2), in table order.
pub(crate) fn trigger_queries<S: AsRef<str>>(texts: &[S]) -> Vec<String> {
    let mut out = Vec::new();
    for (needles, phrases) in EMOTION_TRIGGERS {
        let hit = texts.iter().any(|text| {
            let text = text.as_ref();
            needles.iter().any(|needle| text.contains(needle))
        });
        if hit {
            out.push((*phrases).to_string());
        }
    }
    out
}

/// Leading emotion tag: the part before any label separator (A3).
pub(crate) fn emotion_bucket(emotion: &str) -> String {
    emotion
        .split(['、', '，', ','])
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

/// At most [`MAX_STICKER_OFFERS`] picks, greedy on the already-sorted order but
/// skipping a pick whose leading emotion is already taken, then filling any
/// remaining slots from the leftovers (A3). Sorted input means the highest
/// scores survive whenever diversity and score disagree.
pub(crate) fn pick_diverse<'a>(
    scored: &[(f64, &'a crate::store::StickerRecord)],
) -> Vec<&'a crate::store::StickerRecord> {
    let mut picked: Vec<&crate::store::StickerRecord> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for (_, record) in scored {
        if picked.len() == MAX_STICKER_OFFERS {
            break;
        }
        let bucket = emotion_bucket(&record.emotion);
        if seen.contains(&bucket) {
            continue;
        }
        seen.push(bucket);
        picked.push(record);
    }
    for (_, record) in scored {
        if picked.len() == MAX_STICKER_OFFERS {
            break;
        }
        if picked.iter().any(|kept| kept.hash == record.hash) {
            continue;
        }
        picked.push(record);
    }
    picked
}

/// Rank the over-threshold rows for the offer menu (A6, 2026-11-13).
///
/// Replaces the A5 arrangement (a flat ×0.25 damping, plus the A4 habit
/// slot that re-offered the group's most-sent sticker every round): in
/// group, one broadly-matching self-sticker still took 30% of
/// two weeks' sends, because a fixed discount is eaten by a strong raw
/// score and the habit slot kept putting the same face back on the menu.
///
/// Two layers now, each with one job:
/// * the hard cooldown (hashes in `cooldown`, the group's newest
///   [`OFFER_COOLDOWN_SENDS`] sends) keeps a just-seen sticker off the
///   menu entirely — the share cap;
/// * the frequency divisor ([`crate::memory::stickers::divide_by_usage`]
///   over `counts`, sends inside the [`RECENT_SEND_SCAN_LIMIT`] window)
///   ranks what remains, so a favourite returns gradually as its old
///   sends age out of the window instead of snapping back at full score.
///
/// Only rows whose divided score still clears `OFFER_MIN_SCORE` rank.
/// An empty result stays empty: neither cooldown nor the divisor can be
/// bypassed to fill the menu. Ties break on hash for deterministic ordering.
pub(crate) fn rank_offer_candidates<'a>(
    raw: &[(f64, &'a crate::store::StickerRecord)],
    cooldown: &[String],
    counts: &std::collections::HashMap<String, i64>,
) -> Vec<(f64, &'a crate::store::StickerRecord)> {
    let cooled = |hash: &str| cooldown.iter().any(|seen| seen == hash);
    let sort_ranked = |mut rows: Vec<(f64, &'a crate::store::StickerRecord)>| {
        rows.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.hash.cmp(&b.1.hash))
        });
        rows
    };
    let fresh: Vec<(f64, &crate::store::StickerRecord)> = raw
        .iter()
        .filter(|(_, record)| !cooled(&record.hash))
        .map(|(score, record)| {
            let sends = counts.get(&record.hash).copied().unwrap_or(0);
            (
                crate::memory::stickers::divide_by_usage(*score, sends),
                *record,
            )
        })
        .filter(|(score, _)| *score >= crate::memory::stickers::OFFER_MIN_SCORE)
        .collect();
    sort_ranked(fresh)
}

/// One-line, length-capped summary of a quoted message's logged text.
pub(crate) fn quote_summary(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(40)
        .collect()
}

/// First non-blank of `candidates`, else `fallback`.
pub(crate) fn first_label(candidates: &[&str], fallback: &str) -> String {
    candidates
        .iter()
        .map(|s| s.trim())
        .find(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| fallback.to_string())
}
