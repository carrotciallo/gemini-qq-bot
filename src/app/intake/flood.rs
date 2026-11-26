//! Flood handling for Intake: pool draining, trim, staged background
//! and the overflow-summary helpers. Moved out of intake.rs verbatim
//! (std-split-intake); logic unchanged.

use super::*;

impl Intake {
    /// Take (and clear) a group's staged flood background: exactly one
    /// claimed job carries it, like the flood mark beside it.
    pub(super) fn take_flood_background(&self, group_id: &str) -> Option<String> {
        match self.flood_backgrounds.lock() {
            Ok(mut map) => map.remove(group_id),
            Err(_) => None,
        }
    }

    /// Drain a flooded pool when this tick cannot serve any claim for the
    /// group (blown fuse or a pacing cap of zero). Without this the pool
    /// sits growing behind the fuse until the age guardrail eats it
    /// minutes later, and the first claim after recovery starts from the
    /// oldest rows again. The survivors stay pending; the flood mark
    /// flags the recovery claim so its prompt can demote the drained
    /// context to background.
    pub(super) async fn drain_pool_if_flooded(
        &self,
        group_id: &str,
        rows: Vec<crate::store::PendingEventRow>,
        now: i64,
    ) {
        if rows.len() <= FLOOD_POOL_THRESHOLD {
            return;
        }
        let (_kept, drained) = self.apply_flood_trim(group_id, rows, now).await;
        if drained {
            self.mark_flooded(group_id);
        }
    }

    /// Remember that a group's pool was drained, until its next claim.
    pub(super) fn mark_flooded(&self, group_id: &str) {
        if let Ok(mut marks) = self.flood_marks.lock() {
            marks.insert(group_id.to_string());
        }
    }

    /// Take (and clear) a group's flood mark: exactly one claim episode
    /// after a drain is flagged, later claims are ordinary again.
    pub(super) fn take_flood_mark(&self, group_id: &str) -> bool {
        match self.flood_marks.lock() {
            Ok(mut marks) => marks.remove(group_id),
            Err(_) => false,
        }
    }

    /// Drain a flooded pending pool down to its live tail, returning the
    /// survivors and whether this tick drained (or merely saw) a flood.
    ///
    /// The live tail is the newest [`FLOOD_LIVE_MAX`] rows, extended
    /// backwards to include the latest addressed row (a mention of the bot
    /// or the owner speaking) and everything after it: a direct question is
    /// never demoted to background, and neither is the conversation it
    /// started. Drained rows are deleted and audited once — the same
    /// delete-then-record contract as the stale drop — because their bodies
    /// remain in the group log and in the model's context window; only
    /// their candidacy for a paid reply ends.
    pub(super) async fn apply_flood_trim(
        &self,
        group_id: &str,
        rows: Vec<crate::store::PendingEventRow>,
        now: i64,
    ) -> (Vec<crate::store::PendingEventRow>, bool) {
        if !self.limits.flood_enabled || rows.len() <= FLOOD_POOL_THRESHOLD {
            return (rows, false);
        }
        let entries = entries_from_rows(&rows);
        let mut keep_from = rows.len().saturating_sub(FLOOD_LIVE_MAX);
        if let Some(latest_addressed) = entries.iter().rposition(|e| e.mentions_bot || e.from_owner)
        {
            keep_from = keep_from.min(latest_addressed);
        }
        if keep_from == 0 {
            return (rows, true);
        }
        let (drop_rows, keep_rows) = rows.split_at(keep_from);
        let dropped: Vec<i64> = drop_rows.iter().map(|r| r.id).collect();
        let count = dropped.len();
        let live = keep_rows.len();
        if self.store.delete_pending(&dropped).await.is_ok() {
            let _ = self
                .store
                .audit(
                    "system",
                    &format!("intake.flood.drain.g{group_id}.overflow{count}.live{live}"),
                    Some(&self.bot_id),
                    Some(group_id),
                    now,
                )
                .await;
            self.stage_flood_background(group_id, drop_rows);
        }
        (keep_rows.to_vec(), true)
    }

    /// Stage the drained overflow as the group's next-job background
    /// (flood stage 4): the script-truncated rendering, staged
    /// synchronously. Until 2026-11-24 a gateway summary could replace
    /// the truncation when it landed in time; with the gateway gone the
    /// truncation is the only rendering, which was always the fail-open
    /// floor of that design.
    pub(super) fn stage_flood_background(
        &self,
        group_id: &str,
        drop_rows: &[crate::store::PendingEventRow],
    ) {
        let overflow_text = render_overflow_text(drop_rows);
        if overflow_text.trim().is_empty() {
            return;
        }
        let fallback = truncate_background(&overflow_text);
        if let Ok(mut map) = self.flood_backgrounds.lock() {
            map.insert(group_id.to_string(), fallback);
        }
    }

    /// Delete stale rows and leave one audit row saying how many and how old.
    pub(super) async fn drop_stale_rows(
        &self,
        group_id: &str,
        stale: &[crate::store::PendingEventRow],
        now: i64,
    ) {
        if stale.is_empty() {
            return;
        }
        let ids: Vec<i64> = stale.iter().map(|r| r.id).collect();
        let oldest_age = stale.iter().map(|r| now - r.received_at).max().unwrap_or(0);
        if self.store.delete_pending(&ids).await.is_ok() {
            let _ = self
                .store
                .audit(
                    "system",
                    &format!(
                        "scheduler.stale_drop.g{group_id}.n{}.age{oldest_age}",
                        stale.len()
                    ),
                    Some(&self.bot_id),
                    Some(group_id),
                    now,
                )
                .await;
        }
    }
}

/// Per-line cap when rendering overflow rows for the summary input.
pub const FLOOD_LINE_CHARS: usize = 200;

/// Cap on the rendered overflow text fed to the summary call.
pub const FLOOD_OVERFLOW_MAX_CHARS: usize = 8000;

/// Cap on the background block itself, whichever source produced it: the
/// summary is asked for ~300 chars and hard-capped here, and the
/// script-truncated fallback is cut to the same budget.
pub const FLOOD_BACKGROUND_MAX_CHARS: usize = 1000;

/// Render drained overflow rows as plain `name：text` lines for the
/// summary input (and, truncated, for the fail-open background). Rows
/// arrive oldest first; rows whose payload no longer parses, and
/// image-only rows with no text, contribute nothing.
pub(super) fn render_overflow_text(rows: &[crate::store::PendingEventRow]) -> String {
    let mut out = String::new();
    for row in rows {
        let message: Option<crate::onebot::normalize::IncomingMessage> =
            serde_json::from_str(&row.normalized_payload).ok();
        let Some(message) = message else { continue };
        let text = message.text.trim();
        if text.is_empty() {
            continue;
        }
        let name = if message.display_name.trim().is_empty() {
            "群友"
        } else {
            message.display_name.trim()
        };
        let text: String = text.chars().take(FLOOD_LINE_CHARS).collect();
        out.push_str(&format!("{name}：{text}\n"));
        if out.chars().count() >= FLOOD_OVERFLOW_MAX_CHARS {
            break;
        }
    }
    out.trim_end().to_string()
}

/// The fail-open background: the overflow itself, truncated per line and
/// as a whole — the script rendering the summary bypass improves on when
/// it works.
pub(super) fn truncate_background(overflow_text: &str) -> String {
    let mut out = String::new();
    for line in overflow_text.lines() {
        let line: String = line.chars().take(120).collect();
        if out.chars().count() + line.chars().count() > FLOOD_BACKGROUND_MAX_CHARS {
            break;
        }
        out.push_str(&line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

/// A pending pool larger than this is a flood, not a backlog: the group is
/// producing messages faster than the hourly budget can ever answer them
/// one batch at a time. Working it oldest-first burns the fuse on answers
/// the freshness gate then discards, and while the fuse is blown even a
/// fresh direct question cannot be claimed — the queue blocks the replies
/// it exists to serve (2026-10-27: 900000003 sat at 60/60 calls with 37
/// rows pending and rising).
pub const FLOOD_POOL_THRESHOLD: usize = 20;

/// How many of the newest rows survive a flood drain as the live batch.
/// The rest are consumed as background: their bodies stay in the group log
/// (and so in the model's context window), they just stop being questions
/// that each demand a paid answer.
pub const FLOOD_LIVE_MAX: usize = 15;
