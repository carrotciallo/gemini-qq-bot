//! Cadence and damper bookkeeping for Intake: verdict audits, wait
//! recording, burst shape observation and hand-over checks. Moved out
//! of intake.rs verbatim (std-split-intake); logic unchanged.

use super::*;

impl Intake {
    /// Write a cadence verdict to the audit log, but only when it changed.
    ///
    /// Without this the tick would insert two rows per group per second, which turns the
    /// audit table into a heartbeat and buries the events worth reading. With it, the log
    /// answers "why did this burst wait eight seconds" exactly once per episode.
    pub(super) async fn record_cadence(&self, group_id: &str, action: &str, now: i64) {
        let changed = match self.last_cadence.lock() {
            Ok(mut seen) => match seen.get(group_id) {
                Some(previous) if previous == action => false,
                _ => {
                    seen.insert(group_id.to_string(), action.to_string());
                    true
                }
            },
            Err(_) => false,
        };
        if changed {
            let _ = self
                .store
                .audit(
                    "system",
                    &format!("intake.cadence.{action}"),
                    Some(&self.bot_id),
                    Some(group_id),
                    now,
                )
                .await;
        }
    }

    /// Write one `wait.damper_*` row, at most once per
    /// [`DAMPER_WAIT_AUDIT_INTERVAL_SECONDS`] per `(group, reason)`.
    ///
    /// `record_cadence` alone cannot cover this: a held batch also produces a fresh
    /// `wait.*` / `hand_over.*` verdict every tick, so the per-group "last action" keeps
    /// flipping and every flip looks like a change. Throttling by time is what actually
    /// bounds the rows. Edge events (`enter.*`, `reset.*`, `cap.*`) go through
    /// [`Self::audit_damper_events`] and are never throttled: they fire once each.
    pub(super) async fn record_damper_wait(&self, group_id: &str, reason: &str, now: i64) {
        let allowed = match self.damper_wait_marks.lock() {
            Ok(mut marks) => {
                let key = (group_id.to_string(), reason.to_string());
                if should_record_damper_wait(marks.get(&key).copied(), now) {
                    marks.insert(key, now);
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        };
        if !allowed {
            return;
        }
        let _ = self
            .store
            .audit(
                "system",
                &format!("intake.cadence.wait.damper_{reason}"),
                Some(&self.bot_id),
                Some(group_id),
                now,
            )
            .await;
    }

    /// Fold this tick's pending rows into the damper's window and report the shape.
    ///
    /// The same rows are offered again every tick until they are claimed, which is why the
    /// state machine dedupes by row id: calling this unconditionally is safe, even for a
    /// group nowhere near a claim.
    ///
    /// Every input it cannot make sense of (a payload that will not parse, a failed query)
    /// counts as "not addressed" rather than as an error: the damper must never be able to
    /// hold a batch because something broke.
    pub(super) async fn observe_shape(
        &self,
        group_id: &str,
        now: i64,
        rows: &[crate::store::PendingEventRow],
    ) -> DamperShape {
        // One query per distinct quoted id per tick: a burst that quotes the same bot
        // message twice must not pay for it twice.
        let mut quoted_by_bot: HashMap<String, bool> = HashMap::new();
        let mut senders: HashMap<i64, (String, bool)> = HashMap::new();
        let mut spans: Vec<(i64, i64, bool, bool)> = Vec::new();
        let mut any_addressed = false;
        let mut any_strict = false;
        let mut strict_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for row in rows {
            let Ok(message) = serde_json::from_str::<IncomingMessage>(&row.normalized_payload)
            else {
                continue;
            };
            let from_owner = message.speaker_role == SpeakerRole::Owner;
            // Strict addressing first: an @, or a reply to something
            // the bot wrote. Both are structural — no model decides
            // them — which is what the addressed_only tier requires.
            let mut strict = message.mentions_bot;
            if !strict && let Some(quoted) = message.quote_of.as_deref() {
                let hit = match quoted_by_bot.get(quoted) {
                    Some(hit) => *hit,
                    None => {
                        let hit = self.quote_targets_bot(group_id, quoted).await;
                        quoted_by_bot.insert(quoted.to_string(), hit);
                        hit
                    }
                };
                strict = hit;
            }
            // Full addressing widens strict with the owner and a
            // text name-call; it is what exempts a batch at the
            // other tiers.
            let mut addressed = strict || from_owner;
            if !addressed && text_names_bot(&message.text, &self.call_names) {
                addressed = true;
            }
            any_addressed |= addressed;
            any_strict |= strict;
            if strict {
                strict_ids.insert(row.id);
            }
            senders.insert(row.id, (message.sender_id, addressed));
            spans.push((row.id, row.received_at, addressed, from_owner));
        }
        let observations: Vec<crate::app::damper::Observation<'_>> = spans
            .iter()
            .filter_map(|(id, at, addressed, from_owner)| {
                let (sender, _) = senders.get(id)?;
                Some(crate::app::damper::Observation {
                    id: *id,
                    sender: sender.as_str(),
                    at: *at,
                    addressed: *addressed,
                    from_owner: *from_owner,
                })
            })
            .collect();
        let (verdict, events) = self.damper.observe(group_id, now, &observations);
        self.audit_damper_events(group_id, now, &events).await;

        // Whoever this batch is mostly made of; ties resolve to the lowest sender id so
        // identical input cannot flip the answer between ticks.
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for (sender, _) in senders.values() {
            *counts.entry(sender.as_str()).or_insert(0) += 1;
        }
        let pending_from_primary = counts.values().copied().max().unwrap_or(0);
        let primary = counts
            .iter()
            .filter(|(_, count)| **count == pending_from_primary)
            .map(|(sender, _)| (*sender).to_string())
            .min()
            .unwrap_or_default();
        DamperShape {
            verdict,
            primary,
            pending_from_primary,
            addressed: any_addressed,
            strict_addressed: any_strict,
            strict_ids,
            senders,
        }
    }

    /// Whether a quoted message id was written by the bot.
    ///
    /// Only this group's own log is consulted, so a quote that points at nothing resolves
    /// to `false` and cannot talk the damper into exempting a batch.
    pub(super) async fn quote_targets_bot(&self, group_id: &str, message_id: &str) -> bool {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT speaker_role FROM group_messages
             WHERE bot_id = ? AND group_id = ? AND message_id = ? LIMIT 1",
        )
        .bind(&self.bot_id)
        .bind(group_id)
        .bind(message_id)
        .fetch_optional(self.store.pool())
        .await
        .ok()
        .flatten();
        row.map(|(role,)| role == "bot").unwrap_or(false)
    }

    /// Write the damper's edge-triggered events to the audit log.
    ///
    /// The state machine emits a transition once (entering a level, ending an episode,
    /// capping a sender), so these are already deduplicated upstream and cannot turn the
    /// table into a per-tick heartbeat the way a raw dump would.
    pub(super) async fn audit_damper_events(
        &self,
        group_id: &str,
        now: i64,
        events: &[crate::app::damper::Event],
    ) {
        for event in events {
            let action = match event {
                crate::app::damper::Event::Enter { tier, sender } => format!(
                    "intake.damper.enter.l{}.g{group_id}.s{sender}",
                    tier.level()
                ),
                crate::app::damper::Event::Reset { reason } => {
                    format!("intake.damper.reset.g{group_id}.{}", reason.as_str())
                }
                crate::app::damper::Event::Cap { sender } => {
                    format!("intake.damper.cap.g{group_id}.s{sender}")
                }
            };
            let _ = self
                .store
                .audit("system", &action, Some(&self.bot_id), Some(group_id), now)
                .await;
        }
    }

    /// Whether this burst is complete enough to send on.
    ///
    /// Hand-over decision for a burst that is not at the ceiling.
    ///
    /// There is no pacing judge any more (2026-11-24, owner's decision:
    /// the aux gate is gone). A burst is handed over as soon as the
    /// structural rules (damper, ceiling, budget) allow; whether the
    /// text looks finished is her judgement inside the turn now. This
    /// matches what production already did on ~98% of windows, where
    /// the dead judge answered Unavailable and bought no wait at all.
    ///
    /// The digest preparation stays: it is the memory pipeline's
    /// background summary for the group's next turn, launched here
    /// because this is where a burst's arrival is first observed. It
    /// finishes on its own budget; the turn falls back to raw entries
    /// when none is ready.
    pub(super) async fn burst_may_hand_over(
        &self,
        group_id: &str,
        rows: &[crate::store::PendingEventRow],
    ) -> (bool, &'static str) {
        if let Some(engine) = self.digest.clone() {
            let messages: Vec<IncomingMessage> = rows
                .iter()
                .filter_map(|r| serde_json::from_str::<IncomingMessage>(&r.normalized_payload).ok())
                .collect();
            if !messages.is_empty() {
                engine.prepare_background(group_id, messages);
            }
        }
        (true, "unavailable")
    }
}

/// A message names the bot: any configured call name appears in its
/// text. Members address her by name far more often than they @ her,
/// and a name in the text is as structural a signal as the @ itself.
/// ASCII case-insensitive; CJK names match as plain substrings.
pub(crate) fn text_names_bot(text: &str, names: &[String]) -> bool {
    if text.is_empty() || names.is_empty() {
        return false;
    }
    let lowered = text.to_lowercase();
    names.iter().any(|name| {
        let name = name.trim();
        !name.is_empty() && lowered.contains(&name.to_lowercase())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_names_bot_matches_names_case_insensitively() {
        let names = vec!["Gemini".to_string(), "小双".to_string()];
        assert!(text_names_bot("Gemini 在吗", &names));
        assert!(text_names_bot("gemini 快看这个", &names));
        assert!(text_names_bot("小双又睡着了", &names));
        assert!(!text_names_bot("今天 gem 好闪", &names));
        assert!(!text_names_bot("", &names));
        assert!(!text_names_bot("Gemini", &[]));
    }
}
