//! Outbound delivery for TurnExecutor: pokes, part sending and outbox payload loading.
//! Moved out of turn.rs verbatim (std-split-turn-executor); logic unchanged.

use super::*;

impl TurnExecutor {
    /// Resolve one validated `poke_ref` to a QQ user id and fire a single
    /// `send_poke`.
    ///
    /// The ref is already proven batch-local by validation, so the only way this
    /// fails is a batch member with no resolvable sender id — a drop, not an
    /// error. A poke carries no outbox row (there is nothing to retry and nothing
    /// the group sees beyond the poke itself), so the audit trail is the only
    /// record: `agent.poke.sent` on success, `agent.poke.dropped.*` otherwise.
    pub(super) async fn send_poke(
        &self,
        messages: &[IncomingMessage],
        ref_id: &str,
        group_id: &str,
    ) -> bool {
        let Some(target) = messages.iter().find(|m| m.speaker_ref == ref_id) else {
            let _ = self
                .store
                .audit(
                    "system",
                    &format!("agent.poke.dropped.g{group_id}.not_in_batch"),
                    Some(&self.bot_id),
                    Some(group_id),
                    crate::now_unix(),
                )
                .await;
            return false;
        };
        let user_id = target.sender_id.clone();
        if user_id.is_empty() || user_id == self.bot_id {
            // Poking herself is a client-side no-op at best; a batch member with no
            // id cannot be poked at all. Both are drops with a reason.
            let why = if user_id.is_empty() { "no_id" } else { "self" };
            let _ = self
                .store
                .audit(
                    "system",
                    &format!("agent.poke.dropped.g{group_id}.{why}"),
                    Some(&self.bot_id),
                    Some(group_id),
                    crate::now_unix(),
                )
                .await;
            return false;
        }
        // Group and private routes differ only in whether `group_id` rides along:
        // the OneBot action derives the chat from it. A group poke names both ids;
        // without a group context only the target goes out.
        let params = if group_id.is_empty() {
            serde_json::json!({ "user_id": user_id })
        } else {
            serde_json::json!({ "user_id": user_id, "group_id": group_id })
        };
        let outcome = self
            .connection
            .call("send_poke", params, std::time::Duration::from_secs(10))
            .await;
        let action = match &outcome {
            crate::onebot::action::SendOutcome::Sent { .. } => {
                format!("agent.poke.sent.g{group_id}.ref{ref_id}")
            }
            crate::onebot::action::SendOutcome::Rejected { retcode, .. } => {
                format!("agent.poke.dropped.g{group_id}.rejected{retcode}")
            }
            crate::onebot::action::SendOutcome::Unknown { .. } => {
                format!("agent.poke.unknown.g{group_id}.ref{ref_id}")
            }
        };
        let _ = self
            .store
            .audit(
                "system",
                &action,
                Some(&self.bot_id),
                Some(group_id),
                crate::now_unix(),
            )
            .await;
        matches!(outcome, crate::onebot::action::SendOutcome::Sent { .. })
    }

    pub(super) async fn deliver_parts(
        &self,
        ids: &[String],
        gaps: &[u64],
        group_id: &str,
        batch_id: &str,
        deadline: std::time::Instant,
    ) {
        for (index, id) in ids.iter().enumerate() {
            if index > 0 {
                let gap = gaps.get(index - 1).copied().unwrap_or(800);
                tokio::time::sleep(std::time::Duration::from_millis(gap)).await;
            }
            if std::time::Instant::now() >= deadline
                || tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.deliver(id, group_id),
                )
                .await
                .is_err()
            {
                let _ = self
                    .store
                    .seal_pending_outbox(&self.bot_id, group_id, batch_id, crate::now_unix())
                    .await;
                break;
            }
            let sent = self
                .store
                .get_outbox(id)
                .await
                .ok()
                .flatten()
                .is_some_and(|row| row.state == "sent");
            if !sent {
                let _ = self
                    .store
                    .seal_pending_outbox(&self.bot_id, group_id, batch_id, crate::now_unix())
                    .await;
                break;
            }
        }
    }

    /// Deliver one outbox row.
    pub async fn deliver(&self, outbox_id: &str, group_id: &str) {
        let now = crate::now_unix();
        if self.outbox.begin(outbox_id, now).await.is_err() {
            return;
        }
        // The row is reread so a crash-restart cannot lose the payload. A sticker row
        // stores only its hash; the bytes are read from the sticker directory now.
        let row = match self.store.get_outbox(outbox_id).await {
            Ok(Some(row)) => row,
            _ => {
                let _ = self
                    .outbox
                    .finish(
                        outbox_id,
                        &DeliveryResult::Unknown {
                            detail: "row-missing".into(),
                        },
                        crate::now_unix(),
                    )
                    .await;
                return;
            }
        };
        let frame = if row.kind == "sticker" {
            let Some(hash) = row.sticker_hash.as_deref() else {
                let _ = self
                    .outbox
                    .finish(
                        outbox_id,
                        &DeliveryResult::Unknown {
                            detail: "sticker-row-without-hash".into(),
                        },
                        crate::now_unix(),
                    )
                    .await;
                return;
            };
            let path = self.sticker_dir.join(format!("{hash}.jpg"));
            match tokio::fs::read(&path).await {
                Ok(bytes) if !bytes.is_empty() => {
                    let Some((msg, _payload)) =
                        crate::onebot::action::SendGroupMsg::sticker_tracked(
                            group_id,
                            &bytes,
                            row.quote_message_id.as_deref(),
                        )
                    else {
                        let _ = self
                            .outbox
                            .finish(
                                outbox_id,
                                &DeliveryResult::Unknown {
                                    detail: format!("sticker-unbuildable:{hash}"),
                                },
                                crate::now_unix(),
                            )
                            .await;
                        return;
                    };
                    msg
                }
                // The file vanished or is empty: the picture cannot be sent, and
                // guessing is worse than an honest unknown.
                _ => {
                    let _ = self
                        .outbox
                        .finish(
                            outbox_id,
                            &DeliveryResult::Unknown {
                                detail: format!("sticker-bytes-missing:{hash}"),
                            },
                            crate::now_unix(),
                        )
                        .await;
                    return;
                }
            }
        } else if row.kind == "image" {
            // A generated picture: `text` holds the bare file name under the
            // imagegen output directory. Anything that is not a bare file
            // name is refused outright — the row must never become a path
            // traversal into arbitrary files.
            let name = row.text.as_str();
            let valid_name = !name.is_empty()
                && !name.contains('/')
                && !name.contains('\\')
                && !name.contains("..");
            let bytes = if valid_name {
                tokio::fs::read(self.imagegen_dir.join(name))
                    .await
                    .ok()
                    .filter(|b| !b.is_empty())
            } else {
                None
            };
            match bytes.and_then(|b| crate::onebot::action::SendGroupMsg::image(group_id, &b)) {
                Some(msg) => msg,
                None => {
                    let _ = self
                        .outbox
                        .finish(
                            outbox_id,
                            &DeliveryResult::Unknown {
                                detail: format!("image-bytes-missing:{name}"),
                            },
                            crate::now_unix(),
                        )
                        .await;
                    return;
                }
            }
        } else {
            match row.quote_message_id.as_deref() {
                Some(target) => {
                    crate::onebot::action::SendGroupMsg::text_quoted(group_id, &row.text, target)
                }
                None => crate::onebot::action::SendGroupMsg::text(group_id, &row.text),
            }
        };
        // A picture frame carries ~1 MB of base64 the OneBot side uploads
        // to QQ before it can answer; the first production image send blew
        // the 10 s text budget and landed `unknown` (2026-11-11). Pictures
        // get the longer budget, text keeps the tight one.
        let send_timeout = if row.kind == "image" {
            std::time::Duration::from_secs(45)
        } else {
            std::time::Duration::from_secs(10)
        };
        let outcome = self
            .connection
            .call("send_group_msg", frame.params(), send_timeout)
            .await;
        let result = crate::pipeline::outbox::classify(outcome);
        let _ = self
            .outbox
            .finish(outbox_id, &result, crate::now_unix())
            .await;
        if let DeliveryResult::Sent { message_id } = &result {
            let mid = message_id
                .clone()
                .unwrap_or_else(|| format!("bot:{outbox_id}"));
            let text = if row.kind == "sticker" {
                "[表情]".to_string()
            } else if row.kind == "image" {
                "[图片]".to_string()
            } else {
                row.text.clone()
            };
            let _ = self
                .store
                .insert_group_message(
                    &self.bot_id,
                    group_id,
                    &mid,
                    &self.bot_id,
                    None,
                    SpeakerRole::Bot.as_str(),
                    &text,
                    row.kind == "sticker",
                    row.sticker_hash.as_deref(),
                    None,
                    crate::now_unix(),
                )
                .await;
        }
    }

    pub(super) async fn load_payloads(&self, event_ids: &[i64]) -> Option<Vec<String>> {
        let mut out = Vec::with_capacity(event_ids.len());
        for id in event_ids {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT normalized_payload FROM pending_events WHERE id = ?")
                    .bind(id)
                    .fetch_optional(self.store.pool())
                    .await
                    .ok()?;
            if let Some((payload,)) = row {
                out.push(payload);
            }
        }
        Some(out)
    }

    /// Memories and members for one group. The rolling summary is intentionally not
    /// loaded: it is no longer injected, so fetching it would only cost a query.
    pub(super) async fn load_memory_inputs(
        &self,
        group_id: &str,
    ) -> (
        Vec<crate::store::MemoryRecord>,
        Vec<crate::store::MemberRecord>,
    ) {
        let mut memories = self
            .store
            .list_memories(&self.bot_id, group_id, 500)
            .await
            .unwrap_or_default();
        // Sealed topics ([seal-4]): legacy rows that predate write-side
        // enforcement must not reach the prompt through retrieval.
        match self.store.list_sealed_topics(&self.bot_id, group_id).await {
            Ok(sealed) => crate::memory::sealed::retain_unsealed(&mut memories, &sealed),
            Err(_) => memories.clear(),
        }
        let members = self
            .store
            .list_members(&self.bot_id, group_id)
            .await
            .unwrap_or_default();
        (memories, members)
    }
}
