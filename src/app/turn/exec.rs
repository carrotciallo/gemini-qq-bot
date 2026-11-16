//! Batch entry point for TurnExecutor: run_batch and its failure bookkeeping.
//! Moved out of turn.rs verbatim (std-split-turn-executor); logic unchanged.

use super::*;

impl TurnExecutor {
    /// Execute one batch. Never returns an error: failures are encoded in
    /// [`TurnResult`] so the scheduler can decide whether to continue the lane.
    pub async fn run_batch(&self, job: &BatchJob) -> TurnResult {
        let turn_started = std::time::Instant::now();
        let deadline = turn_started
            + std::time::Duration::from_secs(self.resident_limits.hard_cap_s.max(1) as u64);
        // Register insertion ownership before the claim await: cancellation can
        // race a successful SQLite commit before its returned ids are observed.
        let mut insert_batches = Vec::new();
        let result = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.run_batch_inner(job, turn_started, &mut insert_batches),
        )
        .await
        {
            Ok(result) if std::time::Instant::now() < deadline => result,
            _ => {
                self.expire_round(job, &insert_batches).await;
                TurnResult::AgentFailed
            }
        };
        // Every return is terminal for an opening, even an inactive group or a
        // failed preparation. Human inputs keep their existing retry policy.
        // On cleanup failure the persisted source still excludes intake replay;
        // startup recovery removes the remaining synthetic row.
        if job.spontaneous.is_some() {
            let _ = self.store.cancel_spontaneous_events(&job.event_ids).await;
        }
        result
    }

    pub(super) async fn expire_round(&self, job: &BatchJob, insert_batches: &[String]) {
        // Settlement is deliberately outside the work budget. Never abandon a
        // DB transaction or process reap merely to report a shorter latency.
        self.resident.abort(&job.group_id).await;
        if self
            .store
            .expire_turn(
                &self.bot_id,
                &job.group_id,
                &job.batch_id,
                &job.event_ids,
                insert_batches,
                crate::now_unix(),
            )
            .await
            .is_err()
        {
            tracing::error!(group_id = %job.group_id, "deadline settlement failed; startup reconciliation required");
        }
        self.resident
            .note_respawn_failure(&job.group_id, crate::now_unix())
            .await;
        self.resident
            .audit(&job.group_id, "resident.turn.deadline")
            .await;
    }

    async fn run_batch_inner(
        &self,
        job: &BatchJob,
        turn_started: std::time::Instant,
        insert_batches: &mut Vec<String>,
    ) -> TurnResult {
        let now = crate::now_unix();

        // 1. The group may have been disabled while the batch waited.
        let group = match self.store.get_group(&self.bot_id, &job.group_id).await {
            Ok(Some(g)) => g,
            _ => return TurnResult::SkippedInactive,
        };
        if !group.enabled || group.paused {
            return TurnResult::SkippedInactive;
        }

        // 1.5 The hourly fuse, re-checked where the call actually starts.
        //
        // The intake checks the same fuse before it claims, but claiming and
        // starting are different instants: a batch can queue behind the
        // scheduler's permits for a while, and a deferred batch is re-claimed
        // by a later tick. The claim-time count therefore says nothing about
        // how many calls started since (one observation window saw 64 starts in
        // a rolling hour against a budget of 60). Nothing is sent to the agent
        // here; the batch goes back to the pool and the lane defers, so the next
        // tick re-claims it once calls age out.
        if let Some(remaining) = self.hourly_budget_remaining(&job.group_id, now).await
            && remaining == 0
        {
            let _ = self.store.release_events(&job.event_ids).await;
            let ages = self.pending_ages(&job.event_ids).await;
            let oldest_age = ages.values().min().map(|at| now - at).unwrap_or(0);
            let _ = self
                .store
                .audit(
                    "system",
                    &format!(
                        "scheduler.budget.exec_denied.g{}.age{oldest_age}",
                        job.group_id
                    ),
                    Some(&self.bot_id),
                    Some(&job.group_id),
                    now,
                )
                .await;
            return TurnResult::BudgetDeferred;
        }

        // 2. Proxy gate. A closed gate keeps the batch pending and defers the lane.
        if !self.mihomo.ready().await {
            let _ = self.store.release_events(&job.event_ids).await;
            let _ = self
                .store
                .audit(
                    "system",
                    "agent.waiting_for_us_proxy",
                    Some(&self.bot_id),
                    Some(&job.group_id),
                    now,
                )
                .await;
            return TurnResult::DeferredProxy;
        }

        // 2.5 Backoff gate. The resident session is the only execution path, so a
        // group whose last session was retired waits for its respawn deadline here.
        // The batch goes back to the pool and the lane defers — waiting *is* the
        // behaviour. There is no print lane behind this gate: an engine that cannot
        // produce a session leaves the whole group silent until it is fixed.
        if let Some(until) = self.resident.backoff_until(&job.group_id, now).await {
            let _ = self.store.release_events(&job.event_ids).await;
            let _ = self
                .store
                .audit(
                    "system",
                    &format!(
                        "resident.turn.deferred_backoff.g{}.until{until}",
                        job.group_id
                    ),
                    Some(&self.bot_id),
                    Some(&job.group_id),
                    now,
                )
                .await;
            return TurnResult::DeferredProxy;
        }
        let result = self
            .run_resident_batch(job, &group, now, turn_started, insert_batches)
            .await;
        // Close whichever input generation this batch last confirmed. An older
        // completion must never disarm a different batch's replacement arm.
        if let Ok(Some(arm)) = self.store.armed_turn(&job.group_id).await
            && arm.batch_id == job.batch_id
        {
            let _ = self.store.disarm_turn(&job.group_id, &arm.turn_token).await;
        }
        // A consumed batch ends any run of prompt rejections for the
        // group: the refused material has left the pool, so the next
        // refusal starts a fresh count instead of dropping a batch that
        // was never refused before.
        if matches!(
            result,
            TurnResult::Replied | TurnResult::Silent | TurnResult::DroppedStale
        ) {
            self.resident.clear_prompt_rejections(&job.group_id).await;
        }
        result
    }

    /// Close a round whose prompt an upstream content filter refused. The
    /// prompt never ran, so this is not a contract failure and the session
    /// is not at fault: the call closes under the dedicated error class, no
    /// probe window (contract validity or degraded) hears about the round,
    /// the seed the session was born with is quarantined so the next spawn
    /// degrades instead of resubmitting it, and the session retires through
    /// the ordinary backoff ladder.
    pub(super) async fn retire_for_content_filter(
        &self,
        manager: &Arc<crate::app::resident::ResidentManager>,
        job: &BatchJob,
        call_id: &str,
        coverage: &ResidentCoverage,
    ) -> TurnResult {
        let _ = self
            .store
            .mark_seed_rejected(&self.bot_id, &job.group_id, crate::now_unix())
            .await;
        manager
            .audit(
                &job.group_id,
                &format!("agent.content_filtered.g{}", job.group_id),
            )
            .await;
        // The filter's answer to a prompt is deterministic, so a batch
        // refused twice running will be refused forever: drop the
        // covered events instead of handing them back to the pool for
        // another identical submission (2026-11-06, group 943: thirteen
        // refusals in five minutes, one per respawned session). The
        // retirement below still runs; its release is a no-op for rows
        // that are already gone.
        let covered = coverage.event_ids();
        if manager.note_prompt_rejection(&job.group_id, &covered).await {
            let n = covered.len();
            let _ = self.store.delete_pending(&covered).await;
            manager
                .audit(
                    &job.group_id,
                    &format!("scheduler.poison_drop.g{}.n{n}", job.group_id),
                )
                .await;
        }
        self.retire_resident(
            manager,
            job,
            call_id,
            "content_filtered",
            ErrorClass::ContentFiltered.as_str(),
            coverage,
        )
        .await
    }

    /// Why a turn's answer was unusable. The preview is a flattened head of the
    /// redacted contract text the session produced. Filtering precedes truncation
    /// so a cut cannot leave a credential fragment without its marker.
    pub(super) async fn audit_bad_output(&self, group_id: &str, detail: &str, preview: &str) {
        let now = crate::now_unix();
        let kind = crate::agent::agy::output_preview(
            &crate::agent::segments::filter_secrets(detail, &self.secrets),
            160,
        );
        let text = crate::agent::agy::output_preview(
            &crate::agent::segments::filter_secrets(preview, &self.secrets),
            80,
        );
        let _ = self
            .store
            .audit(
                "system",
                &format!("agent.bad_output.g{group_id}.kind.{kind}"),
                Some(&self.bot_id),
                Some(group_id),
                now,
            )
            .await;
        let _ = self
            .store
            .audit(
                "system",
                &format!("agent.bad_output.g{group_id}.text.{text}"),
                Some(&self.bot_id),
                Some(group_id),
                now,
            )
            .await;
    }
}
