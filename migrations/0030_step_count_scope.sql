-- Split the two step-count meanings that used to share one column.
--
-- Migration 0012 added `steps_count` for "how many steps the step stream held
-- when the round finished" on a resident call, which resumes across many
-- batches. That is a session cumulative number. `turn_steps_count` is the
-- delta of the round that just finished, and `last_progress_step` is the idx of
-- the last step observed, so an operator can tell「会话累计」from「本轮规模」
-- without recomputing anything. All nullable: print-mode calls have no step
-- stream, and rows written before this migration simply have no delta.
ALTER TABLE agent_calls ADD COLUMN turn_steps_count INTEGER;
ALTER TABLE agent_calls ADD COLUMN last_progress_step INTEGER;
