# Schema upgrade and rollback compatibility

This is the current compatibility note added by R21. Historical migrations are immutable: do not edit their SQL or checksums after release.

## Offline evidence

`cargo test populated_` creates three **synthetic**, file-backed databases using the real SQLx migration runner and its version/checksum table. Starting versions are 17, 22 and 25. Each fixture upgrades through 27, then the current embedded migration set (30 at this change), closes/reopens, and runs migrations again.

Assertions cover memory identities/content, archived-to-state backfill, timestamps, TTL/slot/tag preservation or defaults, submission id/payload/verdict, image ledger retention, quota defaults, sealed topics, pending source and armed-input defaults, historical checksums, foreign keys and integrity. These fixtures do not prove that every possible historical production row is valid. No production database is opened by the tests.

## Compatibility matrix

| Starting application/schema | Upgrade | Blind application downgrade after upgrade |
| --- | --- | --- |
| 17 | Tested with populated memories and submissions through 27/current | Unsafe: migrations 18/19 remove the resident fallback/enabled columns. An old migrator also rejects newer applied versions. |
| 22 | Tested through 27/current; pre-v1 memories remain permanent and acquire structured lifecycle state | Rejected by the old migration set; memory lifecycle metadata cannot be represented fully by that reader. |
| 25 | Tested through 27/current with image jobs and v1 TTL/tags | Rejected by the old migration set; keep lifecycle and sealed-topic data rather than dropping columns/tables. |
| 27 baseline | Current adds 28 pending-source, 29 confirmed-input and 30 split step-count columns | Do not remove applied migrations from the embedded set. An old source checkout is not automatically compatible with an upgraded DB. |
| Current (30) | Reopen/idempotent migration tested | A code-only revert is safe only in a build retaining the applied migration set **and** understanding retained ledger/state semantics. |

R15 introduced `generated` image jobs without a schema migration: an older executor does not recover that state. R16 `cancelled` jobs must not be requeued to simulate rollback. R19's optional JSON batch identity is additive, but reverting its reader restores weak attribution. R27's `turn_steps_count`/`last_progress_step` are additive and nullable; an older build ignores them and keeps writing only the session cumulative `steps_count`. Schema compatibility is not sufficient to prove business-state compatibility.

## Deployment and recovery rules

Before an authorized deployment, quiesce writers and obtain a consistent SQLite backup with a verified restore copy and integrity check. A raw copy of only the main file while WAL writers are active is not a consistent backup. Keep the matching application artifact/version and relevant artifact files; do not expose configuration or credential values in logs.

Prefer forward repair or a compatibility build retaining all applied migrations. Never delete `_sqlx_migrations` rows, rewrite checksums, or claim `DROP TABLE` is a lossless downgrade. A restore loses post-snapshot writes and requires explicit approval naming the target database and chosen snapshot. Validate the restore on a separate offline copy first. These instructions do not authorize a production restore, table drop, or data deletion.

The historical migration comments describing additive changes as “reversible” refer only to schema shape, not data preservation or old-application compatibility. They remain unedited to preserve released checksums.
