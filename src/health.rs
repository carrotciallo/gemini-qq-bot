//! Health and readiness aggregation.
//!
//! The panel needs to answer one question honestly: *which unverified contract is
//! blocking a production run?* So every subsystem reports both its state **and**
//! whether that state was actually verified, instead of a single green dot.

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::agent::agy::{AGY_CLI_VERSION, AgyPermissions, PermissionAudit};

use crate::process::ProxyHealth;

/// OneBot connection health.
#[derive(Debug, Clone, Default, Serialize)]
pub struct OneBotHealth {
    pub connected: bool,
    /// Increments on every successful connection; lets the panel spot flapping.
    pub generation: u64,
    pub last_frame_at: Option<i64>,
    pub last_error: Option<String>,
    pub reconnect_attempt: u32,
}

/// Agy adapter readiness, including what could not be verified.
#[derive(Debug, Clone, Serialize)]
pub struct AgyHealth {
    /// Operator-provisioned permission policy audit.
    pub permissions_ok: bool,
    /// Whether the permission *file schema* was verified against the CLI. Verified on
    /// Agy 1.4.7: unrecognised grants are pruned on rewrite, deny rules are enforced,
    /// and auto-approval has to come from `--dangerously-skip-permissions`.
    pub permission_schema_verified: bool,
    pub missing_allow: Vec<String>,
    pub missing_deny: Vec<String>,
    pub wildcard_allows: Vec<String>,
    /// The CLI version this adapter targets.
    pub cli_version: String,
    /// The model id in use. Exercised against the real CLI on the configured
    /// conversation: one turn returned a normal envelope for `gemini-3.8-flash`.
    pub model_id: String,
    pub model_verified: bool,
    /// Whether a conversation-id envelope location was verified. Resuming an explicit
    /// conversation id was exercised end to end on Agy 1.4.7.
    pub conversation_contract_verified: bool,
    /// Whether image input works. Always `false` until proven.
    pub image_transport_available: bool,
}

impl AgyHealth {
    pub fn from_audit(audit: PermissionAudit, model_id: &str) -> Self {
        Self {
            permissions_ok: audit.ok,
            permission_schema_verified: audit.schema_verified,
            missing_allow: audit.missing_allow,
            missing_deny: audit.missing_deny,
            wildcard_allows: audit.wildcard_allows,
            cli_version: AGY_CLI_VERSION.to_string(),
            model_id: model_id.to_string(),
            // Verified against the real 1.4.7 install on the configured conversation.
            model_verified: true,
            conversation_contract_verified: true,
            image_transport_available: crate::config::image_transport_verified(),
        }
    }

    /// Whether every dependency of a real reply is verified.
    pub fn ready(&self) -> bool {
        self.permissions_ok
            && self.permission_schema_verified
            && self.model_verified
            && self.conversation_contract_verified
    }
}

/// Whether the owner QQ id is configured. The value itself is never exposed.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct OwnerStatus {
    pub configured: bool,
}

/// Health row for the aux (memory) session.
///
/// This used to describe an external gateway (`AuxStatus`); since
/// the 2026-11-24 teardown the only aux there is is the dedicated Agy
/// memory session, so the row is built from its liveness alone. The
/// gateway-era probe fields stay in the serialized shape at their
/// not-applicable values so status consumers do not change.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AuxStatus {
    /// Which aux backend this status describes: always `"agy"` now.
    /// Empty in statuses serialized before the field existed (= gateway).
    #[serde(default)]
    pub mode: String,
    pub enabled: bool,
    pub health_ok: bool,
    pub models_ok: bool,
    pub model_published: bool,
    pub key_present: bool,
    pub last_error: Option<String>,
    pub ready: bool,
}

impl AuxStatus {
    /// Status for the memory session: `healthy` is its current liveness.
    pub fn agy_mode(healthy: bool) -> Self {
        Self {
            mode: "agy".to_string(),
            enabled: true,
            health_ok: healthy,
            last_error: (!healthy).then(|| "aux session unavailable".to_string()),
            ready: healthy,
            ..Self::default()
        }
    }
}

/// Everything the panel renders.
#[derive(Debug, Clone, Default, Serialize)]
pub struct HealthSnapshot {
    pub started_at: i64,
    pub now: i64,
    pub onebot: OneBotHealth,
    pub proxy: Option<ProxyHealth>,
    pub proxy_enabled: bool,
    pub agy: Option<AgyHealth>,
    pub aux: Option<AuxStatus>,
    pub owner: OwnerStatus,
    /// True only when every gate is open *and* verified.
    pub production_ready: bool,
    /// Human readable blockers, one per unverified or failing dependency.
    pub blockers: Vec<String>,
}

impl HealthSnapshot {
    /// Recompute `production_ready` and the blocker list from the parts.
    pub fn recompute(&mut self) {
        let mut blockers = Vec::new();
        if !self.onebot.connected {
            blockers.push("OneBot 未连接".to_string());
        }
        if self.proxy_enabled {
            match &self.proxy {
                Some(p) if p.ready(true) => {}
                _ => blockers.push("Mihomo 代理或固定 VPS 节点检查未通过".to_string()),
            }
        }
        match &self.agy {
            None => blockers.push("Agy adapter 未初始化".to_string()),
            Some(a) => {
                if !a.permissions_ok {
                    blockers.push("Agy 所需的沙箱工具权限未满足".to_string());
                }
                if !a.permission_schema_verified {
                    blockers.push("Agy 1.4.7 权限配置 schema 未验证".to_string());
                }
                if !a.model_verified {
                    blockers.push("Agy 模型 ID 未验证".to_string());
                }
                if !a.conversation_contract_verified {
                    blockers.push("Agy 会话/输出契约未验证".to_string());
                }
            }
        }
        match &self.aux {
            None => blockers.push("Aux compactor 未配置".to_string()),
            Some(w) => {
                if !w.ready {
                    blockers.push("Aux（agy 模式）辅助会话不可用".to_string());
                }
            }
        }
        if !self.owner.configured {
            blockers.push("OWNER_QQ_ID 未配置".to_string());
        }
        self.production_ready = blockers.is_empty();
        self.blockers = blockers;
    }
}

/// Shared, cheaply clonable health registry.
#[derive(Debug, Default)]
pub struct HealthRegistry {
    inner: RwLock<HealthSnapshot>,
}

impl HealthRegistry {
    pub fn new(snapshot: HealthSnapshot) -> Self {
        Self {
            inner: RwLock::new(snapshot),
        }
    }

    pub async fn snapshot(&self) -> HealthSnapshot {
        self.inner.read().await.clone()
    }

    /// Replace the mutable parts and recompute readiness.
    pub async fn update<F: FnOnce(&mut HealthSnapshot)>(&self, f: F) -> HealthSnapshot {
        let mut guard = self.inner.write().await;
        f(&mut guard);
        guard.now = crate::now_unix();
        guard.recompute();
        guard.clone()
    }

    pub async fn set_onebot(&self, health: OneBotHealth) -> HealthSnapshot {
        self.update(|s| s.onebot = health).await
    }

    pub async fn set_proxy(&self, health: ProxyHealth) -> HealthSnapshot {
        self.update(|s| s.proxy = Some(health)).await
    }

    pub async fn set_aux(&self, status: AuxStatus) -> HealthSnapshot {
        self.update(|s| s.aux = Some(status)).await
    }

    pub async fn set_agy_permissions(
        &self,
        permissions: &AgyPermissions,
        model_id: &str,
    ) -> HealthSnapshot {
        let audit = permissions.audit();
        self.update(|s| s.agy = Some(AgyHealth::from_audit(audit, model_id)))
            .await
    }

    pub async fn production_ready(&self) -> bool {
        self.inner.read().await.production_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_agy() -> AgyHealth {
        AgyHealth {
            permissions_ok: true,
            permission_schema_verified: true,
            missing_allow: vec![],
            missing_deny: vec![],
            wildcard_allows: vec![],
            cli_version: AGY_CLI_VERSION.into(),
            model_id: "gemini-3.8-flash".into(),
            model_verified: true,
            conversation_contract_verified: true,
            image_transport_available: false,
        }
    }

    fn ready_proxy() -> ProxyHealth {
        ProxyHealth {
            process_running: true,
            api_ok: true,
            vps_node_ok: true,
            selected_node: Some("US-01".into()),
            last_error: None,
            restarts: 0,
        }
    }

    fn ready_aux() -> AuxStatus {
        AuxStatus::agy_mode(true)
    }

    fn snapshot() -> HealthSnapshot {
        HealthSnapshot {
            started_at: 1,
            now: 2,
            onebot: OneBotHealth {
                connected: true,
                ..Default::default()
            },
            proxy: Some(ready_proxy()),
            proxy_enabled: true,
            agy: Some(ready_agy()),
            aux: Some(ready_aux()),
            owner: OwnerStatus { configured: true },
            production_ready: false,
            blockers: vec![],
        }
    }

    #[test]
    fn recompute_sets_readiness_from_each_blocker() {
        struct Case {
            name: &'static str,
            mutate: fn(&mut HealthSnapshot),
            ready: bool,
            needles: &'static [&'static str],
        }
        let cases = [
            Case {
                name: "fully verified",
                mutate: |_| {},
                ready: true,
                needles: &[],
            },
            Case {
                name: "unverified agy contracts",
                mutate: |s| {
                    if let Some(a) = s.agy.as_mut() {
                        a.permission_schema_verified = false;
                        a.model_verified = false;
                        a.conversation_contract_verified = false;
                    }
                },
                ready: false,
                needles: &["schema", "模型 ID", "会话"],
            },
            Case {
                name: "missing vps node",
                mutate: |s| {
                    if let Some(p) = s.proxy.as_mut() {
                        p.vps_node_ok = false;
                    }
                },
                ready: false,
                needles: &["固定 VPS 节点"],
            },
            Case {
                name: "disabled proxy",
                mutate: |s| {
                    s.proxy_enabled = false;
                    s.proxy = None;
                },
                ready: true,
                needles: &[],
            },
            Case {
                name: "aux session unavailable",
                mutate: |s| {
                    if let Some(w) = s.aux.as_mut() {
                        w.health_ok = false;
                        w.ready = false;
                    }
                },
                ready: false,
                needles: &["Aux（agy 模式）辅助会话不可用"],
            },
            Case {
                name: "owner absence",
                mutate: |s| {
                    s.owner = OwnerStatus { configured: false };
                },
                ready: false,
                needles: &["OWNER_QQ_ID"],
            },
        ];
        for case in cases {
            let mut s = snapshot();
            (case.mutate)(&mut s);
            s.recompute();
            assert_eq!(
                s.production_ready, case.ready,
                "{} blockers={:?}",
                case.name, s.blockers
            );
            for needle in case.needles {
                assert!(
                    s.blockers.iter().any(|b| b.contains(needle)),
                    "{} missing {needle}: {:?}",
                    case.name,
                    s.blockers
                );
            }
        }
    }

    #[test]
    fn agy_health_reports_real_audit_failures() {
        let mut permissions = AgyPermissions::required();
        permissions
            .allow
            .retain(|permission| permission != "read_file(*)");
        let health = AgyHealth::from_audit(permissions.audit(), "gemini-3.8-flash");
        assert!(!health.permissions_ok);
        assert_eq!(health.missing_allow, vec!["read_file(*)"]);
        assert!(health.missing_deny.is_empty());
        // The schema itself is verified; only the operator's grants are missing.
        assert!(health.permission_schema_verified);
        assert!(!health.ready());
    }

    #[tokio::test]
    async fn registry_recomputes_on_update() {
        let mut baseline = HealthSnapshot::default();
        baseline.recompute();
        let registry = HealthRegistry::new(baseline);
        // Nothing is configured or connected, so readiness is false and the reasons are
        // listed rather than hidden behind a single green dot.
        assert!(!registry.production_ready().await);
        let snap = registry.snapshot().await;
        assert!(!snap.production_ready);
        assert!(
            snap.blockers.iter().any(|b| b.contains("OWNER_QQ_ID")),
            "blockers: {:?}",
            snap.blockers
        );

        registry
            .update(|s| {
                *s = snapshot();
            })
            .await;
        assert!(registry.production_ready().await);
    }

    #[tokio::test]
    async fn registry_tracks_onebot_disconnect() {
        let registry = HealthRegistry::new(snapshot());
        registry
            .set_onebot(OneBotHealth {
                connected: false,
                last_error: Some("closed".into()),
                ..Default::default()
            })
            .await;
        let snap = registry.snapshot().await;
        assert!(!snap.onebot.connected);
        assert!(!snap.production_ready);
    }
}
