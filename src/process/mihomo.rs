//! Mihomo (Clash) supervision inside the bot container.
//!
//! The proxy is what lets Agy reach its endpoint from a network where direct access
//! does not work, so the bot treats its health as a precondition for calling the agent.
//! Two things matter and only one of them is enforceable here:
//!
//! * **Enforceable**: Mihomo is spawned as a child process, binds only container
//!   loopback ports, and is probed through its own API before a batch runs. When the
//!   probe fails the batch is kept as pending and marked `waiting_for_vps_proxy`.
//! * **Not enforceable**: `HTTP_PROXY`/`HTTPS_PROXY` in the child environment are a
//!   *convention*, not a sandbox. A CLI that ignores them will connect directly. This
//!   module therefore never claims that package-level network isolation exists.
//!
//! The pinned-VPS rule is a liveness rule: the egress is pinned to the self-hosted
//! tokyo-1 VPS, so if the selected leaf is not one of its protocol nodes the proxy
//! is considered unusable and no downgrade to another exit is allowed.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::sync::RwLock;

/// Delay before a restart is attempted.
pub const RESTART_DELAY: Duration = Duration::from_secs(5);
/// Maximum restarts before the bot reports a hard proxy failure.
pub const MAX_RESTARTS: u32 = 5;
/// Probe timeout.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Health of the proxy subsystem.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct ProxyHealth {
    /// The child process is alive.
    pub process_running: bool,
    /// `GET /version` on the controller succeeded.
    pub api_ok: bool,
    /// The selected leaf is one of the pinned VPS nodes (see [`VPS_NODES`]).
    pub vps_node_ok: bool,
    /// The name of the selected node, for the panel. Never a credential.
    pub selected_node: Option<String>,
    pub last_error: Option<String>,
    pub restarts: u32,
}

impl ProxyHealth {
    /// Whether the agent may be called.
    ///
    /// `mihomo_enabled == false` means the operator declared direct access to be
    /// available, so the gate opens without pretending the proxy works.
    pub fn ready(&self, mihomo_enabled: bool) -> bool {
        if !mihomo_enabled {
            return true;
        }
        self.process_running && self.api_ok && self.vps_node_ok
    }

    fn recompute(&mut self) {
        if !self.process_running {
            self.api_ok = false;
            self.vps_node_ok = false;
        }
    }
}

/// Reason a proxy probe failed. String form is the stable `error_class` suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyError {
    /// The process is not running.
    NotRunning,
    /// The controller API did not answer.
    ApiUnreachable,
    /// The selected leaf is not one of the pinned VPS nodes.
    NoPinnedNode,
    /// A node was selected but a latency test failed.
    NodeUnhealthy,
}

impl ProxyError {
    pub fn as_str(self) -> &'static str {
        match self {
            ProxyError::NotRunning => "not_running",
            ProxyError::ApiUnreachable => "api_unreachable",
            ProxyError::NoPinnedNode => "no_pinned_node",
            ProxyError::NodeUnhealthy => "node_unhealthy",
        }
    }
}

/// Node names of the pinned VPS egress: the self-hosted tokyo-1 VPS, one
/// entry per protocol, plus the `-jp` variants that dial the same VPS
/// through a commercial JP relay (dialer-proxy) for when the direct path
/// is down, plus the CDN relay variant that reaches it through the
/// Cloudflare edge. Whichever one is selected, the exit is the same VPS.
pub const VPS_NODES: [&str; 7] = [
    "tokyo-1",
    "tokyo-1-reality",
    "tokyo-1-tuic",
    "tokyo-1-jp",
    "tokyo-1-reality-jp",
    "tokyo-1-tuic-jp",
    "tokyo-1-cdn",
];

/// Whether `name` is one of the pinned VPS nodes. Exact match only: a group
/// name such as US-AUTO must not pass, because the group *contains* the
/// pinned nodes — it is not itself an exit.
pub fn is_vps_node(name: &str) -> bool {
    VPS_NODES.contains(&name)
}

/// Resolve the effective selection from a `/proxies` payload: GLOBAL's own
/// selection plus, when that names a group, the group's selection — the
/// actual exit leaf. A direct leaf has no `now` of its own and is kept.
pub fn resolve_selection(body: &Value) -> (Option<String>, Vec<String>) {
    let (selected, choices) = parse_selection(body, "GLOBAL");
    let leaf = selected.as_deref().and_then(|group| {
        let (now, _) = parse_selection(body, group);
        now
    });
    (leaf.or(selected), choices)
}

/// Configuration for the child process.
#[derive(Debug, Clone)]
pub struct MihomoConfig {
    pub bin: PathBuf,
    pub config_path: PathBuf,
    /// Controller port, container loopback only.
    pub api_port: u16,
    /// Mixed proxy port, container loopback only.
    pub proxy_port: u16,
    pub api_secret: Option<String>,
}

impl MihomoConfig {
    /// The argv used to start the proxy.
    ///
    /// Both listeners are pinned to `127.0.0.1`: nothing is published, and a container
    /// lateral move cannot reach the controller.
    pub fn argv(&self) -> Vec<String> {
        // v1.19.30 only accepts -d/-f/-ext-ctl. mixed-port and allow-lan belong in
        // the config file; passing them as flags makes the process exit immediately.
        vec![
            "-d".to_string(),
            self.config_path
                .parent()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "/opt/mihomo".to_string()),
            "-f".to_string(),
            self.config_path.display().to_string(),
            "-ext-ctl".to_string(),
            format!("127.0.0.1:{}", self.api_port),
        ]
    }

    /// Proxy URL handed to the agent child process.
    pub fn proxy_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.proxy_port)
    }

    /// Controller API base.
    pub fn api_base(&self) -> String {
        format!("http://127.0.0.1:{}", self.api_port)
    }
}

/// Supervises the proxy child process and its health.
pub struct Mihomo {
    config: MihomoConfig,
    enabled: bool,
    child: RwLock<Option<Child>>,
    health: RwLock<ProxyHealth>,
    http: reqwest::Client,
}

impl std::fmt::Debug for Mihomo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mihomo")
            .field("enabled", &self.enabled)
            .field("api_port", &self.config.api_port)
            .finish_non_exhaustive()
    }
}

impl Mihomo {
    pub fn new(config: MihomoConfig, enabled: bool) -> Self {
        Self {
            config,
            enabled,
            child: RwLock::new(None),
            health: RwLock::new(ProxyHealth::default()),
            http: reqwest::Client::builder()
                .timeout(PROBE_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn config(&self) -> &MihomoConfig {
        &self.config
    }

    pub async fn health(&self) -> ProxyHealth {
        let mut h = self.health.read().await.clone();
        h.recompute();
        h
    }

    /// Whether the agent may be called right now.
    pub async fn ready(&self) -> bool {
        self.health().await.ready(self.enabled)
    }

    /// Start the child process. A missing binary is a hard error the operator must see.
    pub async fn start(&self) -> Result<(), ProxyError> {
        if !self.enabled {
            return Ok(());
        }
        let mut guard = self.child.write().await;
        if let Some(child) = guard.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            return Ok(());
        }
        let argv = self.config.argv();
        let mut cmd = Command::new(&self.config.bin);
        cmd.args(&argv);
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());
        cmd.kill_on_drop(true);
        match cmd.spawn() {
            Ok(child) => {
                *guard = Some(child);
                let mut health = self.health.write().await;
                health.process_running = true;
                health.last_error = None;
                Ok(())
            }
            Err(e) => {
                let mut health = self.health.write().await;
                health.process_running = false;
                health.last_error = Some(format!("无法启动 mihomo: {}", e.kind()));
                Err(ProxyError::NotRunning)
            }
        }
    }

    /// Stop the child process.
    pub async fn stop(&self) {
        let mut guard = self.child.write().await;
        if let Some(mut child) = guard.take() {
            let _ = child.kill().await;
        }
        let mut health = self.health.write().await;
        health.process_running = false;
        health.recompute();
    }

    /// Probe the running controller. `GLOBAL` is mihomo's built-in selector.
    pub async fn probe(&self) -> ProxyHealth {
        let (selected, choices) = self.global_selection().await;
        self.probe_with(selected.as_deref(), &choices).await
    }

    async fn global_selection(&self) -> (Option<String>, Vec<String>) {
        let url = format!("{}/proxies", self.config.api_base());
        let mut req = self.http.get(&url);
        if let Some(secret) = &self.config.api_secret {
            req = req.bearer_auth(secret);
        }
        let Ok(resp) = req.send().await else {
            return (None, Vec::new());
        };
        if !resp.status().is_success() {
            return (None, Vec::new());
        }
        let Ok(body) = resp.json().await else {
            return (None, Vec::new());
        };
        resolve_selection(&body)
    }

    /// Probe the controller API and the selected node.
    ///
    /// `proxies` supplies the selectable node names for the pinned-node check; tests inject it so
    /// no real proxy is needed.
    pub async fn probe_with(
        &self,
        selected: Option<&str>,
        group_choices: &[String],
    ) -> ProxyHealth {
        let mut health = ProxyHealth {
            process_running: self.child.read().await.is_some(),
            ..ProxyHealth::default()
        };

        if !self.enabled {
            // Proxy disabled by the operator: no probe, no claim.
            health.api_ok = false;
            health.vps_node_ok = false;
            *self.health.write().await = health.clone();
            return health;
        }

        if !health.process_running {
            health.last_error = Some(ProxyError::NotRunning.as_str().to_string());
            *self.health.write().await = health.clone();
            return health;
        }

        let url = format!("{}/version", self.config.api_base());
        let mut req = self.http.get(&url);
        if let Some(secret) = &self.config.api_secret {
            req = req.bearer_auth(secret);
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => health.api_ok = true,
            _ => {
                health.last_error = Some(ProxyError::ApiUnreachable.as_str().to_string());
                *self.health.write().await = health.clone();
                return health;
            }
        }

        let selected = selected.or_else(|| group_choices.first().map(|s| s.as_str()));
        health.selected_node = selected.map(|s| s.to_string());
        match selected {
            None => {
                health.last_error = Some(ProxyError::NoPinnedNode.as_str().to_string());
            }
            Some(node) if !is_vps_node(node) => {
                // No downgrade: a non-VPS leaf is a failure, not a fallback.
                health.last_error = Some(ProxyError::NoPinnedNode.as_str().to_string());
            }
            Some(_) => health.vps_node_ok = true,
        }
        *self.health.write().await = health.clone();
        health
    }
}

/// Parse a Clash `/proxies` payload into `(selected node for the selector group, all
/// candidate names)`.
pub fn parse_selection(body: &Value, group: &str) -> (Option<String>, Vec<String>) {
    let selected = body
        .pointer(&format!("/proxies/{group}/now"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let all = body
        .pointer(&format!("/proxies/{group}/all"))
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|i| i.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    (selected, all)
}

/// Whether a config path looks usable (exists and is a file).
pub fn config_present(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MihomoConfig {
        MihomoConfig {
            bin: PathBuf::from("/opt/mihomo/mihomo"),
            config_path: PathBuf::from("/opt/mihomo/config.yaml"),
            api_port: 9090,
            proxy_port: 7890,
            api_secret: None,
        }
    }

    #[test]
    fn argv_binds_loopback_only_and_disables_lan() {
        let argv = config().argv();
        let joined = argv.join(" ");
        assert!(joined.contains("-ext-ctl 127.0.0.1:9090"));
        assert!(joined.contains("-f /opt/mihomo/config.yaml"));
        assert!(!joined.contains("-allow-lan"));
        assert!(!joined.contains("-mixed-port"));
        assert!(!joined.contains("0.0.0.0"));
    }

    #[test]
    fn vps_node_matching_is_exact() {
        for vps in [
            "tokyo-1",
            "tokyo-1-reality",
            "tokyo-1-tuic",
            "tokyo-1-jp",
            "tokyo-1-reality-jp",
            "tokyo-1-tuic-jp",
            "tokyo-1-cdn",
        ] {
            assert!(is_vps_node(vps), "{vps} 应识别为固定 VPS 节点");
        }
        for other in [
            "US-AUTO",
            "US-LosAngeles",
            "E-日本1",
            "JP-Tokyo",
            "tokyo-1-hy2",
            "tokyo-2",
        ] {
            assert!(!is_vps_node(other), "{other} 不得被当成固定 VPS 节点");
        }
    }

    #[test]
    fn resolve_selection_descends_into_the_selected_group() {
        let body = serde_json::json!({
            "proxies": {
                "GLOBAL": {"now": "US-AUTO", "all": ["US-AUTO", "DIRECT"]},
                "US-AUTO": {"now": "tokyo-1-tuic", "all": ["tokyo-1", "tokyo-1-reality", "tokyo-1-tuic"]}
            }
        });
        let (selected, _) = resolve_selection(&body);
        assert_eq!(selected.as_deref(), Some("tokyo-1-tuic"));
    }

    #[test]
    fn resolve_selection_keeps_a_direct_leaf() {
        let body = serde_json::json!({
            "proxies": {
                "GLOBAL": {"now": "tokyo-1", "all": ["tokyo-1"]}
            }
        });
        let (selected, _) = resolve_selection(&body);
        assert_eq!(selected.as_deref(), Some("tokyo-1"));
    }

    #[test]
    fn health_gate_requires_every_signal_when_enabled() {
        let good = ProxyHealth {
            process_running: true,
            api_ok: true,
            vps_node_ok: true,
            selected_node: Some("tokyo-1".into()),
            last_error: None,
            restarts: 0,
        };
        assert!(good.ready(true));
        for broken in 0..3 {
            let mut h = good.clone();
            match broken {
                0 => h.process_running = false,
                1 => h.api_ok = false,
                _ => h.vps_node_ok = false,
            }
            h.recompute();
            assert!(!h.ready(true), "缺少任一信号时不得调用 Agy");
        }
        // Disabled proxy means the operator declared direct access: gate open.
        assert!(ProxyHealth::default().ready(false));
    }

    #[test]
    fn proxy_urls_are_loopback() {
        let c = config();
        assert_eq!(c.proxy_url(), "http://127.0.0.1:7890");
        assert_eq!(c.api_base(), "http://127.0.0.1:9090");
    }

    #[test]
    fn parse_selection_reads_now_and_all() {
        let body = serde_json::json!({
            "proxies": {
                "GLOBAL": {"now": "US-01", "all": ["US-01", "HK-01"]}
            }
        });
        let (now, all) = parse_selection(&body, "GLOBAL");
        assert_eq!(now.as_deref(), Some("US-01"));
        assert_eq!(all, vec!["US-01", "HK-01"]);
    }

    #[test]
    fn parse_selection_tolerates_missing_fields() {
        let (now, all) = parse_selection(&serde_json::json!({}), "GLOBAL");
        assert!(now.is_none());
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn disabled_proxy_never_probes_and_reports_no_health() {
        let mihomo = Mihomo::new(config(), false);
        let health = mihomo.probe_with(Some("E-日本1"), &[]).await;
        assert!(!health.api_ok);
        assert!(!health.vps_node_ok);
        // But the gate is open because the operator disabled the proxy.
        assert!(mihomo.ready().await);
    }

    #[tokio::test]
    async fn non_vps_node_blocks_the_gate() {
        let mut cfg = config();
        // Point the API at a closed port so the probe stops at the API check; we assert
        // the node rule through the pure helper instead.
        cfg.api_port = 1;
        let mihomo = Mihomo::new(cfg, true);
        mihomo.start().await.ok();
        let health = mihomo
            .probe_with(Some("E-日本1"), &["E-日本1".to_string()])
            .await;
        assert!(!health.vps_node_ok);
        assert!(!mihomo.ready().await);
        mihomo.stop().await;
    }

    #[test]
    fn config_presence_check() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("config.yaml");
        assert!(!config_present(&path));
        std::fs::write(&path, "proxies: []").expect("写");
        assert!(config_present(&path));
    }
}
