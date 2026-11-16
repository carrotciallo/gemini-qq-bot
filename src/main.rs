//! Entry point.
//!
//! Startup is intentionally boring: load and validate config, build the app, serve. Any
//! configuration problem is fatal here — the process must not run with a weak internal
//! token or an unset owner id. Unverified *external contracts* are not fatal: they show
//! up as readiness blockers instead, because refusing to boot would hide the very
//! diagnostics the operator needs.

use std::process::ExitCode;

use gemini_qq_bot::app::App;
use gemini_qq_bot::config::Config;
use gemini_qq_bot::mcp;
use gemini_qq_bot::store::sqlite::SqliteStore;
use tracing_subscriber::EnvFilter;

/// Serve the read-only memory tool over MCP stdio.
///
/// The group scope is resolved per call: the server asks the client which directories
/// this turn works in, then reads `scope.json` from that workspace. It is never taken
/// from the request arguments, so the model cannot address another group, and the shared
/// config file is never rewritten, so two concurrent groups cannot collide.
///
/// stdout carries protocol frames only; every diagnostic goes to stderr, because a
/// stray line on stdout would corrupt the transport.
async fn serve_memory_mcp() -> ExitCode {
    let db_path = std::env::var("GEMINI_MEMORY_DB").unwrap_or_else(|_| "/data/bot.sqlite3".into());
    let pool =
        match gemini_qq_bot::store::migrations::open_and_migrate(std::path::Path::new(&db_path))
            .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("打开记忆库失败: {e}");
                return ExitCode::from(3);
            }
        };
    let store = std::sync::Arc::new(SqliteStore::new(pool));

    let session = std::sync::Arc::new(mcp::Session::new());
    match session.run(store).await {
        Ok(()) | Err(mcp::SessionError::Closed) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("MCP 会话中断: {err:?}");
            ExitCode::from(4)
        }
    }
}

/// Minimal TCP probe used by the container health check.
///
/// It deliberately does **not** authenticate: the only thing a liveness probe should
/// assert is that the admin listener accepts connections. Reading the internal token
/// to call `/internal/v1/status` would copy a secret into a process that has no other
/// reason to hold it. Deep checks stay behind the token, where an operator inspects
/// them.
async fn health_probe() -> ExitCode {
    let bind = std::env::var("ADMIN_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let target = if bind.starts_with("0.0.0.0:") {
        bind.replacen("0.0.0.0", "127.0.0.1", 1)
    } else if bind.starts_with("[::]:") {
        bind.replacen("[::]", "[::1]", 1)
    } else {
        bind
    };
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&target),
    )
    .await
    {
        Ok(Ok(_)) => ExitCode::SUCCESS,
        _ => ExitCode::from(1),
    }
}

/// One-shot migration of pre-v1 memory rows (`--migrate-memories-v1`).
///
/// Usage: `gemini-qq-bot --migrate-memories-v1 --db <path> [--out <report.json>] [--apply]`
///
/// Dry-run unless `--apply` is given. The production database path is refused
/// in code (see `memory::migrate_v1::PRODUCTION_DB_PATHS`): the intended target
/// is always a copy, and `--apply` first writes a timestamped backup beside it.
/// The aux gateway is configured from the same environment as the bot
/// (`AUX_BASE_URL`, `AUX_MODEL`, `AUX_API_KEY_FILE`).
async fn migrate_memories_v1() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let arg_value = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let Some(db_path) = arg_value("--db") else {
        eprintln!("--migrate-memories-v1 需要 --db <数据库路径>");
        return ExitCode::from(2);
    };
    let db_path = std::path::PathBuf::from(db_path);
    if gemini_qq_bot::memory::migrate_v1::is_production_path(&db_path) {
        eprintln!("拒绝在生产库路径上运行迁移工具: {}", db_path.display());
        return ExitCode::from(2);
    }
    let apply = args.iter().any(|a| a == "--apply");
    let out_path = arg_value("--out")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("memories-v1-report.json"));

    let pool = match gemini_qq_bot::store::migrations::open_and_migrate(&db_path).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("打开数据库失败: {e}");
            return ExitCode::from(3);
        }
    };
    let store = SqliteStore::new(pool);

    let key_file = std::env::var("AUX_API_KEY_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/data/secrets/workbuddy_api_key"));
    let mut gateway_config = gemini_qq_bot::integrations::aux_gateway::AuxGatewayConfig::new(
        std::env::var("AUX_BASE_URL").ok(),
        std::env::var("AUX_MODEL").ok(),
        key_file,
        true,
    );
    // Same env contract as the bot: production names the sticky-session
    // header `X-Workbuddy-Session` via AUX_SESSION_HEADER.
    if let Ok(header) = std::env::var("AUX_SESSION_HEADER") {
        if !header.trim().is_empty() {
            gateway_config.session_header = header;
        }
    }
    let model = gemini_qq_bot::integrations::aux_gateway::AuxGatewayClient::new(gateway_config);

    let now = gemini_qq_bot::now_unix();
    let mut backup_path = None;
    if apply {
        let backup = gemini_qq_bot::memory::migrate_v1::backup_path_for(&db_path, now);
        match std::fs::copy(&db_path, &backup) {
            Ok(_) => backup_path = Some(backup.display().to_string()),
            Err(e) => {
                eprintln!("写备份失败，拒绝 apply: {e}");
                return ExitCode::from(3);
            }
        }
    }
    let mut report =
        gemini_qq_bot::memory::migrate_v1::run_migration(&store, &model, apply, now).await;
    report.backup_path = backup_path;
    match serde_json::to_string_pretty(&report) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&out_path, &json) {
                eprintln!("写报告失败: {e}");
                return ExitCode::from(3);
            }
            println!(
                "迁移{}完成: 候选 {}、已转 {}、失败 {}、预计首轮淘汰 {}；报告 {}",
                if apply {
                    "（已回写）"
                } else {
                    "（dry-run）"
                },
                report.candidates,
                report.converted,
                report.failed.len(),
                report.would_expire,
                out_path.display()
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("序列化报告失败: {e}");
            ExitCode::from(3)
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().any(|a| a == "--health-probe") {
        return health_probe().await;
    }
    if std::env::args().any(|a| a == "--mcp-memory") {
        return serve_memory_mcp().await;
    }
    if std::env::args().any(|a| a == "--migrate-memories-v1") {
        return migrate_memories_v1().await;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        // Never log request bodies or headers: message text and tokens live there.
        .with_target(false)
        .init();

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "配置无效，拒绝启动");
            return ExitCode::from(2);
        }
    };

    tracing::info!(
        bot_id = %config.bot_id,
        proxy_enabled = config.mihomo_enabled,
        aux_enabled = config.aux_enabled,
        image_transport_verified = gemini_qq_bot::config::image_transport_verified(),
        "启动 gemini-qq-bot"
    );

    let app = match App::build(config).await {
        Ok(a) => std::sync::Arc::new(a),
        Err(e) => {
            tracing::error!(error = %e, "初始化失败");
            return ExitCode::from(3);
        }
    };

    let snapshot = app.health.snapshot().await;
    if snapshot.production_ready {
        tracing::info!("所有依赖已验证，生产就绪");
    } else {
        tracing::warn!(
            blockers = snapshot.blockers.len(),
            "未达到生产就绪，回复链路受闸门限制"
        );
        for blocker in &snapshot.blockers {
            tracing::warn!(blocker = %blocker, "阻塞项");
        }
    }

    if let Err(e) = app.serve().await {
        tracing::error!(error = %e, "服务退出");
        return ExitCode::from(4);
    }
    ExitCode::SUCCESS
}
