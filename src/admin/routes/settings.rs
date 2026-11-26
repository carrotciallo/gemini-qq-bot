//! Runtime settings endpoints (`/api/v1/settings*`).
//!
//! The write path behind the web console's settings page: the two
//! runtime switches (session rotation, quota degradation). Changes
//! persist as an overlay next to the database and apply to the live
//! consumers immediately (see [`crate::app::runtime_settings`]).
//!
//! The aux gateway section that used to live here was removed with the
//! gateway teardown (2026-11-24); the endpoints are gone, not stubbed.

use std::sync::Arc;

use axum::extract::State;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{AdminState, ApiError, ApiResult, ok_json};
use crate::store::ActorKind;

pub fn routes() -> Router<Arc<AdminState>> {
    Router::new()
        .route("/settings", axum::routing::get(get_settings))
        .route("/settings/runtime", axum::routing::put(put_runtime))
}

async fn get_settings(State(state): State<Arc<AdminState>>) -> ApiResult {
    ok_json(json!({
        "runtime": state.settings.switch_view(),
    }))
}

#[derive(Debug, Deserialize)]
struct RuntimeUpdate {
    session_rotation_enabled: Option<bool>,
    quota_degradation_enabled: Option<bool>,
}

async fn put_runtime(State(state): State<Arc<AdminState>>, Json(body): Json<Value>) -> ApiResult {
    let update: RuntimeUpdate =
        serde_json::from_value(body).map_err(|_| ApiError::bad_request("invalid_body"))?;
    if update.session_rotation_enabled.is_none() && update.quota_degradation_enabled.is_none() {
        return Err(ApiError::bad_request("empty_update"));
    }
    let view = state
        .settings
        .update_switches(
            update.session_rotation_enabled,
            update.quota_degradation_enabled,
        )
        .map_err(|_| ApiError::internal())?;
    let _ = state
        .store
        .audit(
            ActorKind::Panel.as_str(),
            "settings.runtime.update",
            None,
            None,
            crate::now_unix(),
        )
        .await;
    ok_json(json!({ "runtime": view }))
}
