//! Image generation quota endpoints.
//!
//! The three panel-tunable numbers (self-initiated cap per group per day,
//! per-person request cap, request cooldown) live in `imagegen_settings`
//! and are read by the MCP gate on every `generate_image` call, so a PUT
//! here takes effect without a restart. Usage is reported through the
//! same store counters the gate enforces — one source, no drift. Per the
//! admin API's standing rule, the owner id itself is never echoed; the
//! settings view reports only whether one is configured.

use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use super::{AdminState, ApiError, ApiResult, ok_json};
use crate::store::ActorKind;

pub fn routes() -> Router<Arc<AdminState>> {
    Router::new()
        .route(
            "/imagegen/settings",
            axum::routing::get(get_settings).put(put_settings),
        )
        .route(
            "/groups/{group_id}/imagegen/usage",
            axum::routing::get(get_usage),
        )
        .route(
            "/groups/{group_id}/imagegen/reset",
            axum::routing::post(post_reset),
        )
}

async fn get_settings(State(state): State<Arc<AdminState>>) -> ApiResult {
    let (s, owner) = state
        .store
        .imagegen_settings()
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    ok_json(json!({
        "self_daily_cap": s.self_daily_cap,
        "request_daily_cap": s.request_daily_cap,
        "request_cooldown_min": s.request_cooldown_minutes,
        "owner_configured": !owner.is_empty(),
    }))
}

#[derive(Debug, Deserialize)]
struct SettingsUpdate {
    self_daily_cap: Option<i64>,
    request_daily_cap: Option<i64>,
    request_cooldown_min: Option<i64>,
}

async fn put_settings(
    State(state): State<Arc<AdminState>>,
    Json(update): Json<SettingsUpdate>,
) -> ApiResult {
    for (value, code) in [
        (update.self_daily_cap, "invalid_self_daily_cap"),
        (update.request_daily_cap, "invalid_request_daily_cap"),
    ] {
        if let Some(v) = value
            && !(0..=200).contains(&v)
        {
            return Err(ApiError::bad_request(code));
        }
    }
    if let Some(v) = update.request_cooldown_min
        && !(0..=120).contains(&v)
    {
        return Err(ApiError::bad_request("invalid_request_cooldown_min"));
    }
    let (cur, _owner) = state
        .store
        .imagegen_settings()
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let next = crate::store::ImagegenSettings {
        self_daily_cap: update.self_daily_cap.unwrap_or(cur.self_daily_cap),
        request_daily_cap: update.request_daily_cap.unwrap_or(cur.request_daily_cap),
        request_cooldown_minutes: update
            .request_cooldown_min
            .unwrap_or(cur.request_cooldown_minutes),
        updated_at: cur.updated_at,
    };
    state
        .store
        .update_imagegen_settings(&next, crate::now_unix())
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let _ = state
        .store
        .audit(
            ActorKind::Panel.as_str(),
            "imagegen.settings",
            Some(&state.bot_id),
            None,
            crate::now_unix(),
        )
        .await;
    get_settings(State(state)).await
}

/// The counting window the gate uses: the later of today's Shanghai day
/// start and the group's reset watermark.
async fn get_usage(
    State(state): State<Arc<AdminState>>,
    AxumPath(group_id): AxumPath<String>,
) -> ApiResult {
    let group_id = super::checked_group_id(&group_id)?;
    let now = crate::now_unix();
    let day_start = crate::store::shanghai_day_start(now);
    let (settings, _owner) = state
        .store
        .imagegen_settings()
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let reset_at = state
        .store
        .imagegen_reset_at(&state.bot_id, &group_id)
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let self_used = state
        .store
        .count_image_jobs(&state.bot_id, &group_id, "self", None, day_start)
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let counts = state
        .store
        .image_requester_counts(&state.bot_id, &group_id, day_start)
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let requests = counts
        .into_iter()
        .map(|(requester_id, used)| {
            json!({
                "requester_id": requester_id,
                "used": used,
                "cap": settings.request_daily_cap,
            })
        })
        .collect::<Vec<_>>();
    // Listing uses an exclusive lower bound; counts above use inclusive day
    // start plus the independent exclusive reset watermark.
    let since = day_start.saturating_sub(1).max(reset_at);
    let jobs = state
        .store
        .list_image_jobs(&state.bot_id, &group_id, since, 100)
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let job_rows = jobs
        .iter()
        .map(|job| {
            json!({
                "id": job.id,
                "bucket": job.bucket,
                "requester_id": job.requester_id,
                "self_portrait": job.self_portrait,
                "prompt": job.prompt,
                "state": job.state,
                "error_class": job.error_class,
                "created_at": job.created_at,
            })
        })
        .collect::<Vec<_>>();
    ok_json(json!({
        "group_id": group_id,
        "day_start": crate::store::shanghai_day_start(now),
        "reset_at": reset_at,
        "self": { "used": self_used, "cap": settings.self_daily_cap },
        "request_cap": settings.request_daily_cap,
        "request_cooldown_min": settings.request_cooldown_minutes,
        "requests": requests,
        "jobs": job_rows,
    }))
}

async fn post_reset(
    State(state): State<Arc<AdminState>>,
    AxumPath(group_id): AxumPath<String>,
) -> ApiResult {
    let group_id = super::checked_group_id(&group_id)?;
    let now = crate::now_unix();
    state
        .store
        .imagegen_reset(&state.bot_id, &group_id, now)
        .await
        .map_err(|_| ApiError::bad_request("store_error"))?;
    let _ = state
        .store
        .audit(
            ActorKind::Panel.as_str(),
            "imagegen.reset",
            Some(&state.bot_id),
            Some(&group_id),
            now,
        )
        .await;
    get_usage(State(state), AxumPath(group_id)).await
}
