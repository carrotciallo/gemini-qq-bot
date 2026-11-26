//! Operator settings that can change at runtime (settings API).
//!
//! The config file stays the base layer. Changes made through
//! `PUT /api/v1/settings/*` are kept as an overlay in
//! `runtime-settings.json` next to the database (the deployed config file
//! is a read-only mount, so it is never rewritten), applied to the live
//! consumers immediately, and re-applied on top of the file config at the
//! next startup.
//!
//! The overlay used to also carry the aux gateway configuration; that
//! whole surface was removed with the gateway teardown (2026-11-24).
//! Old overlay files may still contain `aux_*` keys — serde ignores
//! unknown fields on load, and the next persist rewrites the file
//! without them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::app::pacing::PacingEngine;

/// The persisted overlay. Every field is optional: absent means "the file
/// config decides".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SettingsOverlay {
    pub session_rotation_enabled: Option<bool>,
    pub quota_degradation_enabled: Option<bool>,
}

impl SettingsOverlay {
    /// Load the overlay from `path`; a missing or malformed file is the
    /// empty overlay (the file config then decides everything).
    pub fn load(path: Option<&Path>) -> Self {
        let Some(path) = path else {
            return Self::default();
        };
        std::fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default()
    }
}

/// What the settings API reports about the two runtime switches.
#[derive(Debug, Clone, Serialize)]
pub struct SwitchView {
    pub session_rotation_enabled: bool,
    pub quota_degradation_enabled: bool,
}

/// Live settings: the overlay plus the handles it steers.
pub struct RuntimeSettings {
    path: Option<PathBuf>,
    overlay: RwLock<SettingsOverlay>,
    pacing: Arc<PacingEngine>,
    rotation: Arc<AtomicI64>,
    /// The cap that applies while rotation is on (already resolved).
    rotation_cap: Option<i64>,
    file_rotation_enabled: bool,
    file_degradation_enabled: bool,
}

impl RuntimeSettings {
    pub fn new(
        path: Option<PathBuf>,
        overlay: SettingsOverlay,
        pacing: Arc<PacingEngine>,
        rotation: Arc<AtomicI64>,
        rotation_cap: Option<i64>,
        file_rotation_enabled: bool,
        file_degradation_enabled: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            path,
            overlay: RwLock::new(overlay),
            pacing,
            rotation,
            rotation_cap,
            file_rotation_enabled,
            file_degradation_enabled,
        })
    }

    /// A settings handle over a scratch directory, for admin route tests.
    #[cfg(test)]
    pub fn for_tests(dir: PathBuf, pacing: Arc<PacingEngine>) -> Arc<Self> {
        Self::new(
            Some(dir.join("runtime-settings.json")),
            SettingsOverlay::default(),
            pacing,
            Arc::new(AtomicI64::new(-1)),
            Some(35),
            true,
            true,
        )
    }

    fn overlay(&self) -> SettingsOverlay {
        self.overlay
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn switch_view(&self) -> SwitchView {
        let overlay = self.overlay();
        SwitchView {
            session_rotation_enabled: overlay
                .session_rotation_enabled
                .unwrap_or(self.file_rotation_enabled),
            quota_degradation_enabled: overlay
                .quota_degradation_enabled
                .unwrap_or(self.file_degradation_enabled),
        }
    }

    /// Apply the runtime switches: persist first, then flip the live
    /// handles (pacing degradation flag, shared rotation cap).
    pub fn update_switches(
        &self,
        rotation_enabled: Option<bool>,
        degradation_enabled: Option<bool>,
    ) -> Result<SwitchView, String> {
        let mut overlay = self.overlay();
        if let Some(enabled) = rotation_enabled {
            overlay.session_rotation_enabled = Some(enabled);
        }
        if let Some(enabled) = degradation_enabled {
            overlay.quota_degradation_enabled = Some(enabled);
        }
        self.persist(&overlay)?;
        *self.overlay.write().unwrap_or_else(|e| e.into_inner()) = overlay;
        if let Some(enabled) = degradation_enabled {
            self.pacing.set_degradation_enabled(enabled);
        }
        if let Some(enabled) = rotation_enabled {
            let raw = if enabled {
                self.rotation_cap.unwrap_or(-1)
            } else {
                -1
            };
            self.rotation.store(raw, Ordering::Relaxed);
        }
        Ok(self.switch_view())
    }

    fn persist(&self, overlay: &SettingsOverlay) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let raw = serde_json::to_string_pretty(overlay).map_err(|e| e.to_string())?;
        std::fs::write(path, raw).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_round_trips_through_json() {
        let overlay = SettingsOverlay {
            session_rotation_enabled: Some(true),
            quota_degradation_enabled: Some(false),
        };
        let raw = serde_json::to_string(&overlay).expect("serialize");
        let back: SettingsOverlay = serde_json::from_str(&raw).expect("parse");
        assert_eq!(back, overlay);
    }

    #[test]
    fn missing_overlay_file_loads_as_default() {
        let overlay = SettingsOverlay::load(Some(Path::new("/absent/runtime-settings.json")));
        assert_eq!(overlay, SettingsOverlay::default());
        assert_eq!(SettingsOverlay::load(None), SettingsOverlay::default());
    }

    #[test]
    fn legacy_aux_keys_in_an_old_overlay_are_ignored() {
        let raw = r#"{"aux_enabled": true, "aux_model": "hy3", "aux_api_key": "x",
                      "quota_degradation_enabled": false}"#;
        let overlay: SettingsOverlay = serde_json::from_str(raw).expect("parse");
        assert_eq!(overlay.quota_degradation_enabled, Some(false));
        assert_eq!(overlay.session_rotation_enabled, None);
    }
}
