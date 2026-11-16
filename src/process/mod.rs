//! Child process and owned process-group supervision.

#[cfg(unix)]
pub(crate) mod group;
pub mod mihomo;
pub mod reaper;

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod test_support;

pub use mihomo::{Mihomo, MihomoConfig, ProxyError, ProxyHealth};
pub use reaper::spawn_reaper;
