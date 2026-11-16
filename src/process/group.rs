//! Ownership of a process group created by this process (never name-based).
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

pub(crate) struct OwnedProcessGroup {
    pgid: AtomicI32,
    stopping: tokio::sync::Mutex<()>,
}

impl OwnedProcessGroup {
    /// The caller must have created a fresh group whose leader is `pid`.
    pub(crate) fn new(pid: u32) -> Self {
        assert!(pid > 1 && pid <= i32::MAX as u32);
        Self {
            pgid: AtomicI32::new(pid as i32),
            stopping: tokio::sync::Mutex::new(()),
        }
    }

    fn exists(&self) -> bool {
        let id = self.pgid.load(Ordering::Acquire);
        if id == 0 {
            return false;
        }
        if matches!(
            killpg(Pid::from_raw(id), None),
            Err(nix::errno::Errno::ESRCH)
        ) {
            self.pgid.store(0, Ordering::Release);
            return false;
        }
        true
    }

    /// Keep ownership armed across awaits: cancelling this cleanup still kills
    /// the group when its owner is dropped. A leader's exit is not group exit.
    pub(crate) async fn stop(&self, grace: Duration) {
        let _serial = self.stopping.lock().await;
        if !self.exists() {
            return;
        }
        let id = self.pgid.load(Ordering::Acquire);
        let _ = killpg(Pid::from_raw(id), Signal::SIGTERM);
        let deadline = tokio::time::Instant::now() + grace;
        while self.exists() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.kill_now();
    }

    /// Synchronous cancellation backstop. Clear ownership after one final kill,
    /// so later Drop/close cannot signal a reused process-group identifier.
    pub(crate) fn kill_now(&self) {
        let id = self.pgid.swap(0, Ordering::AcqRel);
        if id != 0 {
            let _ = killpg(Pid::from_raw(id), Signal::SIGKILL);
        }
    }
}

impl Drop for OwnedProcessGroup {
    fn drop(&mut self) {
        self.kill_now();
    }
}
