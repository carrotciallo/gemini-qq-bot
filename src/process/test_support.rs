//! Isolated process fixtures. Never signal a process name or the test runner's group.
use std::path::Path;
use std::time::Duration;

pub(crate) fn executable(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, script).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

pub(crate) async fn pid_file(path: &Path) -> i32 {
    for _ in 0..100 {
        if let Ok(text) = std::fs::read_to_string(path)
            && let Ok(pid) = text.trim().parse()
        {
            return pid;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("fixture did not publish pid: {}", path.display());
}

pub(crate) fn live(pid: i32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ")
                .map(|(_, fields)| !fields.starts_with('Z'))
        })
        .unwrap_or(false)
}

pub(crate) async fn stopped(pid: i32) -> bool {
    for _ in 0..100 {
        if !live(pid) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

pub(crate) fn cleanup(pid: i32) {
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    );
}
