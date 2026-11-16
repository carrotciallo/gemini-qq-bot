//! PTY screen plumbing: reader thread, snapshots and audit sink.
//! Moved out of resident.rs verbatim (std-split-resident); logic unchanged.

use super::*;

pub(super) fn spawn_reader(mut reader: Box<dyn Read + Send>, screen: Arc<Mutex<Vec<u8>>>) {
    std::thread::spawn(move || {
        let mut chunk = [0u8; 8192];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    if let Ok(mut buffer) = screen.lock() {
                        buffer.extend_from_slice(&chunk[..n]);
                        let len = buffer.len();
                        if len > SCREEN_BUFFER_MAX_BYTES {
                            let drop_from = len - SCREEN_BUFFER_MAX_BYTES;
                            buffer.drain(..drop_from);
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
}

pub(super) fn screen_snapshot(screen: &Arc<Mutex<Vec<u8>>>) -> String {
    match screen.lock() {
        Ok(buffer) => strip_ansi(&String::from_utf8_lossy(&buffer)),
        Err(_) => String::new(),
    }
}

pub(super) fn screen_tail(screen: &Arc<Mutex<Vec<u8>>>) -> String {
    // A bounded tail may begin halfway through a credential whose marker was
    // already discarded. Do not persist any arbitrary terminal content.
    sanitize(&screen_snapshot(screen))
}

pub(super) fn digest_of(screen: &str) -> String {
    let tail: String = screen.chars().rev().take(2_000).collect();
    tail
}

pub(super) fn progress_signal(events: &[StepEvent]) -> (usize, String) {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut digest = DefaultHasher::new();
    for event in events {
        // Content and task/tool transitions are progress even at equal length.
        // Timestamps and PTY repaint noise alone must not postpone the watchdog.
        (
            event.idx,
            &event.kind,
            &event.text,
            &event.tool,
            &event.task_id,
            event.raw_type,
        )
            .hash(&mut digest);
    }
    (events.len(), format!("{:016x}", digest.finish()))
}

pub(super) fn sanitize(text: &str) -> String {
    // This low-level process layer has no loaded secret inventory. Preserve
    // only fixed protocol labels; arbitrary error/screen text becomes counts
    // before either the tracing sink or retained audit queue can see it.
    match text {
        "color_scheme" | "tos" | "trust" | "onboard_next" => text.to_owned(),
        _ => format!(
            "chars{}_lines{}",
            text.chars().count(),
            text.lines().count()
        ),
    }
}

#[cfg(test)]
mod redaction_tests {
    use super::*;

    #[test]
    fn audit_redaction_resident_screen_and_error_sink_never_emit_raw_secrets() {
        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logged = Arc::new(Mutex::new(Vec::new()));
        let writer = Capture(Arc::clone(&logged));
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let audit = Arc::new(Mutex::new(Vec::new()));
        // The credential marker is outside the old 240-char tail. Filtering
        // only the tail would lose the marker and expose its remaining value.
        let stdout = format!("api_key={} 中文", "FIXTURE_RAW_SECRET".repeat(30));
        let stderr = "FIXTURE_OPAQUE_KNOWN_VALUE_123456";
        let screen = Arc::new(Mutex::new(stdout.into_bytes()));
        tracing::subscriber::with_default(subscriber, || {
            push_audit(&audit, format!("resident.screen.{}", screen_tail(&screen)));
            push_audit(&audit, format!("resident.error.{}", sanitize(stderr)));
        });
        let retained = audit.lock().unwrap().join("\n");
        let trace = String::from_utf8(logged.lock().unwrap().clone()).unwrap();
        assert!(!trace.is_empty(), "test must exercise the tracing sink");
        for output in [&retained, &trace] {
            assert!(
                !output.contains("FIXTURE"),
                "resident audit exposed raw screen/error text"
            );
            assert!(
                output.contains("chars"),
                "structural diagnostics must remain"
            );
        }
        for tag in ["color_scheme", "tos", "trust", "onboard_next"] {
            assert_eq!(
                sanitize(tag),
                tag,
                "fixed onboarding event names remain stable"
            );
        }
    }
}

pub(super) fn push_audit(audit: &Arc<Mutex<Vec<String>>>, action: String) {
    tracing::info!(target: "resident", action = %action, "resident audit");
    if let Ok(mut guard) = audit.lock() {
        guard.push(action);
    }
}
