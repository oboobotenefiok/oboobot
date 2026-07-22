//! The daemon's own health, tracked separately from any individual
//! position or order's state. A handful of independent conditions can
//! each push the system toward a worse state; `HealthMonitor` always
//! reports the *worst* currently active condition, not just the most
//! recently reported one, so a resolved problem doesn't mask one that's
//! still ongoing.
//!
//! The severity mapping below includes the fix flagged in review: broker
//! heartbeat failure used to map to `Degraded` ("log and keep trading"),
//! which doesn't hold up, since without a broker connection there's no
//! safe way to size a new position against current price or confirm an
//! existing one is still open. It now maps to `ReadOnly`, matching how
//! `NewsApiDown` was already treated.

use domain::SystemState;

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatError<E: std::fmt::Display> {
    #[error("broker call failed: {0}")]
    CallFailed(E),
    #[error("broker call exceeded {0:?} timeout")]
    TimedOut(std::time::Duration),
}

/// Times a broker call and reports `BrokerHeartbeatFailure` if it either
/// errors or takes longer than `timeout`. Generic over the call itself
/// so it works for whatever broker method the caller wants to use as
/// the heartbeat (typically `get_snapshot`, since every real cycle
/// calls that anyway).
///
/// Returns `HeartbeatError` rather than trying to force "the call
/// failed" and "the call never finished in time" into the same error
/// type: they're genuinely different failure modes with different `E`
/// shapes underneath (a real broker error vs. no error at all, just no
/// response), and collapsing them would have meant either losing that
/// distinction or reaching for `unreachable!()` on a branch that isn't
/// actually unreachable.
pub async fn check_broker_heartbeat<F, T, E>(
    monitor: &HealthMonitor,
    timeout: std::time::Duration,
    call: F,
) -> Result<T, HeartbeatError<E>>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(timeout, call).await {
        Ok(Ok(value)) => {
            monitor.clear_failure(HealthCheckFailure::BrokerHeartbeatFailure);
            Ok(value)
        }
        Ok(Err(error)) => {
            monitor.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
            Err(HeartbeatError::CallFailed(error))
        }
        Err(_elapsed) => {
            monitor.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
            Err(HeartbeatError::TimedOut(timeout))
        }
    }
}

/// Best-effort available disk space, in megabytes, for the filesystem
/// containing `path`. Shells out to `df` rather than pulling in a crate
/// for this, since the daemon's real deployment target (GitHub Actions'
/// `ubuntu-latest` runners) always has it, and a failure to run or parse
/// it degrades to `None` rather than an error: a disk check that can't
/// itself run shouldn't be able to take the whole cycle down.
pub async fn available_disk_mb(path: &std::path::Path) -> Option<u64> {
    let output = tokio::process::Command::new("df")
        .arg("-Pm") // POSIX output format, megabyte blocks
        .arg(path)
        .output()
        .await
        .ok()?;

    let text = String::from_utf8(output.stdout).ok()?;
    let data_line = text.lines().nth(1)?;
    let available_field = data_line.split_whitespace().nth(3)?;
    available_field.parse::<u64>().ok()
}

/// Best-effort resident memory usage of this process, in megabytes,
/// read from `/proc/self/status`. Same reasoning as the disk check:
/// Linux-specific, matching the actual deployment target, degrades to
/// `None` rather than erroring if unavailable.
pub async fn resident_memory_mb() -> Option<u64> {
    let contents = tokio::fs::read_to_string("/proc/self/status").await.ok()?;
    let line = contents.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kb_str = line.split_whitespace().nth(1)?;
    let kb: u64 = kb_str.parse().ok()?;
    Some(kb / 1024)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HealthCheckFailure {
    BrokerHeartbeatFailure,
    DiskUsageCritical,
    MemoryUsageCritical,
    NewsApiDown,
    CorrelationStale,
    SpreadHistoryStale,
    SnapshotLatencyExceeded,
    QueueBackpressure,
}

/// The severity a given failure escalates the daemon to, on its own,
/// independent of whatever else might also be failing at the same time.
pub fn severity_for(failure: HealthCheckFailure) -> SystemState {
    match failure {
        // The fix: this used to be `Degraded`. Losing the broker
        // connection means we can't safely size, submit, or reconcile
        // anything, which is exactly the same category of problem
        // `NewsApiDown` already represented, so it gets the same
        // response.
        HealthCheckFailure::BrokerHeartbeatFailure => SystemState::ReadOnly,
        HealthCheckFailure::NewsApiDown => SystemState::ReadOnly,
        HealthCheckFailure::DiskUsageCritical => SystemState::EmergencyShutdown,
        HealthCheckFailure::MemoryUsageCritical => SystemState::EmergencyShutdown,
        HealthCheckFailure::CorrelationStale => SystemState::Degraded,
        HealthCheckFailure::SpreadHistoryStale => SystemState::Degraded,
        HealthCheckFailure::SnapshotLatencyExceeded => SystemState::Degraded,
        HealthCheckFailure::QueueBackpressure => SystemState::Degraded,
    }
}

/// Whether the daemon should be opening brand new positions in a given
/// state. `Healthy` and `Degraded` both still allow it (Degraded just
/// means "with alerts raised"); `ReadOnly` and `EmergencyShutdown` both
/// mean no new entries, they only differ in what happens to positions
/// that are already open.
pub fn allows_new_entries(state: SystemState) -> bool {
    matches!(state, SystemState::Healthy | SystemState::Degraded)
}

fn severity_rank(state: SystemState) -> u8 {
    match state {
        SystemState::Healthy => 0,
        SystemState::Degraded => 1,
        SystemState::ReadOnly => 2,
        SystemState::EmergencyShutdown => 3,
    }
}

/// What the daemon's event loop should actually do in a given state. This
/// is descriptive (for logging and for the event loop to match on), not
/// something that enforces itself; see `daemon::lib` for where these
/// descriptions turn into real gating of new signals and orders.
pub fn auto_action(state: SystemState) -> &'static str {
    match state {
        SystemState::Healthy => "trade normally",
        SystemState::Degraded => "log a warning, continue trading with alerts raised",
        SystemState::ReadOnly => "close all open positions, accept no new orders",
        SystemState::EmergencyShutdown => "flatten all open positions, then exit the process",
    }
}

pub struct HealthMonitor {
    active_failures: parking_lot::Mutex<std::collections::HashSet<HealthCheckFailure>>,
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthMonitor {
    pub fn new() -> Self {
        HealthMonitor {
            active_failures: parking_lot::Mutex::new(std::collections::HashSet::new()),
        }
    }

    pub fn report_failure(&self, failure: HealthCheckFailure) {
        self.active_failures.lock().insert(failure);
    }

    /// Call this once whatever caused a failure has genuinely recovered
    /// (the broker heartbeat succeeded again, the news API responded).
    /// Health only improves when a condition is explicitly cleared, never
    /// just by the passage of time, so a flapping check can't silently
    /// heal itself out of the log.
    pub fn clear_failure(&self, failure: HealthCheckFailure) {
        self.active_failures.lock().remove(&failure);
    }

    pub fn current_state(&self) -> SystemState {
        self.active_failures
            .lock()
            .iter()
            .map(|&failure| severity_for(failure))
            .max_by_key(|&state| severity_rank(state))
            .unwrap_or(SystemState::Healthy)
    }

    pub fn active_failure_count(&self) -> usize {
        self.active_failures.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_failures_means_healthy() {
        let monitor = HealthMonitor::new();
        assert_eq!(monitor.current_state(), SystemState::Healthy);
    }

    #[tokio::test]
    async fn heartbeat_check_clears_the_failure_on_success() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);

        let result: Result<u32, HeartbeatError<String>> =
            check_broker_heartbeat(&monitor, std::time::Duration::from_secs(1), async {
                Ok::<u32, String>(42)
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(monitor.current_state(), SystemState::Healthy);
    }

    #[tokio::test]
    async fn heartbeat_check_reports_failure_when_the_call_errors() {
        let monitor = HealthMonitor::new();
        let result: Result<u32, HeartbeatError<String>> =
            check_broker_heartbeat(&monitor, std::time::Duration::from_secs(1), async {
                Err::<u32, String>("boom".to_string())
            })
            .await;

        assert!(matches!(result, Err(HeartbeatError::CallFailed(_))));
        assert_eq!(monitor.current_state(), SystemState::ReadOnly);
    }

    #[tokio::test]
    async fn heartbeat_check_reports_failure_on_timeout_without_panicking() {
        let monitor = HealthMonitor::new();
        let result: Result<u32, HeartbeatError<String>> =
            check_broker_heartbeat(&monitor, std::time::Duration::from_millis(10), async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Ok::<u32, String>(42)
            })
            .await;

        assert!(matches!(result, Err(HeartbeatError::TimedOut(_))));
        assert_eq!(monitor.current_state(), SystemState::ReadOnly);
    }

    #[tokio::test]
    async fn resident_memory_reports_something_plausible_on_linux() {
        // This sandbox runs Linux, same as the real deployment target
        // (GitHub Actions' ubuntu-latest), so this should always resolve
        // to Some(...) here; a non-Linux environment would see None,
        // which the function is documented to degrade to gracefully
        // rather than error.
        let mb = resident_memory_mb().await;
        assert!(mb.is_some());
        assert!(mb.unwrap() > 0);
    }

    #[test]
    fn broker_heartbeat_failure_now_escalates_to_read_only() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
        assert_eq!(monitor.current_state(), SystemState::ReadOnly);
    }

    #[test]
    fn worst_active_failure_wins_even_if_reported_first() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::CorrelationStale); // Degraded
        monitor.report_failure(HealthCheckFailure::DiskUsageCritical); // EmergencyShutdown
        assert_eq!(monitor.current_state(), SystemState::EmergencyShutdown);
    }

    #[test]
    fn clearing_the_worst_failure_reveals_the_next_worst() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::CorrelationStale); // Degraded
        monitor.report_failure(HealthCheckFailure::DiskUsageCritical); // EmergencyShutdown
        monitor.clear_failure(HealthCheckFailure::DiskUsageCritical);
        assert_eq!(monitor.current_state(), SystemState::Degraded);
    }

    #[test]
    fn clearing_every_failure_returns_to_healthy() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::QueueBackpressure);
        monitor.clear_failure(HealthCheckFailure::QueueBackpressure);
        assert_eq!(monitor.current_state(), SystemState::Healthy);
    }

    #[test]
    fn spread_history_stale_escalates_to_degraded() {
        let monitor = HealthMonitor::new();
        monitor.report_failure(HealthCheckFailure::SpreadHistoryStale);
        assert_eq!(monitor.current_state(), SystemState::Degraded);
    }

    #[test]
    fn read_only_and_emergency_shutdown_both_block_new_entries() {
        assert!(allows_new_entries(SystemState::Healthy));
        assert!(allows_new_entries(SystemState::Degraded));
        assert!(!allows_new_entries(SystemState::ReadOnly));
        assert!(!allows_new_entries(SystemState::EmergencyShutdown));
    }
}
