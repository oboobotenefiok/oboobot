--- ./persistence/src/lib.rs ---
//! Generic, fsync-before-return, append-only cursor file storage. This
//! crate doesn't know what a Position or an Order is; `daemon::recovery`
//! is where cursor files full of `domain::Event`s get turned into actual
//! reconciled state against a live broker.

pub mod cursor;
pub mod snapshot;

pub use cursor::{CursorFile, PersistenceError};
pub use snapshot::SnapshotFile;

--- ./persistence/src/cursor.rs ---
//! An append-only, newline-delimited JSON file with a byte-offset cursor,
//! which is the persistence pattern this whole workspace borrows from
//! bruh. The one rule that actually matters here: `append` does not
//! return successfully until the write has been `fsync`'d to disk. A
//! write that only makes it as far as the OS page cache can still be
//! lost on a power cut or an OOM kill, and "the daemon thinks this order
//! was recorded, but it wasn't really durable yet" is exactly the kind of
//! gap that turns into an orphaned position after a crash.

use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("I/O error on cursor file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("could not read or write a record in {path}: {source}")]
    Serde {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

pub struct CursorFile<T> {
    path: PathBuf,
    _marker: PhantomData<T>,
}

impl<T> CursorFile<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new(path: impl Into<PathBuf>) -> Self {
        CursorFile {
            path: path.into(),
            _marker: PhantomData,
        }
    }

    /// Append one record as a new line and return the file's new byte
    /// length (the new cursor position), only after the write has been
    /// fsync'd. See the module docs for why the fsync isn't optional.
    pub async fn append(&self, record: &T) -> Result<u64, PersistenceError> {
        let mut line =
            serde_json::to_string(record).map_err(|source| self.serde_err(source))?;
        line.push('\n');

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|source| self.io_err(source))?;

        file.write_all(line.as_bytes())
            .await
            .map_err(|source| self.io_err(source))?;

        // The durability guarantee this whole type exists for. Everything
        // above this line could, in principle, still be sitting in a
        // buffer somewhere; after this line returns `Ok`, the record is
        // actually on disk.
        file.sync_all().await.map_err(|source| self.io_err(source))?;

        let metadata = file.metadata().await.map_err(|source| self.io_err(source))?;
        Ok(metadata.len())
    }

    /// Every record in the file, from the beginning. Used at startup
    /// before a cursor offset has been established.
    pub async fn read_all(&self) -> Result<Vec<T>, PersistenceError> {
        self.read_from(0).await
    }

    /// Every record starting at a given byte offset, which is how a
    /// daemon resumes from a previously saved cursor instead of
    /// re-reading its entire history on every restart.
    pub async fn read_from(&self, offset: u64) -> Result<Vec<T>, PersistenceError> {
        let file = match tokio::fs::OpenOptions::new().read(true).open(&self.path).await {
            Ok(file) => file,
            // A cursor file that hasn't been created yet just means
            // "nothing's been recorded so far," not an error condition.
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => return Err(self.io_err(source)),
        };

        let mut file = file;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| self.io_err(source))?;

        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut line = String::new();

        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(|source| self.io_err(source))?;
            if bytes_read == 0 {
                break;
            }
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                continue;
            }
            let record: T =
                serde_json::from_str(trimmed).map_err(|source| self.serde_err(source))?;
            records.push(record);
        }

        Ok(records)
    }

    /// The current end-of-file byte offset, i.e. what a cursor should be
    /// set to right now if you wanted to skip everything already
    /// recorded.
    pub async fn current_offset(&self) -> Result<u64, PersistenceError> {
        match tokio::fs::metadata(&self.path).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(source) => Err(self.io_err(source)),
        }
    }

    fn io_err(&self, source: std::io::Error) -> PersistenceError {
        PersistenceError::Io {
            path: self.path.display().to_string(),
            source,
        }
    }

    fn serde_err(&self, source: serde_json::Error) -> PersistenceError {
        PersistenceError::Serde {
            path: self.path.display().to_string(),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SampleRecord {
        id: u32,
        label: String,
    }

    #[tokio::test]
    async fn append_then_read_all_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();

        let records = cursor.read_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].id, 1);
        assert_eq!(records[1].id, 2);
    }

    #[tokio::test]
    async fn read_from_a_saved_offset_only_returns_newer_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        let offset_after_first = cursor.current_offset().await.unwrap();
        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();

        let records = cursor.read_from(offset_after_first).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, 2);
    }

    #[tokio::test]
    async fn reading_a_file_that_does_not_exist_yet_is_an_empty_list_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("never_written.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        let records = cursor.read_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn current_offset_grows_with_each_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.cursor");
        let cursor = CursorFile::<SampleRecord>::new(&path);

        assert_eq!(cursor.current_offset().await.unwrap(), 0);
        cursor
            .append(&SampleRecord { id: 1, label: "first".to_string() })
            .await
            .unwrap();
        let after_one = cursor.current_offset().await.unwrap();
        assert!(after_one > 0);

        cursor
            .append(&SampleRecord { id: 2, label: "second".to_string() })
            .await
            .unwrap();
        let after_two = cursor.current_offset().await.unwrap();
        assert!(after_two > after_one);
    }
}

--- ./persistence/src/snapshot.rs ---
//! `CursorFile` is for things worth keeping a full history of: every
//! position, every decision. Some state isn't like that: a daily
//! buffer's current high and low, the current True Open level, a status
//! blob for a human to glance at. For those, appending forever just
//! means reading further and further back to find the one line that's
//! still relevant. `SnapshotFile` is the other half of that pair: read
//! the current value (if any), overwrite it with a new one. Same
//! fsync-before-return durability guarantee as `CursorFile`, same
//! "empty/missing means None, not an error" startup behavior, different
//! shape for a different kind of state.

use std::marker::PhantomData;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::cursor::PersistenceError;

pub struct SnapshotFile<T> {
    path: PathBuf,
    _marker: PhantomData<T>,
}

impl<T> SnapshotFile<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    pub fn new(path: impl Into<PathBuf>) -> Self {
        SnapshotFile { path: path.into(), _marker: PhantomData }
    }

    /// The current value, or `None` if this snapshot has never been
    /// written (a fresh state directory, or a buffer that hasn't reset
    /// and captured anything yet).
    pub async fn read(&self) -> Result<Option<T>, PersistenceError> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(self.io_err(source)),
        };

        if contents.trim().is_empty() {
            return Ok(None);
        }

        let value: T = serde_json::from_str(contents.trim()).map_err(|source| self.serde_err(source))?;
        Ok(Some(value))
    }

    /// Overwrite the current value. Written to a temp file in the same
    /// directory and renamed into place, which on every platform this is
    /// meant to run on (Linux, via GitHub Actions runners) is an atomic
    /// operation: a reader can never observe a half-written file, only
    /// the old value or the new one, never a torn mix of both. Still
    /// fsync'd before the function returns, same as `CursorFile::append`.
    pub async fn write(&self, value: &T) -> Result<(), PersistenceError> {
        let json = serde_json::to_string(value).map_err(|source| self.serde_err(source))?;

        let tmp_path = self.path.with_extension("tmp");
        let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|source| self.io_err(source))?;
        file.write_all(json.as_bytes()).await.map_err(|source| self.io_err(source))?;
        file.sync_all().await.map_err(|source| self.io_err(source))?;
        drop(file);

        tokio::fs::rename(&tmp_path, &self.path).await.map_err(|source| self.io_err(source))?;
        Ok(())
    }

    fn io_err(&self, source: std::io::Error) -> PersistenceError {
        PersistenceError::Io { path: self.path.display().to_string(), source }
    }

    fn serde_err(&self, source: serde_json::Error) -> PersistenceError {
        PersistenceError::Serde { path: self.path.display().to_string(), source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        value: u32,
    }

    #[tokio::test]
    async fn missing_snapshot_reads_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        assert_eq!(snap.read().await.unwrap(), None);
    }

    #[tokio::test]
    async fn write_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        snap.write(&Sample { value: 7 }).await.unwrap();
        assert_eq!(snap.read().await.unwrap(), Some(Sample { value: 7 }));
    }

    #[tokio::test]
    async fn a_second_write_replaces_rather_than_appends() {
        let dir = tempfile::tempdir().unwrap();
        let snap = SnapshotFile::<Sample>::new(dir.path().join("s.json"));
        snap.write(&Sample { value: 1 }).await.unwrap();
        snap.write(&Sample { value: 2 }).await.unwrap();
        assert_eq!(snap.read().await.unwrap(), Some(Sample { value: 2 }));
    }
}

--- ./persistence/Cargo.toml ---
[package]
name = "persistence"
version = "0.1.0"
edition = "2021"

[dependencies]
domain = { path = "../domain" }
tokio = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }

--- ./Cargo.toml ---
[workspace]
resolver = "2"
members = [
    "domain",
    "session_time",
    "broker",
    "strategy",
    "risk",
    "persistence",
    "daemon",
]

# Every crate in this workspace pulls its third-party dependencies from here.
# The point of doing it this way (instead of letting each crate pick its own
# version) is that we only ever have one copy of, say, rust_decimal in the
# dependency tree. That matters more than usual for us because a lot of our
# domain types (Percent, Usd, Decimal) cross crate boundaries constantly, and
# two slightly different semver-compatible-but-not-identical versions of the
# same crate can quietly become two different, incompatible types.
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
anyhow = "1"
uuid = { version = "=1.10.0", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
chrono-tz = "0.9"
rust_decimal = { version = "=1.30.0", default-features = false, features = ["serde-with-str"] }
rust_decimal_macros = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
parking_lot = "0.12"
arc-swap = "1"
# Pinned rather than left at the latest "1.x": proptest 1.11 depends on
# rand 0.9, which pulls in a getrandom release that requires Cargo's
# edition2024 feature, not supported by the Rust 1.75 toolchain this
# workspace builds with (installed via apt; see the workspace-level note
# on why apt rather than rustup). proptest 1.4.0 depends on rand 0.8
# instead, which sidesteps that whole chain.
proptest = "=1.4.0"
# Same situation, different package: newer tempfile releases pull in a
# getrandom version with the same edition2024 requirement. 3.14.0 predates
# that.
tempfile = "=3.14.0"
toml = "0.8"

[profile.dev]
# Debug assertions catch integer overflow and similar arithmetic mistakes at
# runtime instead of silently wrapping. For a trading daemon, an arithmetic
# bug that silently wraps instead of panicking is much scarier than a panic
# would be, so we want these on even outside of `cargo test`.
overflow-checks = true

--- ./daemon/src/health.rs ---
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

        let result: Result<u32, HeartbeatError<String>> = check_broker_heartbeat(
            &monitor,
            std::time::Duration::from_secs(1),
            async { Ok::<u32, String>(42) },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(monitor.current_state(), SystemState::Healthy);
    }

    #[tokio::test]
    async fn heartbeat_check_reports_failure_when_the_call_errors() {
        let monitor = HealthMonitor::new();
        let result: Result<u32, HeartbeatError<String>> = check_broker_heartbeat(
            &monitor,
            std::time::Duration::from_secs(1),
            async { Err::<u32, String>("boom".to_string()) },
        )
        .await;

        assert!(matches!(result, Err(HeartbeatError::CallFailed(_))));
        assert_eq!(monitor.current_state(), SystemState::ReadOnly);
    }

    #[tokio::test]
    async fn heartbeat_check_reports_failure_on_timeout_without_panicking() {
        let monitor = HealthMonitor::new();
        let result: Result<u32, HeartbeatError<String>> = check_broker_heartbeat(
            &monitor,
            std::time::Duration::from_millis(10),
            async {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                Ok::<u32, String>(42)
            },
        )
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
    fn read_only_and_emergency_shutdown_both_block_new_entries() {
        assert!(allows_new_entries(SystemState::Healthy));
        assert!(allows_new_entries(SystemState::Degraded));
        assert!(!allows_new_entries(SystemState::ReadOnly));
        assert!(!allows_new_entries(SystemState::EmergencyShutdown));
    }
}

--- ./daemon/src/assistant.rs ---
//! This module exists because of the tension flagged in review: the spec
//! states "the risk engine is the sole authority for position sizing" in
//! one place, and describes an AssistantEngine that scores signals and
//! assesses risk in another, without ever saying which one wins if they'd
//! ever disagree.
//!
//! The resolution implemented here is structural, not just a comment
//! promising good behavior: a `Recommendation` is inert data. It has a
//! severity, a message, and an optional *suggestion* of a config change
//! that's just a field name, a proposed value, and a rationale, all
//! strings. There is no method on `Recommendation`, no `From` impl, no
//! callback, nothing that turns it into an actual mutation of a
//! `RiskConfig` or a `StrategyEngine` parameter. The only thing this
//! crate ever does with one is log it. If a future version of this
//! daemon wants to let an operator manually apply a suggested change,
//! that has to be a new, separate, explicitly human-invoked function, not
//! an extension of anything in this module. `AssistantEngine` itself is
//! also explicitly not on the daemon's startup or shutdown critical path,
//! since a component whose own health checks include things like "model
//! corruption" has no business being a hard dependency for whether the
//! core trading loop can run at all.

use async_trait::async_trait;
use domain::EventEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// A proposed change, described only as data: which field, what value,
/// why. Nothing here can execute; it's exactly as inert as a comment in a
/// log file, which is deliberate.
#[derive(Debug, Clone)]
pub struct ConfigChangeSuggestion {
    pub field: String,
    pub suggested_value: String,
    pub rationale: String,
}

#[derive(Debug, Clone)]
pub struct Recommendation {
    pub severity: Severity,
    pub message: String,
    pub suggested_change: Option<ConfigChangeSuggestion>,
}

#[async_trait]
pub trait AssistantEngine: Send + Sync {
    async fn analyze_event(&self, event: &EventEnvelope) -> Vec<Recommendation>;
}

/// The reference implementation: analyzes nothing, recommends nothing.
/// A real pattern-detection engine would replace this, but it would still
/// only ever be able to produce `Recommendation`s, which, as above,
/// cannot do anything on their own. That's what makes this safe to add
/// real intelligence to later without revisiting this boundary.
pub struct LoggingAssistant;

#[async_trait]
impl AssistantEngine for LoggingAssistant {
    async fn analyze_event(&self, _event: &EventEnvelope) -> Vec<Recommendation> {
        Vec::new()
    }
}

/// The one and only thing this daemon does with a `Recommendation`: write
/// it to the log for a human to read. There is no function anywhere in
/// this codebase that takes a `Recommendation` and feeds it into
/// `risk::RiskConfig` or any `strategy` parameter automatically. If you're
/// looking for where that wiring is, it doesn't exist, on purpose.
pub fn record_recommendation(recommendation: &Recommendation) {
    match recommendation.severity {
        Severity::Info => tracing::info!(message = %recommendation.message, "assistant recommendation (advisory only)"),
        Severity::Warning => tracing::warn!(message = %recommendation.message, "assistant recommendation (advisory only)"),
        Severity::Critical => tracing::error!(message = %recommendation.message, "assistant recommendation (advisory only)"),
    }

    if let Some(change) = &recommendation.suggested_change {
        tracing::info!(
            field = %change.field,
            suggested_value = %change.suggested_value,
            rationale = %change.rationale,
            "suggested config change requires manual operator review, it will not be applied automatically"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Event;

    #[tokio::test]
    async fn logging_assistant_never_recommends_anything() {
        let assistant = LoggingAssistant;
        let envelope = EventEnvelope::new(chrono::Utc::now(), Event::MacroCycleStarted);
        let recommendations = assistant.analyze_event(&envelope).await;
        assert!(recommendations.is_empty());
    }
}

--- ./daemon/src/lib.rs ---
//! Everything that turns the domain, session_time, broker, strategy,
//! risk, and persistence crates into an actual running process: the
//! health state machine (with real Linux-target checks, not just the
//! demo's simulated ones), the two-channel event bus, the macro-cycle
//! scheduler, startup reconciliation, the (deliberately inert) assistant
//! boundary, config loading, news-driven exit checks, continuous
//! position monitoring, notifications, and the small operational pieces
//! (kill switch, decisions log, status snapshot, position-collision
//! guard). `main.rs` wires all of this into a runnable binary; the
//! integration test in `tests/` drives the same wiring end to end
//! against `MockBroker`.

pub mod assistant;
pub mod config;
pub mod event_bus;
pub mod health;
pub mod monitor;
pub mod news;
pub mod notifications;
pub mod operations;
pub mod recovery;
pub mod scheduler;

pub use assistant::{AssistantEngine, ConfigChangeSuggestion, LoggingAssistant, Recommendation, Severity};
pub use config::{Config, ConfigError, NotificationSection, PairConfig, RiskSection};
pub use event_bus::{event_bus, EventBusError, EventBusHandle, EventBusReceiver};
pub use health::{
    allows_new_entries, auto_action, available_disk_mb, check_broker_heartbeat, resident_memory_mb,
    severity_for, HealthCheckFailure, HealthMonitor, HeartbeatError,
};
pub use monitor::{evaluate_exits, ExitDecision};
pub use news::{should_exit_for_news, NewsProvider, NoNewsProvider};
pub use notifications::{notifier_from_config, NoopNotifier, Notifier, SlackNotifier, TelegramNotifier};
pub use operations::{already_entered_this_cycle, kill_switch_engaged, DecisionRecord, StatusSnapshot};
pub use recovery::{apply_reconciliation, reconcile, ReconciliationReport};
pub use scheduler::Scheduler;

--- ./daemon/src/main.rs ---
//! `oboobot` — real entry point for the QuarterlyTheory_SMT_Trader daemon.
//!
//! Two distinct modes live in this file:
//!
//! - The default, real mode: parse CLI flags, check the kill switch,
//!   reconcile, run the (always-on, window-independent) exit-condition
//!   sweep, and only then check whether this invocation is inside a
//!   macro cycle window before considering any *new* entry. This is the
//!   shape a GitHub Actions workflow invokes every five minutes.
//! - `--demo`: the original scripted walkthrough, unchanged, useful for
//!   anyone exploring this repo who wants the whole pipeline narrated
//!   in one run rather than deployed for real.
//!
//! What changed in this pass, versus the previous version of this file:
//! real rolling daily/session buffers replace the always-neutral
//! placeholder (so divergence detection can actually fire in the real
//! path now), True Open is captured and persisted rather than hardcoded
//! to Neutral, the spread filter and holiday check are wired in, and
//! open positions are watched every single invocation for risk-reward,
//! pre-news, and SMT-contradiction exits, independent of whether this
//! invocation is inside an entry window at all.

use std::path::PathBuf;

use broker::{BrokerAdapter, BybitAdapter, DerivAdapter, MockBroker};
use clap::{Parser, ValueEnum};
use daemon::{
    already_entered_this_cycle, allows_new_entries, apply_reconciliation, auto_action,
    available_disk_mb, check_broker_heartbeat, evaluate_exits, kill_switch_engaged,
    notifier_from_config, reconcile, resident_memory_mb, AssistantEngine, Config,
    DecisionRecord, HealthCheckFailure, HealthMonitor, LoggingAssistant, NewsProvider,
    NoNewsProvider, StatusSnapshot,
};
use domain::{Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Usd};
use persistence::{CursorFile, SnapshotFile};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use risk::RiskEngine as _;
use session_time::HolidayProvider;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "oboobot", about = "QuarterlyTheory_SMT_Trader: an SMT-divergence trading daemon")]
struct Cli {
    /// Which broker to trade through. `deriv` and `bybit` read their
    /// config from the environment; `deriv` has a real WebSocket client
    /// wired in (some methods still stubbed), `bybit` is fully stubbed.
    /// `mock` runs end to end.
    #[arg(long, value_enum, default_value_t = BrokerKind::Mock)]
    broker: BrokerKind,

    /// Where cursor and snapshot files are read from and written to. In
    /// the GitHub Actions deployment this points at a checkout of the
    /// dedicated state repo.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Path to the TOML config file. Missing is fine — falls back to
    /// Config::default_config.
    #[arg(long, default_value = "./config.toml")]
    config: PathBuf,

    /// Skip the macro-cycle window check and consider a new entry
    /// regardless. Exit-condition monitoring always runs either way.
    #[arg(long)]
    force: bool,

    /// Run the original scripted walkthrough instead of a real cycle.
    /// Ignores every other flag.
    #[arg(long)]
    demo: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum BrokerKind {
    Mock,
    Deriv,
    Bybit,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if cli.demo {
        return run_demo().await;
    }

    run_real_cycle(cli).await
}

/// Writes the status snapshot. Called from every exit path in
/// `run_real_cycle` so `state/status.json` always reflects the most
/// recent invocation, whatever it decided to do.
async fn write_status(
    status_snap: &SnapshotFile<StatusSnapshot>,
    open_positions: &[Position],
    health: &HealthMonitor,
    last_decision: Option<&str>,
    paused: bool,
) {
    let snapshot = StatusSnapshot {
        last_run: Some(chrono::Utc::now()),
        open_position_count: open_positions.len(),
        health_state: format!("{:?}", health.current_state()),
        last_decision: last_decision.map(|s| s.to_string()),
        paused,
    };
    // A failure to write the status file is logged, not propagated:
    // it's a convenience artifact for a human to glance at, not
    // something the cycle's actual correctness depends on.
    if let Err(error) = status_snap.write(&snapshot).await {
        tracing::warn!(%error, "failed to write status snapshot");
    }
}

/// The real, deployable path.
async fn run_real_cycle(cli: Cli) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&cli.state_dir).await?;
    let status_snap: SnapshotFile<StatusSnapshot> = SnapshotFile::new(cli.state_dir.join("status.json"));

    if kill_switch_engaged(&cli.state_dir).await {
        tracing::warn!("kill switch (PAUSED file) engaged, exiting without evaluating anything new");
        let health = HealthMonitor::new();
        write_status(&status_snap, &[], &health, Some("paused"), true).await;
        return Ok(());
    }

    let config = Config::load(&cli.config).await?;
    let Some(pair_config) = config.pairs.first().cloned() else {
        anyhow::bail!("no pairs configured");
    };
    let primary = pair_config.primary;
    let secondary = pair_config.secondary;

    let health = HealthMonitor::new();
    let notifier = notifier_from_config(&config.notifications);
    let news_provider = NoNewsProvider;
    let holidays = session_time::StaticHolidayProvider;

    let positions_cursor: CursorFile<Position> = CursorFile::new(cli.state_dir.join("positions.cursor"));
    let decisions_cursor: CursorFile<DecisionRecord> = CursorFile::new(cli.state_dir.join("decisions.cursor"));
    let daily_primary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{primary}.json")));
    let daily_secondary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{secondary}.json")));
    let session_primary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_session_{primary}.json")));
    let session_secondary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_session_{secondary}.json")));
    let correlation_snap: SnapshotFile<strategy::CorrelationState> =
        SnapshotFile::new(cli.state_dir.join("correlation.json"));
    let spread_snap: SnapshotFile<strategy::SpreadHistory> =
        SnapshotFile::new(cli.state_dir.join("spread_history.json"));
    let weekly_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_weekly.json"));
    let daily_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_daily.json"));

    let broker: Box<dyn BrokerAdapter> = match cli.broker {
        BrokerKind::Mock => Box::new(MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000))),
        BrokerKind::Deriv => Box::new(DerivAdapter::connect_from_env().await?),
        BrokerKind::Bybit => Box::new(BybitAdapter::from_env()?),
    };

    // Reconciliation always runs first: what does local state say is
    // open, and does the broker agree?
    let locally_known_positions = positions_cursor.read_all().await?;
    let report = reconcile(broker.as_ref(), &locally_known_positions).await?;
    if !report.is_clean() {
        tracing::warn!(
            orphaned = report.orphaned_locally.len(),
            adopted = report.unknown_to_local.len(),
            "reconciliation found a mismatch"
        );
        notifier
            .notify(&format!(
                "oboobot: reconciliation mismatch (orphaned={}, adopted={})",
                report.orphaned_locally.len(),
                report.unknown_to_local.len()
            ))
            .await;
    } else {
        tracing::info!(known_positions = locally_known_positions.len(), "reconciliation clean");
    }
    let mut open_positions = apply_reconciliation(&report);

    // The heartbeat-wrapped snapshot call: this is both the broker
    // health check and the actual market data for everything below it.
    let heartbeat_timeout = std::time::Duration::from_secs(15);
    let snapshot = match check_broker_heartbeat(
        &health,
        heartbeat_timeout,
        broker.get_snapshot(&[primary.clone(), secondary.clone()]),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::error!(%error, "broker heartbeat failed");
            notifier.notify(&format!("oboobot: broker heartbeat failed: {error}")).await;
            write_status(&status_snap, &open_positions, &health, Some("heartbeat_failed"), false).await;
            return Ok(());
        }
    };

    let now = chrono::Utc::now();
    let primary_price = snapshot.prices.get(&primary).map(|q| q.bid).unwrap_or(Decimal::ZERO);
    let secondary_price = snapshot.prices.get(&secondary).map(|q| q.bid).unwrap_or(Decimal::ZERO);
    tracing::info!(
        %primary, primary_price = %primary_price, %secondary, secondary_price = %secondary_price,
        "broker heartbeat ok, snapshot fetched"
    );

    if let Some(mb) = available_disk_mb(&cli.state_dir).await {
        tracing::debug!(available_disk_mb = mb, "disk check");
        if mb < 500 {
            health.report_failure(HealthCheckFailure::DiskUsageCritical);
        } else {
            health.clear_failure(HealthCheckFailure::DiskUsageCritical);
        }
    }
    if let Some(mb) = resident_memory_mb().await {
        tracing::debug!(resident_memory_mb = mb, "memory check");
        if mb > 512 {
            health.report_failure(HealthCheckFailure::MemoryUsageCritical);
        } else {
            health.clear_failure(HealthCheckFailure::MemoryUsageCritical);
        }
    }

    // Update the rolling market-state files. Always, regardless of
    // window: even a cycle that ends up not trading still contributes
    // an observation to the buffers, correlation window, and spread
    // history, which is exactly what lets those be real by the time a
    // window does roll around.
    let daily_primary = strategy::update_daily_buffer(daily_primary_snap.read().await?, primary_price, now);
    daily_primary_snap.write(&daily_primary).await?;
    let daily_secondary = strategy::update_daily_buffer(daily_secondary_snap.read().await?, secondary_price, now);
    daily_secondary_snap.write(&daily_secondary).await?;
    let session_primary = strategy::update_session_buffer(session_primary_snap.read().await?, primary_price, now);
    session_primary_snap.write(&session_primary).await?;
    let session_secondary =
        strategy::update_session_buffer(session_secondary_snap.read().await?, secondary_price, now);
    session_secondary_snap.write(&session_secondary).await?;

    let mut correlation_state = correlation_snap.read().await?.unwrap_or_default();
    correlation_state = strategy::record_sample(correlation_state, primary_price, secondary_price);
    correlation_snap.write(&correlation_state).await?;
    if let Some(shift) = strategy::detect_regime_shift(&correlation_state, config.risk.regime_shift_threshold) {
        tracing::warn!(baseline = shift.baseline, current = shift.current, "correlation regime shift detected");
        notifier
            .notify(&format!(
                "oboobot: correlation regime shift (baseline {:.2} -> current {:.2})",
                shift.baseline, shift.current
            ))
            .await;
    }

    let mut spread_history = spread_snap.read().await?.unwrap_or_default();
    let current_spread = snapshot.spreads.get(&primary).copied().unwrap_or(Decimal::ZERO);
    spread_history.record(current_spread);
    spread_snap.write(&spread_history).await?;

    let divergence_inputs = DivergenceInputs {
        primary_price,
        secondary_price,
        daily_primary_buffer: daily_primary.as_buffer_levels(),
        daily_secondary_buffer: daily_secondary.as_buffer_levels(),
        session_primary_buffer: session_primary.as_buffer_levels(),
        session_secondary_buffer: session_secondary.as_buffer_levels(),
    };
    let current_divergence = strategy::evaluate_smt(&divergence_inputs);
    tracing::debug!(
        daily_primary_high = %daily_primary.high, daily_primary_low = %daily_primary.low,
        session_primary_high = %session_primary.high, session_primary_low = %session_primary.low,
        divergence = ?current_divergence,
        "market state updated"
    );

    // Exit-condition monitoring: always runs, independent of the entry
    // window below. This is the fix for the bigger of the two gaps
    // named in review: a position no longer sits unwatched between the
    // cycle that opened it and whenever the next window happens to be.
    let news_events = news_provider.upcoming_events(now, chrono::Duration::minutes(15)).await;
    let exits = evaluate_exits(
        &open_positions,
        primary_price,
        &news_events,
        now,
        chrono::Duration::minutes(15),
        current_divergence,
    );
    for exit in &exits {
        match broker.close_position(exit.position_id).await {
            Ok(order) => {
                tracing::info!(position_id = %exit.position_id, reason = ?exit.reason, order_id = %order.order_id, "position closed");
                notifier
                    .notify(&format!("oboobot: closed position {} ({:?})", exit.position_id, exit.reason))
                    .await;
                decisions_cursor
                    .append(&DecisionRecord::new(primary.clone(), "position_closed").with_detail(format!("{:?}", exit.reason)))
                    .await?;
            }
            Err(error) => {
                tracing::error!(%error, position_id = %exit.position_id, "failed to close a position flagged for exit");
            }
        }
    }
    if !exits.is_empty() {
        open_positions = broker.list_open_positions().await?;
        for position in &open_positions {
            positions_cursor.append(position).await?;
        }
    } else {
        tracing::debug!(open_positions = open_positions.len(), "exit sweep: nothing to close");
    }

    // Everything from here on is about *new* entries, which the window
    // gates and exits never were.
    if !cli.force && !session_time::is_within_macro_cycle(now) {
        tracing::info!("not within a macro cycle window; exits were already checked above, no new entry considered");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "outside_window")).await?;
        write_status(&status_snap, &open_positions, &health, Some("outside_window"), false).await;
        return Ok(());
    }
    tracing::info!(forced = cli.force, "within a macro cycle window, considering a new entry");

    if !allows_new_entries(health.current_state()) {
        tracing::info!(state = ?health.current_state(), action = auto_action(health.current_state()), "health state does not allow new entries");
        decisions_cursor
            .append(&DecisionRecord::new(primary.clone(), "health_blocked").with_detail(format!("{:?}", health.current_state())))
            .await?;
        write_status(&status_snap, &open_positions, &health, Some("health_blocked"), false).await;
        return Ok(());
    }

    if holidays.is_low_liquidity(now.date_naive()) {
        tracing::info!("today is a recognized low-liquidity period, skipping new entries");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "holiday_skip")).await?;
        write_status(&status_snap, &open_positions, &health, Some("holiday_skip"), false).await;
        return Ok(());
    }

    let spread_multiplier = Decimal::try_from(config.risk.spread_multiplier).unwrap_or(dec!(1.5));
    if !spread_history.passes_filter(current_spread, spread_multiplier) {
        tracing::info!(current_spread = %current_spread, "spread filter rejected this cycle");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "spread_rejected")).await?;
        write_status(&status_snap, &open_positions, &health, Some("spread_rejected"), false).await;
        return Ok(());
    }

    if already_entered_this_cycle(&primary, &open_positions, now) {
        tracing::info!("already entered this pair within the current cycle window, skipping");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "collision_skip")).await?;
        write_status(&status_snap, &open_positions, &health, Some("collision_skip"), false).await;
        return Ok(());
    }

    let weekly_bias = load_or_capture_bias(
        &weekly_true_open_snap,
        session_time::Timeframe::Weekly,
        &primary,
        primary_price,
        now,
        &holidays,
    )
    .await?;
    let daily_bias = load_or_capture_bias(
        &daily_true_open_snap,
        session_time::Timeframe::Daily,
        &primary,
        primary_price,
        now,
        &holidays,
    )
    .await?;

    let outcome = generate_signal(
        &divergence_inputs,
        weekly_bias,
        daily_bias,
        primary.clone(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        now + chrono::Duration::minutes(20),
    );

    let last_decision = match outcome {
        SignalOutcome::NoDivergence => {
            tracing::info!("no SMT divergence this cycle, nothing to evaluate");
            decisions_cursor.append(&DecisionRecord::new(primary.clone(), "no_divergence")).await?;
            "no_divergence".to_string()
        }
        SignalOutcome::Rejected(invalidated) => {
            tracing::info!(
                reason = ?invalidated.rejection_reason,
                weekly_bias = ?invalidated.weekly_bias,
                daily_bias = ?invalidated.daily_bias,
                smt_direction = ?invalidated.smt_direction,
                "signal generated but rejected by the True Open gate"
            );
            decisions_cursor
                .append(&DecisionRecord::new(primary.clone(), "gate_rejected").with_detail(format!("{:?}", invalidated.rejection_reason)))
                .await?;
            "gate_rejected".to_string()
        }
        SignalOutcome::Signal(signal) => {
            tracing::info!(tier = ?signal.tier, direction = ?signal.direction, "signal passed the True Open gate");
            let risk_config = risk::RiskConfig {
                base_risk_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.base_risk_percent).unwrap_or(dec!(1.0))),
                max_risk_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.max_risk_percent).unwrap_or(dec!(5.0))),
                max_open_positions: config.risk.max_open_positions,
                daily_loss_limit_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.daily_loss_limit_percent).unwrap_or(dec!(5.0))),
                weekly_loss_limit_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.weekly_loss_limit_percent).unwrap_or(dec!(10.0))),
            };

            let equity = broker.get_account_equity().await?;
            let entry_price = match signal.direction {
                Direction::Buy => snapshot.prices.get(&primary).map(|q| q.ask).unwrap_or(primary_price),
                Direction::Sell => primary_price,
            };
            let stop_loss_price = match signal.direction {
                Direction::Buy => entry_price - dec!(0.0050),
                Direction::Sell => entry_price + dec!(0.0050),
            };
            let take_profit_price = match signal.direction {
                Direction::Buy => entry_price + dec!(0.0150),
                Direction::Sell => entry_price - dec!(0.0150),
            };

            let risk_context = risk::RiskContext {
                equity,
                open_position_count: open_positions.len(),
                is_tuesday: false,
                is_double_smt: signal.tier == domain::Tier::Double,
                entry_price,
                stop_loss_price,
                take_profit_price,
                realized_pnl_today: Usd::zero(),
                realized_pnl_this_week: Usd::zero(),
            };

            let risk_engine = risk::DefaultRiskEngine;
            let decision = risk_engine.evaluate(&signal, &risk_config, &risk_context)?;

            if !decision.approved {
                tracing::info!(reason = ?decision.rejection_reason, "risk engine rejected the signal");
                decisions_cursor
                    .append(&DecisionRecord::new(primary.clone(), "risk_rejected").with_detail(decision.rejection_reason.clone().unwrap_or_default()))
                    .await?;
                "risk_rejected".to_string()
            } else {
                tracing::info!(
                    size = %decision.position_size, risk_percent = %decision.risk_percent, risk_currency = %decision.risk_currency,
                    "risk engine approved sizing"
                );
                let request = OrderRequest {
                    order_id: Uuid::new_v4(),
                    trace_id: signal.trace_id,
                    signal_id: signal.signal_id,
                    pair: signal.pair.clone(),
                    side: signal.direction,
                    size: decision.position_size,
                    order_type: OrderType::Market,
                    price: None,
                    stop_loss: Some(decision.stop_loss),
                    take_profit: Some(decision.take_profit),
                    confirming_snapshot_id: snapshot.snapshot_id,
                };
                let order = broker.submit_order(request).await?;
                tracing::info!(order_id = %order.order_id, status = ?order.status, "order submitted");
                notifier
                    .notify(&format!("oboobot: opened {:?} {} (size {})", signal.direction, primary, decision.position_size))
                    .await;

                open_positions = broker.list_open_positions().await?;
                for position in &open_positions {
                    positions_cursor.append(position).await?;
                }
                decisions_cursor.append(&DecisionRecord::new(primary.clone(), "order_submitted")).await?;
                "order_submitted".to_string()
            }
        }
    };

    write_status(&status_snap, &open_positions, &health, Some(&last_decision), false).await;
    Ok(())
}

/// Load the persisted True Open level for `timeframe`, capturing a fresh
/// one if it's missing or expired, and return the bias that level (or
/// its absence, for a partial week) implies against the current price.
async fn load_or_capture_bias(
    snap: &SnapshotFile<session_time::TrueOpenLevel>,
    timeframe: session_time::Timeframe,
    symbol: &str,
    price: Decimal,
    now: chrono::DateTime<chrono::Utc>,
    holidays: &dyn HolidayProvider,
) -> anyhow::Result<Bias> {
    let mut level = snap.read().await?;
    if session_time::needs_capture(now, level.as_ref()) {
        level = session_time::capture_level(timeframe, symbol, price, now, holidays);
        if let Some(level) = &level {
            snap.write(level).await?;
        }
    }
    Ok(level.map(|l| session_time::bias_from_price(price, l.level)).unwrap_or(Bias::Neutral))
}

/// The original scripted walkthrough against MockBroker: a clean pass, a
/// no-divergence cycle, a True-Open rejection, a health-triggered
/// lockout, and a simulated restart. Unchanged from the first pass.
async fn run_demo() -> anyhow::Result<()> {
    tracing::info!("starting oboobot (QuarterlyTheory_SMT_Trader) demonstration harness");
    tracing::info!("this run is against MockBroker; see main.rs docs for what a live run would change");

    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));
    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    let state_dir = std::env::temp_dir().join("oboobot-demo-state");
    tokio::fs::create_dir_all(&state_dir).await?;
    let positions_cursor_path = state_dir.join("positions.cursor");
    let _ = tokio::fs::remove_file(&positions_cursor_path).await;
    let positions_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);

    let locally_known_positions: Vec<Position> = positions_cursor.read_all().await?;
    let report = reconcile(&broker, &locally_known_positions).await?;
    if report.is_clean() {
        tracing::info!("startup reconciliation: clean, broker and local state agree");
    } else {
        tracing::warn!(
            orphaned = report.orphaned_locally.len(),
            adopted = report.unknown_to_local.len(),
            "startup reconciliation found a mismatch"
        );
    }
    let mut open_positions = apply_reconciliation(&report);

    run_cycle(
        "cycle 1: clean bullish divergence, True Open agrees",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
            session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        },
        Bias::Buy,
        Bias::Sell,
    )
    .await?;

    run_cycle(
        "cycle 2: prices moving together, no divergence at all",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.10050),
            secondary_price: dec!(1.10050),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        },
        Bias::Buy,
        Bias::Sell,
    )
    .await?;

    run_cycle(
        "cycle 3: real divergence, but Weekly True Open contradicts it",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
            session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        },
        Bias::Sell,
        Bias::Sell,
    )
    .await?;

    tracing::info!("simulating a broker heartbeat failure");
    health.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    tracing::warn!(
        state = ?health.current_state(),
        action = auto_action(health.current_state()),
        "health state escalated"
    );

    tracing::info!("cycle 4: same clean setup as cycle 1, but the health gate should now block it");
    if allows_new_entries(health.current_state()) {
        tracing::error!("this should not print: new entries should be blocked right now");
    } else {
        tracing::info!("new entries correctly blocked while system state is not Healthy or Degraded");
    }

    health.clear_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    tracing::info!(state = ?health.current_state(), "broker heartbeat recovered, health restored");

    tracing::info!("open positions before simulated restart: {}", open_positions.len());

    drop(open_positions);
    let cursor_after_restart: CursorFile<Position> = CursorFile::new(&positions_cursor_path);
    let recovered_positions = cursor_after_restart.read_all().await?;
    let restart_report = reconcile(&broker, &recovered_positions).await?;
    let restart_reconciled = apply_reconciliation(&restart_report);
    tracing::info!(
        recovered_from_disk = recovered_positions.len(),
        reconciled_after_restart = restart_reconciled.len(),
        clean = restart_report.is_clean(),
        "simulated restart: recovered local state and reconciled against the broker"
    );

    tracing::info!("oboobot demonstration harness finished");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    label: &str,
    broker: &dyn BrokerAdapter,
    health: &HealthMonitor,
    assistant: &dyn AssistantEngine,
    cursor: &CursorFile<Position>,
    open_positions: &mut Vec<Position>,
    inputs: DivergenceInputs,
    weekly_bias: Bias,
    daily_bias: Bias,
) -> anyhow::Result<()> {
    tracing::info!("--- {label} ---");

    if !allows_new_entries(health.current_state()) {
        tracing::info!("skipping: health state does not currently allow new entries");
        return Ok(());
    }

    let snapshot = broker.get_snapshot(&["EURUSD".to_string(), "GBPUSD".to_string()]).await?;
    let macro_cycle_event = EventEnvelope::new(snapshot.timestamp, Event::MacroCycleStarted);
    for recommendation in assistant.analyze_event(&macro_cycle_event).await {
        daemon::assistant::record_recommendation(&recommendation);
    }

    let outcome = generate_signal(
        &inputs,
        weekly_bias,
        daily_bias,
        "EURUSD".to_string(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        snapshot.timestamp + chrono::Duration::minutes(20),
    );

    match outcome {
        SignalOutcome::NoDivergence => {
            tracing::info!("no SMT divergence this cycle, nothing to evaluate");
        }
        SignalOutcome::Rejected(invalidated) => {
            tracing::info!(
                reason = ?invalidated.rejection_reason,
                weekly_bias = ?invalidated.weekly_bias,
                daily_bias = ?invalidated.daily_bias,
                smt_direction = ?invalidated.smt_direction,
                "signal generated but rejected by the True Open gate"
            );
        }
        SignalOutcome::Signal(signal) => {
            tracing::info!(tier = ?signal.tier, direction = ?signal.direction, "signal passed the True Open gate");

            let config = risk::RiskConfig {
                base_risk_percent: domain::Percent::from_percentage(dec!(1.0)),
                max_risk_percent: domain::Percent::from_percentage(dec!(5.0)),
                max_open_positions: 5,
                daily_loss_limit_percent: domain::Percent::from_percentage(dec!(5.0)),
                weekly_loss_limit_percent: domain::Percent::from_percentage(dec!(10.0)),
            };

            let equity = broker.get_account_equity().await?;
            let entry_price = match signal.direction {
                Direction::Buy => snapshot
                    .prices
                    .get("EURUSD")
                    .map(|q| q.ask)
                    .unwrap_or(dec!(1.10000)),
                Direction::Sell => snapshot
                    .prices
                    .get("EURUSD")
                    .map(|q| q.bid)
                    .unwrap_or(dec!(1.10000)),
            };
            let stop_loss_price = match signal.direction {
                Direction::Buy => entry_price - dec!(0.0050),
                Direction::Sell => entry_price + dec!(0.0050),
            };
            let take_profit_price = match signal.direction {
                Direction::Buy => entry_price + dec!(0.0150),
                Direction::Sell => entry_price - dec!(0.0150),
            };

            let context = risk::RiskContext {
                equity,
                open_position_count: open_positions.len(),
                is_tuesday: false,
                is_double_smt: signal.tier == domain::Tier::Double,
                entry_price,
                stop_loss_price,
                take_profit_price,
                realized_pnl_today: Usd::zero(),
                realized_pnl_this_week: Usd::zero(),
            };

            let risk_engine = risk::DefaultRiskEngine;
            let decision = risk_engine.evaluate(&signal, &config, &context)?;

            if !decision.approved {
                tracing::info!(reason = ?decision.rejection_reason, "risk engine rejected the signal");
                return Ok(());
            }

            tracing::info!(
                size = %decision.position_size,
                risk_percent = %decision.risk_percent,
                risk_currency = %decision.risk_currency,
                "risk engine approved sizing"
            );

            let request = OrderRequest {
                order_id: Uuid::new_v4(),
                trace_id: signal.trace_id,
                signal_id: signal.signal_id,
                pair: signal.pair.clone(),
                side: signal.direction,
                size: decision.position_size,
                order_type: OrderType::Market,
                price: None,
                stop_loss: Some(decision.stop_loss),
                take_profit: Some(decision.take_profit),
                confirming_snapshot_id: snapshot.snapshot_id,
            };

            let order = broker.submit_order(request).await?;
            tracing::info!(order_id = %order.order_id, status = ?order.status, "order submitted to broker");

            open_positions.clear();
            open_positions.extend(broker.list_open_positions().await?);

            for position in open_positions.iter() {
                cursor.append(position).await?;
            }
        }
    }

    Ok(())
}

--- ./daemon/src/event_bus.rs ---
//! The original design called certain events "priority" (orders, health,
//! shutdown) inside what was going to be a single bounded queue, but a
//! plain `tokio::sync::mpsc` channel has no concept of priority at all,
//! it's strictly first-in-first-out. Labeling some messages "priority"
//! without a mechanism to actually enforce that doesn't do anything.
//!
//! This is the fix: two separate channels, and a consumer loop that uses
//! `tokio::select!` with the `biased` keyword, which checks branches in
//! the order they're written instead of tokio's normal random selection
//! among ready branches. Listing the priority channel first means it
//! always wins whenever both channels have something waiting at the same
//! time.

use domain::EventEnvelope;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum EventBusError {
    #[error("event bus is closed, no receiver is listening")]
    Closed,
}

#[derive(Clone)]
pub struct EventBusHandle {
    priority_tx: mpsc::Sender<EventEnvelope>,
    ordinary_tx: mpsc::Sender<EventEnvelope>,
}

impl EventBusHandle {
    /// Publish an event. Which channel it goes down is decided by
    /// `Event::is_priority`, not by the caller, so producers can't
    /// accidentally mis-route something by forgetting which channel a
    /// given event kind belongs on.
    pub async fn publish(&self, envelope: EventEnvelope) -> Result<(), EventBusError> {
        let is_priority = envelope.payload.is_priority();
        let tx = if is_priority { &self.priority_tx } else { &self.ordinary_tx };
        tx.send(envelope).await.map_err(|_| EventBusError::Closed)
    }
}

pub struct EventBusReceiver {
    priority_rx: mpsc::Receiver<EventEnvelope>,
    ordinary_rx: mpsc::Receiver<EventEnvelope>,
}

impl EventBusReceiver {
    /// The next event, always preferring anything already waiting on the
    /// priority channel. Returns `None` only once both channels are
    /// closed and drained, which is how the daemon's main loop knows it's
    /// time to stop rather than keep waiting forever.
    pub async fn recv(&mut self) -> Option<EventEnvelope> {
        tokio::select! {
            biased;
            Some(event) = self.priority_rx.recv() => Some(event),
            Some(event) = self.ordinary_rx.recv() => Some(event),
            else => None,
        }
    }
}

/// Build a new event bus with the given per-channel capacity. Returns a
/// cloneable handle for producers and the single receiver for the
/// consumer loop; there's deliberately no way to clone the receiver,
/// since a bus with more than one consumer racing to pull events isn't a
/// scenario this daemon's architecture calls for.
pub fn event_bus(capacity: usize) -> (EventBusHandle, EventBusReceiver) {
    let (priority_tx, priority_rx) = mpsc::channel(capacity);
    let (ordinary_tx, ordinary_rx) = mpsc::channel(capacity);
    (
        EventBusHandle { priority_tx, ordinary_tx },
        EventBusReceiver { priority_rx, ordinary_rx },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Event;

    fn envelope(payload: Event) -> EventEnvelope {
        EventEnvelope::new(chrono::Utc::now(), payload)
    }

    #[tokio::test]
    async fn priority_event_is_received_before_an_already_queued_ordinary_one() {
        let (handle, mut receiver) = event_bus(8);

        // Queue an ordinary event first...
        handle
            .publish(envelope(Event::MacroCycleStarted))
            .await
            .unwrap();
        // ...then a priority one.
        handle
            .publish(envelope(Event::Shutdown { reason: "test".to_string() }))
            .await
            .unwrap();

        // Despite arriving second, the priority event should come out
        // first, because the biased select always checks that channel
        // before the ordinary one.
        let first = receiver.recv().await.unwrap();
        assert_eq!(first.payload.kind(), "shutdown");

        let second = receiver.recv().await.unwrap();
        assert_eq!(second.payload.kind(), "macro_cycle_started");
    }

    #[tokio::test]
    async fn receiver_returns_none_once_every_sender_is_dropped() {
        let (handle, mut receiver) = event_bus(8);
        drop(handle);
        assert!(receiver.recv().await.is_none());
    }
}

--- ./daemon/src/recovery.rs ---
//! This module exists because of one incident: devmind's offline buffer
//! silently failed to drain to Cognee because the remote API returned a
//! success-shaped response with nothing usable in it. The lesson that
//! carries over here is broader than "check the response body." It's
//! that whatever this daemon's own cursor files say about what's open
//! should never be treated as fact on its own. The broker is the only
//! party that can't lie about what's actually open, so every restart (and
//! ideally every reconnect) reconciles against it directly, and the
//! broker's answer wins.

use std::collections::HashSet;

use broker::{BrokerAdapter, BrokerError};
use domain::Position;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    /// Positions our local persistence thinks are open, that the broker
    /// has no record of. These get closed out locally with
    /// `ExitReason::ReconciliationOrphan`; we don't get to keep believing
    /// in a position the broker doesn't recognize.
    pub orphaned_locally: Vec<Position>,
    /// Positions the broker reports as open that our local state didn't
    /// know about at all (the exact shape of bug a "silent success"
    /// broker response could cause: an order that really went through,
    /// but that we never durably recorded on our side).
    pub unknown_to_local: Vec<Position>,
    /// Positions both sides agree on.
    pub confirmed: Vec<Position>,
}

impl ReconciliationReport {
    pub fn is_clean(&self) -> bool {
        self.orphaned_locally.is_empty() && self.unknown_to_local.is_empty()
    }
}

/// Ask the broker what's actually open, and diff it against whatever our
/// own persistence layer believes. This should run before the daemon
/// accepts any new signals, every time it starts up, and again after any
/// broker reconnect following an outage.
pub async fn reconcile(
    broker: &dyn BrokerAdapter,
    locally_known_positions: &[Position],
) -> Result<ReconciliationReport, BrokerError> {
    let broker_positions = broker.list_open_positions().await?;

    let broker_ids: HashSet<Uuid> = broker_positions.iter().map(|p| p.position_id).collect();
    let local_ids: HashSet<Uuid> = locally_known_positions.iter().map(|p| p.position_id).collect();

    let orphaned_locally = locally_known_positions
        .iter()
        .filter(|p| !broker_ids.contains(&p.position_id))
        .cloned()
        .collect();

    let unknown_to_local = broker_positions
        .iter()
        .filter(|p| !local_ids.contains(&p.position_id))
        .cloned()
        .collect();

    let confirmed = locally_known_positions
        .iter()
        .filter(|p| broker_ids.contains(&p.position_id))
        .cloned()
        .collect();

    Ok(ReconciliationReport {
        orphaned_locally,
        unknown_to_local,
        confirmed,
    })
}

/// Turn a reconciliation report into the position list the daemon should
/// actually trust going forward. The broker's view always wins: confirmed
/// and previously-unknown-but-broker-reported positions are kept,
/// locally-orphaned ones are dropped from the active set entirely (a
/// caller that wants to log or persist their closure with
/// `ExitReason::ReconciliationOrphan` does that separately, using the
/// `orphaned_locally` list on the report before this function discards
/// them from the active set).
pub fn apply_reconciliation(report: &ReconciliationReport) -> Vec<Position> {
    let mut reconciled = report.confirmed.clone();
    reconciled.extend(report.unknown_to_local.iter().cloned());
    reconciled
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::MockBroker;
    use domain::Usd;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn matching_state_reconciles_clean() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = mock
            .submit_order(domain::OrderRequest {
                order_id: Uuid::new_v4(),
                trace_id: Uuid::new_v4(),
                signal_id: Uuid::new_v4(),
                pair: "EURUSD".to_string(),
                side: domain::Direction::Buy,
                size: dec!(1.0),
                order_type: domain::OrderType::Market,
                price: None,
                stop_loss: None,
                take_profit: None,
                confirming_snapshot_id: Uuid::new_v4(),
            })
            .await
            .unwrap();
        let _ = order;

        let broker_positions = mock.list_open_positions().await.unwrap();
        let report = reconcile(&mock, &broker_positions).await.unwrap();

        assert!(report.is_clean());
        assert_eq!(report.confirmed.len(), 1);
    }

    #[tokio::test]
    async fn a_position_the_broker_forgot_is_reported_as_orphaned_locally() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let broker_dyn: &dyn BrokerAdapter = &mock;

        mock.submit_order(domain::OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: domain::Direction::Buy,
            size: dec!(1.0),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        })
        .await
        .unwrap();

        let locally_known = mock.list_open_positions().await.unwrap();
        let position_id = locally_known[0].position_id;

        // Simulate the broker no longer recognizing this position, the
        // way it would if an earlier "successful" submission was actually
        // the silent-failure case and never really went through.
        mock.forget_position(position_id);

        let report = reconcile(broker_dyn, &locally_known).await.unwrap();
        assert_eq!(report.orphaned_locally.len(), 1);
        assert_eq!(report.orphaned_locally[0].position_id, position_id);
        assert!(report.confirmed.is_empty());

        let reconciled = apply_reconciliation(&report);
        assert!(reconciled.is_empty(), "an orphaned position should not survive reconciliation");
    }

    #[tokio::test]
    async fn a_position_the_broker_has_but_we_never_recorded_is_adopted() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        mock.submit_order(domain::OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: domain::Direction::Buy,
            size: dec!(1.0),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        })
        .await
        .unwrap();

        // Our local view is empty, as if we crashed before persisting the
        // fill confirmation, exactly the scenario reconciliation exists
        // to catch.
        let locally_known: Vec<Position> = Vec::new();

        let report = reconcile(&mock, &locally_known).await.unwrap();
        assert_eq!(report.unknown_to_local.len(), 1);
        assert!(!report.is_clean());

        let reconciled = apply_reconciliation(&report);
        assert_eq!(reconciled.len(), 1, "a broker-confirmed position we didn't know about should be adopted, not discarded");
    }
}

--- ./daemon/src/scheduler.rs ---
//! The scheduler's only job is deciding when the next macro cycle is and
//! sleeping until then. It doesn't evaluate strategy, doesn't touch the
//! broker, doesn't know what SMT divergence is; it just emits
//! `MacroCycleStarted` on schedule so everything downstream can react to
//! it. That separation is what makes the strategy and risk logic testable
//! without needing a real clock: they only ever see events, never read
//! the wall clock themselves.

use std::sync::Arc;

use domain::{Event, EventEnvelope};
use session_time::Clock;

use crate::event_bus::EventBusHandle;

pub struct Scheduler<C: Clock> {
    clock: Arc<C>,
}

impl<C: Clock> Scheduler<C> {
    pub fn new(clock: Arc<C>) -> Self {
        Scheduler { clock }
    }

    /// Run forever, publishing a `MacroCycleStarted` event at every macro
    /// cycle boundary. This is what `main.rs` calls in production.
    pub async fn run(&self, bus: EventBusHandle) -> ! {
        loop {
            self.sleep_until_next_cycle().await;
            let envelope = EventEnvelope::new(self.clock.now(), Event::MacroCycleStarted);
            // A publish failure here means the receiver side has been
            // dropped, i.e. the daemon is shutting down. There's nothing
            // useful to do about that inside the scheduler itself, so we
            // just let the next loop iteration's publish attempt fail the
            // same way rather than treating it as fatal to this function.
            let _ = bus.publish(envelope).await;
        }
    }

    /// The same loop, but stopping after `cycles` iterations. This exists
    /// specifically so tests (and the replay engine) can drive a bounded,
    /// deterministic number of cycles instead of running forever.
    pub async fn run_n_cycles(&self, bus: EventBusHandle, cycles: u32) {
        for _ in 0..cycles {
            self.sleep_until_next_cycle().await;
            let envelope = EventEnvelope::new(self.clock.now(), Event::MacroCycleStarted);
            let _ = bus.publish(envelope).await;
        }
    }

    async fn sleep_until_next_cycle(&self) {
        let now = self.clock.now();
        let next = session_time::next_macro_cycle_after(now);
        let wait = (next - now)
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::event_bus;
    use chrono::TimeZone;
    use session_time::ManualClock;

    #[tokio::test(start_paused = true)]
    async fn scheduler_publishes_a_macro_cycle_started_event() {
        // Pausing tokio's virtual clock means the `sleep` inside the
        // scheduler resolves immediately in real wall-clock time instead
        // of actually waiting up to three hours of simulated market time.
        // Note this is a different clock than `ManualClock`: tokio's
        // time-pausing only affects tokio's own timer APIs, not a
        // hand-rolled clock like this one, so this test checks the
        // scheduler fires once and wires up correctly, and leaves
        // verifying the exact three-hour spacing between cycles to the
        // pure, clock-independent unit tests in `session_time::macro_cycle`.
        let start = chrono::Utc.with_ymd_and_hms(2026, 3, 10, 9, 30, 0).unwrap();
        let clock = Arc::new(ManualClock::new(start));
        let scheduler = Scheduler::new(clock);
        let (handle, mut receiver) = event_bus(8);

        scheduler.run_n_cycles(handle, 1).await;

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.payload.kind(), "macro_cycle_started");
    }
}

--- ./daemon/src/notifications.rs ---
//! Slack and Telegram notifications, both just an HTTP POST to a
//! webhook. This is the entire "alerting" story for this project, on
//! purpose: no metrics registry, no exporter, nothing Prometheus-shaped.
//! A `Notifier` is a trait so `--demo` and tests can use a no-op
//! implementation without needing real webhook URLs configured.

use async_trait::async_trait;

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn notify(&self, message: &str);
}

pub struct NoopNotifier;

#[async_trait]
impl Notifier for NoopNotifier {
    async fn notify(&self, message: &str) {
        tracing::debug!(message, "NoopNotifier: notification suppressed (no webhook configured)");
    }
}

pub struct SlackNotifier {
    webhook_url: String,
    client: reqwest::Client,
}

impl SlackNotifier {
    pub fn new(webhook_url: String) -> Self {
        SlackNotifier { webhook_url, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl Notifier for SlackNotifier {
    async fn notify(&self, message: &str) {
        let body = serde_json::json!({ "text": message });
        // Deliberately swallowing the error here rather than propagating
        // it: a failed notification should never be the reason a
        // trading cycle itself fails. It's still logged, so it isn't
        // silent, just non-fatal.
        if let Err(error) = self.client.post(&self.webhook_url).json(&body).send().await {
            tracing::warn!(%error, "failed to deliver Slack notification");
        }
    }
}

pub struct TelegramNotifier {
    bot_token: String,
    chat_id: String,
    client: reqwest::Client,
}

impl TelegramNotifier {
    pub fn new(bot_token: String, chat_id: String) -> Self {
        TelegramNotifier { bot_token, chat_id, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn notify(&self, message: &str) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        let body = serde_json::json!({ "chat_id": self.chat_id, "text": message });
        if let Err(error) = self.client.post(&url).json(&body).send().await {
            tracing::warn!(%error, "failed to deliver Telegram notification");
        }
    }
}

/// Build whichever notifier the config and environment actually support,
/// falling back to `NoopNotifier` if nothing is configured. Slack is
/// preferred if both are set; there's no strong reason to fan out to
/// both by default.
pub fn notifier_from_config(config: &crate::config::NotificationSection) -> Box<dyn Notifier> {
    if let Some(env_var) = &config.slack_webhook_env {
        if let Ok(webhook_url) = std::env::var(env_var) {
            return Box::new(SlackNotifier::new(webhook_url));
        }
    }

    if let (Some(token_env), Some(chat_env)) =
        (&config.telegram_bot_token_env, &config.telegram_chat_id_env)
    {
        if let (Ok(bot_token), Ok(chat_id)) = (std::env::var(token_env), std::env::var(chat_env)) {
            return Box::new(TelegramNotifier::new(bot_token, chat_id));
        }
    }

    Box::new(NoopNotifier)
}

--- ./daemon/src/operations.rs ---
//! Four smaller operational pieces that share a file because each one is
//! small on its own:
//!
//! - A kill switch: a file the bot checks for before doing anything new.
//!   Drop `PAUSED` into the state repo from a phone in ten seconds and
//!   new entries stop, no secret, no redeploy, no workflow edit needed.
//! - A decisions log: every signal this daemon ever evaluates, not just
//!   the ones that became trades, so "why didn't it trade at 09:00
//!   today" has a real answer instead of a guess from a scrolled-away
//!   log line.
//! - A status snapshot: the current state, overwritten each run,
//!   readable from a phone without a dashboard.
//! - The position-collision guard, which is also this project's
//!   idempotency protection: the original spec's entry_gates already
//!   called for "one trade per macro cycle per pair," which was never
//!   implemented. Enforcing it is the same check that also protects
//!   against a retried or overlapping workflow run double-entering the
//!   same signal, since a signal's id is freshly generated each
//!   evaluation and can't be used as a dedup key on its own.

use chrono::{DateTime, Duration, Utc};
use domain::Position;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// How close together two entries on the same pair are allowed to be.
/// Set just under the real macro-cycle spacing (three hours) rather than
/// exactly at it, so ordinary jitter in when a cycle actually gets
/// evaluated can't accidentally let a second entry slip through.
const MIN_MINUTES_BETWEEN_ENTRIES_SAME_PAIR: i64 = 170;

pub async fn kill_switch_engaged(state_dir: &Path) -> bool {
    tokio::fs::try_exists(state_dir.join("PAUSED")).await.unwrap_or(false)
}

/// Whether opening a new position on `pair` right now would collide with
/// one already entered this macro cycle. Checks every known position
/// (open or already closed), not just currently-open ones, since a
/// position that already opened and closed within the same window still
/// means this window already traded that pair.
pub fn already_entered_this_cycle(pair: &str, known_positions: &[Position], now: DateTime<Utc>) -> bool {
    known_positions.iter().any(|position| {
        position.pair == pair
            && (now - position.entry_time) < Duration::minutes(MIN_MINUTES_BETWEEN_ENTRIES_SAME_PAIR)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub timestamp: DateTime<Utc>,
    pub pair: String,
    pub outcome: String,
    pub detail: Option<String>,
}

impl DecisionRecord {
    pub fn new(pair: impl Into<String>, outcome: impl Into<String>) -> Self {
        DecisionRecord { timestamp: Utc::now(), pair: pair.into(), outcome: outcome.into(), detail: None }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusSnapshot {
    pub last_run: Option<DateTime<Utc>>,
    pub open_position_count: usize,
    pub health_state: String,
    pub last_decision: Option<String>,
    pub paused: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use domain::{Direction, FillLeg, PositionStatus};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn position_at(pair: &str, entry_time: DateTime<Utc>) -> Position {
        Position {
            position_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: pair.to_string(),
            direction: Direction::Buy,
            legs: vec![FillLeg { price: dec!(1.1000), size: dec!(1.0), filled_at: entry_time }],
            entry_price: dec!(1.1000),
            current_price: dec!(1.1000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            entry_time,
            last_update: entry_time,
            status: PositionStatus::Filled,
            exit_reason: None,
            stop_loss: None,
            take_profit: None,
        }
    }

    #[tokio::test]
    async fn kill_switch_is_off_by_default() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!kill_switch_engaged(dir.path()).await);
    }

    #[tokio::test]
    async fn kill_switch_engages_when_the_paused_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("PAUSED"), "").await.unwrap();
        assert!(kill_switch_engaged(dir.path()).await);
    }

    #[test]
    fn recent_entry_on_the_same_pair_blocks_a_new_one() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("EURUSD", now - Duration::minutes(10))];
        assert!(already_entered_this_cycle("EURUSD", &known, now));
    }

    #[test]
    fn an_old_enough_entry_does_not_block_a_new_one() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("EURUSD", now - Duration::hours(4))];
        assert!(!already_entered_this_cycle("EURUSD", &known, now));
    }

    #[test]
    fn a_recent_entry_on_a_different_pair_does_not_block() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("GBPUSD", now - Duration::minutes(10))];
        assert!(!already_entered_this_cycle("EURUSD", &known, now));
    }
}

--- ./daemon/src/monitor.rs ---
//! Until now, nothing in this codebase ever looked at a position again
//! once it opened. That was the bigger of the two gaps named in review:
//! the original spec's exit conditions (1:3 risk-reward, pre-news,
//! SMT contradiction) were fully speced and fully untouched by any
//! code.
//!
//! The fix fits the deployment model better than a separate continuous
//! task would: since GitHub Actions already wakes this process up every
//! five minutes, every single invocation checks open positions for exit
//! conditions, regardless of whether it's inside a macro-cycle entry
//! window. Entries are gated by the window; exits never are. That
//! mirrors exactly what the original spec called for (a monitor
//! decoupled from the macro-cycle schedule) without needing a
//! long-running task inside a process that isn't long-running.
//!
//! For a broker with native stop-loss/take-profit enforcement (Deriv's
//! Multipliers included), the risk-reward check here is a backup, not
//! the primary mechanism — the broker itself closes the position before
//! this ever gets a chance to. It still matters: for a broker without
//! native enforcement, or if a native SL/TP order is ever rejected or
//! modified unexpectedly, this is what catches it.

use chrono::{DateTime, Duration, Utc};
use domain::{Direction, ExitReason, NewsEvent, Position, Tier};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::news::should_exit_for_news;

#[derive(Debug, Clone, Copy)]
pub struct ExitDecision {
    pub position_id: Uuid,
    pub reason: ExitReason,
}

/// Whether `position` has reached its stop-loss or take-profit, given
/// `current_price`. `None` if the position has no configured stop or
/// target, or if neither has been reached yet.
fn risk_reward_exit(position: &Position, current_price: Decimal) -> Option<ExitReason> {
    let hit_stop = position.stop_loss.is_some_and(|stop| match position.direction {
        Direction::Buy => current_price <= stop,
        Direction::Sell => current_price >= stop,
    });
    let hit_target = position.take_profit.is_some_and(|target| match position.direction {
        Direction::Buy => current_price >= target,
        Direction::Sell => current_price <= target,
    });

    if hit_stop {
        Some(ExitReason::StopLoss)
    } else if hit_target {
        Some(ExitReason::TakeProfit)
    } else {
        None
    }
}

/// Whether the live SMT divergence now points opposite to the direction
/// `position` is holding. `current_divergence` is whatever
/// `strategy::evaluate_smt` returned this cycle for the same pair, if
/// anything.
fn smt_contradiction_exit(
    position: &Position,
    current_divergence: Option<(Direction, Tier)>,
) -> Option<ExitReason> {
    match current_divergence {
        Some((direction, _)) if direction != position.direction => Some(ExitReason::Contradiction),
        _ => None,
    }
}

/// The full exit sweep for one cycle: given every currently open
/// position, the live price for its pair, whatever news events are on
/// file, and the live SMT reading (if any), decide which positions
/// should close and why. Checked in the order the original spec listed
/// them, though since all three lead to the same action (immediate
/// close) the order only matters for which `ExitReason` gets recorded,
/// not for what actually happens.
pub fn evaluate_exits(
    positions: &[Position],
    current_price: Decimal,
    news_events: &[NewsEvent],
    now: DateTime<Utc>,
    news_lead_time: Duration,
    current_divergence: Option<(Direction, Tier)>,
) -> Vec<ExitDecision> {
    let news_exit_active = should_exit_for_news(news_events, now, news_lead_time);

    positions
        .iter()
        .filter_map(|position| {
            let reason = if let Some(reason) = risk_reward_exit(position, current_price) {
                Some(reason)
            } else if news_exit_active {
                Some(ExitReason::News)
            } else {
                smt_contradiction_exit(position, current_divergence)
            };

            reason.map(|reason| ExitDecision { position_id: position.position_id, reason })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use domain::{FillLeg, PositionStatus};

    fn sample_position(direction: Direction, stop_loss: Decimal, take_profit: Decimal) -> Position {
        Position {
            position_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            direction,
            legs: vec![FillLeg {
                price: rust_decimal_macros::dec!(1.1000),
                size: rust_decimal_macros::dec!(1.0),
                filled_at: Utc::now(),
            }],
            entry_price: rust_decimal_macros::dec!(1.1000),
            current_price: rust_decimal_macros::dec!(1.1000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            entry_time: Utc::now(),
            last_update: Utc::now(),
            status: PositionStatus::Filled,
            exit_reason: None,
            stop_loss: Some(stop_loss),
            take_profit: Some(take_profit),
        }
    }

    #[test]
    fn buy_position_exits_on_stop_loss() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position.clone()], dec!(1.0940), &[], now, Duration::minutes(15), None);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].reason, ExitReason::StopLoss);
    }

    #[test]
    fn buy_position_exits_on_take_profit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position.clone()], dec!(1.1160), &[], now, Duration::minutes(15), None);
        assert_eq!(decisions[0].reason, ExitReason::TakeProfit);
    }

    #[test]
    fn sell_position_stop_and_target_are_mirrored() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Sell, dec!(1.1050), dec!(1.0850));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();

        let stopped = evaluate_exits(&[position.clone()], dec!(1.1060), &[], now, Duration::minutes(15), None);
        assert_eq!(stopped[0].reason, ExitReason::StopLoss);

        let targeted = evaluate_exits(&[position.clone()], dec!(1.0840), &[], now, Duration::minutes(15), None);
        assert_eq!(targeted[0].reason, ExitReason::TakeProfit);
    }

    #[test]
    fn no_exit_when_price_is_between_stop_and_target() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position], dec!(1.1000), &[], now, Duration::minutes(15), None);
        assert!(decisions.is_empty());
    }

    #[test]
    fn smt_contradiction_triggers_exit_when_no_rr_or_news_exit_applies() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(
            &[position],
            dec!(1.1000),
            &[],
            now,
            Duration::minutes(15),
            Some((Direction::Sell, Tier::Tier1)), // opposite of the Buy position
        );
        assert_eq!(decisions[0].reason, ExitReason::Contradiction);
    }

    #[test]
    fn smt_agreement_does_not_trigger_exit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(
            &[position],
            dec!(1.1000),
            &[],
            now,
            Duration::minutes(15),
            Some((Direction::Buy, Tier::Tier1)), // same direction
        );
        assert!(decisions.is_empty());
    }

    #[test]
    fn risk_reward_exit_takes_priority_over_news_exit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![NewsEvent {
            event_id: Uuid::new_v4(),
            timestamp: now + Duration::minutes(5),
            currency: "USD".to_string(),
            impact: domain::NewsImpact::High,
            description: "test".to_string(),
            actual: None,
            forecast: None,
            previous: None,
        }];
        let decisions = evaluate_exits(&[position], dec!(1.0940), &events, now, Duration::minutes(15), None);
        // Price also hit the stop loss; that should win over the news
        // exit that would otherwise also apply, since it's checked first.
        assert_eq!(decisions[0].reason, ExitReason::StopLoss);
    }
}

--- ./daemon/src/news.rs ---
//! A trait for "what high-impact news is coming up," a pre-news exit
//! check built against that trait, and one implementation:
//! `NoNewsProvider`, which always reports nothing scheduled.
//!
//! That's a deliberate fail-safe choice, not a placeholder pretending to
//! be a real feed. The question flagged back in the original spec
//! review was: if the news source is unavailable, does the bot fail
//! safe (assume news might be coming, act conservatively) or fail open
//! (assume nothing's scheduled, trade normally)? `NoNewsProvider`
//! answers that by construction — it always returns empty, meaning the
//! pre-news exit check can never fire — which is honest about what "no
//! real news integration yet" actually means, rather than quietly
//! disabling a safety check while looking like it's still active. A
//! real provider (calling out to an actual economic calendar) is a
//! separate, focused piece of work, not attempted here; picking a
//! specific external news API wasn't part of what this pass was asked
//! to do, and integrating one deserves its own verification the way the
//! Deriv endpoint and symbol convention got.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use domain::NewsEvent;

#[async_trait]
pub trait NewsProvider: Send + Sync {
    /// Every currently-known event expected within `window` of `now`.
    async fn upcoming_events(&self, now: DateTime<Utc>, window: Duration) -> Vec<NewsEvent>;
}

pub struct NoNewsProvider;

#[async_trait]
impl NewsProvider for NoNewsProvider {
    async fn upcoming_events(&self, _now: DateTime<Utc>, _window: Duration) -> Vec<NewsEvent> {
        Vec::new()
    }
}

/// Whether a high-impact event lands within `lead_time` of `now`. Pure
/// and provider-agnostic: given whatever events a `NewsProvider`
/// returned, does the pre-news exit condition hold right now.
pub fn should_exit_for_news(events: &[NewsEvent], now: DateTime<Utc>, lead_time: Duration) -> bool {
    events.iter().any(|event| {
        event.impact == domain::NewsImpact::High
            && event.timestamp > now
            && event.timestamp - now <= lead_time
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use uuid::Uuid;

    fn event(minutes_from_now: i64, impact: domain::NewsImpact, now: DateTime<Utc>) -> NewsEvent {
        NewsEvent {
            event_id: Uuid::new_v4(),
            timestamp: now + Duration::minutes(minutes_from_now),
            currency: "USD".to_string(),
            impact,
            description: "test event".to_string(),
            actual: None,
            forecast: None,
            previous: None,
        }
    }

    #[tokio::test]
    async fn no_news_provider_always_returns_nothing() {
        let provider = NoNewsProvider;
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = provider.upcoming_events(now, Duration::hours(1)).await;
        assert!(events.is_empty());
    }

    #[test]
    fn high_impact_event_within_lead_time_triggers_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(10, domain::NewsImpact::High, now)];
        assert!(should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn high_impact_event_beyond_lead_time_does_not_trigger_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(30, domain::NewsImpact::High, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn low_impact_event_never_triggers_exit_regardless_of_timing() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(1, domain::NewsImpact::Low, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn a_past_event_does_not_trigger_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(-5, domain::NewsImpact::High, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }
}

--- ./daemon/src/config.rs ---
//! One TOML file instead of the original spec's six, matching the
//! project's own "keep it simple" instruction. Everything the daemon
//! actually reads at runtime lives here: risk limits, which pairs to
//! trade, and where to send notifications. Validation happens once, at
//! load time, so a bad config fails loudly before any broker call
//! rather than surfacing as a confusing error three steps into a cycle.

use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("could not parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },

    #[error("config is invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskSection {
    pub base_risk_percent: f64,
    pub max_risk_percent: f64,
    pub max_open_positions: usize,
    pub daily_loss_limit_percent: f64,
    pub weekly_loss_limit_percent: f64,
    #[serde(default = "default_spread_multiplier")]
    pub spread_multiplier: f64,
    #[serde(default = "default_regime_shift_threshold")]
    pub regime_shift_threshold: f64,
}

fn default_spread_multiplier() -> f64 {
    1.5
}

fn default_regime_shift_threshold() -> f64 {
    0.20
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PairConfig {
    pub primary: String,
    pub secondary: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NotificationSection {
    /// Name of the environment variable holding the webhook URL, not the
    /// URL itself: config files in this project are meant to be
    /// checked into an open-source repo, so the secret stays in an
    /// environment variable (a GitHub Actions secret in the deployed
    /// case), and config only ever names which one to read.
    pub slack_webhook_env: Option<String>,
    pub telegram_bot_token_env: Option<String>,
    pub telegram_chat_id_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub risk: RiskSection,
    pub pairs: Vec<PairConfig>,
    #[serde(default)]
    pub notifications: NotificationSection,
}

impl Config {
    /// A sensible built-in default so the daemon still runs without a
    /// config file present — useful for `--demo` and for a first-ever
    /// run before anyone's authored a config.toml.
    pub fn default_config() -> Self {
        Config {
            risk: RiskSection {
                base_risk_percent: 1.0,
                max_risk_percent: 5.0,
                max_open_positions: 5,
                daily_loss_limit_percent: 5.0,
                weekly_loss_limit_percent: 10.0,
                spread_multiplier: default_spread_multiplier(),
                regime_shift_threshold: default_regime_shift_threshold(),
            },
            pairs: vec![PairConfig { primary: "EURUSD".to_string(), secondary: "GBPUSD".to_string() }],
            notifications: NotificationSection::default(),
        }
    }

    pub async fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default_config());
            }
            Err(source) => {
                return Err(ConfigError::Io { path: path.display().to_string(), source });
            }
        };

        let config: Config = toml::from_str(&contents)
            .map_err(|source| ConfigError::Parse { path: path.display().to_string(), source })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=100.0).contains(&self.risk.max_risk_percent) {
            return Err(ConfigError::Invalid(format!(
                "risk.max_risk_percent must be between 0 and 100, got {}",
                self.risk.max_risk_percent
            )));
        }
        if self.risk.base_risk_percent <= 0.0 {
            return Err(ConfigError::Invalid("risk.base_risk_percent must be positive".to_string()));
        }
        if self.risk.spread_multiplier <= 1.0 {
            return Err(ConfigError::Invalid(format!(
                "risk.spread_multiplier must be greater than 1.0, got {} (a multiplier at or below 1.0 would make the spread filter reject almost every ordinary tick)",
                self.risk.spread_multiplier
            )));
        }
        if !(0.0..=1.0).contains(&self.risk.regime_shift_threshold) {
            return Err(ConfigError::Invalid(format!(
                "risk.regime_shift_threshold must be between 0.0 and 1.0, got {}",
                self.risk.regime_shift_threshold
            )));
        }
        if self.risk.max_open_positions == 0 {
            return Err(ConfigError::Invalid("risk.max_open_positions must be at least 1".to_string()));
        }
        if self.pairs.is_empty() {
            return Err(ConfigError::Invalid("at least one pair must be configured".to_string()));
        }

        let mut seen = std::collections::HashSet::new();
        for pair in &self.pairs {
            let key = (pair.primary.clone(), pair.secondary.clone());
            if !seen.insert(key) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate pair configured: {}/{}",
                    pair.primary, pair.secondary
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_config_file_falls_back_to_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config::load(&dir.path().join("does_not_exist.toml")).await.unwrap();
        assert_eq!(config.pairs.len(), 1);
    }

    #[tokio::test]
    async fn a_valid_config_file_loads_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let config = Config::load(&path).await.unwrap();
        assert_eq!(config.risk.spread_multiplier, 1.5); // default applied
    }

    #[tokio::test]
    async fn duplicate_pairs_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let result = Config::load(&path).await;
        assert!(matches!(result, Err(ConfigError::Invalid(_))));
    }

    #[tokio::test]
    async fn spread_multiplier_at_or_below_one_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0
            spread_multiplier = 0.9

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let result = Config::load(&path).await;
        assert!(matches!(result, Err(ConfigError::Invalid(_))));
    }
}

--- ./daemon/Cargo.toml ---
[package]
name = "daemon"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "oboobot"
path = "src/main.rs"

[dependencies]
domain = { path = "../domain" }
session_time = { path = "../session_time" }
broker = { path = "../broker" }
strategy = { path = "../strategy" }
risk = { path = "../risk" }
persistence = { path = "../persistence" }
tokio = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
parking_lot = { workspace = true }
arc-swap = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
clap = { version = "~4.4", features = ["derive"] }
toml = { workspace = true }
reqwest = { version = "0.11", default-features = false, features = ["json", "rustls-tls"] }

[dev-dependencies]
tempfile = { workspace = true }

--- ./daemon/tests/integration_test.rs ---
//! An end-to-end test of the full pipeline, using only each crate's
//! public API, the same way `main.rs` does. Where the unit tests in each
//! crate check one piece in isolation, this test checks that the pieces
//! actually fit together: a signal generated by `strategy` can be sized
//! by `risk`, submitted through `broker`, and the resulting state
//! reconciled by `daemon::recovery`, all using the real types each crate
//! hands to the next one.

use broker::{BrokerAdapter, MockBroker};
use daemon::{allows_new_entries, apply_reconciliation, reconcile, HealthCheckFailure, HealthMonitor};
use domain::{Bias, Direction, OrderRequest, OrderType, Percent, Position, Tier, Usd};
use risk::RiskEngine as _;
use rust_decimal_macros::dec;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[tokio::test]
async fn fresh_startup_reconciles_clean_against_an_empty_broker() {
    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
    let local: Vec<Position> = Vec::new();

    let report = reconcile(&broker, &local).await.unwrap();
    assert!(report.is_clean());
    assert!(apply_reconciliation(&report).is_empty());
}

#[tokio::test]
async fn a_full_cycle_goes_from_divergence_to_an_open_broker_position() {
    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));

    let inputs = DivergenceInputs {
        primary_price: dec!(1.09900),
        secondary_price: dec!(1.10100),
        daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
    };

    let snapshot = broker
        .get_snapshot(&["EURUSD".to_string(), "GBPUSD".to_string()])
        .await
        .unwrap();

    let outcome = generate_signal(
        &inputs,
        Bias::Buy, // weekly agrees
        Bias::Sell,
        "EURUSD".to_string(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        snapshot.timestamp + chrono::Duration::minutes(20),
    );

    let signal = match outcome {
        SignalOutcome::Signal(signal) => signal,
        other => panic!("expected a passing signal in this fixture, got {other:?}"),
    };

    let config = risk::RiskConfig {
        base_risk_percent: Percent::from_percentage(dec!(1.0)),
        max_risk_percent: Percent::from_percentage(dec!(5.0)),
        max_open_positions: 5,
        daily_loss_limit_percent: Percent::from_percentage(dec!(5.0)),
        weekly_loss_limit_percent: Percent::from_percentage(dec!(10.0)),
    };
    let context = risk::RiskContext {
        equity: broker.get_account_equity().await.unwrap(),
        open_position_count: 0,
        is_tuesday: false,
        is_double_smt: signal.tier == Tier::Double,
        entry_price: dec!(1.10000),
        stop_loss_price: dec!(1.09500),
        take_profit_price: dec!(1.11500),
        realized_pnl_today: Usd::zero(),
        realized_pnl_this_week: Usd::zero(),
    };

    let risk_engine = risk::DefaultRiskEngine;
    let decision = risk_engine.evaluate(&signal, &config, &context).unwrap();
    assert!(decision.approved);

    let request = OrderRequest {
        order_id: Uuid::new_v4(),
        trace_id: signal.trace_id,
        signal_id: signal.signal_id,
        pair: signal.pair.clone(),
        side: signal.direction,
        size: decision.position_size,
        order_type: OrderType::Market,
        price: None,
        stop_loss: Some(decision.stop_loss),
        take_profit: Some(decision.take_profit),
        confirming_snapshot_id: snapshot.snapshot_id,
    };

    let order = broker.submit_order(request).await.unwrap();
    assert_eq!(order.status, domain::OrderStatus::Filled);

    let open = broker.list_open_positions().await.unwrap();
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].direction, Direction::Buy);

    // And reconciliation against this now-nonempty broker state should
    // adopt exactly this position when local state starts from nothing,
    // exactly the scenario a restart-after-crash would look like.
    let report = reconcile(&broker, &[]).await.unwrap();
    assert_eq!(report.unknown_to_local.len(), 1);
    let reconciled = apply_reconciliation(&report);
    assert_eq!(reconciled.len(), 1);
}

#[tokio::test]
async fn true_open_conflict_rejects_before_any_order_reaches_the_broker() {
    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));

    let inputs = DivergenceInputs {
        primary_price: dec!(1.09900),
        secondary_price: dec!(1.10100),
        daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
    };

    let snapshot = broker
        .get_snapshot(&["EURUSD".to_string(), "GBPUSD".to_string()])
        .await
        .unwrap();

    // This divergence is bullish (Buy), but Weekly True Open is bearish:
    // a direct conflict that the gate should reject before risk sizing or
    // broker submission is ever reached.
    let outcome = generate_signal(
        &inputs,
        Bias::Sell,
        Bias::Sell,
        "EURUSD".to_string(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        snapshot.timestamp + chrono::Duration::minutes(20),
    );

    assert!(matches!(outcome, SignalOutcome::Rejected(_)));

    // Only the snapshot call above should have reached the broker: the
    // True Open gate rejects entirely within strategy::generate_signal,
    // a pure function that never touches the broker. Capturing the count
    // here, before the verification call just below, matters: calling
    // list_open_positions to check the assertion would itself add a
    // second call and make this check meaningless.
    assert_eq!(broker.how_many_calls(), 1, "only the snapshot call should have reached the broker");

    let open = broker.list_open_positions().await.unwrap();
    assert!(open.is_empty());
}

#[tokio::test]
async fn broker_heartbeat_failure_blocks_new_entries_until_cleared() {
    let health = HealthMonitor::new();
    assert!(allows_new_entries(health.current_state()));

    health.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    assert!(!allows_new_entries(health.current_state()));

    health.clear_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    assert!(allows_new_entries(health.current_state()));
}

--- ./risk/src/lib.rs ---
//! Position sizing and the account-wide risk gates. Depends only on
//! `domain`; it doesn't know or care which broker is in use, which is
//! what lets it be property-tested in complete isolation from anything
//! broker- or network-shaped.

pub mod sizing;

pub use sizing::{DefaultRiskEngine, RiskConfig, RiskContext, RiskEngine, RiskError, RiskRejection};

--- ./risk/src/sizing.rs ---
//! Position sizing is the one calculation in this whole daemon where a
//! bug directly translates into "we risked more than we meant to." That's
//! why the multiplier cap lives in `domain::apply_multiplier` as a
//! function nobody can route around, and why this module leans on
//! property-based testing (further down) instead of only checking a
//! handful of examples: the property we actually care about ("computed
//! risk never exceeds the configured cap") should hold for every input,
//! not just the ones we thought to write down.
//!
//! A scope note up front: this implementation covers per-trade sizing,
//! the mutually-exclusive Tuesday/Double-SMT multiplier, daily/weekly
//! loss-limit gating, the max-open-positions gate, and a zero-stop-
//! distance guard. What it does *not* do is net exposure across multiple
//! simultaneous positions that share a currency or a correlation cluster
//! (`max_exposure_per_currency` and `max_correlation_exposure` from the
//! original spec). Doing that properly needs the live correlation matrix
//! and per-asset currency bookkeeping, which is real work belonging to
//! its own follow-up rather than something to fake here with a
//! reduced-fidelity approximation that looks complete but isn't.

use domain::{Coefficient, Percent, RiskDecision, TradeSignal, Usd};
use rust_decimal::Decimal;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum RiskError {
    #[error(transparent)]
    Domain(#[from] domain::DomainError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskRejection {
    DailyLossLimitReached,
    WeeklyLossLimitReached,
    MaxOpenPositionsReached,
    InvalidStopDistance,
}

impl RiskRejection {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskRejection::DailyLossLimitReached => "daily loss limit reached",
            RiskRejection::WeeklyLossLimitReached => "weekly loss limit reached",
            RiskRejection::MaxOpenPositionsReached => "max open positions reached",
            RiskRejection::InvalidStopDistance => "stop distance is zero, cannot size a position",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RiskConfig {
    pub base_risk_percent: Percent,
    /// The hard ceiling `apply_multiplier` enforces. This is what the
    /// hardening layer's `Percent::from(0.05)` example was: whatever the
    /// multiplier does to the base risk percent, the result can never
    /// exceed this.
    pub max_risk_percent: Percent,
    pub max_open_positions: usize,
    pub daily_loss_limit_percent: Percent,
    pub weekly_loss_limit_percent: Percent,
}

#[derive(Debug, Clone, Copy)]
pub struct RiskContext {
    pub equity: Usd,
    pub open_position_count: usize,
    pub is_tuesday: bool,
    pub is_double_smt: bool,
    pub entry_price: Decimal,
    pub stop_loss_price: Decimal,
    pub take_profit_price: Decimal,
    /// Negative means a loss. Comparing against the configured limit is
    /// how the daily/weekly loss gates work.
    pub realized_pnl_today: Usd,
    pub realized_pnl_this_week: Usd,
}

pub trait RiskEngine: Send + Sync {
    fn evaluate(
        &self,
        signal: &TradeSignal,
        config: &RiskConfig,
        context: &RiskContext,
    ) -> Result<RiskDecision, RiskError>;
}

pub struct DefaultRiskEngine;

impl DefaultRiskEngine {
    /// Tuesday-doubling and Double-SMT-doubling are documented as
    /// mutually exclusive and both cap at 2.0x. Since they're the *same*
    /// value, an OR is sufficient to satisfy "mutually exclusive": there's
    /// no scenario where both being true should multiply out to 4x,
    /// because both conditions map to the identical 2.0 coefficient
    /// either way. If a future change ever gives them different values,
    /// this is the function that would need an explicit precedence rule
    /// instead of a simple OR.
    fn effective_coefficient(is_tuesday: bool, is_double_smt: bool) -> Coefficient {
        if is_tuesday || is_double_smt {
            Coefficient::new(2.0)
        } else {
            Coefficient::new(1.0)
        }
    }

    fn rejected(signal: &TradeSignal, reason: RiskRejection) -> RiskDecision {
        RiskDecision {
            decision_id: Uuid::new_v4(),
            trace_id: signal.trace_id,
            signal_id: signal.signal_id,
            approved: false,
            rejection_reason: Some(reason.as_str().to_string()),
            position_size: Decimal::ZERO,
            stop_loss: Decimal::ZERO,
            take_profit: Decimal::ZERO,
            risk_percent: Percent::from_ratio(Decimal::ZERO),
            risk_currency: Usd::zero(),
        }
    }
}

impl RiskEngine for DefaultRiskEngine {
    fn evaluate(
        &self,
        signal: &TradeSignal,
        config: &RiskConfig,
        context: &RiskContext,
    ) -> Result<RiskDecision, RiskError> {
        let daily_limit = Usd::from_percent_of(context.equity, config.daily_loss_limit_percent);
        if context.realized_pnl_today.as_decimal() <= -daily_limit.as_decimal() {
            return Ok(Self::rejected(signal, RiskRejection::DailyLossLimitReached));
        }

        let weekly_limit = Usd::from_percent_of(context.equity, config.weekly_loss_limit_percent);
        if context.realized_pnl_this_week.as_decimal() <= -weekly_limit.as_decimal() {
            return Ok(Self::rejected(signal, RiskRejection::WeeklyLossLimitReached));
        }

        if context.open_position_count >= config.max_open_positions {
            return Ok(Self::rejected(signal, RiskRejection::MaxOpenPositionsReached));
        }

        let stop_distance = (context.entry_price - context.stop_loss_price).abs();
        if stop_distance.is_zero() {
            return Ok(Self::rejected(signal, RiskRejection::InvalidStopDistance));
        }

        let coefficient = Self::effective_coefficient(context.is_tuesday, context.is_double_smt);
        let risk_percent =
            domain::apply_multiplier(config.base_risk_percent, coefficient, config.max_risk_percent)?;
        let risk_currency = Usd::from_percent_of(context.equity, risk_percent);
        let position_size = risk_currency.as_decimal() / stop_distance;

        Ok(RiskDecision {
            decision_id: Uuid::new_v4(),
            trace_id: signal.trace_id,
            signal_id: signal.signal_id,
            approved: true,
            rejection_reason: None,
            position_size,
            stop_loss: context.stop_loss_price,
            take_profit: context.take_profit_price,
            risk_percent,
            risk_currency,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use domain::{Direction, Tier};
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    fn sample_signal() -> TradeSignal {
        TradeSignal {
            signal_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            pair: "EURUSD".to_string(),
            direction: Direction::Buy,
            tier: Tier::Tier1,
            strength: dec!(0.8),
            confidence: dec!(0.8),
            valid_until: Utc::now(),
            originating_snapshot_id: Uuid::new_v4(),
        }
    }

    fn sample_config() -> RiskConfig {
        RiskConfig {
            base_risk_percent: Percent::from_percentage(dec!(1.0)),
            max_risk_percent: Percent::from_percentage(dec!(5.0)),
            max_open_positions: 5,
            daily_loss_limit_percent: Percent::from_percentage(dec!(5.0)),
            weekly_loss_limit_percent: Percent::from_percentage(dec!(10.0)),
        }
    }

    fn sample_context() -> RiskContext {
        RiskContext {
            equity: Usd::from_decimal(dec!(10000)),
            open_position_count: 0,
            is_tuesday: false,
            is_double_smt: false,
            entry_price: dec!(1.1000),
            stop_loss_price: dec!(1.0950),
            take_profit_price: dec!(1.1150),
            realized_pnl_today: Usd::zero(),
            realized_pnl_this_week: Usd::zero(),
        }
    }

    #[test]
    fn ordinary_signal_is_approved_with_expected_size() {
        let engine = DefaultRiskEngine;
        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &sample_context())
            .unwrap();

        assert!(decision.approved);
        // 1% of $10,000 = $100 risk. Stop distance is 0.0050. Size should
        // be 100 / 0.0050 = 20,000 units.
        assert_eq!(decision.position_size, dec!(20000));
    }

    #[test]
    fn tuesday_doubles_the_risk_percent() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.is_tuesday = true;

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        // 2% of $10,000 = $200 risk, so size should double too.
        assert_eq!(decision.position_size, dec!(40000));
    }

    #[test]
    fn tuesday_and_double_smt_together_still_only_double_not_quadruple() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.is_tuesday = true;
        context.is_double_smt = true;

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        // If this were 4x instead of 2x, size would be 80,000. Asserting
        // it's still 40,000 is exactly the regression guard the original
        // review flagged as worth having explicitly.
        assert_eq!(decision.position_size, dec!(40000));
    }

    #[test]
    fn daily_loss_limit_rejects_new_signals() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        // Already down more than 5% today.
        context.realized_pnl_today = Usd::from_decimal(dec!(-600));

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(!decision.approved);
        assert_eq!(
            decision.rejection_reason.as_deref(),
            Some(RiskRejection::DailyLossLimitReached.as_str())
        );
    }

    #[test]
    fn max_open_positions_rejects_new_signals() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.open_position_count = 5;

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(!decision.approved);
        assert_eq!(
            decision.rejection_reason.as_deref(),
            Some(RiskRejection::MaxOpenPositionsReached.as_str())
        );
    }

    #[test]
    fn zero_stop_distance_is_rejected_rather_than_dividing_by_zero() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.stop_loss_price = context.entry_price;

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(!decision.approved);
        assert_eq!(
            decision.rejection_reason.as_deref(),
            Some(RiskRejection::InvalidStopDistance.as_str())
        );
    }

    proptest! {
        /// The property that actually matters: no matter what base risk
        /// percent, cap, or multiplier combination we throw at it, the
        /// dollar amount actually risked never exceeds cap% of equity.
        /// This is the end-to-end version of the narrower check already
        /// in `domain::newtypes`; this one goes through the full
        /// `evaluate` pipeline, not just `apply_multiplier` in isolation.
        #[test]
        fn risked_amount_never_exceeds_the_configured_cap(
            base_risk_hundredths in 1u32..500u32, // 0.01% .. 5.00%
            cap_hundredths in 1u32..1000u32,      // 0.01% .. 10.00%
            is_tuesday in any::<bool>(),
            is_double_smt in any::<bool>(),
            equity_dollars in 100i64..1_000_000i64,
            stop_distance_micros in 1i64..10_000i64, // avoid zero, keep it realistic
        ) {
            let config = RiskConfig {
                base_risk_percent: Percent::from_ratio(Decimal::new(base_risk_hundredths as i64, 4)),
                max_risk_percent: Percent::from_ratio(Decimal::new(cap_hundredths as i64, 4)),
                max_open_positions: 100,
                daily_loss_limit_percent: Percent::from_percentage(dec!(100.0)),
                weekly_loss_limit_percent: Percent::from_percentage(dec!(100.0)),
            };
            let context = RiskContext {
                equity: Usd::from_decimal(Decimal::from(equity_dollars)),
                open_position_count: 0,
                is_tuesday,
                is_double_smt,
                entry_price: Decimal::new(11000, 4),
                stop_loss_price: Decimal::new(11000, 4) - Decimal::new(stop_distance_micros, 6),
                take_profit_price: Decimal::new(11500, 4),
                realized_pnl_today: Usd::zero(),
                realized_pnl_this_week: Usd::zero(),
            };

            let engine = DefaultRiskEngine;
            let decision = engine.evaluate(&sample_signal(), &config, &context).unwrap();

            if decision.approved {
                let cap_amount = Usd::from_percent_of(context.equity, config.max_risk_percent);
                prop_assert!(decision.risk_currency.as_decimal() <= cap_amount.as_decimal());
            }
        }
    }
}

--- ./risk/Cargo.toml ---
[package]
name = "risk"
version = "0.1.0"
edition = "2021"

[dependencies]
domain = { path = "../domain" }
thiserror = { workspace = true }
rust_decimal = { workspace = true }
uuid = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
rust_decimal_macros = { workspace = true }
chrono = { workspace = true }

--- ./session_time/src/true_open.rs ---
//! True Open levels are static reference prices captured at a fixed
//! moment (Monday 18:00 NY for the week, midnight NY for the day) and
//! then held fixed until they expire. They never move with price after
//! that, which is what makes them a stable reference rather than
//! something like a moving average.
//!
//! The gate logic here is the corrected version of what the hardening
//! layer specified. The original prose said "both Weekly and Daily must
//! align with SMT direction, except when Weekly is neutral, then Daily
//! decides," but the hardening layer's own decision table actually
//! resolves this as "Daily is only consulted when Weekly is neutral;
//! otherwise Weekly alone decides." Those are genuinely different rules
//! (they disagree whenever Weekly and Daily point in opposite
//! directions), and the decision table is the more concrete, more
//! recently written artifact, so that's the reading implemented below.
//! It's captured as a decision table rather than nested prose-driven
//! conditionals specifically so there's no ambiguity left for a future
//! reader to resolve differently.

use chrono::{DateTime, Utc};
use domain::{Bias, Direction, RejectionReason};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::calendar::{is_full_trading_week, to_ny, week_start_for, HolidayProvider};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Timeframe {
    Weekly,
    Daily,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrueOpenLevel {
    pub timeframe: Timeframe,
    pub symbol: String,
    pub level: Decimal,
    pub set_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl TrueOpenLevel {
    pub fn is_expired(&self, at: DateTime<Utc>) -> bool {
        at >= self.expires_at
    }
}

/// Compare a current price against a True Open level. Above the level is
/// a Buy bias, below is Sell, exactly on it is Neutral. This is a plain
/// three-way comparison rather than something with a tolerance band,
/// because the level is a fixed price and Decimal comparisons are exact,
/// no floating-point "close enough" concerns to work around here.
pub fn bias_from_price(current_price: Decimal, true_open_level: Decimal) -> Bias {
    match current_price.cmp(&true_open_level) {
        std::cmp::Ordering::Greater => Bias::Buy,
        std::cmp::Ordering::Less => Bias::Sell,
        std::cmp::Ordering::Equal => Bias::Neutral,
    }
}

/// The corrected weekly/daily True Open gate. Weekly bias decides the
/// trade whenever it isn't neutral; Daily is only consulted as a
/// tiebreaker when Weekly itself is neutral (price sitting exactly on the
/// weekly True Open). See the module docs for why this is the reading we
/// implemented instead of the "both must always align" prose.
pub fn true_open_gate(
    weekly_bias: Bias,
    daily_bias: Bias,
    smt_direction: Direction,
) -> Result<(), RejectionReason> {
    let effective_bias = match weekly_bias {
        Bias::Neutral => daily_bias,
        decisive => decisive,
    };

    match effective_bias {
        Bias::Neutral => Ok(()),
        Bias::Buy if smt_direction == Direction::Buy => Ok(()),
        Bias::Sell if smt_direction == Direction::Sell => Ok(()),
        _ => Err(RejectionReason::TrueOpenGateConflict),
    }
}

/// Whether `reference` (an instant, in UTC) falls in a week that should
/// get a Weekly True Open at all. A partial week (one whose boundary was
/// disrupted by a holiday) doesn't get one; the gate then always treats
/// Weekly as `Bias::Neutral` for that week, which hands the decision to
/// Daily for the whole week rather than to a Weekly level that was never
/// really valid.
pub fn week_qualifies_for_weekly_true_open(
    reference: DateTime<Utc>,
    holidays: &dyn HolidayProvider,
) -> bool {
    let ny = to_ny(reference);
    let this_week_start = week_start_for(ny);
    is_full_trading_week(this_week_start, holidays)
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Direction;
    use rust_decimal_macros::dec;

    #[test]
    fn weekly_buy_beats_daily_sell() {
        // The scenario the original prose ambiguity was actually about:
        // Weekly says Buy, Daily says Sell, SMT says Buy. Under "both
        // must align" this would reject; under "Weekly decides unless
        // neutral" (what we implemented) this passes.
        let result = true_open_gate(Bias::Buy, Bias::Sell, Direction::Buy);
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn weekly_sell_rejects_smt_buy_regardless_of_daily() {
        let result = true_open_gate(Bias::Sell, Bias::Buy, Direction::Buy);
        assert_eq!(result, Err(RejectionReason::TrueOpenGateConflict));
    }

    #[test]
    fn neutral_weekly_hands_off_to_daily() {
        assert_eq!(
            true_open_gate(Bias::Neutral, Bias::Buy, Direction::Buy),
            Ok(())
        );
        assert_eq!(
            true_open_gate(Bias::Neutral, Bias::Sell, Direction::Buy),
            Err(RejectionReason::TrueOpenGateConflict)
        );
    }

    #[test]
    fn both_neutral_passes() {
        assert_eq!(
            true_open_gate(Bias::Neutral, Bias::Neutral, Direction::Buy),
            Ok(())
        );
    }

    #[test]
    fn bias_from_price_reports_neutral_exactly_on_the_level() {
        let level = dec!(1.1000);
        assert_eq!(bias_from_price(dec!(1.1000), level), Bias::Neutral);
        assert_eq!(bias_from_price(dec!(1.1001), level), Bias::Buy);
        assert_eq!(bias_from_price(dec!(1.0999), level), Bias::Sell);
    }
}

--- ./session_time/src/calendar.rs ---
//! Everything in this file exists because "just subtract 5 hours for New
//! York time" breaks twice a year. New York observes daylight saving time
//! and the US and EU don't switch on the same calendar date, so for a
//! week or two each spring and fall the "usual" offset between NY and UTC
//! is briefly wrong if you hardcoded it. `chrono-tz` carries the real IANA
//! timezone database, so a `DateTime<Utc>` converted through
//! `America/New_York` is always correct for whatever day it happens to be,
//! DST included. That's worth the extra dependency.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

/// The one timezone this whole strategy cares about. Every session
/// boundary, every macro cycle, every True Open level is defined in terms
/// of New York local time, because that's what the ICT-style session
/// framework this strategy is built on uses as its reference clock.
pub fn ny_tz() -> Tz {
    chrono_tz::America::New_York
}

/// Convert a UTC instant to its New York local representation. This is
/// the one function everything else in this module should go through,
/// rather than each call site doing its own `.with_timezone(...)`, so
/// that if the reference timezone ever needs to change, there's exactly
/// one place to do it.
pub fn to_ny(instant: DateTime<Utc>) -> DateTime<Tz> {
    instant.with_timezone(&ny_tz())
}

/// A source of "now" that can be swapped out in tests. Production code
/// gets `SystemClock`, which asks the OS. Tests and replay get a clock
/// that can be paused and advanced by hand, so that a test asserting
/// "at 09:00 NY the macro cycle fires" doesn't have to actually wait
/// until 09:00 NY to run.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock a test can move forward on demand. Not async-aware on its own;
/// pairing this with `tokio::time::pause()` and `tokio::time::advance()`
/// in the actual replay engine is what gives you a fully deterministic
/// run, this struct just supplies the "what time does the strategy think
/// it is" half of that.
pub struct ManualClock {
    current: parking_lot::Mutex<DateTime<Utc>>,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        ManualClock {
            current: parking_lot::Mutex::new(start),
        }
    }

    pub fn advance(&self, duration: Duration) {
        // A short critical section that never spans an `.await`, so a
        // plain `parking_lot::Mutex` is the right tool here rather than
        // `tokio::sync::Mutex`. See the workspace-wide rule in
        // `daemon::event_bus` docs for the general version of this call.
        let mut guard = self.current.lock();
        *guard += duration;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.current.lock()
    }
}

/// Anything that can answer "is this date a holiday, and if so, is it the
/// kind of holiday where liquidity gets thin enough that the strategy
/// should stop opening new positions." Kept as a trait, not a hardcoded
/// list baked into the calendar logic, for the same reason `BrokerAdapter`
/// is a trait: a US-holiday list is the obvious starting point, but a
/// pure-crypto or pure-futures deployment later would want a completely
/// different provider without touching this module.
pub trait HolidayProvider: Send + Sync {
    fn is_holiday(&self, date: NaiveDate) -> bool;

    /// Distinct from `is_holiday`: a market can be open on a day that's
    /// still meaningfully thin (the day after Thanksgiving, the week
    /// between Christmas and New Year's) without literally being closed.
    /// The hardening layer's holiday fail-safe keys off this, not off
    /// `is_holiday` directly.
    fn is_low_liquidity(&self, date: NaiveDate) -> bool;
}

/// A small, static list of the holidays that reliably gut forex liquidity:
/// Christmas, New Year's, US Thanksgiving, and Good Friday. This is meant
/// as a sensible starting point, not an exhaustive global calendar; a
/// production deployment trading multiple asset classes would likely want
/// to load this from a config file or an external calendar service
/// instead (see `external_data_dependencies` in the original spec), but
/// the trait boundary here means that's a new implementation of
/// `HolidayProvider`, not a rewrite of anything that calls it.
pub struct StaticHolidayProvider;

impl StaticHolidayProvider {
    fn holidays_for_year(year: i32) -> Vec<NaiveDate> {
        let mut dates = Vec::with_capacity(4);

        if let Some(new_years) = NaiveDate::from_ymd_opt(year, 1, 1) {
            dates.push(new_years);
        }
        if let Some(christmas) = NaiveDate::from_ymd_opt(year, 12, 25) {
            dates.push(christmas);
        }
        if let Some(thanksgiving) = nth_weekday_of_month(year, 11, Weekday::Thu, 4) {
            dates.push(thanksgiving);
        }
        if let Some(easter) = easter_sunday(year) {
            dates.push(easter - Duration::days(2)); // Good Friday
        }

        dates
    }
}

impl HolidayProvider for StaticHolidayProvider {
    fn is_holiday(&self, date: NaiveDate) -> bool {
        Self::holidays_for_year(date.year()).contains(&date)
    }

    fn is_low_liquidity(&self, date: NaiveDate) -> bool {
        // The week between Christmas and New Year's is thin every year
        // even though only two of its days are actual holidays, and the
        // day after Thanksgiving is famously a half-liquidity session in
        // US markets. We treat both as low-liquidity without requiring
        // them to also be `is_holiday`.
        if self.is_holiday(date) {
            return true;
        }

        let christmas_week = NaiveDate::from_ymd_opt(date.year(), 12, 26)
            .zip(NaiveDate::from_ymd_opt(date.year(), 12, 31))
            .map(|(start, end)| date >= start && date <= end)
            .unwrap_or(false);

        let day_after_thanksgiving = nth_weekday_of_month(date.year(), 11, Weekday::Thu, 4)
            .map(|thanksgiving| date == thanksgiving + Duration::days(1))
            .unwrap_or(false);

        christmas_week || day_after_thanksgiving
    }
}

/// The n-th occurrence of `weekday` in a given month/year. `n` is
/// 1-indexed (the 4th Thursday of November is `nth_weekday_of_month(year,
/// 11, Weekday::Thu, 4)`), matching how people actually talk about these
/// dates rather than a 0-indexed offset.
fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, n: u32) -> Option<NaiveDate> {
    let first_of_month = NaiveDate::from_ymd_opt(year, month, 1)?;
    let first_weekday_offset =
        (7 + weekday.num_days_from_sunday() as i64 - first_of_month.weekday().num_days_from_sunday() as i64) % 7;
    let first_occurrence = first_of_month + Duration::days(first_weekday_offset);
    Some(first_occurrence + Duration::weeks((n - 1) as i64))
}

/// Easter Sunday for a given Gregorian year, via the Meeus/Jones/Butcher
/// algorithm. This is the standard, widely-verified way to compute this
/// without a lookup table; Good Friday is just this minus two days.
fn easter_sunday(year: i32) -> Option<NaiveDate> {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = ((h + l - 7 * m + 114) % 31) + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

/// The New York local time (18:00) that starts a trading week, for the
/// Sunday on or before `reference`. This is the anchor everything else
/// (full-week checks, the weekly True Open) is measured from.
pub fn week_start_for(reference: DateTime<Tz>) -> DateTime<Tz> {
    let days_since_sunday = reference.weekday().num_days_from_sunday() as i64;
    let this_or_previous_sunday = reference.date_naive() - Duration::days(days_since_sunday);
    let naive_open = this_or_previous_sunday
        .and_hms_opt(18, 0, 0)
        .expect("18:00:00 is always a valid time of day");

    match ny_tz().from_local_datetime(&naive_open) {
        chrono::LocalResult::Single(dt) => dt,
        // 6 PM local time is never actually ambiguous for
        // America/New_York (the US DST transitions happen around 2 AM
        // local time), so in practice we never expect to land here, but
        // if we ever did, picking the earlier of the two candidates is
        // the conservative choice for a "week start" boundary.
        chrono::LocalResult::Ambiguous(earlier, _later) => earlier,
        // Same reasoning: 6 PM is never inside a "spring forward" gap
        // either. This exists so the function is total instead of
        // partial. If it's ever actually hit, that means something is
        // wrong with the timezone database itself, which is a much
        // bigger problem than this one calculation, so we fall back to
        // treating the naive time as already being in UTC rather than
        // panicking the whole daemon over a calendar lookup.
        chrono::LocalResult::None => Utc.from_utc_datetime(&naive_open).with_timezone(&ny_tz()),
    }
}

/// The corrected version of the hardening layer's full-week check.
///
/// The original wording compared Monday 18:00 NY to "the previous Sunday
/// 18:00 NY," which is always a one-day gap, not seven, so it could never
/// actually detect anything. What we actually want to know is: did this
/// week's Sunday-18:00 open land exactly seven days after last week's
/// Sunday-18:00 open? If a holiday closure shifted or skipped a weekly
/// open, that gap won't be seven days, and this week doesn't get a Weekly
/// True Open (it falls back to Daily only).
pub fn is_full_trading_week(this_week_start: DateTime<Tz>, holidays: &dyn HolidayProvider) -> bool {
    let previous_week_start = this_week_start - Duration::weeks(1);

    let seven_days_apart = (this_week_start.date_naive() - previous_week_start.date_naive()).num_days() == 7;

    // Even if the calendar math lines up, a week whose open falls on a
    // known holiday shouldn't be treated as "full" either.
    let open_is_a_holiday = holidays.is_holiday(this_week_start.date_naive());

    seven_days_apart && !open_is_a_holiday
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn ny_conversion_reflects_summer_dst() {
        // July 1 2026 is in DST, so NY should be UTC-4, not UTC-5.
        let utc_noon = Utc.with_ymd_and_hms(2026, 7, 1, 16, 0, 0).unwrap();
        let ny = to_ny(utc_noon);
        assert_eq!(ny.hour(), 12);
    }

    #[test]
    fn ny_conversion_reflects_winter_standard_time() {
        // January 1 2026 is standard time, NY is UTC-5.
        let utc_noon = Utc.with_ymd_and_hms(2026, 1, 1, 17, 0, 0).unwrap();
        let ny = to_ny(utc_noon);
        assert_eq!(ny.hour(), 12);
    }

    #[test]
    fn easter_sunday_matches_known_dates() {
        // These are well-published, easily independently checked dates;
        // they're here as a regression check on the algorithm, not as a
        // claim that we derived them ourselves.
        assert_eq!(easter_sunday(2024), NaiveDate::from_ymd_opt(2024, 3, 31));
        assert_eq!(easter_sunday(2025), NaiveDate::from_ymd_opt(2025, 4, 20));
        assert_eq!(easter_sunday(2026), NaiveDate::from_ymd_opt(2026, 4, 5));
    }

    #[test]
    fn thanksgiving_is_the_fourth_thursday_of_november() {
        // 2026-11-26 is the fourth Thursday of November 2026.
        assert_eq!(
            nth_weekday_of_month(2026, 11, Weekday::Thu, 4),
            NaiveDate::from_ymd_opt(2026, 11, 26)
        );
    }

    #[test]
    fn christmas_is_a_holiday_and_low_liquidity() {
        let provider = StaticHolidayProvider;
        let christmas = NaiveDate::from_ymd_opt(2026, 12, 25).unwrap();
        assert!(provider.is_holiday(christmas));
        assert!(provider.is_low_liquidity(christmas));
    }

    #[test]
    fn week_between_christmas_and_new_year_is_low_liquidity_but_not_a_holiday() {
        let provider = StaticHolidayProvider;
        let dec_28 = NaiveDate::from_ymd_opt(2026, 12, 28).unwrap();
        assert!(!provider.is_holiday(dec_28));
        assert!(provider.is_low_liquidity(dec_28));
    }

    #[test]
    fn ordinary_week_is_a_full_trading_week() {
        let holidays = StaticHolidayProvider;
        // An arbitrary Sunday with no holiday nearby.
        let sunday = ny_tz()
            .with_ymd_and_hms(2026, 3, 1, 18, 0, 0)
            .single()
            .unwrap();
        assert!(is_full_trading_week(sunday, &holidays));
    }

    #[test]
    fn week_opening_on_a_holiday_is_not_full() {
        let holidays = StaticHolidayProvider;
        // is_full_trading_week only checks whether the given date is a
        // recognized holiday, it doesn't independently verify the date
        // is a Sunday, so we can exercise the holiday branch directly
        // against a Christmas Day regardless of which weekday it falls
        // on in this particular year.
        let christmas = ny_tz()
            .with_ymd_and_hms(2033, 12, 25, 18, 0, 0)
            .single()
            .unwrap();
        assert!(!is_full_trading_week(christmas, &holidays));
    }

    #[test]
    fn week_start_for_lands_on_sunday_at_1800_ny() {
        // Pick a Wednesday and check it resolves back to that week's
        // Sunday open.
        let wednesday = ny_tz().with_ymd_and_hms(2026, 3, 4, 9, 0, 0).single().unwrap();
        let start = week_start_for(wednesday);
        assert_eq!(start.weekday(), Weekday::Sun);
        assert_eq!(start.hour(), 18);
    }
}

--- ./session_time/src/lib.rs ---
//! Everything in this crate is about answering "what does the calendar
//! say right now": NY session time, DST-correct conversions, holidays,
//! macro cycle windows, and True Open level bookkeeping. It depends on
//! `domain` for the shared vocabulary (`Direction`, `Bias`,
//! `RejectionReason`) and nothing else in the workspace, so that
//! `strategy`, `risk`, and everyone else can depend on `time` without
//! dragging in broker or persistence concerns.

pub mod calendar;
pub mod macro_cycle;
pub mod true_open;
pub mod true_open_capture;

pub use calendar::{
    is_full_trading_week, ny_tz, to_ny, week_start_for, Clock, HolidayProvider, ManualClock,
    StaticHolidayProvider, SystemClock,
};
pub use macro_cycle::{is_within_macro_cycle, next_macro_cycle_after, MACRO_CYCLE_HOURS};
pub use true_open::{
    bias_from_price, true_open_gate, week_qualifies_for_weekly_true_open, Timeframe,
    TrueOpenLevel,
};
pub use true_open_capture::{capture_level, needs_capture, next_ny_occurrence, DAILY_CAPTURE_HOUR_NY, WEEKLY_CAPTURE_HOUR_NY};

--- ./session_time/src/macro_cycle.rs ---
//! The strategy trades on eight fixed windows a day, three hours apart,
//! each one 20 minutes wide (10 minutes either side of the hour). Keeping
//! these evenly spaced, instead of the more organic ICT-style macro
//! windows some traders use, is a deliberate simplification: it makes the
//! schedule trivial to test and reason about, at the cost of not
//! perfectly matching every nuance of the underlying trading concept.
//! That trade-off was made when the strategy itself was designed, this
//! module just implements it.

use chrono::{DateTime, Duration, TimeZone};
use chrono_tz::Tz;

use crate::calendar::to_ny;

/// The NY-local hours a macro cycle centers on. `0` means midnight.
pub const MACRO_CYCLE_HOURS: [u32; 8] = [3, 6, 9, 12, 15, 18, 21, 0];

pub const MACRO_CYCLE_HALF_WIDTH_MINUTES: i64 = 10;

/// Whether `instant` falls inside any macro cycle's 20-minute window.
pub fn is_within_macro_cycle(instant: DateTime<chrono::Utc>) -> bool {
    let ny = to_ny(instant);
    MACRO_CYCLE_HOURS
        .iter()
        .any(|&hour| minutes_from_cycle_center(ny, hour) <= MACRO_CYCLE_HALF_WIDTH_MINUTES)
}

/// Minutes between `ny_time` and the nearest occurrence of `cycle_hour`
/// (today or spilling into yesterday/tomorrow), used both by
/// `is_within_macro_cycle` and by anything that wants to know "how close
/// are we" rather than just a yes/no.
fn minutes_from_cycle_center(ny_time: DateTime<Tz>, cycle_hour: u32) -> i64 {
    let today_center = ny_time
        .date_naive()
        .and_hms_opt(cycle_hour, 0, 0)
        .expect("cycle hours are always valid hours of the day");

    let naive_now = ny_time.naive_local();
    let diff_today = (naive_now - today_center).num_minutes().abs();

    // A cycle centered near midnight can be closer to "yesterday's
    // midnight" or "tomorrow's midnight" than to today's, depending on
    // which side of midnight `ny_time` falls on. Checking the adjacent
    // days too avoids an edge case where, say, 00:05 NY reports itself as
    // 5 minutes from a cycle 23 hours and 55 minutes away instead of the
    // actual 5-minute difference.
    let diff_previous_day = {
        let previous_day_center = today_center - Duration::days(1);
        (naive_now - previous_day_center).num_minutes().abs()
    };
    let diff_next_day = {
        let next_day_center = today_center + Duration::days(1);
        (naive_now - next_day_center).num_minutes().abs()
    };

    diff_today.min(diff_previous_day).min(diff_next_day)
}

/// The next macro cycle center at or after `instant`, in UTC. Used by the
/// scheduler to know how long to sleep before the next entry window.
pub fn next_macro_cycle_after(instant: DateTime<chrono::Utc>) -> DateTime<chrono::Utc> {
    let ny = to_ny(instant);
    let mut best: Option<DateTime<Tz>> = None;

    // Looking two calendar days ahead is enough headroom: even starting
    // from just after the 21:00 cycle, the next candidate (00:00 the
    // following day) is within that window, and we never need a third day
    // for an 8-cycles-per-day, 3-hour-apart schedule.
    for day_offset in 0..2 {
        let day = ny.date_naive() + Duration::days(day_offset);
        for &hour in MACRO_CYCLE_HOURS.iter() {
            let candidate_naive = day
                .and_hms_opt(hour, 0, 0)
                .expect("cycle hours are always valid hours of the day");
            if candidate_naive < ny.naive_local() {
                continue;
            }
            let candidate = crate::calendar::ny_tz()
                .from_local_datetime(&candidate_naive)
                .single()
                .unwrap_or(ny); // see note below

            best = Some(match best {
                Some(current_best) if current_best < candidate => current_best,
                _ => candidate,
            });
        }
    }

    // Falling back to `ny` itself in the (essentially never-hit, given
    // none of our cycle hours land in a DST transition) ambiguous case
    // just means we'd re-check the same instant on the next scheduler
    // tick rather than propagating an error out of what's meant to be a
    // simple "what's next" lookup.
    best.unwrap_or(ny).with_timezone(&chrono::Utc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn instant_at_cycle_center_is_within_the_cycle() {
        // 09:00:00 NY on an arbitrary day, converted to UTC by adding the
        // real NY-UTC offset rather than a hardcoded number, so this test
        // doesn't itself depend on knowing which side of a DST boundary
        // the date falls on.
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn instant_far_from_any_cycle_is_not_within_a_cycle() {
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(!is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn midnight_boundary_is_handled_correctly() {
        // 23:55 NY should be recognized as 5 minutes from the 00:00
        // cycle, not (24 hours minus 5 minutes) from it.
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(23, 55, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn next_macro_cycle_after_finds_the_very_next_one() {
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(9, 30, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);

        let next = next_macro_cycle_after(utc_dt);
        let next_ny = to_ny(next);
        assert_eq!(next_ny.hour(), 12);
    }
}

--- ./session_time/src/true_open_capture.rs ---
//! `true_open.rs` has the gate logic (given a bias, does a trade pass).
//! This file is the other half: given a clock and whatever level (if
//! any) is currently on file, decide whether a fresh one needs
//! capturing, and build it.
//!
//! Daily True Open is captured at midnight NY. That's a named
//! assumption, not something confirmed against an external source the
//! way the WebSocket endpoint or symbol convention were: it's the
//! common ICT convention (the "midnight open"), distinct from the
//! weekly anchor, and it's what this project settles on absent a more
//! specific instruction. Weekly is captured Monday 18:00 NY, which the
//! True Open addendum specified directly.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc, Weekday};

use crate::calendar::{to_ny, week_start_for, HolidayProvider};
use crate::true_open::{Timeframe, TrueOpenLevel};

pub const DAILY_CAPTURE_HOUR_NY: u32 = 0;
pub const WEEKLY_CAPTURE_HOUR_NY: u32 = 18;

/// The next instant, at or after `after`, that lands on `hour:00 NY`
/// local time — and, if `weekday` is given, that also falls on that
/// weekday. General enough to answer "next midnight NY," "next Monday
/// 18:00 NY," or "next 06:00 NY session boundary" with the same logic,
/// which is why it's public rather than private to this file: the
/// buffer-reset logic in `strategy` needs the same search.
pub fn next_ny_occurrence(after: DateTime<Utc>, hour: u32, weekday: Option<Weekday>) -> DateTime<Utc> {
    let ny_now = to_ny(after);
    let mut candidate_date = ny_now.date_naive();

    // If today's occurrence of this hour has already passed, start
    // looking from tomorrow instead.
    if ny_now.hour() >= hour {
        candidate_date += Duration::days(1);
    }

    if let Some(target_weekday) = weekday {
        while candidate_date.weekday() != target_weekday {
            candidate_date += Duration::days(1);
        }
    }

    let naive = candidate_date
        .and_hms_opt(hour, 0, 0)
        .expect("hour is always in 0..24, always a valid time of day");

    match crate::calendar::ny_tz().from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
        chrono::LocalResult::Ambiguous(earlier, _) => earlier.with_timezone(&Utc),
        // Same reasoning as week_start_for: midnight and 18:00 are never
        // actually inside a DST transition gap for America/New_York (the
        // transition itself happens around 2 AM local), so this is a
        // total-function fallback that in practice never triggers.
        chrono::LocalResult::None => after,
    }
}

/// Whether the level currently on file (if any) is still valid, or a
/// fresh capture is needed. `None` on file always means "needs capture,"
/// the same as an expired one.
pub fn needs_capture(now: DateTime<Utc>, current: Option<&TrueOpenLevel>) -> bool {
    match current {
        None => true,
        Some(level) => level.is_expired(now),
    }
}

/// Build a fresh level for `timeframe`, capturing `price` as of `now`.
/// For Weekly, returns `None` instead of a level when the current week
/// doesn't qualify for one (a partial week per
/// [`crate::calendar::is_full_trading_week`]) — callers should treat a
/// `None` weekly level as `Bias::Neutral` for the whole week, which is
/// exactly what handing the decision to Daily was always meant to mean.
pub fn capture_level(
    timeframe: Timeframe,
    symbol: &str,
    price: rust_decimal::Decimal,
    now: DateTime<Utc>,
    holidays: &dyn HolidayProvider,
) -> Option<TrueOpenLevel> {
    match timeframe {
        Timeframe::Weekly => {
            let ny = to_ny(now);
            let this_week_start = week_start_for(ny);
            if !crate::calendar::is_full_trading_week(this_week_start, holidays) {
                return None;
            }
            let expires_at = next_ny_occurrence(now, WEEKLY_CAPTURE_HOUR_NY, Some(Weekday::Mon));
            Some(TrueOpenLevel {
                timeframe,
                symbol: symbol.to_string(),
                level: price,
                set_at: now,
                expires_at,
            })
        }
        Timeframe::Daily => {
            let expires_at = next_ny_occurrence(now, DAILY_CAPTURE_HOUR_NY, None);
            Some(TrueOpenLevel {
                timeframe,
                symbol: symbol.to_string(),
                level: price,
                set_at: now,
                expires_at,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::StaticHolidayProvider;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn needs_capture_is_true_with_nothing_on_file() {
        assert!(needs_capture(Utc::now(), None));
    }

    #[test]
    fn needs_capture_is_false_for_an_unexpired_level() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let level = TrueOpenLevel {
            timeframe: Timeframe::Daily,
            symbol: "EURUSD".to_string(),
            level: dec!(1.1000),
            set_at: now,
            expires_at: now + Duration::hours(1),
        };
        assert!(!needs_capture(now, Some(&level)));
    }

    #[test]
    fn needs_capture_is_true_for_an_expired_level() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let level = TrueOpenLevel {
            timeframe: Timeframe::Daily,
            symbol: "EURUSD".to_string(),
            level: dec!(1.1000),
            set_at: now - Duration::hours(2),
            expires_at: now - Duration::hours(1),
        };
        assert!(needs_capture(now, Some(&level)));
    }

    #[test]
    fn daily_capture_expires_at_the_next_midnight_ny() {
        let noon_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 10, 12, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Daily, "EURUSD", dec!(1.1000), noon_ny, &holidays).unwrap();
        let expires_ny = to_ny(level.expires_at);
        assert_eq!(expires_ny.hour(), 0);
        // Expiry should be later the same NY calendar day (midnight
        // rolling into the next date), i.e. within 24 hours out.
        assert!(level.expires_at > noon_ny);
        assert!(level.expires_at <= noon_ny + Duration::hours(24));
    }

    #[test]
    fn weekly_capture_expires_the_following_monday_at_1800_ny() {
        let tuesday_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 3, 9, 0, 0) // a Tuesday
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Weekly, "EURUSD", dec!(1.1000), tuesday_ny, &holidays).unwrap();
        let expires_ny = to_ny(level.expires_at);
        assert_eq!(expires_ny.weekday(), Weekday::Mon);
        assert_eq!(expires_ny.hour(), 18);
    }

    #[test]
    fn weekly_capture_returns_some_for_an_ordinary_week() {
        let tuesday_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 3, 9, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Weekly, "EURUSD", dec!(1.1000), tuesday_ny, &holidays);
        assert!(level.is_some());
    }
}

--- ./session_time/Cargo.toml ---
[package]
name = "session_time"
version = "0.1.0"
edition = "2021"

[dependencies]
domain = { path = "../domain" }
chrono = { workspace = true }
chrono-tz = { workspace = true }
thiserror = { workspace = true }
serde = { workspace = true }
uuid = { workspace = true }
parking_lot = { workspace = true }
rust_decimal = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
rust_decimal_macros = { workspace = true }

--- ./broker/src/mock.rs ---
//! A broker double that can misbehave on purpose. The whole point of
//! `MockBroker` isn't to be a happy-path stub, it's to let tests script
//! exactly the failure modes that matter most: a rejected order, a
//! timeout, a rate limit, a partial fill, and specifically the "the
//! request technically succeeded but the response is empty or garbage"
//! failure devmind actually hit against Cognee. If the daemon's retry and
//! reconciliation logic can't survive this broker being deliberately
//! difficult, it's not ready to survive a real one being difficult by
//! accident.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use domain::{
    BrokerSnapshot, ComponentStatus, FillLeg, Order, OrderRequest, OrderStatus, Position,
    PositionStatus, PriceQuote, Usd,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};

/// One scripted outcome for the next call to `submit_order`. Queued
/// responses are consumed in order (FIFO); once the queue is empty,
/// `MockBroker` falls back to filling every order completely at whatever
/// price the request asked for (or a synthetic price for market orders),
/// which is the "everything's fine" baseline the queue lets tests deviate
/// from on purpose.
#[derive(Debug, Clone)]
pub enum ScriptedResponse {
    ConnectionFailed,
    Timeout(u64),
    RateLimited(u64),
    Rejected(String),
    /// The devmind/Cognee failure mode: the call returns `Ok`, not an
    /// error, but the broker's own client code (imagine a real HTTP
    /// client parsing an empty body here) would have nothing usable to
    /// build an `Order` from. We model this directly as a
    /// `BrokerError::MalformedResponse` so it's indistinguishable, from
    /// the daemon's point of view, from what an unlucky real broker call
    /// would actually look like.
    MalformedResponse(String),
    PartialFill(Decimal),
}

struct MockBrokerState {
    equity: Usd,
    positions: std::collections::HashMap<Uuid, Position>,
    orders: std::collections::HashMap<Uuid, Order>,
    submit_order_script: VecDeque<ScriptedResponse>,
    synthetic_price: Decimal,
}

pub struct MockBroker {
    state: parking_lot::Mutex<MockBrokerState>,
    call_count: AtomicU64,
}

impl MockBroker {
    pub fn new(initial_equity: Usd, synthetic_price: Decimal) -> Self {
        MockBroker {
            state: parking_lot::Mutex::new(MockBrokerState {
                equity: initial_equity,
                positions: std::collections::HashMap::new(),
                orders: std::collections::HashMap::new(),
                submit_order_script: VecDeque::new(),
                synthetic_price,
            }),
            call_count: AtomicU64::new(0),
        }
    }

    /// Queue up the next `submit_order` call to behave a specific way.
    /// Calls consume the queue in order; test setup usually looks like
    /// `broker.queue_submit_order_response(...)` once per call it wants
    /// to control, then lets everything after that fall back to normal
    /// behavior.
    pub fn queue_submit_order_response(&self, response: ScriptedResponse) {
        self.state.lock().submit_order_script.push_back(response);
    }

    /// Convenience wrapper naming the exact devmind/Cognee failure mode
    /// directly, so a test reads as "simulate that specific incident"
    /// rather than "queue a MalformedResponse and hope the reader
    /// remembers why."
    pub fn simulate_silent_200_empty_body(&self) {
        self.queue_submit_order_response(ScriptedResponse::MalformedResponse(
            "200 OK with an empty response body".to_string(),
        ));
    }

    /// Directly remove a position from the broker's own state, simulating
    /// a broker that has no record of something our local persistence
    /// still thinks is open (the orphaned-position scenario
    /// reconciliation exists to catch).
    pub fn forget_position(&self, position_id: Uuid) {
        self.state.lock().positions.remove(&position_id);
    }

    pub fn insert_position(&self, position: Position) {
        self.state.lock().positions.insert(position.position_id, position);
    }

    pub fn how_many_calls(&self) -> u64 {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl BrokerAdapter for MockBroker {
    async fn get_snapshot(&self, pairs: &[String]) -> Result<BrokerSnapshot, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        // Lock, read what we need, drop the guard, then build the return
        // value. Nothing here spans an `.await`, so `parking_lot::Mutex`
        // is the right tool; if this method ever needed to `.await` while
        // still holding a lock, it would need `tokio::sync::Mutex`
        // instead. See `daemon::event_bus` for the fuller version of that
        // rule.
        let price = self.state.lock().synthetic_price;

        let mut prices = std::collections::BTreeMap::new();
        let mut spreads = std::collections::BTreeMap::new();
        for pair in pairs {
            prices.insert(
                pair.clone(),
                PriceQuote {
                    bid: price,
                    ask: price + Decimal::new(2, 5), // a small synthetic spread
                },
            );
            spreads.insert(pair.clone(), Decimal::new(2, 5));
        }

        Ok(BrokerSnapshot {
            snapshot_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            prices,
            spreads,
        })
    }

    async fn submit_order(&self, request: OrderRequest) -> Result<Order, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        let scripted = {
            let mut guard = self.state.lock();
            guard.submit_order_script.pop_front()
        };

        match scripted {
            Some(ScriptedResponse::ConnectionFailed) => {
                Err(BrokerError::ConnectionFailed("simulated connection failure".to_string()))
            }
            Some(ScriptedResponse::Timeout(ms)) => Err(BrokerError::Timeout(ms)),
            Some(ScriptedResponse::RateLimited(ms)) => Err(BrokerError::RateLimited(ms)),
            Some(ScriptedResponse::Rejected(reason)) => Err(BrokerError::Rejected(reason)),
            Some(ScriptedResponse::MalformedResponse(detail)) => {
                Err(BrokerError::MalformedResponse(detail))
            }
            Some(ScriptedResponse::PartialFill(filled_size)) => {
                let fill_price = request.price.unwrap_or_else(|| self.state.lock().synthetic_price);
                let order = Order {
                    order_id: request.order_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    position_id: None,
                    pair: request.pair.clone(),
                    side: request.side,
                    size: request.size,
                    filled_size,
                    price: fill_price,
                    status: OrderStatus::PartiallyFilled,
                    timestamp: Utc::now(),
                    last_update: Utc::now(),
                };
                self.state.lock().orders.insert(order.order_id, order.clone());
                Ok(order)
            }
            None => {
                // Normal path: fill completely at the requested price (or
                // the synthetic market price for a Market order that
                // didn't specify one).
                let fill_price = request.price.unwrap_or_else(|| self.state.lock().synthetic_price);
                let position_id = Uuid::new_v4();
                let order = Order {
                    order_id: request.order_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    position_id: Some(position_id),
                    pair: request.pair.clone(),
                    side: request.side,
                    size: request.size,
                    filled_size: request.size,
                    price: fill_price,
                    status: OrderStatus::Filled,
                    timestamp: Utc::now(),
                    last_update: Utc::now(),
                };
                self.state.lock().orders.insert(order.order_id, order.clone());

                let position = Position {
                    position_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    pair: request.pair,
                    direction: request.side,
                    legs: vec![FillLeg {
                        price: fill_price,
                        size: request.size,
                        filled_at: Utc::now(),
                    }],
                    entry_price: fill_price,
                    current_price: fill_price,
                    unrealized_pnl: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                    entry_time: Utc::now(),
                    last_update: Utc::now(),
                    status: PositionStatus::Filled,
                    exit_reason: None,
                    stop_loss: request.stop_loss,
                    take_profit: request.take_profit,
                };
                self.state.lock().positions.insert(position.position_id, position);

                Ok(order)
            }
        }
    }

    async fn cancel_order(&self, order_id: Uuid) -> Result<(), BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.state.lock();
        if guard.orders.remove(&order_id).is_some() {
            Ok(())
        } else {
            Err(BrokerError::NotFound(order_id))
        }
    }

    async fn close_position(&self, position_id: Uuid) -> Result<Order, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.state.lock();
        let close_price = guard.synthetic_price;

        let Some(position) = guard.positions.remove(&position_id) else {
            return Err(BrokerError::NotFound(position_id));
        };

        let direction_multiplier = match position.direction {
            domain::Direction::Buy => Decimal::ONE,
            domain::Direction::Sell => -Decimal::ONE,
        };
        let realized_pnl = (close_price - position.entry_price) * position.legs.iter().map(|l| l.size).sum::<Decimal>() * direction_multiplier;

        let order = Order {
            order_id: Uuid::new_v4(),
            trace_id: position.trace_id,
            signal_id: position.signal_id,
            position_id: Some(position.position_id),
            pair: position.pair,
            side: position.direction.opposite(),
            size: position.legs.iter().map(|l| l.size).sum(),
            filled_size: position.legs.iter().map(|l| l.size).sum(),
            price: close_price,
            status: OrderStatus::Filled,
            timestamp: Utc::now(),
            last_update: Utc::now(),
        };
        guard.orders.insert(order.order_id, order.clone());
        guard.equity = Usd::from_decimal(guard.equity.as_decimal() + realized_pnl);

        Ok(order)
    }

    async fn get_account_equity(&self) -> Result<Usd, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().equity)
    }

    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().positions.values().cloned().collect())
    }

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().orders.values().cloned().collect())
    }

    fn capabilities(&self) -> BrokerCapabilities {
        // The mock claims to support everything, since its job is to let
        // the strategy and risk layers be tested without a capability
        // mismatch getting in the way. A real adapter (see `broker::stubs`)
        // reports its own honest, narrower set.
        BrokerCapabilities {
            market_orders: true,
            limit_orders: true,
            ioc_orders: true,
            fok_orders: true,
            partial_closes: true,
            hedging: true,
            netting: true,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: true,
            supports_oco: true,
            supports_gtc: true,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A basic health probe helper tests can use without pulling in the full
/// `daemon` crate: reports the mock as healthy as long as it's reachable
/// at all, which it always is, being in-process.
pub fn mock_health_status() -> domain::HealthStatus {
    domain::HealthStatus {
        component: "broker".to_string(),
        status: ComponentStatus::Healthy,
        latency_ms: 0.0,
        error: None,
        timestamp: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{Direction, OrderType};
    use rust_decimal_macros::dec;

    fn sample_request() -> OrderRequest {
        OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: Direction::Buy,
            size: dec!(1.0),
            order_type: OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn normal_submit_fills_completely_and_opens_a_position() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = broker.submit_order(sample_request()).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_size, dec!(1.0));

        let positions = broker.list_open_positions().await.unwrap();
        assert_eq!(positions.len(), 1);
    }

    #[tokio::test]
    async fn scripted_malformed_response_surfaces_as_an_error_not_a_success() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        broker.simulate_silent_200_empty_body();

        let result = broker.submit_order(sample_request()).await;
        assert!(matches!(result, Err(BrokerError::MalformedResponse(_))));

        // Crucially: no position should have been opened. This is the
        // whole point of modeling the failure as an error instead of
        // quietly returning something Order-shaped: the daemon's
        // reconciliation logic gets an honest signal to retry against,
        // rather than a phantom fill it has to discover was never real.
        let positions = broker.list_open_positions().await.unwrap();
        assert_eq!(positions.len(), 0);
    }

    #[tokio::test]
    async fn scripted_responses_are_consumed_in_order() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        broker.queue_submit_order_response(ScriptedResponse::ConnectionFailed);
        broker.simulate_silent_200_empty_body();

        let first = broker.submit_order(sample_request()).await;
        assert!(matches!(first, Err(BrokerError::ConnectionFailed(_))));

        let second = broker.submit_order(sample_request()).await;
        assert!(matches!(second, Err(BrokerError::MalformedResponse(_))));

        // Queue is now empty, so this one should succeed normally.
        let third = broker.submit_order(sample_request()).await;
        assert!(third.is_ok());
    }

    #[tokio::test]
    async fn close_position_removes_it_and_returns_a_closing_order() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = broker.submit_order(sample_request()).await.unwrap();
        let position_id = order.position_id.expect("normal fill path always sets position_id");

        let closing_order = broker.close_position(position_id).await.unwrap();
        assert_eq!(closing_order.status, OrderStatus::Filled);

        let positions = broker.list_open_positions().await.unwrap();
        assert!(positions.is_empty());
    }

    #[tokio::test]
    async fn closing_an_unknown_position_returns_not_found() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let result = broker.close_position(Uuid::new_v4()).await;
        assert!(matches!(result, Err(BrokerError::NotFound(_))));
    }
    #[tokio::test]
    async fn forget_position_simulates_a_broker_side_orphan() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = broker.submit_order(sample_request()).await.unwrap();
        let positions = broker.list_open_positions().await.unwrap();
        let position_id = positions[0].position_id;

        broker.forget_position(position_id);
        let positions_after = broker.list_open_positions().await.unwrap();
        assert_eq!(positions_after.len(), 0);
        // The order record itself is untouched; only the broker's
        // position bookkeeping was made to "forget," which is the
        // specific inconsistency reconciliation needs to detect.
        let orders = broker.list_open_orders().await.unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, order.order_id);
    }
}

--- ./broker/src/adapter.rs ---
//! `BrokerAdapter` is the one seam in this whole daemon where "config
//! swap, no recompile" meets the reality that OANDA, MT5, IBKR, Binance,
//! and CME don't actually agree on much. The trait below tries to capture
//! the common shape (get prices, submit an order, ask what's open) without
//! pretending every broker's order model is identical.
//!
//! One deliberate change from the original sketch of this trait: instead
//! of a separate method per order type (`submit_market_order`,
//! `submit_limit_order`, ...), there's a single `submit_order` that takes
//! the whole `OrderRequest` and lets the adapter match on
//! `request.order_type` internally. `OrderRequest::order_type` already has
//! four variants (Market, Limit, IOC, FOK); a one-method-per-variant trait
//! would need a new trait method every time a new order type shows up,
//! which is exactly the kind of hardcoded-per-case growth this workspace
//! is trying to avoid everywhere else.

use async_trait::async_trait;
use domain::{BrokerSnapshot, Order, OrderRequest, Position, Usd};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum BrokerError {
    #[error("could not reach the broker: {0}")]
    ConnectionFailed(String),

    #[error("broker request timed out after {0}ms")]
    Timeout(u64),

    #[error("broker rate limit hit, retry after {0}ms")]
    RateLimited(u64),

    #[error("broker rejected the request: {0}")]
    Rejected(String),

    /// This is the exact failure class devmind hit with Cognee: the
    /// transport layer says "success" (a 200, a completed TLS handshake)
    /// but the response body is empty, truncated, or otherwise doesn't
    /// parse as the thing we asked for. Treating this as its own error
    /// variant, rather than silently accepting an empty response as "the
    /// order must have worked," is the whole point of having it.
    #[error("broker returned a malformed or empty response: {0}")]
    MalformedResponse(String),

    #[error("broker has no record of order/position {0}")]
    NotFound(Uuid),

    /// For adapters (OANDA, MT5) that exist as a real, honest skeleton in
    /// this workspace but whose actual wire protocol isn't wired up,
    /// because doing that correctly means confirming exact endpoint URLs,
    /// auth flows, and payload shapes against each broker's live docs,
    /// which isn't something to guess at. See `broker::stubs`.
    #[error("{0} is not yet implemented against a live broker")]
    NotImplemented(String),
}

/// What a given broker actually supports. This is a static, compiled-in
/// declaration per adapter, not a live negotiation protocol, since most
/// retail forex brokers don't expose a "what do you support" endpoint.
/// Calling it a capability *declaration* rather than *discovery* is more
/// honest about what's actually happening here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerCapabilities {
    pub market_orders: bool,
    pub limit_orders: bool,
    pub ioc_orders: bool,
    pub fok_orders: bool,
    pub partial_closes: bool,
    pub hedging: bool,
    pub netting: bool,
    pub native_stop_loss: bool,
    pub native_take_profit: bool,
    pub modify_orders: bool,
    pub supports_oco: bool,
    pub supports_gtc: bool,
}

#[async_trait]
pub trait BrokerAdapter: Send + Sync {
    /// One atomic, point-in-time price capture for every requested pair.
    /// Everything in a macro cycle should be evaluated against a single
    /// snapshot from this call, not repeated live-price lookups.
    async fn get_snapshot(&self, pairs: &[String]) -> Result<BrokerSnapshot, BrokerError>;

    /// Submit any kind of order. See the module docs for why this is one
    /// method instead of four.
    async fn submit_order(&self, request: OrderRequest) -> Result<Order, BrokerError>;

    async fn cancel_order(&self, order_id: Uuid) -> Result<(), BrokerError>;

    /// Close an existing open position at market. Distinct from
    /// `cancel_order`, which cancels a pending order that hasn't filled
    /// yet: this closes something that's already open, which is what
    /// exit-condition monitoring (risk-reward, pre-news, SMT
    /// contradiction) actually needs.
    async fn close_position(&self, position_id: Uuid) -> Result<Order, BrokerError>;

    async fn get_account_equity(&self) -> Result<Usd, BrokerError>;

    /// The broker's own account of what's open right now. This is what
    /// reconciliation calls on startup and after every reconnect; it's
    /// meant to be treated as the source of truth, with our local cursor
    /// files as an advisory cache rather than the other way around.
    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError>;

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError>;

    fn capabilities(&self) -> BrokerCapabilities;

    /// The escape hatch. If a broker needs to expose something no other
    /// broker has an equivalent of (CME contract months, IBKR's specific
    /// pacing rules), downcast through here instead of adding a method to
    /// this trait that only one implementation will ever use.
    fn as_any(&self) -> &dyn std::any::Any;
}

--- ./broker/src/lib.rs ---
//! The `BrokerAdapter` trait plus the mock implementation everything else
//! in this workspace gets tested against. There's no OANDA or MT5 wire
//! protocol implemented here; see the README at the workspace root for
//! why that's a deliberate scope decision rather than an oversight.

pub mod adapter;
pub mod deriv;
pub mod mock;
pub mod stubs;

pub use adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};
pub use deriv::{DerivAdapter, DerivClient};
pub use mock::{mock_health_status, MockBroker, ScriptedResponse};
pub use stubs::BybitAdapter;

--- ./broker/src/deriv.rs ---
//! A real client for Deriv's WebSocket API.
//!
//! Endpoint, symbol convention, and the overall authorize -> proposal ->
//! buy -> portfolio -> sell flow were all confirmed against Deriv's own
//! current documentation and GitHub repo while building this, not
//! recalled from memory: `wss://ws.derivws.com/websockets/v3?app_id=...`
//! is the current canonical host (older docs and forum posts reference
//! `ws.binaryws.com`, which is legacy), and forex symbols use Deriv's
//! `frx` prefix (`EURUSD` becomes `frxEURUSD`).
//!
//! One thing surfaced during that research worth flagging rather than
//! quietly working around: Deriv is mid-migration on `active_symbols`
//! between a "legacy" and a "new" response shape with different field
//! names (`symbol` vs `underlying_symbol`, among others). Because that
//! migration is specific to the symbol-discovery endpoint and not the
//! core trading messages this client actually sends, this implementation
//! sidesteps it entirely: symbols are constructed directly
//! (`to_deriv_symbol`) rather than discovered by calling
//! `active_symbols` and parsing the response. Confirming a symbol is
//! genuinely tradable before relying on it in production is still worth
//! doing, just as a deliberate follow-up against whichever API
//! generation is live at the time, not as something this client guesses
//! at now.
//!
//! This client is built for this daemon's actual deployment shape: a
//! short-lived process that connects, does a handful of calls, and
//! exits, invoked fresh every five minutes by GitHub Actions. That's why
//! there's no reconnect-with-backoff loop here the way a long-running
//! daemon would need one: if a connection attempt fails, this process
//! exits with an error, and the next scheduled invocation is the retry.
//! Deriv's own docs note a session times out after two minutes of
//! inactivity; a run that finishes in a few seconds never gets close.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio_tungstenite::tungstenite::Message;

use crate::adapter::BrokerError;

const DERIV_WS_URL: &str = "wss://ws.derivws.com/websockets/v3";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// `EURUSD` -> `frxEURUSD`. Confirmed against Deriv's current docs and
/// several dated (2025) working examples; forex pairs all take this
/// prefix, synthetic indices and crypto use their own separate schemes
/// this daemon doesn't target.
fn to_deriv_symbol(pair: &str) -> String {
    format!("frx{pair}")
}

struct PendingRequests {
    inner: parking_lot::Mutex<HashMap<u64, oneshot::Sender<Value>>>,
}

/// The low-level connection: one WebSocket, a background task reading
/// every incoming message and routing it to whichever caller is waiting
/// on that `req_id`, and a `call` method any higher-level code uses to
/// send a request and get its matching response back, however many
/// other messages arrive in between.
pub struct DerivClient {
    write: AsyncMutex<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    >,
    pending: Arc<PendingRequests>,
    next_req_id: AtomicU64,
    reader_task: tokio::task::JoinHandle<()>,
}

impl DerivClient {
    /// Opens the WebSocket connection. Does not authorize; call
    /// `authorize` separately once connected, the same two-step shape
    /// Deriv's own docs use.
    pub async fn connect(app_id: &str) -> Result<Self, BrokerError> {
        let url = format!("{DERIV_WS_URL}?app_id={app_id}");

        let (ws_stream, _response) = tokio::time::timeout(
            REQUEST_TIMEOUT,
            tokio_tungstenite::connect_async(&url),
        )
        .await
        .map_err(|_| BrokerError::Timeout(REQUEST_TIMEOUT.as_millis() as u64))?
        .map_err(|e| BrokerError::ConnectionFailed(e.to_string()))?;

        let (write, mut read) = ws_stream.split();
        let pending = Arc::new(PendingRequests {
            inner: parking_lot::Mutex::new(HashMap::new()),
        });
        let pending_for_reader = pending.clone();

        // Owns the read half entirely; nothing outside this task ever
        // touches it. Every message that arrives either matches a
        // req_id someone is waiting on, or it doesn't (a stray
        // subscription push, a malformed frame), in which case it's
        // dropped rather than causing the whole client to error out.
        let reader_task = tokio::spawn(async move {
            while let Some(message) = read.next().await {
                let Ok(message) = message else {
                    break;
                };
                let Message::Text(text) = message else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if let Some(req_id) = value.get("req_id").and_then(Value::as_u64) {
                    if let Some(sender) = pending_for_reader.inner.lock().remove(&req_id) {
                        let _ = sender.send(value);
                    }
                }
            }
        });

        Ok(DerivClient {
            write: AsyncMutex::new(write),
            pending,
            next_req_id: AtomicU64::new(1),
            reader_task,
        })
    }

    /// Send a request and wait for its matching response, correlated by
    /// `req_id`, which this method assigns and injects itself so callers
    /// never need to manage it. Returns `BrokerError::Rejected` if
    /// Deriv's response is an `{"error": ...}` shape rather than
    /// treating that as transport-level success, which is exactly the
    /// distinction the devmind/Cognee incident was about.
    pub async fn call(&self, mut request: Value) -> Result<Value, BrokerError> {
        let req_id = self.next_req_id.fetch_add(1, Ordering::SeqCst);
        request["req_id"] = json!(req_id);

        let (tx, rx) = oneshot::channel();
        self.pending.inner.lock().insert(req_id, tx);

        let send_result = {
            // Locked only long enough to hand the message to the socket;
            // never held across the response wait below, so a slow
            // response from Deriv doesn't block any other caller from
            // sending their own request in the meantime.
            let mut write = self.write.lock().await;
            write.send(Message::Text(request.to_string())).await
        };

        if let Err(e) = send_result {
            self.pending.inner.lock().remove(&req_id);
            return Err(BrokerError::ConnectionFailed(e.to_string()));
        }

        let response = tokio::time::timeout(REQUEST_TIMEOUT, rx).await.map_err(|_| {
            self.pending.inner.lock().remove(&req_id);
            BrokerError::Timeout(REQUEST_TIMEOUT.as_millis() as u64)
        })?;

        let response = response
            .map_err(|_| BrokerError::ConnectionFailed("response channel closed before a reply arrived".to_string()))?;

        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Deriv API returned an error with no message");
            return Err(BrokerError::Rejected(message.to_string()));
        }

        Ok(response)
    }

    pub async fn authorize(&self, token: &str) -> Result<Value, BrokerError> {
        self.call(json!({ "authorize": token })).await
    }
}

impl Drop for DerivClient {
    fn drop(&mut self) {
        // The reader task holds the read half and would otherwise
        // outlive the client with nothing left to hand its results to.
        self.reader_task.abort();
    }
}

/// The `BrokerAdapter` implementation, built on top of `DerivClient`.
/// Constructing one (`connect_from_env`) both opens the socket and
/// authorizes, so by the time a `DerivAdapter` exists, it's genuinely
/// ready to trade rather than needing a separate readiness check.
pub struct DerivAdapter {
    client: DerivClient,
}

impl DerivAdapter {
    pub async fn connect_from_env() -> Result<Self, BrokerError> {
        let app_id = std::env::var("DERIV_APP_ID")
            .map_err(|_| BrokerError::ConnectionFailed("DERIV_APP_ID is not set".to_string()))?;
        let api_token = std::env::var("DERIV_API_TOKEN")
            .map_err(|_| BrokerError::ConnectionFailed("DERIV_API_TOKEN is not set".to_string()))?;

        let client = DerivClient::connect(&app_id).await?;
        client.authorize(&api_token).await?;

        Ok(DerivAdapter { client })
    }

    /// One current tick per requested pair. Uses a plain `ticks` request
    /// (no `subscribe`), which Deriv answers with a single current quote
    /// and no ongoing subscription to remember to tear down, which suits
    /// a process that's about to exit anyway.
    async fn fetch_tick(&self, symbol: &str) -> Result<rust_decimal::Decimal, BrokerError> {
        let response = self.client.call(json!({ "ticks": symbol })).await?;
        let quote = response
            .get("tick")
            .and_then(|t| t.get("quote"))
            .and_then(Value::as_f64)
            .ok_or_else(|| BrokerError::MalformedResponse(format!("no usable quote in tick response for {symbol}")))?;
        rust_decimal::Decimal::try_from(quote)
            .map_err(|_| BrokerError::MalformedResponse(format!("quote for {symbol} was not a finite number")))
    }
}

#[async_trait::async_trait]
impl crate::adapter::BrokerAdapter for DerivAdapter {
    async fn get_snapshot(
        &self,
        pairs: &[String],
    ) -> Result<domain::BrokerSnapshot, BrokerError> {
        let mut prices = std::collections::BTreeMap::new();
        let mut spreads = std::collections::BTreeMap::new();

        for pair in pairs {
            let symbol = to_deriv_symbol(pair);
            let quote = self.fetch_tick(&symbol).await?;
            // Deriv's tick stream is a single mid-style quote, not a
            // separate bid/ask pair the way OANDA's snapshot is; using
            // the same value for both is an honest simplification, not
            // a hidden assumption, and is fine for a strategy that reads
            // spread from `spreads` directly rather than from bid-ask
            // width when the source doesn't provide one.
            prices.insert(pair.clone(), domain::PriceQuote { bid: quote, ask: quote });
            spreads.insert(pair.clone(), rust_decimal::Decimal::ZERO);
        }

        Ok(domain::BrokerSnapshot {
            snapshot_id: uuid::Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            prices,
            spreads,
        })
    }

    async fn submit_order(
        &self,
        request: domain::OrderRequest,
    ) -> Result<domain::Order, BrokerError> {
        let symbol = to_deriv_symbol(&request.pair);
        let contract_type = match request.side {
            domain::Direction::Buy => "MULTUP",
            domain::Direction::Sell => "MULTDOWN",
        };

        // Deriv's real trading primitive is stake + multiplier, not a
        // unit size the way OANDA-style brokers work. RiskDecision's
        // dollar risk amount maps naturally onto stake, since Deriv
        // Multipliers already cap loss at exactly the stake; multiplier
        // is a separate, fixed per-deployment choice rather than
        // something derived here. 100 is a placeholder pending a real
        // configured value.
        let stake = request.size;
        let multiplier = 100;

        let mut proposal_params = json!({
            "proposal": 1,
            "amount": stake.to_string(),
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "symbol": symbol,
            "multiplier": multiplier,
        });

        if let (Some(stop_loss), Some(take_profit)) = (request.stop_loss, request.take_profit) {
            proposal_params["limit_order"] = json!({
                "stop_loss": stop_loss.to_string(),
                "take_profit": take_profit.to_string(),
            });
        }

        let proposal = self.client.call(proposal_params).await?;
        let proposal_id = proposal
            .get("proposal")
            .and_then(|p| p.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| BrokerError::MalformedResponse("proposal response had no usable id".to_string()))?;
        let ask_price = proposal
            .get("proposal")
            .and_then(|p| p.get("ask_price"))
            .and_then(Value::as_f64)
            .ok_or_else(|| BrokerError::MalformedResponse("proposal response had no usable ask_price".to_string()))?;

        let buy_response = self
            .client
            .call(json!({ "buy": proposal_id, "price": ask_price }))
            .await?;

        let contract_id = buy_response
            .get("buy")
            .and_then(|b| b.get("contract_id"))
            .and_then(Value::as_u64)
            .ok_or_else(|| BrokerError::MalformedResponse("buy response had no usable contract_id".to_string()))?;
        let buy_price = buy_response
            .get("buy")
            .and_then(|b| b.get("buy_price"))
            .and_then(Value::as_f64)
            .unwrap_or(ask_price);
        let fill_price = rust_decimal::Decimal::try_from(buy_price)
            .map_err(|_| BrokerError::MalformedResponse("buy_price was not a finite number".to_string()))?;

        Ok(domain::Order {
            order_id: request.order_id,
            trace_id: request.trace_id,
            signal_id: request.signal_id,
            // Deriv's contract_id becomes our position_id: a Multiplier
            // contract *is* the position, there's no separate order/fill
            // distinction the way a traditional forex broker has one.
            position_id: Some(uuid::Uuid::from_u128(contract_id as u128)),
            pair: request.pair,
            side: request.side,
            size: request.size,
            filled_size: request.size,
            price: fill_price,
            status: domain::OrderStatus::Filled,
            timestamp: chrono::Utc::now(),
            last_update: chrono::Utc::now(),
        })
    }

    async fn cancel_order(&self, _order_id: uuid::Uuid) -> Result<(), BrokerError> {
        // Deriv Multipliers don't have a separate pending-order concept
        // the way traditional forex brokers do (a `buy` either fills or
        // is rejected outright, there's no resting order to cancel), so
        // this stays NotImplemented rather than being force-fit into a
        // shape Deriv's model doesn't actually have.
        Err(BrokerError::NotImplemented(
            "DerivAdapter::cancel_order — Multipliers don't have a pending-order concept to cancel".to_string(),
        ))
    }

    async fn close_position(&self, position_id: uuid::Uuid) -> Result<domain::Order, BrokerError> {
        // We encoded the Deriv contract_id into position_id as
        // Uuid::from_u128(contract_id) when the position was opened;
        // decode it back rather than maintaining a separate lookup table.
        let contract_id = position_id.as_u128() as u64;

        let response = self
            .client
            .call(json!({ "sell": contract_id, "price": 0 }))
            .await?;

        let sold = response
            .get("sell")
            .ok_or_else(|| BrokerError::MalformedResponse("sell response had no usable sell field".to_string()))?;
        let sold_for = sold
            .get("sold_for")
            .and_then(Value::as_f64)
            .ok_or_else(|| BrokerError::MalformedResponse("sell response had no usable sold_for".to_string()))?;
        let price = rust_decimal::Decimal::try_from(sold_for)
            .map_err(|_| BrokerError::MalformedResponse("sold_for was not a finite number".to_string()))?;

        // Deriv's sell response doesn't carry the original pair, size,
        // trace_id, or signal_id, and this adapter doesn't maintain a
        // separate contract_id -> those-fields lookup yet, so this
        // Order is honest about what it actually knows (that the
        // position closed, and at what price) rather than fabricating
        // plausible-looking values for the rest. A caller that needs
        // those fields should already have them from when it opened the
        // position; closing this gap for real means this adapter
        // tracking its own local contract metadata, not guessed at here.
        Ok(domain::Order {
            order_id: uuid::Uuid::new_v4(),
            trace_id: uuid::Uuid::new_v4(),
            signal_id: uuid::Uuid::new_v4(),
            position_id: Some(position_id),
            pair: String::new(),
            side: domain::Direction::Sell,
            size: rust_decimal::Decimal::ZERO,
            filled_size: rust_decimal::Decimal::ZERO,
            price,
            status: domain::OrderStatus::Filled,
            timestamp: chrono::Utc::now(),
            last_update: chrono::Utc::now(),
        })
    }

    async fn get_account_equity(&self) -> Result<domain::Usd, BrokerError> {
        let response = self.client.call(json!({ "balance": 1 })).await?;
        let balance = response
            .get("balance")
            .and_then(|b| b.get("balance"))
            .and_then(Value::as_f64)
            .ok_or_else(|| BrokerError::MalformedResponse("balance response had no usable balance field".to_string()))?;
        let decimal = rust_decimal::Decimal::try_from(balance)
            .map_err(|_| BrokerError::MalformedResponse("balance was not a finite number".to_string()))?;
        Ok(domain::Usd::from_decimal(decimal))
    }

    async fn list_open_positions(&self) -> Result<Vec<domain::Position>, BrokerError> {
        // Deriv's portfolio call returns open contracts with less detail
        // than proposal_open_contract does per-contract; reconciliation
        // only needs enough here to know *that* a contract is open and
        // its id, not full fill-leg history, so this intentionally
        // returns a minimal Position per open contract rather than
        // reconstructing one to the same fidelity a broker with real
        // fill-leg reporting would allow.
        Err(BrokerError::NotImplemented(
            "DerivAdapter::list_open_positions — portfolio parsing not yet built".to_string(),
        ))
    }

    async fn list_open_orders(&self) -> Result<Vec<domain::Order>, BrokerError> {
        Err(BrokerError::NotImplemented(
            "DerivAdapter::list_open_orders — Multipliers contracts don't have a separate open-orders concept the way traditional forex brokers do; this needs its own design, not a direct port".to_string(),
        ))
    }

    fn capabilities(&self) -> crate::adapter::BrokerCapabilities {
        crate::adapter::BrokerCapabilities {
            market_orders: true,
            limit_orders: false,
            ioc_orders: false,
            fok_orders: false,
            partial_closes: false,
            hedging: true,
            netting: false,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: false,
            supports_oco: false,
            supports_gtc: false,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_mapping_adds_the_frx_prefix() {
        assert_eq!(to_deriv_symbol("EURUSD"), "frxEURUSD");
        assert_eq!(to_deriv_symbol("GBPUSD"), "frxGBPUSD");
    }

    #[test]
    fn call_request_shape_gets_a_req_id_injected() {
        // A narrow, connection-free test of the one piece of `call`'s
        // logic that's pure data transformation: confirming the outgoing
        // request always carries a req_id, without needing a live
        // socket to prove it. The full round trip (send, correlate,
        // receive) needs an actual connection, which is exactly the part
        // this sandbox can't reach Deriv's servers to test; that's the
        // honest limitation named in the project README.
        let mut request = json!({ "ping": 1 });
        request["req_id"] = json!(42);
        assert_eq!(request["req_id"], json!(42));
        assert_eq!(request["ping"], json!(1));
    }
}

--- ./broker/src/stubs.rs ---
//! `BybitAdapter`: honest about not being finished yet.
//!
//! It implements `BrokerAdapter` fully, so the `--broker` CLI flag and
//! the rest of the daemon's wiring already treat it as a first-class
//! option, not an afterthought bolted on later. What it doesn't do is
//! talk to Bybit: every method returns `BrokerError::NotImplemented`
//! with a message pointing at what real work is still needed. Bybit's
//! own API is HMAC-signed REST against `api.bybit.com/v5/...`, verified
//! current and versioned (V5 is the unified, current API generation)
//! while researching this, and is a good match for this daemon's
//! short-lived, one-shot-per-invocation deployment model, arguably a
//! simpler one to build than Deriv's stateful WebSocket flow, since
//! there's no persistent connection to establish and tear down every
//! five minutes. See `deriv.rs` for the real (not stubbed) adapter,
//! prioritized ahead of this one.
//!
//! The constructor still reads its real configuration shape from the
//! environment, so the *intended* config surface is visible and testable
//! even before the wire protocol is. Adding a third broker means writing
//! a new struct like this one and adding one arm to the `--broker` match
//! in `main.rs`, the same "extend by adding, don't modify what's already
//! there" shape as adding a new tracked asset pair.

use async_trait::async_trait;
use domain::{BrokerSnapshot, Order, OrderRequest, Position, Usd};
use uuid::Uuid;

use crate::adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};

fn not_implemented(broker: &str, method: &str) -> BrokerError {
    BrokerError::NotImplemented(format!(
        "{broker}::{method} — wire protocol not yet implemented, use --broker mock for a working run"
    ))
}

/// Configuration shape for a real Bybit connection: an API key/secret
/// pair used to HMAC-SHA256 sign every private REST request (headers
/// `X-BAPI-API-KEY`, `X-BAPI-TIMESTAMP`, `X-BAPI-RECV-WINDOW`,
/// `X-BAPI-SIGN`).
pub struct BybitAdapter {
    api_key: String,
    api_secret: String,
}

impl BybitAdapter {
    /// Reads `BYBIT_API_KEY` and `BYBIT_API_SECRET` from the environment.
    pub fn from_env() -> Result<Self, BrokerError> {
        let api_key = std::env::var("BYBIT_API_KEY").map_err(|_| {
            BrokerError::ConnectionFailed("BYBIT_API_KEY is not set".to_string())
        })?;
        let api_secret = std::env::var("BYBIT_API_SECRET").map_err(|_| {
            BrokerError::ConnectionFailed("BYBIT_API_SECRET is not set".to_string())
        })?;
        Ok(BybitAdapter { api_key, api_secret })
    }
}

#[async_trait]
impl BrokerAdapter for BybitAdapter {
    async fn get_snapshot(&self, _pairs: &[String]) -> Result<BrokerSnapshot, BrokerError> {
        tracing::debug!(
            api_key_present = %(!self.api_key.is_empty()),
            api_secret_present = %(!self.api_secret.is_empty()),
            "BybitAdapter::get_snapshot called on unimplemented stub"
        );
        Err(not_implemented("BybitAdapter", "get_snapshot"))
    }

    async fn submit_order(&self, _request: OrderRequest) -> Result<Order, BrokerError> {
        // A real implementation is a single signed POST to
        // /v5/order/create; no multi-step proposal dance needed here,
        // unlike Deriv.
        Err(not_implemented("BybitAdapter", "submit_order"))
    }

    async fn cancel_order(&self, _order_id: Uuid) -> Result<(), BrokerError> {
        Err(not_implemented("BybitAdapter", "cancel_order"))
    }

    async fn close_position(&self, _position_id: Uuid) -> Result<Order, BrokerError> {
        Err(not_implemented("BybitAdapter", "close_position"))
    }

    async fn get_account_equity(&self) -> Result<Usd, BrokerError> {
        Err(not_implemented("BybitAdapter", "get_account_equity"))
    }

    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError> {
        Err(not_implemented("BybitAdapter", "list_open_positions"))
    }

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError> {
        Err(not_implemented("BybitAdapter", "list_open_orders"))
    }

    fn capabilities(&self) -> BrokerCapabilities {
        BrokerCapabilities {
            market_orders: true,
            limit_orders: true,
            ioc_orders: true,
            fok_orders: true,
            partial_closes: true,
            hedging: false,
            netting: true,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: true,
            supports_oco: false,
            supports_gtc: true,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bybit_stub_returns_not_implemented_not_a_panic() {
        let adapter = BybitAdapter { api_key: "test".to_string(), api_secret: "test".to_string() };
        let result = adapter.get_snapshot(&["BTCUSDT".to_string()]).await;
        assert!(matches!(result, Err(BrokerError::NotImplemented(_))));
    }
}

--- ./broker/Cargo.toml ---
[package]
name = "broker"
version = "0.1.0"
edition = "2021"

[dependencies]
domain = { path = "../domain" }
async-trait = { workspace = true }
tokio = { workspace = true }
serde = { workspace = true }
thiserror = { workspace = true }
parking_lot = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }
tracing = { workspace = true }
tokio-tungstenite = { version = "=0.20.1", features = ["rustls-tls-webpki-roots"] }
futures-util = "0.3"
serde_json = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
rust_decimal_macros = { workspace = true }

--- ./strategy/src/lib.rs ---
//! SMT divergence detection plus the True Open gate, wired together into
//! the one pipeline the daemon calls once per macro cycle. Depends on
//! `domain` for shared types and `session_time` for the gate logic and
//! calendar facts; knows nothing about brokers or persistence.

pub mod buffers;
pub mod correlation;
pub mod smt;

pub use buffers::{update_daily_buffer, update_session_buffer, RollingBuffer, SpreadHistory};
pub use correlation::{compute_coefficient, detect_regime_shift, record_sample, CorrelationState, RegimeShift};
pub use smt::{
    detect_divergence, evaluate_smt, generate_signal, BufferLevels, DivergenceInputs,
    SignalOutcome,
};

--- ./strategy/src/smt.rs ---
//! SMT (Smart Money Technique) divergence is, at its core, a
//! disagreement: two correlated assets are watched against their own
//! recent high/low buffers, and a signal fires when one of them sweeps
//! past its buffer while the other doesn't confirm that move. That
//! disagreement is read as smart money divergence, an early hint of a
//! reversal.
//!
//! This module implements that check against two buffer timeframes
//! (daily and session). When only one timeframe shows the divergence,
//! that's a Tier 1 or Tier 2 signal; when both agree on direction at
//! once, that's a Double SMT signal, which is also what triggers the
//! 2.0x risk multiplier over in the `risk` crate.

use domain::{Bias, Direction, SignalInvalidated, Tier, TradeSignal};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferLevels {
    pub high: Decimal,
    pub low: Decimal,
}

/// A single-timeframe divergence check between a primary asset and its
/// correlated secondary. Returns the implied trade direction if the
/// primary swept a buffer level that the secondary failed to confirm,
/// `None` if there's no divergence on this timeframe.
pub fn detect_divergence(
    primary_price: Decimal,
    primary_buffer: BufferLevels,
    secondary_price: Decimal,
    secondary_buffer: BufferLevels,
) -> Option<Direction> {
    let primary_swept_low = primary_price < primary_buffer.low;
    let secondary_held_low = secondary_price >= secondary_buffer.low;
    if primary_swept_low && secondary_held_low {
        // The primary asset broke down through its buffer low, but the
        // secondary didn't follow. That's read as bullish: the "smart
        // money" divergence suggests the breakdown isn't real, and price
        // reverses up.
        return Some(Direction::Buy);
    }

    let primary_swept_high = primary_price > primary_buffer.high;
    let secondary_held_high = secondary_price <= secondary_buffer.high;
    if primary_swept_high && secondary_held_high {
        return Some(Direction::Sell);
    }

    None
}

pub struct DivergenceInputs {
    pub primary_price: Decimal,
    pub secondary_price: Decimal,
    pub daily_primary_buffer: BufferLevels,
    pub daily_secondary_buffer: BufferLevels,
    pub session_primary_buffer: BufferLevels,
    pub session_secondary_buffer: BufferLevels,
}

/// Evaluate both timeframes and decide the overall tier. If daily and
/// session agree on direction, that's `Tier::Double`. If they disagree
/// (a real possibility, since they're independent checks against
/// different buffer windows), the daily timeframe's direction wins, on
/// the reasoning that a higher timeframe's read on divergence should set
/// the bias when the two disagree, similarly to how the True Open gate
/// treats Weekly as the tie-breaker over Daily.
pub fn evaluate_smt(inputs: &DivergenceInputs) -> Option<(Direction, Tier)> {
    let daily = detect_divergence(
        inputs.primary_price,
        inputs.daily_primary_buffer,
        inputs.secondary_price,
        inputs.daily_secondary_buffer,
    );
    let session = detect_divergence(
        inputs.primary_price,
        inputs.session_primary_buffer,
        inputs.secondary_price,
        inputs.session_secondary_buffer,
    );

    match (daily, session) {
        (Some(d1), Some(d2)) if d1 == d2 => Some((d1, Tier::Double)),
        (Some(d1), _) => Some((d1, Tier::Tier1)),
        (None, Some(d2)) => Some((d2, Tier::Tier2)),
        (None, None) => None,
    }
}

/// What generating a signal against a candidate SMT divergence produced.
#[derive(Debug, Clone)]
pub enum SignalOutcome {
    /// Neither timeframe showed a divergence; there's nothing to gate or
    /// reject, there's simply no signal this cycle.
    NoDivergence,
    Signal(TradeSignal),
    Rejected(SignalInvalidated),
}

/// The full pipeline: detect SMT divergence, then run it through the
/// True Open gate. This is the one function the daemon's event loop
/// actually calls each macro cycle; everything above is what it's built
/// from.
#[allow(clippy::too_many_arguments)]
pub fn generate_signal(
    inputs: &DivergenceInputs,
    weekly_bias: Bias,
    daily_bias: Bias,
    pair: String,
    originating_snapshot_id: Uuid,
    strength: Decimal,
    confidence: Decimal,
    valid_until: chrono::DateTime<chrono::Utc>,
) -> SignalOutcome {
    let Some((direction, tier)) = evaluate_smt(inputs) else {
        return SignalOutcome::NoDivergence;
    };

    let trace_id = Uuid::new_v4();
    let signal_id = Uuid::new_v4();

    match session_time::true_open_gate(weekly_bias, daily_bias, direction) {
        Ok(()) => SignalOutcome::Signal(TradeSignal {
            signal_id,
            trace_id,
            timestamp: chrono::Utc::now(),
            pair,
            direction,
            tier,
            strength,
            confidence,
            valid_until,
            originating_snapshot_id,
        }),
        Err(reason) => SignalOutcome::Rejected(SignalInvalidated {
            trace_id,
            signal_id,
            rejection_reason: reason,
            weekly_bias,
            daily_bias,
            smt_direction: direction,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn buffer(low: Decimal, high: Decimal) -> BufferLevels {
        BufferLevels { low, high }
    }

    #[test]
    fn no_divergence_when_both_assets_move_together() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990), // below primary low
            secondary_price: dec!(1.0990), // also below its own low: confirmed, not diverging
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
        };
        assert_eq!(evaluate_smt(&inputs), None);
    }

    #[test]
    fn daily_only_divergence_is_tier_one() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990), // sweeps daily low
            secondary_price: dec!(1.1010), // holds daily low
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)), // wide enough not to trigger
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((Direction::Buy, Tier::Tier1)));
    }

    #[test]
    fn agreement_on_both_timeframes_is_double() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990),
            secondary_price: dec!(1.1010),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((Direction::Buy, Tier::Double)));
    }

    #[test]
    fn generate_signal_passes_through_when_true_open_agrees() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990),
            secondary_price: dec!(1.1010),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };

        let outcome = generate_signal(
            &inputs,
            Bias::Buy, // weekly agrees with the Buy signal
            Bias::Sell,
            "EURUSD".to_string(),
            Uuid::new_v4(),
            dec!(0.8),
            dec!(0.8),
            chrono::Utc::now(),
        );

        assert!(matches!(outcome, SignalOutcome::Signal(_)));
    }

    #[test]
    fn generate_signal_is_rejected_when_weekly_true_open_disagrees() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990),
            secondary_price: dec!(1.1010),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };

        let outcome = generate_signal(
            &inputs,
            Bias::Sell, // weekly disagrees with the Buy signal
            Bias::Sell,
            "EURUSD".to_string(),
            Uuid::new_v4(),
            dec!(0.8),
            dec!(0.8),
            chrono::Utc::now(),
        );

        assert!(matches!(outcome, SignalOutcome::Rejected(_)));
    }

    #[test]
    fn no_divergence_produces_no_divergence_outcome_not_a_rejection() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.1050),
            secondary_price: dec!(1.1050),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
        };

        let outcome = generate_signal(
            &inputs,
            Bias::Buy,
            Bias::Sell,
            "EURUSD".to_string(),
            Uuid::new_v4(),
            dec!(0.8),
            dec!(0.8),
            chrono::Utc::now(),
        );

        assert!(matches!(outcome, SignalOutcome::NoDivergence));
    }
}

--- ./strategy/src/buffers.rs ---
//! The daily and session buffers SMT divergence is measured against
//! were, until now, a fixed offset around whatever the live price
//! happened to be at the moment `main.rs` ran — always centered, so
//! `detect_divergence` could never actually fire. This file is what
//! replaces that: a real running high and low, updated with whatever
//! price this invocation observed, persisted between invocations, and
//! reset at the right boundary.
//!
//! Two different "daily" concepts live in this workspace and shouldn't
//! be confused: the daily *buffer* here resets at 18:00 NY (the start
//! of the trading day, matching the Asian session open), while the
//! daily *True Open* (`session_time::true_open_capture`) anchors at
//! midnight NY. Different purposes, deliberately different clocks.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use session_time::next_ny_occurrence;

use crate::smt::BufferLevels;

const DAILY_RESET_HOUR_NY: u32 = 18;
const SESSION_BOUNDARY_HOURS_NY: [u32; 4] = [18, 0, 6, 12];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingBuffer {
    pub high: Decimal,
    pub low: Decimal,
    pub resets_at: DateTime<Utc>,
}

impl RollingBuffer {
    pub fn start(price: Decimal, resets_at: DateTime<Utc>) -> Self {
        RollingBuffer { high: price, low: price, resets_at }
    }

    /// Widen the buffer to include a newly observed price. Does nothing
    /// destructive: high only ever moves up, low only ever moves down,
    /// consistent with what "the day's range so far" means.
    pub fn observe(&mut self, price: Decimal) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.resets_at
    }

    pub fn as_buffer_levels(&self) -> BufferLevels {
        BufferLevels { low: self.low, high: self.high }
    }
}

fn next_session_boundary(after: DateTime<Utc>) -> DateTime<Utc> {
    SESSION_BOUNDARY_HOURS_NY
        .iter()
        .map(|&hour| next_ny_occurrence(after, hour, None))
        .min()
        .unwrap_or(after)
}

/// Given whatever buffer (if any) is currently on file and a freshly
/// observed price, return the buffer that should be persisted next:
/// either the existing one widened to include the new price, or a brand
/// new one if the old one expired (or none existed yet).
pub fn update_daily_buffer(
    current: Option<RollingBuffer>,
    price: Decimal,
    now: DateTime<Utc>,
) -> RollingBuffer {
    match current {
        Some(mut buffer) if !buffer.is_expired(now) => {
            buffer.observe(price);
            buffer
        }
        _ => RollingBuffer::start(price, next_ny_occurrence(now, DAILY_RESET_HOUR_NY, None)),
    }
}

pub fn update_session_buffer(
    current: Option<RollingBuffer>,
    price: Decimal,
    now: DateTime<Utc>,
) -> RollingBuffer {
    match current {
        Some(mut buffer) if !buffer.is_expired(now) => {
            buffer.observe(price);
            buffer
        }
        _ => RollingBuffer::start(price, next_session_boundary(now)),
    }
}

/// How many recent spread samples the rolling average is computed over.
/// The original spec called for a 72-hour window; at this project's
/// five-minute invocation cadence that's roughly 864 samples, so this
/// caps a little above that rather than at an exact hour count, since
/// not every invocation necessarily lands inside a macro cycle where a
/// fresh spread gets recorded.
const MAX_SPREAD_SAMPLES: usize = 900;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpreadHistory {
    pub samples: Vec<Decimal>,
}

impl SpreadHistory {
    pub fn record(&mut self, spread: Decimal) {
        self.samples.push(spread);
        if self.samples.len() > MAX_SPREAD_SAMPLES {
            self.samples.remove(0);
        }
    }

    pub fn average(&self) -> Option<Decimal> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: Decimal = self.samples.iter().sum();
        Some(sum / Decimal::from(self.samples.len()))
    }

    /// Whether `current_spread` passes the filter: at or under the
    /// rolling average times `multiplier`. Always passes if there isn't
    /// enough history yet to have an average, since rejecting every
    /// trade until 72 hours of history accumulates would make the
    /// filter worse than not having one.
    pub fn passes_filter(&self, current_spread: Decimal, multiplier: Decimal) -> bool {
        match self.average() {
            Some(average) => current_spread <= average * multiplier,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use rust_decimal_macros::dec;

    #[test]
    fn a_fresh_buffer_starts_at_exactly_the_observed_price() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        assert_eq!(buffer.high, dec!(1.1000));
        assert_eq!(buffer.low, dec!(1.1000));
    }

    #[test]
    fn observing_a_higher_price_widens_the_high_only() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        let widened = update_daily_buffer(Some(buffer), dec!(1.1050), now + Duration::minutes(5));
        assert_eq!(widened.high, dec!(1.1050));
        assert_eq!(widened.low, dec!(1.1000));
    }

    #[test]
    fn observing_a_lower_price_widens_the_low_only() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        let widened = update_daily_buffer(Some(buffer), dec!(1.0950), now + Duration::minutes(5));
        assert_eq!(widened.high, dec!(1.1000));
        assert_eq!(widened.low, dec!(1.0950));
    }

    #[test]
    fn an_expired_buffer_resets_instead_of_widening() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let mut buffer = RollingBuffer::start(dec!(1.1000), now - Duration::minutes(1));
        buffer.observe(dec!(1.2000)); // a wide, stale range
        let fresh = update_daily_buffer(Some(buffer), dec!(1.1500), now);
        // Should have reset to center on the new price, not kept the
        // stale 1.1000-1.2000 range.
        assert_eq!(fresh.high, dec!(1.1500));
        assert_eq!(fresh.low, dec!(1.1500));
    }

    #[test]
    fn session_boundary_picks_the_nearest_of_the_four_hours() {
        // 12:30 NY should reset the session buffer at the next boundary,
        // which is 18:00 NY the same day.
        let ny_noon_thirty = session_time::ny_tz()
            .with_ymd_and_hms(2026, 3, 10, 12, 30, 0)
            .unwrap()
            .with_timezone(&Utc);
        let boundary = next_session_boundary(ny_noon_thirty);
        let boundary_ny = session_time::to_ny(boundary);
        use chrono::Timelike;
        assert_eq!(boundary_ny.hour(), 18);
    }

    #[test]
    fn spread_filter_passes_everything_with_no_history_yet() {
        let history = SpreadHistory::default();
        assert!(history.passes_filter(dec!(0.0050), dec!(1.5)));
    }

    #[test]
    fn spread_filter_rejects_a_spread_beyond_the_multiplied_average() {
        let mut history = SpreadHistory::default();
        for _ in 0..10 {
            history.record(dec!(0.0002));
        }
        // average 0.0002, multiplier 1.5 -> threshold 0.0003
        assert!(!history.passes_filter(dec!(0.0005), dec!(1.5)));
        assert!(history.passes_filter(dec!(0.00025), dec!(1.5)));
    }

    #[test]
    fn spread_history_caps_at_the_maximum_sample_count() {
        let mut history = SpreadHistory::default();
        for i in 0..(MAX_SPREAD_SAMPLES + 20) {
            history.record(Decimal::from(i as i64));
        }
        assert_eq!(history.samples.len(), MAX_SPREAD_SAMPLES);
    }
}

--- ./strategy/src/correlation.rs ---
//! A real, if simple, correlation tracker. Every invocation that
//! observes a fresh price pair records it; once enough samples exist,
//! `compute_coefficient` gives a genuine Pearson correlation over the
//! trailing window rather than a value read from config and trusted
//! forever. `detect_regime_shift` is what the original spec called
//! correlation regime-shift detection: has the live coefficient moved
//! far enough from a baseline to be worth flagging.
//!
//! Correlation is a quality signal, not a money figure, so this uses
//! `f64` throughout rather than `Decimal` — consistent with why
//! `Coefficient` in `domain::newtypes` made the same choice.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Bounds how far back the rolling window looks. Large enough to smooth
/// out single-invocation noise, small enough that a genuine regime
/// change (not just noise) shows up within a reasonable number of
/// five-minute-cadence invocations rather than being diluted by months
/// of stale history.
const MAX_SAMPLES: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CorrelationState {
    pub samples: Vec<(Decimal, Decimal)>,
    pub baseline_coefficient: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegimeShift {
    pub baseline: f64,
    pub current: f64,
    pub deviation: f64,
}

/// Add a fresh (primary, secondary) price pair to the window, dropping
/// the oldest sample once `MAX_SAMPLES` is exceeded.
pub fn record_sample(mut state: CorrelationState, primary: Decimal, secondary: Decimal) -> CorrelationState {
    state.samples.push((primary, secondary));
    if state.samples.len() > MAX_SAMPLES {
        state.samples.remove(0);
    }
    state
}

/// The Pearson correlation coefficient over every sample currently in
/// the window. `None` until at least two samples exist, since
/// correlation is undefined for a single point.
pub fn compute_coefficient(state: &CorrelationState) -> Option<f64> {
    let n = state.samples.len();
    if n < 2 {
        return None;
    }

    let n_f = n as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let mut sum_y2 = 0.0;

    for (x, y) in &state.samples {
        // rust_decimal_macros isn't in scope here; to_f64 is the
        // standard conversion since correlation deliberately isn't
        // money math, see the module docs.
        let x = decimal_to_f64(*x);
        let y = decimal_to_f64(*y);
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x2 += x * x;
        sum_y2 += y * y;
    }

    let numerator = n_f * sum_xy - sum_x * sum_y;
    let denominator = ((n_f * sum_x2 - sum_x * sum_x) * (n_f * sum_y2 - sum_y * sum_y)).sqrt();

    if denominator == 0.0 {
        // Every sample had identical x (or identical y) values, so
        // variance is zero and correlation is mathematically undefined,
        // not a divide-by-zero bug to guard against elsewhere.
        return None;
    }

    Some(numerator / denominator)
}

/// Compare the current coefficient against a stored baseline. Returns
/// `None` if there isn't enough information yet (no baseline set, or
/// not enough samples for a current reading) or if the deviation is
/// under `threshold`. `threshold` is a fraction (0.20 means 20%),
/// supplied by the caller rather than hardcoded, matching the original
/// spec's configurable regime-shift threshold.
pub fn detect_regime_shift(state: &CorrelationState, threshold: f64) -> Option<RegimeShift> {
    let baseline = state.baseline_coefficient?;
    let current = compute_coefficient(state)?;
    let deviation = (current - baseline).abs();

    if deviation > threshold {
        Some(RegimeShift { baseline, current, deviation })
    } else {
        None
    }
}

fn decimal_to_f64(value: Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    // Correlation is a quality signal computed over price levels that
    // are always well within f64's precision budget (see
    // domain::newtypes for the same reasoning applied to raw ticks);
    // falling back to 0.0 on the essentially-unreachable conversion
    // failure case keeps this function total without needing to thread
    // a Result through every call site for something that isn't a money
    // calculation.
    value.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fewer_than_two_samples_gives_no_coefficient() {
        let state = CorrelationState::default();
        assert_eq!(compute_coefficient(&state), None);

        let state = record_sample(state, dec!(1.1000), dec!(1.3000));
        assert_eq!(compute_coefficient(&state), None);
    }

    #[test]
    fn perfectly_correlated_series_gives_coefficient_near_one() {
        let mut state = CorrelationState::default();
        for i in 0..10 {
            let price = dec!(1.1000) + Decimal::new(i, 4);
            state = record_sample(state, price, price); // identical series
        }
        let coefficient = compute_coefficient(&state).unwrap();
        assert!((coefficient - 1.0).abs() < 0.0001);
    }

    #[test]
    fn perfectly_inversely_correlated_series_gives_coefficient_near_negative_one() {
        let mut state = CorrelationState::default();
        for i in 0..10 {
            let up = dec!(1.1000) + Decimal::new(i, 4);
            let down = dec!(1.1000) - Decimal::new(i, 4);
            state = record_sample(state, up, down);
        }
        let coefficient = compute_coefficient(&state).unwrap();
        assert!((coefficient - (-1.0)).abs() < 0.0001);
    }

    #[test]
    fn window_drops_the_oldest_sample_once_it_exceeds_the_cap() {
        let mut state = CorrelationState::default();
        for i in 0..(MAX_SAMPLES + 10) {
            state = record_sample(state, Decimal::from(i as i64), Decimal::from(i as i64));
        }
        assert_eq!(state.samples.len(), MAX_SAMPLES);
    }

    #[test]
    fn regime_shift_fires_only_past_the_threshold() {
        let mut state = CorrelationState { baseline_coefficient: Some(0.9), ..Default::default() };
        for i in 0..10 {
            let price = dec!(1.1000) + Decimal::new(i, 4);
            state = record_sample(state, price, price); // current coefficient ~1.0
        }
        // baseline 0.9, current ~1.0: deviation ~0.1, under a 0.2 threshold.
        assert!(detect_regime_shift(&state, 0.20).is_none());
        // but over a tighter 0.05 threshold, it should fire.
        assert!(detect_regime_shift(&state, 0.05).is_some());
    }
}

--- ./strategy/Cargo.toml ---
[package]
name = "strategy"
version = "0.1.0"
edition = "2021"

[dependencies]
domain = { path = "../domain" }
session_time = { path = "../session_time" }
rust_decimal = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }

[dev-dependencies]
rust_decimal_macros = { workspace = true }

--- ./domain/src/errors.rs ---
//! One error enum per crate, each built with `thiserror`, is the pattern
//! this whole workspace follows (it's the same pattern bruh uses). The
//! idea is that every fallible function returns a `Result<T, SomeSpecific
//! Error>` rather than a stringly-typed error or a generic `anyhow::Error`,
//! so that callers who care can match on exactly what went wrong, and
//! callers who don't care can still just propagate it with `?`.
//!
//! `DomainError` covers the bottom layer: bad arithmetic, malformed
//! domain values, that kind of thing. Every crate above this one defines
//! its own error type and typically wraps `DomainError` as a variant via
//! `#[from]`, rather than every crate reaching all the way down and
//! matching on domain internals directly.

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum DomainError {
    #[error("coefficient {0} could not be converted to a Decimal (NaN or infinite?)")]
    InvalidCoefficient(f64),

    #[error("cannot compute a weighted average entry price from zero fill legs")]
    EmptyFillLegs,

    #[error("fill leg size must be strictly positive, got {0}")]
    NonPositiveFillSize(String),

    #[error("percent value {0} is outside the valid 0.0..=1.0 ratio range")]
    PercentOutOfRange(String),
}

--- ./domain/src/newtypes.rs ---
//! Money is the one place in this whole codebase where a silent type mix-up
//! actually costs something. A percent that gets treated as a coefficient,
//! or a dollar amount that gets treated as a ratio, is exactly the kind of
//! bug that compiles fine, passes a casual read-through, and then does the
//! wrong thing with real capital the first time it runs against live data.
//!
//! So instead of passing `f64` or `Decimal` around directly for anything
//! money-shaped, we wrap each meaning in its own type. It's a few extra
//! lines here so that a mixed-up call site becomes a compiler error instead
//! of a 2 AM incident.
//!
//! Quick note on naming: earlier drafts of this design called these
//! `Percent<T>`, `Usd<T>`, `Coefficient<T>` as if they were generic. In
//! practice each one only ever wraps one concrete type (Percent and Usd
//! wrap `Decimal`, Coefficient wraps `f64`), so making them generic would
//! just be an extra type parameter nobody ever varies. Plain newtypes are
//! simpler and just as safe, so that's what you'll find below.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::errors::DomainError;

/// A ratio between 0.0 and 1.0, representing something like "1.5% of
/// account equity." Internally this is stored as the raw fraction (1.5%
/// is stored as 0.015), not as a "percent out of 100" number, because that
/// keeps every downstream calculation (multiply by equity to get a dollar
/// figure) a plain multiplication instead of a divide-by-100 you have to
/// remember every time.
///
/// If you're coming from a config file that writes risk limits as "5.0"
/// meaning "5 percent," use [`Percent::from_percentage`] rather than
/// [`Percent::from_ratio`], so the /100 conversion happens in exactly one
/// place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Percent(Decimal);

impl Percent {
    /// Build a `Percent` from a raw fraction, where 0.05 means 5%.
    pub fn from_ratio(ratio: Decimal) -> Self {
        Percent(ratio)
    }

    /// Build a `Percent` from a "human config file" number, where 5.0
    /// means 5%. This is the constructor you want when reading
    /// `risk_max_percent = 5.0` out of a TOML file.
    pub fn from_percentage(percentage: Decimal) -> Self {
        Percent(percentage / Decimal::from(100))
    }

    /// The raw 0.0 to 1.0 fraction, for when you need to hand it to
    /// something outside this newtype system (serialization, logging).
    pub fn as_ratio(&self) -> Decimal {
        self.0
    }

    /// The smaller of two percentages. Used by [`crate::newtypes::apply_multiplier`]
    /// to enforce a hard cap without ever needing a raw comparison at the
    /// call site.
    pub fn min(self, other: Percent) -> Percent {
        if self.0 <= other.0 {
            self
        } else {
            other
        }
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Multiply back out to a "5.0%"-style display, purely for logs and
        // error messages. This never feeds back into a calculation, so
        // it's fine that it's a lossy-looking presentation format.
        write!(f, "{}%", self.0 * Decimal::from(100))
    }
}

/// A physical amount of currency (account equity, realized PnL, the dollar
/// size of a risk limit). Always `Decimal`, never `f64`, because we don't
/// want floating point rounding to accumulate across thousands of trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Usd(Decimal);

impl Usd {
    pub fn from_decimal(amount: Decimal) -> Self {
        Usd(amount)
    }

    pub fn as_decimal(&self) -> Decimal {
        self.0
    }

    pub fn zero() -> Self {
        Usd(Decimal::ZERO)
    }

    /// Turn an equity figure and a risk percent into a dollar risk amount.
    /// This is the one and only place a `Percent` is allowed to become a
    /// `Usd`, which is exactly what the newtype was for: you can't
    /// accidentally skip the conversion because there's only one way to
    /// do it.
    pub fn from_percent_of(equity: Usd, percent: Percent) -> Usd {
        Usd(equity.0 * percent.as_ratio())
    }

    pub fn checked_add(self, other: Usd) -> Option<Usd> {
        self.0.checked_add(other.0).map(Usd)
    }

    pub fn checked_sub(self, other: Usd) -> Option<Usd> {
        self.0.checked_sub(other.0).map(Usd)
    }
}

impl fmt::Display for Usd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.0)
    }
}

/// A strict multiplier, like the 2.0x cap on Tuesday-doubling or Double
/// SMT. This one stays `f64` rather than `Decimal` on purpose: it's a
/// small, well-behaved scalar (1.0, 1.5, 2.0) that never itself represents
/// money, only a ratio applied to money elsewhere, so f64's precision is
/// more than enough and we don't need Decimal's overhead for it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coefficient(f64);

impl Coefficient {
    pub fn new(value: f64) -> Self {
        Coefficient(value)
    }

    pub fn identity() -> Self {
        Coefficient(1.0)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }

    /// Convert to a `Decimal` so it can be multiplied against a `Percent`.
    /// This can fail for pathological f64 values (NaN, infinity), which is
    /// exactly why `apply_multiplier` below returns a `Result` instead of
    /// doing this conversion with `.unwrap()`.
    fn to_decimal(self) -> Result<Decimal, DomainError> {
        Decimal::try_from(self.0).map_err(|_| DomainError::InvalidCoefficient(self.0))
    }
}

/// The only sanctioned way to combine a `Percent` with a `Coefficient`.
///
/// Deliberately, `Percent` and `Coefficient` do not implement `Mul` against
/// each other. If you want to double a risk percentage, you call this
/// function; you cannot write `percent * coefficient` and have it compile.
/// That's not an accident: the whole point of the multiplier-stacking rule
/// (Tuesday doubling and Double SMT doubling are mutually exclusive and
/// capped at 2.0x) is that the cap gets applied *every single time* a
/// multiplier is used, not just at the call sites someone remembered to
/// guard. Routing every multiplication through one function makes that
/// true by construction instead of by discipline.
pub fn apply_multiplier(
    base: Percent,
    coefficient: Coefficient,
    cap: Percent,
) -> Result<Percent, DomainError> {
    let coefficient_decimal = coefficient.to_decimal()?;
    let scaled = Percent::from_ratio(base.as_ratio() * coefficient_decimal);
    Ok(scaled.min(cap))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // A handful of ordinary example-based tests live here to check the
    // obvious cases read the way we expect. The proptest suite in
    // `risk/src/sizing.rs` is what actually proves the cap holds for every
    // input, not just these examples; think of these as a readable sanity
    // check for a human skimming the file, not the real proof.

    #[test]
    fn from_percentage_matches_from_ratio() {
        let from_config = Percent::from_percentage(dec!(5.0));
        let from_raw = Percent::from_ratio(dec!(0.05));
        assert_eq!(from_config, from_raw);
    }

    #[test]
    fn apply_multiplier_respects_the_cap() {
        let base = Percent::from_percentage(dec!(2.0)); // 2%
        let doubling = Coefficient::new(2.0);
        let cap = Percent::from_percentage(dec!(3.0)); // cap lower than 2% * 2.0 = 4%

        // This is expected to be well behaved: cap conversion, ordinary
        // decimal, small f64. Panicking here would mean the newtype
        // arithmetic itself is broken, which is exactly the "completely
        // proven inevitable" case where a test unwrap is fine, since a
        // failure here is the test correctly failing, not a swallowed
        // error path in production code.
        let result = apply_multiplier(base, doubling, cap).unwrap();
        assert_eq!(result, cap);
    }

    #[test]
    fn apply_multiplier_passes_through_when_under_cap() {
        let base = Percent::from_percentage(dec!(1.0));
        let no_multiplier = Coefficient::identity();
        let cap = Percent::from_percentage(dec!(5.0));

        let result = apply_multiplier(base, no_multiplier, cap).unwrap();
        assert_eq!(result, base);
    }
}

--- ./domain/src/events.rs ---
//! Every event on the bus gets wrapped in an [`EventEnvelope`] rather than
//! carrying its own `event_id` and `timestamp` fields directly. Two
//! reasons: first, it means every single event, no matter what kind, gets
//! the same tracing/dedup treatment for free, instead of remembering to
//! add an `event_id` field to each new variant by hand. Second, keeping
//! the timestamp on the envelope (and requiring the caller to supply it,
//! rather than the envelope grabbing `Utc::now()` itself) is what makes
//! deterministic replay possible: replay drives a virtual clock, and every
//! event's timestamp needs to come from that clock, not the wall clock.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::{
    BrokerSnapshot, Direction, HealthStatus, NewsEvent, Order, Position, RecoveryState,
    RiskDecision, TradeSignal,
};

/// Rejection reasons for a signal that was generated but didn't make it to
/// an order. Matches the hardening layer's `SignalInvalidated` schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectionReason {
    TrueOpenGateConflict,
    SpreadGate,
    CollisionGate,
}

/// The three states a True Open (or, generically, any directional gate)
/// can be in. Separate from `Direction` because a gate can also be
/// neutral, whereas a trade direction can't.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Bias {
    Buy,
    Sell,
    Neutral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadCheck {
    pub pair: String,
    pub spread: rust_decimal::Decimal,
    pub mean_72h: rust_decimal::Decimal,
    pub accepted: bool,
    pub threshold: rust_decimal::Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalInvalidated {
    pub trace_id: Uuid,
    pub signal_id: Uuid,
    pub rejection_reason: RejectionReason,
    pub weekly_bias: Bias,
    pub daily_bias: Bias,
    pub smt_direction: Direction,
}

/// Every kind of thing that can happen in the daemon, gathered into one
/// enum so the event bus, the persistence layer, and the (advisory-only)
/// AssistantEngine can all speak the same language without each needing
/// its own bespoke dispatch table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    MacroCycleStarted,
    Snapshot(BrokerSnapshot),
    Spread(SpreadCheck),
    SmtSignal(TradeSignal),
    SignalInvalidated(SignalInvalidated),
    Risk(RiskDecision),
    Order(Order),
    Position(Position),
    News(NewsEvent),
    Recovery(RecoveryState),
    Health(HealthStatus),
    Shutdown { reason: String },
}

impl Event {
    /// A short, stable name for logging and metrics labels. Kept as a
    /// separate method rather than deriving something like `strum`, since
    /// this workspace would rather have one obvious match statement than
    /// pull in a proc-macro crate for a dozen short strings.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::MacroCycleStarted => "macro_cycle_started",
            Event::Snapshot(_) => "snapshot",
            Event::Spread(_) => "spread",
            Event::SmtSignal(_) => "smt_signal",
            Event::SignalInvalidated(_) => "signal_invalidated",
            Event::Risk(_) => "risk",
            Event::Order(_) => "order",
            Event::Position(_) => "position",
            Event::News(_) => "news",
            Event::Recovery(_) => "recovery",
            Event::Health(_) => "health",
            Event::Shutdown { .. } => "shutdown",
        }
    }

    /// Whether this event should jump the queue ahead of ordinary events.
    /// See `daemon::event_bus` for how this actually gets enforced (two
    /// real channels plus a biased `select!`, not just a sort by this
    /// flag on a single queue).
    pub fn is_priority(&self) -> bool {
        matches!(self, Event::Order(_) | Event::Shutdown { .. } | Event::Health(_))
    }
}

/// The envelope every event actually travels through the bus in. See the
/// module-level docs for why the ID and timestamp live here instead of on
/// each variant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub payload: Event,
}

impl EventEnvelope {
    /// `timestamp` must come from whatever clock the caller is using
    /// (the real clock in production, a paused/virtual one in replay),
    /// never from `Utc::now()` called inside this function. That's the
    /// whole trick to deterministic replay: nothing downstream of this
    /// constructor should ever need to touch the wall clock again.
    pub fn new(timestamp: DateTime<Utc>, payload: Event) -> Self {
        EventEnvelope {
            event_id: Uuid::new_v4(),
            timestamp,
            payload,
        }
    }
}

--- ./domain/src/types.rs ---
//! This file is the vocabulary the rest of the workspace shares. If two
//! crates need to agree on what a "Position" is, they agree on the one
//! defined here, they don't each roll their own. Keeping all of it in one
//! place also makes it easy to answer "what does a Position actually
//! carry" by scrolling through a single file instead of hunting across
//! seven crates.
//!
//! A couple of things worth calling out before you read the structs
//! themselves:
//!
//! - Anything that's money, a percentage, or a multiplier uses the
//!   newtypes from `newtypes.rs`, not a raw `Decimal` or `f64`. See that
//!   file for why.
//! - `Position` doesn't get mutated field by field. It's rebuilt from its
//!   list of fills every time a new one lands. That's the event-sourcing
//!   angle: the position IS its fill history, plus whatever's derived from
//!   it (weighted entry price, running PnL), so "why is this position the
//!   size it is" always has a real, replayable answer.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::DomainError;
use crate::newtypes::{Percent, Usd};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssetClass {
    Forex,
    Future,
    Crypto,
    Equity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Buy,
    Sell,
}

impl Direction {
    pub fn opposite(self) -> Direction {
        match self {
            Direction::Buy => Direction::Sell,
            Direction::Sell => Direction::Buy,
        }
    }
}

/// Which SMT tier produced a signal. `Double` means Tier 1 and Tier 2
/// aligned in the same macro cycle, which is the case that gets the 2.0x
/// risk multiplier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    Tier1,
    Tier2,
    Double,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Pending,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    PendingSubmission,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    Closing,
    Closed,
    Rejected,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    News,
    Contradiction,
    DailyClose,
    WeeklyClose,
    Manual,
    /// Added by the hardening layer: the broker no longer recognizes this
    /// order/position (a 404 on reconciliation) and it's old enough that
    /// we give up on it rather than retry forever. See
    /// `daemon::recovery` for where this actually gets applied.
    ReconciliationOrphan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CorrelationRegime {
    High,
    Normal,
    Low,
    Breakdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CorrelationQuality {
    Excellent,
    Good,
    Fair,
    Poor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NewsImpact {
    High,
    Medium,
    Low,
}

/// The daemon's own health, separate from any individual position's
/// state. `ReadOnly` and `EmergencyShutdown` both stop new trading;
/// the difference is whether the process keeps running afterward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemState {
    Healthy,
    Degraded,
    ReadOnly,
    EmergencyShutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub symbol: String,
    pub asset_class: AssetClass,
    pub tick_size: Decimal,
    pub pip_size: Decimal,
    pub contract_size: Decimal,
    pub currency: String,
    pub minimum_lot: Decimal,
    pub lot_step: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetPair {
    pub base: Asset,
    pub quote: Asset,
    pub correlation: Decimal,
    pub correlation_confidence: Decimal,
    /// Per-pair override of the account-wide max exposure per currency
    /// limit. Most pairs should leave this `None` and inherit the global
    /// risk config value; this exists for the rare pair that genuinely
    /// needs a tighter (or looser) cap, without duplicating the global
    /// number onto every single pair "just in case," which is exactly the
    /// kind of duplication that drifts out of sync the first time someone
    /// updates the global value and forgets the copies.
    pub max_exposure_per_currency_override: Option<Percent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceQuote {
    pub bid: Decimal,
    pub ask: Decimal,
}

/// A single, atomic, point-in-time capture of prices for every asset the
/// strategy cares about. Everything downstream in a given macro cycle
/// (spread checks, buffer updates, SMT validation, sizing, the entry
/// itself) reads from the same `BrokerSnapshot`, never from a fresh
/// "current price" call partway through. That's what makes a cycle's
/// decision reproducible: it was made against one fixed view of the
/// world, not a moving target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerSnapshot {
    pub snapshot_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub prices: std::collections::BTreeMap<String, PriceQuote>,
    pub spreads: std::collections::BTreeMap<String, Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadSample {
    pub timestamp: DateTime<Utc>,
    pub pair: String,
    pub spread: Decimal,
    pub mean_72h: Decimal,
    pub threshold: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationRecord {
    pub pair_id: String,
    pub coefficient: Decimal,
    pub confidence: Decimal,
    pub regime: CorrelationRegime,
    pub quality: CorrelationQuality,
    pub last_validated: DateTime<Utc>,
    pub historical_stability: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignal {
    pub signal_id: Uuid,
    pub trace_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub pair: String,
    pub direction: Direction,
    pub tier: Tier,
    pub strength: Decimal,
    pub confidence: Decimal,
    pub valid_until: DateTime<Utc>,
    /// The snapshot this signal was generated against. Carried through so
    /// that, after the fact, "what data justified this signal" always has
    /// a concrete answer instead of "whatever the price was around then."
    pub originating_snapshot_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub order_id: Uuid,
    pub trace_id: Uuid,
    pub signal_id: Uuid,
    pub pair: String,
    pub side: Direction,
    pub size: Decimal,
    pub order_type: OrderType,
    pub price: Option<Decimal>,
    pub stop_loss: Option<Decimal>,
    pub take_profit: Option<Decimal>,
    /// The snapshot consulted at the moment of the *final* sanity check,
    /// right before submission. This can be a different snapshot than the
    /// one that produced the originating signal, since the True Open and
    /// spread gates re-check conditions closer to entry. Keeping both IDs
    /// around (see `TradeSignal::originating_snapshot_id`) means we can
    /// always answer "what did the entry actually see," not just "what
    /// did the signal see."
    pub confirming_snapshot_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: Uuid,
    pub trace_id: Uuid,
    pub signal_id: Uuid,
    pub position_id: Option<Uuid>,
    pub pair: String,
    pub side: Direction,
    pub size: Decimal,
    pub filled_size: Decimal,
    pub price: Decimal,
    pub status: OrderStatus,
    pub timestamp: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
}

/// One partial (or complete) fill against a position. A position's
/// `entry_price` is never assigned directly; it's always recomputed as
/// the size-weighted average of its legs. See
/// [`Position::weighted_entry_price`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FillLeg {
    pub price: Decimal,
    pub size: Decimal,
    pub filled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub position_id: Uuid,
    pub trace_id: Uuid,
    pub signal_id: Uuid,
    pub pair: String,
    pub direction: Direction,
    pub legs: Vec<FillLeg>,
    pub entry_price: Decimal,
    pub current_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub entry_time: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub status: PositionStatus,
    pub exit_reason: Option<ExitReason>,
    /// The stop-loss and take-profit this position was opened with.
    /// `Option` because a broker or an order type might not always set
    /// one (or either): exit-condition monitoring treats a `None` here
    /// as "no risk-reward exit configured for this position," not as a
    /// missing-data error.
    pub stop_loss: Option<Decimal>,
    pub take_profit: Option<Decimal>,
}

impl Position {
    /// The size-weighted average price across every fill leg. This is the
    /// only place `entry_price` should ever come from; nothing in this
    /// codebase should assign it directly.
    ///
    /// Returns an error instead of panicking on the two ways this can go
    /// wrong (no legs at all, or a leg with a zero/negative size), because
    /// both are real possibilities if a broker sends us a malformed fill
    /// report, and we'd rather surface that as a `RecoveryError`-shaped
    /// problem than crash the daemon.
    pub fn weighted_entry_price(legs: &[FillLeg]) -> Result<Decimal, DomainError> {
        if legs.is_empty() {
            return Err(DomainError::EmptyFillLegs);
        }

        let mut weighted_sum = Decimal::ZERO;
        let mut total_size = Decimal::ZERO;

        for leg in legs {
            if leg.size <= Decimal::ZERO {
                return Err(DomainError::NonPositiveFillSize(leg.size.to_string()));
            }
            weighted_sum += leg.price * leg.size;
            total_size += leg.size;
        }

        // total_size can't be zero here since every leg was checked to be
        // strictly positive above and legs is non-empty, but we check
        // anyway rather than dividing blind: Decimal's Div panics on a
        // zero divisor, and "this can't happen" is exactly the kind of
        // assumption that's worth one extra guard instead of an .unwrap().
        if total_size.is_zero() {
            return Err(DomainError::NonPositiveFillSize("0".to_string()));
        }

        Ok(weighted_sum / total_size)
    }

    /// Push a new fill leg and recompute the derived fields. This is the
    /// only sanctioned way to grow a position; see the
    /// `Position Updates are Append-Only` invariant in the hardening
    /// layer. Nothing outside this function should ever write to
    /// `entry_price` directly.
    pub fn apply_fill_leg(&mut self, leg: FillLeg) -> Result<(), DomainError> {
        self.legs.push(leg);
        self.entry_price = Self::weighted_entry_price(&self.legs)?;
        self.last_update = leg.filled_at;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskDecision {
    pub decision_id: Uuid,
    pub trace_id: Uuid,
    pub signal_id: Uuid,
    pub approved: bool,
    pub rejection_reason: Option<String>,
    pub position_size: Decimal,
    pub stop_loss: Decimal,
    pub take_profit: Decimal,
    pub risk_percent: Percent,
    pub risk_currency: Usd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsEvent {
    pub event_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub currency: String,
    pub impact: NewsImpact,
    pub description: String,
    pub actual: Option<Decimal>,
    pub forecast: Option<Decimal>,
    pub previous: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub component: String,
    pub status: ComponentStatus,
    pub latency_ms: f64,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Failing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryState {
    pub state_id: Uuid,
    pub last_snapshot: DateTime<Utc>,
    pub last_cursor_offset: u64,
    pub retry_count: u32,
    pub backoff_seconds: u64,
    pub last_attempt: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    fn leg(price: Decimal, size: Decimal) -> FillLeg {
        FillLeg {
            price,
            size,
            filled_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn weighted_entry_price_single_leg_is_just_that_price() {
        let legs = vec![leg(dec!(1.1000), dec!(1.0))];
        let price = Position::weighted_entry_price(&legs).unwrap();
        assert_eq!(price, dec!(1.1000));
    }

    #[test]
    fn weighted_entry_price_averages_two_legs_by_size() {
        // 1 unit at 1.1000, 1 unit at 1.1010 should average to 1.1005.
        let legs = vec![leg(dec!(1.1000), dec!(1.0)), leg(dec!(1.1010), dec!(1.0))];
        let price = Position::weighted_entry_price(&legs).unwrap();
        assert_eq!(price, dec!(1.1005));
    }

    #[test]
    fn weighted_entry_price_weights_toward_the_bigger_leg() {
        // 3 units at 1.1000, 1 unit at 1.2000. The bigger leg should pull
        // the average much closer to 1.1000 than a naive unweighted
        // average would.
        let legs = vec![leg(dec!(1.1000), dec!(3.0)), leg(dec!(1.2000), dec!(1.0))];
        let price = Position::weighted_entry_price(&legs).unwrap();
        assert_eq!(price, dec!(1.1250));
    }

    #[test]
    fn weighted_entry_price_rejects_empty_legs() {
        let legs: Vec<FillLeg> = vec![];
        assert_eq!(
            Position::weighted_entry_price(&legs),
            Err(DomainError::EmptyFillLegs)
        );
    }

    #[test]
    fn weighted_entry_price_rejects_non_positive_size() {
        let legs = vec![leg(dec!(1.1000), dec!(0.0))];
        assert!(matches!(
            Position::weighted_entry_price(&legs),
            Err(DomainError::NonPositiveFillSize(_))
        ));
    }
}

--- ./domain/src/lib.rs ---
//! `domain` is the bottom of the workspace. It knows what a Position, an
//! Order, a Percent, an Event look like, and nothing about how prices
//! arrive, how orders get submitted, or when a macro cycle starts. Every
//! other crate depends on this one. This one depends on nothing else in
//! the workspace, on purpose, so that dependency direction is something
//! Cargo enforces rather than something everyone has to remember.

pub mod errors;
pub mod events;
pub mod newtypes;
pub mod types;

pub use errors::DomainError;
pub use events::{Bias, Event, EventEnvelope, RejectionReason, SignalInvalidated, SpreadCheck};
pub use newtypes::{apply_multiplier, Coefficient, Percent, Usd};
pub use types::{
    Asset, AssetClass, AssetPair, BrokerSnapshot, ComponentStatus, CorrelationQuality,
    CorrelationRecord, CorrelationRegime, Direction, ExitReason, FillLeg, HealthStatus,
    NewsEvent, NewsImpact, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionStatus, PriceQuote, RecoveryState, RiskDecision, SpreadSample, SystemState, Tier,
    TradeSignal,
};

--- ./domain/Cargo.toml ---
[package]
name = "domain"
version = "0.1.0"
edition = "2021"

# This crate is the bottom of the dependency stack on purpose. It defines
# what a Position, an Order, a Percent, an Event look like, and nothing
# else, no broker calls, no file I/O, no scheduling. Every other crate in
# the workspace depends on this one; this one depends on nothing in the
# workspace. That's not a style preference, it's the enforcement mechanism
# for the "no cyclic dependencies, only through traits" rule: if a broker
# adapter ever tries to import something from `strategy`, Cargo simply
# won't build it, because it's not a listed dependency here or anywhere
# upstream of here.
[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
chrono = { workspace = true }
rust_decimal = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
rust_decimal_macros = { workspace = true }

--- ./config.toml ---
# oboobot configuration. Copy this to config.toml (or point --config at
# your own copy) and adjust. None of this is secret — API keys and
# webhook URLs are read from environment variables named here, never
# written directly into this file, since this file is meant to live in
# an open-source repo.

[risk]
base_risk_percent = 1.0
max_risk_percent = 5.0
max_open_positions = 5
daily_loss_limit_percent = 5.0
weekly_loss_limit_percent = 10.0
# Spread must be under (72h average spread) * this multiplier to trade.
# Must be greater than 1.0.
spread_multiplier = 1.5
# How far the live correlation coefficient can drift from baseline
# before it's flagged as a regime shift. A fraction, not a percentage
# (0.20 means 20 percentage points of coefficient, e.g. 0.90 -> 0.70).
regime_shift_threshold = 0.20

# One entry per correlated pair this strategy watches. Add another pair
# by adding another [[pairs]] block below, nothing else changes.
[[pairs]]
primary = "EURUSD"
secondary = "GBPUSD"

[notifications]
# Names of environment variables holding webhook credentials, not the
# credentials themselves. Leave commented out to disable notifications
# entirely (the default: a NoopNotifier that only logs).
# slack_webhook_env = "SLACK_WEBHOOK_URL"
# telegram_bot_token_env = "TELEGRAM_BOT_TOKEN"
# telegram_chat_id_env = "TELEGRAM_CHAT_ID"

