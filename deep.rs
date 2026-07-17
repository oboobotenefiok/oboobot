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

[profile.dev]
# Debug assertions catch integer overflow and similar arithmetic mistakes at
# runtime instead of silently wrapping. For a trading daemon, an arithmetic
# bug that silently wraps instead of panicking is much scarier than a panic
# would be, so we want these on even outside of `cargo test`.
overflow-checks = true

--- ./persistence/src/lib.rs ---
//! Generic, fsync-before-return, append-only cursor file storage. This
//! crate doesn't know what a Position or an Order is; `daemon::recovery`
//! is where cursor files full of `domain::Event`s get turned into actual
//! reconciled state against a live broker.

pub mod cursor;

pub use cursor::{CursorFile, PersistenceError};

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
//! health state machine, the two-channel event bus, the macro-cycle
//! scheduler, startup reconciliation, and the (deliberately inert)
//! assistant boundary. `main.rs` wires all of this into a runnable
//! binary; the integration test in `tests/` drives the same wiring
//! end to end against `MockBroker`.

pub mod assistant;
pub mod event_bus;
pub mod health;
pub mod recovery;
pub mod scheduler;

pub use assistant::{AssistantEngine, ConfigChangeSuggestion, LoggingAssistant, Recommendation, Severity};
pub use event_bus::{event_bus, EventBusError, EventBusHandle, EventBusReceiver};
pub use health::{allows_new_entries, auto_action, severity_for, HealthCheckFailure, HealthMonitor};
pub use recovery::{apply_reconciliation, reconcile, ReconciliationReport};
pub use scheduler::Scheduler;

--- ./daemon/src/main.rs ---
//! This binary is a demonstration harness, not a production entry point.
//! It wires every crate in this workspace together and runs a handful of
//! synthetic macro cycles against `MockBroker`, so that running `cargo
//! run` actually shows the whole pipeline working end to end: SMT
//! divergence detection, the True Open gate, risk sizing, order
//! submission, startup reconciliation, and the health-state gate that
//! blocks new entries when something's wrong.
//!
//! A real production entry point would swap `MockBroker` for a real
//! `BrokerAdapter` implementation, use `session_time::SystemClock`
//! instead of a fixed timestamp, and call `Scheduler::run` (which runs
//! forever, sleeping between real macro cycles) instead of driving a
//! fixed, fast sequence of scenarios by hand. Building that real adapter
//! means confirming exact endpoint URLs and auth flows against a live
//! broker's current docs, which isn't something to guess at, so it's
//! intentionally left as the next step rather than faked here.

use broker::{BrokerAdapter, MockBroker};
use daemon::{
    allows_new_entries, apply_reconciliation, auto_action, reconcile, AssistantEngine,
    HealthCheckFailure, HealthMonitor, LoggingAssistant,
};
use domain::{Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Usd};
use persistence::CursorFile;
use rust_decimal_macros::dec;
use risk::RiskEngine as _;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("starting QuarterlyTheory_SMT_Trader demonstration harness");
    tracing::info!("this run is against MockBroker; see main.rs docs for what a live run would change");

    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));
    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    // A real deployment would keep this under a persistent `state/`
    // directory next to wherever the daemon runs from. For this
    // demonstration harness it goes under the OS temp directory instead,
    // purely so repeated runs of this binary don't accumulate positions
    // from previous runs; the durability behavior (fsync before the
    // append returns, see `persistence::cursor`) is identical either way.
    let state_dir = std::env::temp_dir().join("smt-trader-demo-state");
    tokio::fs::create_dir_all(&state_dir).await?;
    let positions_cursor_path = state_dir.join("positions.cursor");
    // Start every run from a clean cursor file, since this is a
    // from-scratch demonstration each time, not a genuinely persistent
    // deployment.
    let _ = tokio::fs::remove_file(&positions_cursor_path).await;
    let positions_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);

    // Every real daemon startup begins here: what does our own
    // persistence say is open, and does the broker agree? On a genuinely
    // fresh start there's nothing persisted yet, so this should reconcile
    // clean, but running it unconditionally (rather than only when we
    // suspect a problem) is exactly the point: reconciliation isn't a
    // special recovery-mode action, it's just what startup always does.
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
        Bias::Sell, // Weekly is bearish; the divergence above is bullish. Conflict.
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

    // Simulate the daemon actually restarting: build a brand new
    // CursorFile pointing at the same path, with none of the in-memory
    // state above carried over, the same as if this were a fresh process.
    // If persistence and reconciliation are both doing their job, this
    // should recover exactly the position(s) opened above, straight from
    // disk, confirmed against the broker.
    let post_restart_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);
    let recovered_from_disk = post_restart_cursor.read_all().await?;
    let post_restart_report = reconcile(&broker, &recovered_from_disk).await?;
    let post_restart_positions = apply_reconciliation(&post_restart_report);
    tracing::info!(
        recovered_from_disk = recovered_from_disk.len(),
        confirmed_by_broker = post_restart_positions.len(),
        "simulated restart: recovered position state from disk and reconciled it against the broker"
    );

    tracing::info!("QuarterlyTheory_SMT_Trader demonstration harness finished");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    label: &str,
    broker: &MockBroker,
    health: &HealthMonitor,
    assistant: &dyn AssistantEngine,
    positions_cursor: &CursorFile<Position>,
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
    // The assistant sees every event, but as `daemon::assistant` explains,
    // anything it returns only ever gets logged, never applied.
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

            let previously_known_ids: std::collections::HashSet<Uuid> =
                open_positions.iter().map(|p| p.position_id).collect();

            open_positions.clear();
            open_positions.extend(broker.list_open_positions().await?);

            // Persist only what's actually new since the last cycle. This
            // mirrors "state persisted before ack" in spirit: we don't
            // treat a fill as durably recorded until it's been through
            // `CursorFile::append`, which doesn't return until its
            // `fsync` has completed.
            for position in open_positions.iter() {
                if !previously_known_ids.contains(&position.position_id) {
                    positions_cursor.append(position).await?;
                }
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

--- ./daemon/Cargo.toml ---
[package]
name = "daemon"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "smt-trader"
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

pub use calendar::{
    is_full_trading_week, ny_tz, to_ny, week_start_for, Clock, HolidayProvider, ManualClock,
    StaticHolidayProvider, SystemClock,
};
pub use macro_cycle::{is_within_macro_cycle, next_macro_cycle_after, MACRO_CYCLE_HOURS};
pub use true_open::{
    bias_from_price, true_open_gate, week_qualifies_for_weekly_true_open, Timeframe,
    TrueOpenLevel,
};

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
                let order = Order {
                    order_id: request.order_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    position_id: None,
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
                    position_id: Uuid::new_v4(),
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
pub mod mock;

pub use adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};
pub use mock::{mock_health_status, MockBroker, ScriptedResponse};

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

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
rust_decimal_macros = { workspace = true }

--- ./strategy/src/lib.rs ---
//! SMT divergence detection plus the True Open gate, wired together into
//! the one pipeline the daemon calls once per macro cycle. Depends on
//! `domain` for shared types and `session_time` for the gate logic and
//! calendar facts; knows nothing about brokers or persistence.

pub mod smt;

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

