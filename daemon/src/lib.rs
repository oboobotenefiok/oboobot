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
