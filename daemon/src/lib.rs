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
