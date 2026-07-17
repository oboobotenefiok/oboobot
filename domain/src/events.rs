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
