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
    CorrelationRecord, CorrelationRegime, Direction, ExitReason, FillLeg, HealthStatus, NewsEvent,
    NewsImpact, Order, OrderRequest, OrderStatus, OrderType, Position, PositionStatus, PriceQuote,
    RecoveryState, RiskDecision, SpreadSample, SystemState, Tier, TradeSignal,
};
