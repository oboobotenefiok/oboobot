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
