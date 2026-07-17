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
