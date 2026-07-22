//! Position sizing is the one calculation in this whole daemon where a
//! bug directly translates into "we risked more than we meant to." That's
//! why the multiplier cap lives in `domain::apply_multiplier` as a
//! function nobody can route around, and why this module leans on
//! property-based testing (further down) instead of only checking a
//! handful of examples: the property we actually care about ("computed
//! risk never exceeds the configured cap") should hold for every input,
//! not just the ones we thought to write down.
//!
//! This covers per-trade sizing, the mutually-exclusive Tuesday/Double-
//! SMT multiplier, daily/weekly loss-limit gating, the max-open-
//! positions gate, a zero-stop-distance guard, and net exposure across
//! multiple simultaneous positions that share a currency or a
//! correlation cluster (`max_exposure_per_currency` and
//! `max_correlation_exposure` from the original spec, previously an
//! intentional gap named in this same comment). Both of those need data
//! this module doesn't compute itself: `RiskContext::currency_exposure`
//! and `RiskContext::correlated_exposure` are precomputed by the caller,
//! which is the one place that actually has the full open-positions list
//! and the live correlation coefficient (`strategy::compute_coefficient`)
//! to compute them from. This module's job is just the threshold check
//! against whatever it's handed, kept separate from *how* that exposure
//! got computed so it stays testable without needing a real position
//! list or correlation window in every test.

use std::collections::BTreeMap;

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
    MaxCurrencyExposureReached,
    MaxCorrelationExposureReached,
}

impl RiskRejection {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskRejection::DailyLossLimitReached => "daily loss limit reached",
            RiskRejection::WeeklyLossLimitReached => "weekly loss limit reached",
            RiskRejection::MaxOpenPositionsReached => "max open positions reached",
            RiskRejection::InvalidStopDistance => "stop distance is zero, cannot size a position",
            RiskRejection::MaxCurrencyExposureReached => {
                "opening this position would push net exposure to one of its currencies past the configured cap"
            }
            RiskRejection::MaxCorrelationExposureReached => {
                "opening this position would push exposure to an already-open, highly correlated pair past the configured cap"
            }
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
    /// Cap on net exposure (existing open positions plus this
    /// candidate, in risk-dollar terms) to any single currency, checked
    /// against `RiskContext::currency_exposure`.
    pub max_exposure_per_currency_percent: Percent,
    /// Cap on exposure (existing open positions plus this candidate) to
    /// pairs whose live correlation with this candidate's pair is at or
    /// above `correlation_exposure_threshold`, checked against
    /// `RiskContext::correlated_exposure`.
    pub max_correlation_exposure_percent: Percent,
    /// How strong a live correlation coefficient (from
    /// `strategy::compute_coefficient`, so always in -1.0..=1.0) has to
    /// be, in either direction, before two pairs count as the same
    /// correlation cluster for `max_correlation_exposure_percent`.
    /// Deliberately a separate number from `regime_shift_threshold`
    /// over in `strategy`: that one measures how much correlation has
    /// *moved* from its baseline, this one measures how strong it
    /// currently *is*, and there's no reason those should share a value.
    pub correlation_exposure_threshold: f64,
}

#[derive(Debug, Clone)]
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
    /// Net directional exposure already open in each currency, in
    /// risk-dollar terms (positive means net long that currency,
    /// negative means net short), from every currently open position.
    /// Keyed by 3-letter currency code (e.g. "EUR", "USD"). Does not
    /// include the candidate signal being evaluated; `evaluate` adds
    /// that itself once it knows the signal's own pair and direction.
    pub currency_exposure: BTreeMap<String, Decimal>,
    /// Exposure already open, in risk-dollar terms, on pairs whose live
    /// correlation with the candidate signal's own pair meets or
    /// exceeds `correlation_exposure_threshold`. Zero if nothing open
    /// right now is correlated with this candidate strongly enough to
    /// count, which is also the right value when there's no live
    /// correlation reading yet (too few samples).
    pub correlated_exposure: Decimal,
}

pub trait RiskEngine: Send + Sync {
    fn evaluate(
        &self,
        signal: &TradeSignal,
        config: &RiskConfig,
        context: &RiskContext,
    ) -> Result<RiskDecision, RiskError>;
}

/// `"EURUSD"` -> `Some(("EUR", "USD"))`. `None` for anything that isn't
/// a plain 6-ASCII-character forex pair, which is the same shape every
/// pair in this daemon's config already has to be in (see
/// `broker::deriv`'s `to_deriv_symbol`). Currency exposure simply isn't
/// tracked for a pair that doesn't fit that shape, rather than guessing
/// at a split that might be wrong. Public so the daemon can use the
/// same decomposition for existing open positions that `evaluate` below
/// uses for the candidate signal; there's only one correct way to split
/// a pair into currencies, so there's only one function that does it.
pub fn currency_pair(pair: &str) -> Option<(&str, &str)> {
    if pair.len() == 6 && pair.is_ascii() {
        Some((&pair[0..3], &pair[3..6]))
    } else {
        None
    }
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
            return Ok(Self::rejected(
                signal,
                RiskRejection::WeeklyLossLimitReached,
            ));
        }

        if context.open_position_count >= config.max_open_positions {
            return Ok(Self::rejected(
                signal,
                RiskRejection::MaxOpenPositionsReached,
            ));
        }

        let stop_distance = (context.entry_price - context.stop_loss_price).abs();
        if stop_distance.is_zero() {
            return Ok(Self::rejected(signal, RiskRejection::InvalidStopDistance));
        }

        let coefficient = Self::effective_coefficient(context.is_tuesday, context.is_double_smt);
        let risk_percent = domain::apply_multiplier(
            config.base_risk_percent,
            coefficient,
            config.max_risk_percent,
        )?;
        let risk_currency = Usd::from_percent_of(context.equity, risk_percent);

        if let Some((base, quote)) = currency_pair(&signal.pair) {
            // Buying is long the base currency, short the quote; selling
            // is the reverse. Adding this candidate's own exposure to
            // whatever's already open is what "net exposure including
            // this trade" actually means, not just checking what's open
            // right now in isolation.
            let (base_direction, quote_direction) = match signal.direction {
                domain::Direction::Buy => (Decimal::ONE, -Decimal::ONE),
                domain::Direction::Sell => (-Decimal::ONE, Decimal::ONE),
            };
            let max_currency_exposure =
                Usd::from_percent_of(context.equity, config.max_exposure_per_currency_percent);
            for (currency, direction) in [(base, base_direction), (quote, quote_direction)] {
                let existing = context
                    .currency_exposure
                    .get(currency)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let projected = (existing + direction * risk_currency.as_decimal()).abs();
                if projected > max_currency_exposure.as_decimal() {
                    return Ok(Self::rejected(
                        signal,
                        RiskRejection::MaxCurrencyExposureReached,
                    ));
                }
            }
        }

        let max_correlation_exposure =
            Usd::from_percent_of(context.equity, config.max_correlation_exposure_percent);
        if context.correlated_exposure + risk_currency.as_decimal()
            > max_correlation_exposure.as_decimal()
        {
            return Ok(Self::rejected(
                signal,
                RiskRejection::MaxCorrelationExposureReached,
            ));
        }

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
            max_exposure_per_currency_percent: Percent::from_percentage(dec!(15.0)),
            max_correlation_exposure_percent: Percent::from_percentage(dec!(10.0)),
            correlation_exposure_threshold: 0.7,
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
            currency_exposure: BTreeMap::new(),
            correlated_exposure: Decimal::ZERO,
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

    #[test]
    fn stacking_same_direction_currency_exposure_is_rejected_past_the_cap() {
        // sample_signal() is Buy EURUSD. Already long EUR (from some
        // other pair on the same currency) for 14% of equity; the base
        // trade risks 1%, and the cap is 15%, so this candidate would
        // push it to 15% -- right at the edge -- unless it's rejected.
        // Use 14.5% already committed so this candidate's 1% clearly
        // tips it over 15%.
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context
            .currency_exposure
            .insert("EUR".to_string(), dec!(1450)); // 14.5% of $10,000

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(!decision.approved);
        assert_eq!(
            decision.rejection_reason.as_deref(),
            Some(RiskRejection::MaxCurrencyExposureReached.as_str())
        );
    }

    #[test]
    fn opposite_direction_currency_exposure_nets_down_instead_of_stacking() {
        // Already short EUR for 14.5% of equity (e.g. an open Sell
        // EURUSD elsewhere). sample_signal() is Buy EURUSD, which is
        // long EUR: that's exposure in the *opposite* direction, so it
        // should net the existing short down rather than stack with it,
        // and stay well clear of the cap.
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context
            .currency_exposure
            .insert("EUR".to_string(), dec!(-1450));

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(decision.approved);
    }

    #[test]
    fn correlated_exposure_past_the_cap_is_rejected() {
        // The candidate itself risks 1% of equity; the cap is 10%.
        // Already 9.5% of equity committed to a pair correlated highly
        // enough with this one to count, so this candidate should push
        // it over.
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.correlated_exposure = dec!(950); // 9.5% of $10,000

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(!decision.approved);
        assert_eq!(
            decision.rejection_reason.as_deref(),
            Some(RiskRejection::MaxCorrelationExposureReached.as_str())
        );
    }

    #[test]
    fn correlated_exposure_well_under_the_cap_is_approved() {
        let engine = DefaultRiskEngine;
        let mut context = sample_context();
        context.correlated_exposure = dec!(200); // 2% of $10,000, cap is 10%

        let decision = engine
            .evaluate(&sample_signal(), &sample_config(), &context)
            .unwrap();

        assert!(decision.approved);
    }

    #[test]
    fn currency_exposure_is_skipped_for_a_pair_that_does_not_fit_the_six_character_shape() {
        // currency_pair only understands plain 6-character forex codes;
        // anything else should just skip the currency check rather than
        // guess at a split (and definitely not panic on the string
        // slicing).
        let engine = DefaultRiskEngine;
        let mut signal = sample_signal();
        signal.pair = "BTCUSD".to_string(); // 6 chars but not what this daemon trades; still fine, shape matches
        let context = sample_context();

        let decision = engine
            .evaluate(&signal, &sample_config(), &context)
            .unwrap();
        assert!(decision.approved);

        let mut odd_signal = sample_signal();
        odd_signal.pair = "XAU/USD".to_string(); // not 6 plain characters
        let decision = engine
            .evaluate(&odd_signal, &sample_config(), &context)
            .unwrap();
        assert!(decision.approved);
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
                max_exposure_per_currency_percent: Percent::from_percentage(dec!(100.0)),
                max_correlation_exposure_percent: Percent::from_percentage(dec!(100.0)),
                correlation_exposure_threshold: 0.7,
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
                currency_exposure: BTreeMap::new(),
                correlated_exposure: Decimal::ZERO,
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
