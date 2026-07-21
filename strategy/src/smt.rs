//! SMT (Smart Money Technique) divergence is, at its core, a
//! disagreement: two correlated assets are watched against their own
//! recent high/low buffers, and a signal fires when one of them sweeps
//! past its buffer while the other doesn't confirm that move. That
//! disagreement is read as smart money divergence, an early hint of a
//! reversal.
//!
//! The rule is symmetric between the two assets: either one can be the
//! asset that sweeps, and either one can be the asset that holds.
//! "Primary" and "secondary" here are just the two input slots this
//! module's functions take; they aren't fixed roles where one asset
//! always gets traded and the other never does. What decides the trade
//! is purely which asset swept and which one held. Sweeping the high
//! while the other holds means the holder is relatively weak (it should
//! have made a new high too, and didn't), so the holder gets sold.
//! Sweeping the low while the other holds means the holder is
//! relatively strong, so the holder gets bought. Put plainly: always buy
//! the stronger (higher) asset, always sell the weaker (lower) one.
//!
//! This module implements that check against two buffer timeframes
//! (daily and session). When only one timeframe shows the divergence,
//! that's a Tier 1 or Tier 2 signal; when both agree, on both which
//! asset to trade and which direction, that's a Double SMT signal,
//! which is also what triggers the 2.0x risk multiplier over in the
//! `risk` crate.

use domain::{Bias, Direction, SignalInvalidated, Tier, TradeSignal};
use rust_decimal::Decimal;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferLevels {
    pub high: Decimal,
    pub low: Decimal,
}

/// Which of the two assets a divergence check identified as the one to
/// trade: the one that held its buffer level while its counterpart
/// swept past theirs. This only reflects each asset's role in a given
/// call's `primary_price`/`secondary_price` inputs; it carries no
/// meaning beyond that, and doesn't favor either asset when there's a
/// divergence to trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeTarget {
    Primary,
    Secondary,
}

/// A single-timeframe divergence check between two correlated assets,
/// checked both ways: `primary` might be the one that sweeps while
/// `secondary` holds, or the other way around. Returns which asset held
/// (the one to trade) together with the implied direction, or `None` if
/// neither swept without the other confirming.
pub fn detect_divergence(
    primary_price: Decimal,
    primary_buffer: BufferLevels,
    secondary_price: Decimal,
    secondary_buffer: BufferLevels,
) -> Option<(TradeTarget, Direction)> {
    let primary_swept_low = primary_price < primary_buffer.low;
    let secondary_swept_low = secondary_price < secondary_buffer.low;
    let primary_held_low = !primary_swept_low;
    let secondary_held_low = !secondary_swept_low;

    if primary_swept_low && secondary_held_low {
        // Primary broke down through its own buffer low, but secondary
        // didn't follow. Secondary held up, making it the relatively
        // stronger asset right now, so secondary is what gets bought.
        return Some((TradeTarget::Secondary, Direction::Buy));
    }
    if secondary_swept_low && primary_held_low {
        // The mirror image of the case above: secondary broke down and
        // primary held, so this time primary is what gets bought.
        return Some((TradeTarget::Primary, Direction::Buy));
    }

    let primary_swept_high = primary_price > primary_buffer.high;
    let secondary_swept_high = secondary_price > secondary_buffer.high;
    let primary_held_high = !primary_swept_high;
    let secondary_held_high = !secondary_swept_high;

    if primary_swept_high && secondary_held_high {
        // Primary swept above its buffer high but secondary didn't
        // confirm it: secondary is the relatively weaker asset, so
        // secondary is what gets sold.
        return Some((TradeTarget::Secondary, Direction::Sell));
    }
    if secondary_swept_high && primary_held_high {
        return Some((TradeTarget::Primary, Direction::Sell));
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
/// session agree, both on which asset to trade and on direction, that's
/// `Tier::Double`. If they disagree (a real possibility, since they're
/// independent checks against different buffer windows, and could even
/// point at different assets entirely) the daily timeframe's read wins
/// outright, on the reasoning that a higher timeframe's read on
/// divergence should set the bias when the two disagree, similarly to
/// how the True Open gate treats Weekly as the tie-breaker over Daily.
pub fn evaluate_smt(inputs: &DivergenceInputs) -> Option<(TradeTarget, Direction, Tier)> {
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
        (Some((target1, d1)), Some((target2, d2))) if target1 == target2 && d1 == d2 => {
            Some((target1, d1, Tier::Double))
        }
        (Some((target1, d1)), _) => Some((target1, d1, Tier::Tier1)),
        (None, Some((target2, d2))) => Some((target2, d2, Tier::Tier2)),
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

/// The full pipeline: detect SMT divergence, pick whichever of
/// `primary_pair`/`secondary_pair` the divergence identified as the one
/// to trade, then run it through the True Open gate. This is the one
/// function the daemon's event loop actually calls each macro cycle;
/// everything above is what it's built from.
#[allow(clippy::too_many_arguments)]
pub fn generate_signal(
    inputs: &DivergenceInputs,
    weekly_bias: Bias,
    daily_bias: Bias,
    primary_pair: String,
    secondary_pair: String,
    originating_snapshot_id: Uuid,
    strength: Decimal,
    confidence: Decimal,
    valid_until: chrono::DateTime<chrono::Utc>,
) -> SignalOutcome {
    let Some((target, direction, tier)) = evaluate_smt(inputs) else {
        return SignalOutcome::NoDivergence;
    };

    let pair = match target {
        TradeTarget::Primary => primary_pair,
        TradeTarget::Secondary => secondary_pair,
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
    fn primary_sweeps_low_secondary_holds_so_secondary_is_bought() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990), // sweeps daily low
            secondary_price: dec!(1.1010), // holds daily low
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)), // wide enough not to trigger
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Secondary, Direction::Buy, Tier::Tier1)));
    }

    #[test]
    fn secondary_sweeps_low_primary_holds_so_primary_is_bought() {
        // The mirror image of the case above: this time secondary is the
        // one that breaks down and primary is the one that holds, which
        // used to fall through this function's checks entirely before
        // they covered both directions.
        let inputs = DivergenceInputs {
            primary_price: dec!(1.1010), // holds daily low
            secondary_price: dec!(1.0990), // sweeps daily low
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Primary, Direction::Buy, Tier::Tier1)));
    }

    #[test]
    fn primary_sweeps_high_secondary_holds_so_secondary_is_sold() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.1110), // sweeps daily high
            secondary_price: dec!(1.1090), // holds daily high
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Secondary, Direction::Sell, Tier::Tier1)));
    }

    #[test]
    fn secondary_sweeps_high_primary_holds_so_primary_is_sold() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.1090), // holds daily high
            secondary_price: dec!(1.1110), // sweeps daily high
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
            session_secondary_buffer: buffer(dec!(1.0900), dec!(1.1200)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Primary, Direction::Sell, Tier::Tier1)));
    }

    #[test]
    fn agreement_on_both_timeframes_and_both_targets_is_double() {
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990),
            secondary_price: dec!(1.1010),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Secondary, Direction::Buy, Tier::Double)));
    }

    #[test]
    fn agreeing_direction_but_disagreeing_target_falls_back_to_daily_not_double() {
        // Daily says "secondary is bought" (secondary holds while
        // primary sweeps its low). Session, against a much narrower pair
        // of bands, says "primary is bought" instead: same direction on
        // both timeframes, but a different asset entirely. The two
        // timeframes aren't actually agreeing on what to trade, so this
        // should NOT count as a Double, and daily should win.
        let inputs = DivergenceInputs {
            primary_price: dec!(1.0990),
            secondary_price: dec!(1.1010),
            daily_primary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            daily_secondary_buffer: buffer(dec!(1.1000), dec!(1.1100)),
            session_primary_buffer: buffer(dec!(1.0950), dec!(1.1200)), // primary no longer sweeps this
            session_secondary_buffer: buffer(dec!(1.1050), dec!(1.1200)), // secondary sweeps this instead
        };
        assert_eq!(evaluate_smt(&inputs), Some((TradeTarget::Secondary, Direction::Buy, Tier::Tier1)));
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
            "GBPUSD".to_string(),
            "EURUSD".to_string(),
            Uuid::new_v4(),
            dec!(0.8),
            dec!(0.8),
            chrono::Utc::now(),
        );

        match outcome {
            // Primary swept its low and secondary held, so this
            // divergence is about secondary (EURUSD), not primary.
            SignalOutcome::Signal(signal) => {
                assert_eq!(signal.pair, "EURUSD");
                assert_eq!(signal.direction, Direction::Buy);
            }
            other => panic!("expected a passing signal, got {other:?}"),
        }
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
            "GBPUSD".to_string(),
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
            "GBPUSD".to_string(),
            "EURUSD".to_string(),
            Uuid::new_v4(),
            dec!(0.8),
            dec!(0.8),
            chrono::Utc::now(),
        );

        assert!(matches!(outcome, SignalOutcome::NoDivergence));
    }
}
