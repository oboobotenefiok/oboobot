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
