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
