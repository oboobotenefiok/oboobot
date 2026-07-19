//! Until now, nothing in this codebase ever looked at a position again
//! once it opened. That was the bigger of the two gaps named in review:
//! the original spec's exit conditions (1:3 risk-reward, pre-news,
//! SMT contradiction) were fully speced and fully untouched by any
//! code.
//!
//! The fix fits the deployment model better than a separate continuous
//! task would: since GitHub Actions already wakes this process up every
//! five minutes, every single invocation checks open positions for exit
//! conditions, regardless of whether it's inside a macro-cycle entry
//! window. Entries are gated by the window; exits never are. That
//! mirrors exactly what the original spec called for (a monitor
//! decoupled from the macro-cycle schedule) without needing a
//! long-running task inside a process that isn't long-running.
//!
//! For a broker with native stop-loss/take-profit enforcement (Deriv's
//! Multipliers included), the risk-reward check here is a backup, not
//! the primary mechanism — the broker itself closes the position before
//! this ever gets a chance to. It still matters: for a broker without
//! native enforcement, or if a native SL/TP order is ever rejected or
//! modified unexpectedly, this is what catches it.

use chrono::{DateTime, Duration, Utc};
use domain::{Direction, ExitReason, NewsEvent, Position, Tier};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::news::should_exit_for_news;

#[derive(Debug, Clone, Copy)]
pub struct ExitDecision {
    pub position_id: Uuid,
    pub reason: ExitReason,
}

/// Whether `position` has reached its stop-loss or take-profit, given
/// `current_price`. `None` if the position has no configured stop or
/// target, or if neither has been reached yet.
fn risk_reward_exit(position: &Position, current_price: Decimal) -> Option<ExitReason> {
    let hit_stop = position.stop_loss.is_some_and(|stop| match position.direction {
        Direction::Buy => current_price <= stop,
        Direction::Sell => current_price >= stop,
    });
    let hit_target = position.take_profit.is_some_and(|target| match position.direction {
        Direction::Buy => current_price >= target,
        Direction::Sell => current_price <= target,
    });

    if hit_stop {
        Some(ExitReason::StopLoss)
    } else if hit_target {
        Some(ExitReason::TakeProfit)
    } else {
        None
    }
}

/// Whether the live SMT divergence now points opposite to the direction
/// `position` is holding. `current_divergence` is whatever
/// `strategy::evaluate_smt` returned this cycle for the same pair, if
/// anything.
fn smt_contradiction_exit(
    position: &Position,
    current_divergence: Option<(Direction, Tier)>,
) -> Option<ExitReason> {
    match current_divergence {
        Some((direction, _)) if direction != position.direction => Some(ExitReason::Contradiction),
        _ => None,
    }
}

/// The full exit sweep for one cycle: given every currently open
/// position, the live price for its pair, whatever news events are on
/// file, and the live SMT reading (if any), decide which positions
/// should close and why. Checked in the order the original spec listed
/// them, though since all three lead to the same action (immediate
/// close) the order only matters for which `ExitReason` gets recorded,
/// not for what actually happens.
pub fn evaluate_exits(
    positions: &[Position],
    current_price: Decimal,
    news_events: &[NewsEvent],
    now: DateTime<Utc>,
    news_lead_time: Duration,
    current_divergence: Option<(Direction, Tier)>,
) -> Vec<ExitDecision> {
    let news_exit_active = should_exit_for_news(news_events, now, news_lead_time);

    positions
        .iter()
        .filter_map(|position| {
            let reason = if let Some(reason) = risk_reward_exit(position, current_price) {
                Some(reason)
            } else if news_exit_active {
                Some(ExitReason::News)
            } else {
                smt_contradiction_exit(position, current_divergence)
            };

            reason.map(|reason| ExitDecision { position_id: position.position_id, reason })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use domain::{FillLeg, PositionStatus};

    fn sample_position(direction: Direction, stop_loss: Decimal, take_profit: Decimal) -> Position {
        Position {
            position_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            direction,
            legs: vec![FillLeg {
                price: rust_decimal_macros::dec!(1.1000),
                size: rust_decimal_macros::dec!(1.0),
                filled_at: Utc::now(),
            }],
            entry_price: rust_decimal_macros::dec!(1.1000),
            current_price: rust_decimal_macros::dec!(1.1000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            entry_time: Utc::now(),
            last_update: Utc::now(),
            status: PositionStatus::Filled,
            exit_reason: None,
            stop_loss: Some(stop_loss),
            take_profit: Some(take_profit),
        }
    }

    #[test]
    fn buy_position_exits_on_stop_loss() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position.clone()], dec!(1.0940), &[], now, Duration::minutes(15), None);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].reason, ExitReason::StopLoss);
    }

    #[test]
    fn buy_position_exits_on_take_profit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position.clone()], dec!(1.1160), &[], now, Duration::minutes(15), None);
        assert_eq!(decisions[0].reason, ExitReason::TakeProfit);
    }

    #[test]
    fn sell_position_stop_and_target_are_mirrored() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Sell, dec!(1.1050), dec!(1.0850));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();

        let stopped = evaluate_exits(&[position.clone()], dec!(1.1060), &[], now, Duration::minutes(15), None);
        assert_eq!(stopped[0].reason, ExitReason::StopLoss);

        let targeted = evaluate_exits(&[position.clone()], dec!(1.0840), &[], now, Duration::minutes(15), None);
        assert_eq!(targeted[0].reason, ExitReason::TakeProfit);
    }

    #[test]
    fn no_exit_when_price_is_between_stop_and_target() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(&[position], dec!(1.1000), &[], now, Duration::minutes(15), None);
        assert!(decisions.is_empty());
    }

    #[test]
    fn smt_contradiction_triggers_exit_when_no_rr_or_news_exit_applies() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(
            &[position],
            dec!(1.1000),
            &[],
            now,
            Duration::minutes(15),
            Some((Direction::Sell, Tier::Tier1)), // opposite of the Buy position
        );
        assert_eq!(decisions[0].reason, ExitReason::Contradiction);
    }

    #[test]
    fn smt_agreement_does_not_trigger_exit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let decisions = evaluate_exits(
            &[position],
            dec!(1.1000),
            &[],
            now,
            Duration::minutes(15),
            Some((Direction::Buy, Tier::Tier1)), // same direction
        );
        assert!(decisions.is_empty());
    }

    #[test]
    fn risk_reward_exit_takes_priority_over_news_exit() {
        use rust_decimal_macros::dec;
        let position = sample_position(Direction::Buy, dec!(1.0950), dec!(1.1150));
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![NewsEvent {
            event_id: Uuid::new_v4(),
            timestamp: now + Duration::minutes(5),
            currency: "USD".to_string(),
            impact: domain::NewsImpact::High,
            description: "test".to_string(),
            actual: None,
            forecast: None,
            previous: None,
        }];
        let decisions = evaluate_exits(&[position], dec!(1.0940), &events, now, Duration::minutes(15), None);
        // Price also hit the stop loss; that should win over the news
        // exit that would otherwise also apply, since it's checked first.
        assert_eq!(decisions[0].reason, ExitReason::StopLoss);
    }
}
