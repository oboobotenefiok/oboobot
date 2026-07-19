//! Four smaller operational pieces that share a file because each one is
//! small on its own:
//!
//! - A kill switch: a file the bot checks for before doing anything new.
//!   Drop `PAUSED` into the state repo from a phone in ten seconds and
//!   new entries stop, no secret, no redeploy, no workflow edit needed.
//! - A decisions log: every signal this daemon ever evaluates, not just
//!   the ones that became trades, so "why didn't it trade at 09:00
//!   today" has a real answer instead of a guess from a scrolled-away
//!   log line.
//! - A status snapshot: the current state, overwritten each run,
//!   readable from a phone without a dashboard.
//! - The position-collision guard, which is also this project's
//!   idempotency protection: the original spec's entry_gates already
//!   called for "one trade per macro cycle per pair," which was never
//!   implemented. Enforcing it is the same check that also protects
//!   against a retried or overlapping workflow run double-entering the
//!   same signal, since a signal's id is freshly generated each
//!   evaluation and can't be used as a dedup key on its own.

use chrono::{DateTime, Duration, Utc};
use domain::Position;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// How close together two entries on the same pair are allowed to be.
/// Set just under the real macro-cycle spacing (three hours) rather than
/// exactly at it, so ordinary jitter in when a cycle actually gets
/// evaluated can't accidentally let a second entry slip through.
const MIN_MINUTES_BETWEEN_ENTRIES_SAME_PAIR: i64 = 170;

pub async fn kill_switch_engaged(state_dir: &Path) -> bool {
    tokio::fs::try_exists(state_dir.join("PAUSED")).await.unwrap_or(false)
}

/// Whether opening a new position on `pair` right now would collide with
/// one already entered this macro cycle. Checks every known position
/// (open or already closed), not just currently-open ones, since a
/// position that already opened and closed within the same window still
/// means this window already traded that pair.
pub fn already_entered_this_cycle(pair: &str, known_positions: &[Position], now: DateTime<Utc>) -> bool {
    known_positions.iter().any(|position| {
        position.pair == pair
            && (now - position.entry_time) < Duration::minutes(MIN_MINUTES_BETWEEN_ENTRIES_SAME_PAIR)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub timestamp: DateTime<Utc>,
    pub pair: String,
    pub outcome: String,
    pub detail: Option<String>,
}

impl DecisionRecord {
    pub fn new(pair: impl Into<String>, outcome: impl Into<String>) -> Self {
        DecisionRecord { timestamp: Utc::now(), pair: pair.into(), outcome: outcome.into(), detail: None }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusSnapshot {
    pub last_run: Option<DateTime<Utc>>,
    pub open_position_count: usize,
    pub health_state: String,
    pub last_decision: Option<String>,
    pub paused: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use domain::{Direction, FillLeg, PositionStatus};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn position_at(pair: &str, entry_time: DateTime<Utc>) -> Position {
        Position {
            position_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: pair.to_string(),
            direction: Direction::Buy,
            legs: vec![FillLeg { price: dec!(1.1000), size: dec!(1.0), filled_at: entry_time }],
            entry_price: dec!(1.1000),
            current_price: dec!(1.1000),
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            entry_time,
            last_update: entry_time,
            status: PositionStatus::Filled,
            exit_reason: None,
            stop_loss: None,
            take_profit: None,
        }
    }

    #[tokio::test]
    async fn kill_switch_is_off_by_default() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!kill_switch_engaged(dir.path()).await);
    }

    #[tokio::test]
    async fn kill_switch_engages_when_the_paused_file_exists() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("PAUSED"), "").await.unwrap();
        assert!(kill_switch_engaged(dir.path()).await);
    }

    #[test]
    fn recent_entry_on_the_same_pair_blocks_a_new_one() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("EURUSD", now - Duration::minutes(10))];
        assert!(already_entered_this_cycle("EURUSD", &known, now));
    }

    #[test]
    fn an_old_enough_entry_does_not_block_a_new_one() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("EURUSD", now - Duration::hours(4))];
        assert!(!already_entered_this_cycle("EURUSD", &known, now));
    }

    #[test]
    fn a_recent_entry_on_a_different_pair_does_not_block() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let known = vec![position_at("GBPUSD", now - Duration::minutes(10))];
        assert!(!already_entered_this_cycle("EURUSD", &known, now));
    }
}
