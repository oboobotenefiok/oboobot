//! The daily and session buffers SMT divergence is measured against
//! were, until now, a fixed offset around whatever the live price
//! happened to be at the moment `main.rs` ran — always centered, so
//! `detect_divergence` could never actually fire. This file is what
//! replaces that: a real running high and low, updated with whatever
//! price this invocation observed, persisted between invocations, and
//! reset at the right boundary.
//!
//! Two different "daily" concepts live in this workspace and shouldn't
//! be confused: the daily *buffer* here resets at 18:00 NY (the start
//! of the trading day, matching the Asian session open), while the
//! daily *True Open* (`session_time::true_open_capture`) anchors at
//! midnight NY. Different purposes, deliberately different clocks.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use session_time::next_ny_occurrence;

use crate::smt::BufferLevels;

const DAILY_RESET_HOUR_NY: u32 = 18;
const SESSION_BOUNDARY_HOURS_NY: [u32; 4] = [18, 0, 6, 12];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollingBuffer {
    pub high: Decimal,
    pub low: Decimal,
    pub resets_at: DateTime<Utc>,
}

impl RollingBuffer {
    pub fn start(price: Decimal, resets_at: DateTime<Utc>) -> Self {
        RollingBuffer {
            high: price,
            low: price,
            resets_at,
        }
    }

    /// Widen the buffer to include a newly observed price. Does nothing
    /// destructive: high only ever moves up, low only ever moves down,
    /// consistent with what "the day's range so far" means.
    pub fn observe(&mut self, price: Decimal) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.resets_at
    }

    pub fn as_buffer_levels(&self) -> BufferLevels {
        BufferLevels {
            low: self.low,
            high: self.high,
        }
    }
}

fn next_session_boundary(after: DateTime<Utc>) -> DateTime<Utc> {
    SESSION_BOUNDARY_HOURS_NY
        .iter()
        .map(|&hour| next_ny_occurrence(after, hour, None))
        .min()
        .unwrap_or(after)
}

/// Given whatever buffer (if any) is currently on file and a freshly
/// observed price, return the buffer that should be persisted next:
/// either the existing one widened to include the new price, or a brand
/// new one if the old one expired (or none existed yet).
pub fn update_daily_buffer(
    current: Option<RollingBuffer>,
    price: Decimal,
    now: DateTime<Utc>,
) -> RollingBuffer {
    match current {
        Some(mut buffer) if !buffer.is_expired(now) => {
            buffer.observe(price);
            buffer
        }
        _ => RollingBuffer::start(price, next_ny_occurrence(now, DAILY_RESET_HOUR_NY, None)),
    }
}

pub fn update_session_buffer(
    current: Option<RollingBuffer>,
    price: Decimal,
    now: DateTime<Utc>,
) -> RollingBuffer {
    match current {
        Some(mut buffer) if !buffer.is_expired(now) => {
            buffer.observe(price);
            buffer
        }
        _ => RollingBuffer::start(price, next_session_boundary(now)),
    }
}

/// How many recent spread samples the rolling average is computed over.
/// The original spec called for a 72-hour window; at this project's
/// five-minute invocation cadence that's roughly 864 samples, so this
/// caps a little above that rather than at an exact hour count, since
/// not every invocation necessarily lands inside a macro cycle where a
/// fresh spread gets recorded.
const MAX_SPREAD_SAMPLES: usize = 900;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpreadHistory {
    pub samples: Vec<Decimal>,
    /// When `record` last actually added a sample. Same reasoning as
    /// `CorrelationState::last_updated`: this struct only tracks its own
    /// history, not what counts as "too old" for the daemon overall.
    #[serde(default)]
    pub last_updated: Option<DateTime<Utc>>,
}

impl SpreadHistory {
    pub fn record(&mut self, spread: Decimal, now: DateTime<Utc>) {
        self.samples.push(spread);
        if self.samples.len() > MAX_SPREAD_SAMPLES {
            self.samples.remove(0);
        }
        self.last_updated = Some(now);
    }

    pub fn average(&self) -> Option<Decimal> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: Decimal = self.samples.iter().sum();
        Some(sum / Decimal::from(self.samples.len()))
    }

    /// Whether `current_spread` passes the filter: at or under the
    /// rolling average times `multiplier`. Always passes if there isn't
    /// enough history yet to have an average, since rejecting every
    /// trade until 72 hours of history accumulates would make the
    /// filter worse than not having one.
    pub fn passes_filter(&self, current_spread: Decimal, multiplier: Decimal) -> bool {
        match self.average() {
            Some(average) => current_spread <= average * multiplier,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use rust_decimal_macros::dec;

    #[test]
    fn a_fresh_buffer_starts_at_exactly_the_observed_price() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        assert_eq!(buffer.high, dec!(1.1000));
        assert_eq!(buffer.low, dec!(1.1000));
    }

    #[test]
    fn observing_a_higher_price_widens_the_high_only() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        let widened = update_daily_buffer(Some(buffer), dec!(1.1050), now + Duration::minutes(5));
        assert_eq!(widened.high, dec!(1.1050));
        assert_eq!(widened.low, dec!(1.1000));
    }

    #[test]
    fn observing_a_lower_price_widens_the_low_only() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let buffer = update_daily_buffer(None, dec!(1.1000), now);
        let widened = update_daily_buffer(Some(buffer), dec!(1.0950), now + Duration::minutes(5));
        assert_eq!(widened.high, dec!(1.1000));
        assert_eq!(widened.low, dec!(1.0950));
    }

    #[test]
    fn an_expired_buffer_resets_instead_of_widening() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let mut buffer = RollingBuffer::start(dec!(1.1000), now - Duration::minutes(1));
        buffer.observe(dec!(1.2000)); // a wide, stale range
        let fresh = update_daily_buffer(Some(buffer), dec!(1.1500), now);
        // Should have reset to center on the new price, not kept the
        // stale 1.1000-1.2000 range.
        assert_eq!(fresh.high, dec!(1.1500));
        assert_eq!(fresh.low, dec!(1.1500));
    }

    #[test]
    fn session_boundary_picks_the_nearest_of_the_four_hours() {
        // 12:30 NY should reset the session buffer at the next boundary,
        // which is 18:00 NY the same day.
        let ny_noon_thirty = session_time::ny_tz()
            .with_ymd_and_hms(2026, 3, 10, 12, 30, 0)
            .unwrap()
            .with_timezone(&Utc);
        let boundary = next_session_boundary(ny_noon_thirty);
        let boundary_ny = session_time::to_ny(boundary);
        use chrono::Timelike;
        assert_eq!(boundary_ny.hour(), 18);
    }

    #[test]
    fn spread_filter_passes_everything_with_no_history_yet() {
        let history = SpreadHistory::default();
        assert!(history.passes_filter(dec!(0.0050), dec!(1.5)));
    }

    #[test]
    fn spread_filter_rejects_a_spread_beyond_the_multiplied_average() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let mut history = SpreadHistory::default();
        for _ in 0..10 {
            history.record(dec!(0.0002), now);
        }
        // average 0.0002, multiplier 1.5 -> threshold 0.0003
        assert!(!history.passes_filter(dec!(0.0005), dec!(1.5)));
        assert!(history.passes_filter(dec!(0.00025), dec!(1.5)));
    }

    #[test]
    fn spread_history_caps_at_the_maximum_sample_count() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let mut history = SpreadHistory::default();
        for i in 0..(MAX_SPREAD_SAMPLES + 20) {
            history.record(Decimal::from(i as i64), now);
        }
        assert_eq!(history.samples.len(), MAX_SPREAD_SAMPLES);
    }

    #[test]
    fn recording_a_spread_stamps_last_updated() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let mut history = SpreadHistory::default();
        assert_eq!(history.last_updated, None);
        history.record(dec!(0.0002), now);
        assert_eq!(history.last_updated, Some(now));
    }
}
