//! `true_open.rs` has the gate logic (given a bias, does a trade pass).
//! This file is the other half: given a clock and whatever level (if
//! any) is currently on file, decide whether a fresh one needs
//! capturing, and build it.
//!
//! Daily True Open is captured at midnight NY. That's a named
//! assumption, not something confirmed against an external source the
//! way the WebSocket endpoint or symbol convention were: it's the
//! common ICT convention (the "midnight open"), distinct from the
//! weekly anchor, and it's what this project settles on absent a more
//! specific instruction. Weekly is captured Monday 18:00 NY, which the
//! True Open addendum specified directly.

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc, Weekday};

use crate::calendar::{to_ny, week_start_for, HolidayProvider};
use crate::true_open::{Timeframe, TrueOpenLevel};

pub const DAILY_CAPTURE_HOUR_NY: u32 = 0;
pub const WEEKLY_CAPTURE_HOUR_NY: u32 = 18;

/// The next instant, at or after `after`, that lands on `hour:00 NY`
/// local time — and, if `weekday` is given, that also falls on that
/// weekday. General enough to answer "next midnight NY," "next Monday
/// 18:00 NY," or "next 06:00 NY session boundary" with the same logic,
/// which is why it's public rather than private to this file: the
/// buffer-reset logic in `strategy` needs the same search.
pub fn next_ny_occurrence(after: DateTime<Utc>, hour: u32, weekday: Option<Weekday>) -> DateTime<Utc> {
    let ny_now = to_ny(after);
    let mut candidate_date = ny_now.date_naive();

    // If today's occurrence of this hour has already passed, start
    // looking from tomorrow instead.
    if ny_now.hour() >= hour {
        candidate_date += Duration::days(1);
    }

    if let Some(target_weekday) = weekday {
        while candidate_date.weekday() != target_weekday {
            candidate_date += Duration::days(1);
        }
    }

    let naive = candidate_date
        .and_hms_opt(hour, 0, 0)
        .expect("hour is always in 0..24, always a valid time of day");

    match crate::calendar::ny_tz().from_local_datetime(&naive) {
        chrono::LocalResult::Single(dt) => dt.with_timezone(&Utc),
        chrono::LocalResult::Ambiguous(earlier, _) => earlier.with_timezone(&Utc),
        // Same reasoning as week_start_for: midnight and 18:00 are never
        // actually inside a DST transition gap for America/New_York (the
        // transition itself happens around 2 AM local), so this is a
        // total-function fallback that in practice never triggers.
        chrono::LocalResult::None => after,
    }
}

/// Whether the level currently on file (if any) is still valid, or a
/// fresh capture is needed. `None` on file always means "needs capture,"
/// the same as an expired one.
pub fn needs_capture(now: DateTime<Utc>, current: Option<&TrueOpenLevel>) -> bool {
    match current {
        None => true,
        Some(level) => level.is_expired(now),
    }
}

/// Build a fresh level for `timeframe`, capturing `price` as of `now`.
/// For Weekly, returns `None` instead of a level when the current week
/// doesn't qualify for one (a partial week per
/// [`crate::calendar::is_full_trading_week`]) — callers should treat a
/// `None` weekly level as `Bias::Neutral` for the whole week, which is
/// exactly what handing the decision to Daily was always meant to mean.
pub fn capture_level(
    timeframe: Timeframe,
    symbol: &str,
    price: rust_decimal::Decimal,
    now: DateTime<Utc>,
    holidays: &dyn HolidayProvider,
) -> Option<TrueOpenLevel> {
    match timeframe {
        Timeframe::Weekly => {
            let ny = to_ny(now);
            let this_week_start = week_start_for(ny);
            if !crate::calendar::is_full_trading_week(this_week_start, holidays) {
                return None;
            }
            let expires_at = next_ny_occurrence(now, WEEKLY_CAPTURE_HOUR_NY, Some(Weekday::Mon));
            Some(TrueOpenLevel {
                timeframe,
                symbol: symbol.to_string(),
                level: price,
                set_at: now,
                expires_at,
            })
        }
        Timeframe::Daily => {
            let expires_at = next_ny_occurrence(now, DAILY_CAPTURE_HOUR_NY, None);
            Some(TrueOpenLevel {
                timeframe,
                symbol: symbol.to_string(),
                level: price,
                set_at: now,
                expires_at,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::StaticHolidayProvider;
    use chrono::TimeZone;
    use rust_decimal_macros::dec;

    #[test]
    fn needs_capture_is_true_with_nothing_on_file() {
        assert!(needs_capture(Utc::now(), None));
    }

    #[test]
    fn needs_capture_is_false_for_an_unexpired_level() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let level = TrueOpenLevel {
            timeframe: Timeframe::Daily,
            symbol: "EURUSD".to_string(),
            level: dec!(1.1000),
            set_at: now,
            expires_at: now + Duration::hours(1),
        };
        assert!(!needs_capture(now, Some(&level)));
    }

    #[test]
    fn needs_capture_is_true_for_an_expired_level() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let level = TrueOpenLevel {
            timeframe: Timeframe::Daily,
            symbol: "EURUSD".to_string(),
            level: dec!(1.1000),
            set_at: now - Duration::hours(2),
            expires_at: now - Duration::hours(1),
        };
        assert!(needs_capture(now, Some(&level)));
    }

    #[test]
    fn daily_capture_expires_at_the_next_midnight_ny() {
        let noon_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 10, 12, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Daily, "EURUSD", dec!(1.1000), noon_ny, &holidays).unwrap();
        let expires_ny = to_ny(level.expires_at);
        assert_eq!(expires_ny.hour(), 0);
        // Expiry should be later the same NY calendar day (midnight
        // rolling into the next date), i.e. within 24 hours out.
        assert!(level.expires_at > noon_ny);
        assert!(level.expires_at <= noon_ny + Duration::hours(24));
    }

    #[test]
    fn weekly_capture_expires_the_following_monday_at_1800_ny() {
        let tuesday_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 3, 9, 0, 0) // a Tuesday
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Weekly, "EURUSD", dec!(1.1000), tuesday_ny, &holidays).unwrap();
        let expires_ny = to_ny(level.expires_at);
        assert_eq!(expires_ny.weekday(), Weekday::Mon);
        assert_eq!(expires_ny.hour(), 18);
    }

    #[test]
    fn weekly_capture_returns_some_for_an_ordinary_week() {
        let tuesday_ny = crate::calendar::ny_tz()
            .with_ymd_and_hms(2026, 3, 3, 9, 0, 0)
            .unwrap()
            .with_timezone(&Utc);
        let holidays = StaticHolidayProvider;
        let level = capture_level(Timeframe::Weekly, "EURUSD", dec!(1.1000), tuesday_ny, &holidays);
        assert!(level.is_some());
    }
}
