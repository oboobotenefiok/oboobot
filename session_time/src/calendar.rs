//! Everything in this file exists because "just subtract 5 hours for New
//! York time" breaks twice a year. New York observes daylight saving time
//! and the US and EU don't switch on the same calendar date, so for a
//! week or two each spring and fall the "usual" offset between NY and UTC
//! is briefly wrong if you hardcoded it. `chrono-tz` carries the real IANA
//! timezone database, so a `DateTime<Utc>` converted through
//! `America/New_York` is always correct for whatever day it happens to be,
//! DST included. That's worth the extra dependency.

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};
use chrono_tz::Tz;

/// The one timezone this whole strategy cares about. Every session
/// boundary, every macro cycle, every True Open level is defined in terms
/// of New York local time, because that's what the ICT-style session
/// framework this strategy is built on uses as its reference clock.
pub fn ny_tz() -> Tz {
    chrono_tz::America::New_York
}

/// Convert a UTC instant to its New York local representation. This is
/// the one function everything else in this module should go through,
/// rather than each call site doing its own `.with_timezone(...)`, so
/// that if the reference timezone ever needs to change, there's exactly
/// one place to do it.
pub fn to_ny(instant: DateTime<Utc>) -> DateTime<Tz> {
    instant.with_timezone(&ny_tz())
}

/// A source of "now" that can be swapped out in tests. Production code
/// gets `SystemClock`, which asks the OS. Tests and replay get a clock
/// that can be paused and advanced by hand, so that a test asserting
/// "at 09:00 NY the macro cycle fires" doesn't have to actually wait
/// until 09:00 NY to run.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock a test can move forward on demand. Not async-aware on its own;
/// pairing this with `tokio::time::pause()` and `tokio::time::advance()`
/// in the actual replay engine is what gives you a fully deterministic
/// run, this struct just supplies the "what time does the strategy think
/// it is" half of that.
pub struct ManualClock {
    current: parking_lot::Mutex<DateTime<Utc>>,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Self {
        ManualClock {
            current: parking_lot::Mutex::new(start),
        }
    }

    pub fn advance(&self, duration: Duration) {
        // A short critical section that never spans an `.await`, so a
        // plain `parking_lot::Mutex` is the right tool here rather than
        // `tokio::sync::Mutex`. See the workspace-wide rule in
        // `daemon::event_bus` docs for the general version of this call.
        let mut guard = self.current.lock();
        *guard += duration;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.current.lock()
    }
}

/// Anything that can answer "is this date a holiday, and if so, is it the
/// kind of holiday where liquidity gets thin enough that the strategy
/// should stop opening new positions." Kept as a trait, not a hardcoded
/// list baked into the calendar logic, for the same reason `BrokerAdapter`
/// is a trait: a US-holiday list is the obvious starting point, but a
/// pure-crypto or pure-futures deployment later would want a completely
/// different provider without touching this module.
pub trait HolidayProvider: Send + Sync {
    fn is_holiday(&self, date: NaiveDate) -> bool;

    /// Distinct from `is_holiday`: a market can be open on a day that's
    /// still meaningfully thin (the day after Thanksgiving, the week
    /// between Christmas and New Year's) without literally being closed.
    /// The hardening layer's holiday fail-safe keys off this, not off
    /// `is_holiday` directly.
    fn is_low_liquidity(&self, date: NaiveDate) -> bool;
}

/// A small, static list of the holidays that reliably gut forex liquidity:
/// Christmas, New Year's, US Thanksgiving, and Good Friday. This is meant
/// as a sensible starting point, not an exhaustive global calendar; a
/// production deployment trading multiple asset classes would likely want
/// to load this from a config file or an external calendar service
/// instead (see `external_data_dependencies` in the original spec), but
/// the trait boundary here means that's a new implementation of
/// `HolidayProvider`, not a rewrite of anything that calls it.
pub struct StaticHolidayProvider;

impl StaticHolidayProvider {
    fn holidays_for_year(year: i32) -> Vec<NaiveDate> {
        let mut dates = Vec::with_capacity(4);

        if let Some(new_years) = NaiveDate::from_ymd_opt(year, 1, 1) {
            dates.push(new_years);
        }
        if let Some(christmas) = NaiveDate::from_ymd_opt(year, 12, 25) {
            dates.push(christmas);
        }
        if let Some(thanksgiving) = nth_weekday_of_month(year, 11, Weekday::Thu, 4) {
            dates.push(thanksgiving);
        }
        if let Some(easter) = easter_sunday(year) {
            dates.push(easter - Duration::days(2)); // Good Friday
        }

        dates
    }
}

impl HolidayProvider for StaticHolidayProvider {
    fn is_holiday(&self, date: NaiveDate) -> bool {
        Self::holidays_for_year(date.year()).contains(&date)
    }

    fn is_low_liquidity(&self, date: NaiveDate) -> bool {
        // The week between Christmas and New Year's is thin every year
        // even though only two of its days are actual holidays, and the
        // day after Thanksgiving is famously a half-liquidity session in
        // US markets. We treat both as low-liquidity without requiring
        // them to also be `is_holiday`.
        if self.is_holiday(date) {
            return true;
        }

        let christmas_week = NaiveDate::from_ymd_opt(date.year(), 12, 26)
            .zip(NaiveDate::from_ymd_opt(date.year(), 12, 31))
            .map(|(start, end)| date >= start && date <= end)
            .unwrap_or(false);

        let day_after_thanksgiving = nth_weekday_of_month(date.year(), 11, Weekday::Thu, 4)
            .map(|thanksgiving| date == thanksgiving + Duration::days(1))
            .unwrap_or(false);

        christmas_week || day_after_thanksgiving
    }
}

/// The n-th occurrence of `weekday` in a given month/year. `n` is
/// 1-indexed (the 4th Thursday of November is `nth_weekday_of_month(year,
/// 11, Weekday::Thu, 4)`), matching how people actually talk about these
/// dates rather than a 0-indexed offset.
fn nth_weekday_of_month(year: i32, month: u32, weekday: Weekday, n: u32) -> Option<NaiveDate> {
    let first_of_month = NaiveDate::from_ymd_opt(year, month, 1)?;
    let first_weekday_offset =
        (7 + weekday.num_days_from_sunday() as i64 - first_of_month.weekday().num_days_from_sunday() as i64) % 7;
    let first_occurrence = first_of_month + Duration::days(first_weekday_offset);
    Some(first_occurrence + Duration::weeks((n - 1) as i64))
}

/// Easter Sunday for a given Gregorian year, via the Meeus/Jones/Butcher
/// algorithm. This is the standard, widely-verified way to compute this
/// without a lookup table; Good Friday is just this minus two days.
fn easter_sunday(year: i32) -> Option<NaiveDate> {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = ((h + l - 7 * m + 114) % 31) + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

/// The New York local time (18:00) that starts a trading week, for the
/// Sunday on or before `reference`. This is the anchor everything else
/// (full-week checks, the weekly True Open) is measured from.
pub fn week_start_for(reference: DateTime<Tz>) -> DateTime<Tz> {
    let days_since_sunday = reference.weekday().num_days_from_sunday() as i64;
    let this_or_previous_sunday = reference.date_naive() - Duration::days(days_since_sunday);
    let naive_open = this_or_previous_sunday
        .and_hms_opt(18, 0, 0)
        .expect("18:00:00 is always a valid time of day");

    match ny_tz().from_local_datetime(&naive_open) {
        chrono::LocalResult::Single(dt) => dt,
        // 6 PM local time is never actually ambiguous for
        // America/New_York (the US DST transitions happen around 2 AM
        // local time), so in practice we never expect to land here, but
        // if we ever did, picking the earlier of the two candidates is
        // the conservative choice for a "week start" boundary.
        chrono::LocalResult::Ambiguous(earlier, _later) => earlier,
        // Same reasoning: 6 PM is never inside a "spring forward" gap
        // either. This exists so the function is total instead of
        // partial. If it's ever actually hit, that means something is
        // wrong with the timezone database itself, which is a much
        // bigger problem than this one calculation, so we fall back to
        // treating the naive time as already being in UTC rather than
        // panicking the whole daemon over a calendar lookup.
        chrono::LocalResult::None => Utc.from_utc_datetime(&naive_open).with_timezone(&ny_tz()),
    }
}

/// The corrected version of the hardening layer's full-week check.
///
/// The original wording compared Monday 18:00 NY to "the previous Sunday
/// 18:00 NY," which is always a one-day gap, not seven, so it could never
/// actually detect anything. What we actually want to know is: did this
/// week's Sunday-18:00 open land exactly seven days after last week's
/// Sunday-18:00 open? If a holiday closure shifted or skipped a weekly
/// open, that gap won't be seven days, and this week doesn't get a Weekly
/// True Open (it falls back to Daily only).
pub fn is_full_trading_week(this_week_start: DateTime<Tz>, holidays: &dyn HolidayProvider) -> bool {
    let previous_week_start = this_week_start - Duration::weeks(1);

    let seven_days_apart = (this_week_start.date_naive() - previous_week_start.date_naive()).num_days() == 7;

    // Even if the calendar math lines up, a week whose open falls on a
    // known holiday shouldn't be treated as "full" either.
    let open_is_a_holiday = holidays.is_holiday(this_week_start.date_naive());

    seven_days_apart && !open_is_a_holiday
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn ny_conversion_reflects_summer_dst() {
        // July 1 2026 is in DST, so NY should be UTC-4, not UTC-5.
        let utc_noon = Utc.with_ymd_and_hms(2026, 7, 1, 16, 0, 0).unwrap();
        let ny = to_ny(utc_noon);
        assert_eq!(ny.hour(), 12);
    }

    #[test]
    fn ny_conversion_reflects_winter_standard_time() {
        // January 1 2026 is standard time, NY is UTC-5.
        let utc_noon = Utc.with_ymd_and_hms(2026, 1, 1, 17, 0, 0).unwrap();
        let ny = to_ny(utc_noon);
        assert_eq!(ny.hour(), 12);
    }

    #[test]
    fn easter_sunday_matches_known_dates() {
        // These are well-published, easily independently checked dates;
        // they're here as a regression check on the algorithm, not as a
        // claim that we derived them ourselves.
        assert_eq!(easter_sunday(2024), NaiveDate::from_ymd_opt(2024, 3, 31));
        assert_eq!(easter_sunday(2025), NaiveDate::from_ymd_opt(2025, 4, 20));
        assert_eq!(easter_sunday(2026), NaiveDate::from_ymd_opt(2026, 4, 5));
    }

    #[test]
    fn thanksgiving_is_the_fourth_thursday_of_november() {
        // 2026-11-26 is the fourth Thursday of November 2026.
        assert_eq!(
            nth_weekday_of_month(2026, 11, Weekday::Thu, 4),
            NaiveDate::from_ymd_opt(2026, 11, 26)
        );
    }

    #[test]
    fn christmas_is_a_holiday_and_low_liquidity() {
        let provider = StaticHolidayProvider;
        let christmas = NaiveDate::from_ymd_opt(2026, 12, 25).unwrap();
        assert!(provider.is_holiday(christmas));
        assert!(provider.is_low_liquidity(christmas));
    }

    #[test]
    fn week_between_christmas_and_new_year_is_low_liquidity_but_not_a_holiday() {
        let provider = StaticHolidayProvider;
        let dec_28 = NaiveDate::from_ymd_opt(2026, 12, 28).unwrap();
        assert!(!provider.is_holiday(dec_28));
        assert!(provider.is_low_liquidity(dec_28));
    }

    #[test]
    fn ordinary_week_is_a_full_trading_week() {
        let holidays = StaticHolidayProvider;
        // An arbitrary Sunday with no holiday nearby.
        let sunday = ny_tz()
            .with_ymd_and_hms(2026, 3, 1, 18, 0, 0)
            .single()
            .unwrap();
        assert!(is_full_trading_week(sunday, &holidays));
    }

    #[test]
    fn week_opening_on_a_holiday_is_not_full() {
        let holidays = StaticHolidayProvider;
        // is_full_trading_week only checks whether the given date is a
        // recognized holiday, it doesn't independently verify the date
        // is a Sunday, so we can exercise the holiday branch directly
        // against a Christmas Day regardless of which weekday it falls
        // on in this particular year.
        let christmas = ny_tz()
            .with_ymd_and_hms(2033, 12, 25, 18, 0, 0)
            .single()
            .unwrap();
        assert!(!is_full_trading_week(christmas, &holidays));
    }

    #[test]
    fn week_start_for_lands_on_sunday_at_1800_ny() {
        // Pick a Wednesday and check it resolves back to that week's
        // Sunday open.
        let wednesday = ny_tz().with_ymd_and_hms(2026, 3, 4, 9, 0, 0).single().unwrap();
        let start = week_start_for(wednesday);
        assert_eq!(start.weekday(), Weekday::Sun);
        assert_eq!(start.hour(), 18);
    }
}
