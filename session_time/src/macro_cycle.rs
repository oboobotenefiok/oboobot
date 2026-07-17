//! The strategy trades on eight fixed windows a day, three hours apart,
//! each one 20 minutes wide (10 minutes either side of the hour). Keeping
//! these evenly spaced, instead of the more organic ICT-style macro
//! windows some traders use, is a deliberate simplification: it makes the
//! schedule trivial to test and reason about, at the cost of not
//! perfectly matching every nuance of the underlying trading concept.
//! That trade-off was made when the strategy itself was designed, this
//! module just implements it.

use chrono::{DateTime, Duration, TimeZone};
use chrono_tz::Tz;

use crate::calendar::to_ny;

/// The NY-local hours a macro cycle centers on. `0` means midnight.
pub const MACRO_CYCLE_HOURS: [u32; 8] = [3, 6, 9, 12, 15, 18, 21, 0];

pub const MACRO_CYCLE_HALF_WIDTH_MINUTES: i64 = 10;

/// Whether `instant` falls inside any macro cycle's 20-minute window.
pub fn is_within_macro_cycle(instant: DateTime<chrono::Utc>) -> bool {
    let ny = to_ny(instant);
    MACRO_CYCLE_HOURS
        .iter()
        .any(|&hour| minutes_from_cycle_center(ny, hour) <= MACRO_CYCLE_HALF_WIDTH_MINUTES)
}

/// Minutes between `ny_time` and the nearest occurrence of `cycle_hour`
/// (today or spilling into yesterday/tomorrow), used both by
/// `is_within_macro_cycle` and by anything that wants to know "how close
/// are we" rather than just a yes/no.
fn minutes_from_cycle_center(ny_time: DateTime<Tz>, cycle_hour: u32) -> i64 {
    let today_center = ny_time
        .date_naive()
        .and_hms_opt(cycle_hour, 0, 0)
        .expect("cycle hours are always valid hours of the day");

    let naive_now = ny_time.naive_local();
    let diff_today = (naive_now - today_center).num_minutes().abs();

    // A cycle centered near midnight can be closer to "yesterday's
    // midnight" or "tomorrow's midnight" than to today's, depending on
    // which side of midnight `ny_time` falls on. Checking the adjacent
    // days too avoids an edge case where, say, 00:05 NY reports itself as
    // 5 minutes from a cycle 23 hours and 55 minutes away instead of the
    // actual 5-minute difference.
    let diff_previous_day = {
        let previous_day_center = today_center - Duration::days(1);
        (naive_now - previous_day_center).num_minutes().abs()
    };
    let diff_next_day = {
        let next_day_center = today_center + Duration::days(1);
        (naive_now - next_day_center).num_minutes().abs()
    };

    diff_today.min(diff_previous_day).min(diff_next_day)
}

/// The next macro cycle center at or after `instant`, in UTC. Used by the
/// scheduler to know how long to sleep before the next entry window.
pub fn next_macro_cycle_after(instant: DateTime<chrono::Utc>) -> DateTime<chrono::Utc> {
    let ny = to_ny(instant);
    let mut best: Option<DateTime<Tz>> = None;

    // Looking two calendar days ahead is enough headroom: even starting
    // from just after the 21:00 cycle, the next candidate (00:00 the
    // following day) is within that window, and we never need a third day
    // for an 8-cycles-per-day, 3-hour-apart schedule.
    for day_offset in 0..2 {
        let day = ny.date_naive() + Duration::days(day_offset);
        for &hour in MACRO_CYCLE_HOURS.iter() {
            let candidate_naive = day
                .and_hms_opt(hour, 0, 0)
                .expect("cycle hours are always valid hours of the day");
            if candidate_naive < ny.naive_local() {
                continue;
            }
            let candidate = crate::calendar::ny_tz()
                .from_local_datetime(&candidate_naive)
                .single()
                .unwrap_or(ny); // see note below

            best = Some(match best {
                Some(current_best) if current_best < candidate => current_best,
                _ => candidate,
            });
        }
    }

    // Falling back to `ny` itself in the (essentially never-hit, given
    // none of our cycle hours land in a DST transition) ambiguous case
    // just means we'd re-check the same instant on the next scheduler
    // tick rather than propagating an error out of what's meant to be a
    // simple "what's next" lookup.
    best.unwrap_or(ny).with_timezone(&chrono::Utc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn instant_at_cycle_center_is_within_the_cycle() {
        // 09:00:00 NY on an arbitrary day, converted to UTC by adding the
        // real NY-UTC offset rather than a hardcoded number, so this test
        // doesn't itself depend on knowing which side of a DST boundary
        // the date falls on.
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(9, 0, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn instant_far_from_any_cycle_is_not_within_a_cycle() {
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(!is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn midnight_boundary_is_handled_correctly() {
        // 23:55 NY should be recognized as 5 minutes from the 00:00
        // cycle, not (24 hours minus 5 minutes) from it.
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(23, 55, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);
        assert!(is_within_macro_cycle(utc_dt));
    }

    #[test]
    fn next_macro_cycle_after_finds_the_very_next_one() {
        let ny_naive = chrono::NaiveDate::from_ymd_opt(2026, 3, 10)
            .unwrap()
            .and_hms_opt(9, 30, 0)
            .unwrap();
        let ny_dt = crate::calendar::ny_tz().from_local_datetime(&ny_naive).unwrap();
        let utc_dt = ny_dt.with_timezone(&chrono::Utc);

        let next = next_macro_cycle_after(utc_dt);
        let next_ny = to_ny(next);
        assert_eq!(next_ny.hour(), 12);
    }
}
