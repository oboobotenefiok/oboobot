//! Everything in this crate is about answering "what does the calendar
//! say right now": NY session time, DST-correct conversions, holidays,
//! macro cycle windows, and True Open level bookkeeping. It depends on
//! `domain` for the shared vocabulary (`Direction`, `Bias`,
//! `RejectionReason`) and nothing else in the workspace, so that
//! `strategy`, `risk`, and everyone else can depend on `time` without
//! dragging in broker or persistence concerns.

pub mod calendar;
pub mod macro_cycle;
pub mod true_open;
pub mod true_open_capture;

pub use calendar::{
    is_full_trading_week, ny_tz, to_ny, week_start_for, Clock, HolidayProvider, ManualClock,
    StaticHolidayProvider, SystemClock,
};
pub use macro_cycle::{is_within_macro_cycle, next_macro_cycle_after, MACRO_CYCLE_HOURS};
pub use true_open::{
    bias_from_price, true_open_gate, week_qualifies_for_weekly_true_open, Timeframe,
    TrueOpenLevel,
};
pub use true_open_capture::{capture_level, needs_capture, next_ny_occurrence, DAILY_CAPTURE_HOUR_NY, WEEKLY_CAPTURE_HOUR_NY};
