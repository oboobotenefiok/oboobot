//! A real, if simple, correlation tracker. Every invocation that
//! observes a fresh price pair records it; once enough samples exist,
//! `compute_coefficient` gives a genuine Pearson correlation over the
//! trailing window rather than a value read from config and trusted
//! forever. `detect_regime_shift` is what the original spec called
//! correlation regime-shift detection: has the live coefficient moved
//! far enough from a baseline to be worth flagging.
//!
//! Correlation is a quality signal, not a money figure, so this uses
//! `f64` throughout rather than `Decimal` — consistent with why
//! `Coefficient` in `domain::newtypes` made the same choice.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Bounds how far back the rolling window looks. Large enough to smooth
/// out single-invocation noise, small enough that a genuine regime
/// change (not just noise) shows up within a reasonable number of
/// five-minute-cadence invocations rather than being diluted by months
/// of stale history.
const MAX_SAMPLES: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CorrelationState {
    pub samples: Vec<(Decimal, Decimal)>,
    pub baseline_coefficient: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RegimeShift {
    pub baseline: f64,
    pub current: f64,
    pub deviation: f64,
}

/// Add a fresh (primary, secondary) price pair to the window, dropping
/// the oldest sample once `MAX_SAMPLES` is exceeded.
pub fn record_sample(mut state: CorrelationState, primary: Decimal, secondary: Decimal) -> CorrelationState {
    state.samples.push((primary, secondary));
    if state.samples.len() > MAX_SAMPLES {
        state.samples.remove(0);
    }
    state
}

/// The Pearson correlation coefficient over every sample currently in
/// the window. `None` until at least two samples exist, since
/// correlation is undefined for a single point.
pub fn compute_coefficient(state: &CorrelationState) -> Option<f64> {
    let n = state.samples.len();
    if n < 2 {
        return None;
    }

    let n_f = n as f64;
    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut sum_xy = 0.0;
    let mut sum_x2 = 0.0;
    let mut sum_y2 = 0.0;

    for (x, y) in &state.samples {
        // rust_decimal_macros isn't in scope here; to_f64 is the
        // standard conversion since correlation deliberately isn't
        // money math, see the module docs.
        let x = decimal_to_f64(*x);
        let y = decimal_to_f64(*y);
        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_x2 += x * x;
        sum_y2 += y * y;
    }

    let numerator = n_f * sum_xy - sum_x * sum_y;
    let denominator = ((n_f * sum_x2 - sum_x * sum_x) * (n_f * sum_y2 - sum_y * sum_y)).sqrt();

    if denominator == 0.0 {
        // Every sample had identical x (or identical y) values, so
        // variance is zero and correlation is mathematically undefined,
        // not a divide-by-zero bug to guard against elsewhere.
        return None;
    }

    Some(numerator / denominator)
}

/// Compare the current coefficient against a stored baseline. Returns
/// `None` if there isn't enough information yet (no baseline set, or
/// not enough samples for a current reading) or if the deviation is
/// under `threshold`. `threshold` is a fraction (0.20 means 20%),
/// supplied by the caller rather than hardcoded, matching the original
/// spec's configurable regime-shift threshold.
pub fn detect_regime_shift(state: &CorrelationState, threshold: f64) -> Option<RegimeShift> {
    let baseline = state.baseline_coefficient?;
    let current = compute_coefficient(state)?;
    let deviation = (current - baseline).abs();

    if deviation > threshold {
        Some(RegimeShift { baseline, current, deviation })
    } else {
        None
    }
}

fn decimal_to_f64(value: Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    // Correlation is a quality signal computed over price levels that
    // are always well within f64's precision budget (see
    // domain::newtypes for the same reasoning applied to raw ticks);
    // falling back to 0.0 on the essentially-unreachable conversion
    // failure case keeps this function total without needing to thread
    // a Result through every call site for something that isn't a money
    // calculation.
    value.to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fewer_than_two_samples_gives_no_coefficient() {
        let state = CorrelationState::default();
        assert_eq!(compute_coefficient(&state), None);

        let state = record_sample(state, dec!(1.1000), dec!(1.3000));
        assert_eq!(compute_coefficient(&state), None);
    }

    #[test]
    fn perfectly_correlated_series_gives_coefficient_near_one() {
        let mut state = CorrelationState::default();
        for i in 0..10 {
            let price = dec!(1.1000) + Decimal::new(i, 4);
            state = record_sample(state, price, price); // identical series
        }
        let coefficient = compute_coefficient(&state).unwrap();
        assert!((coefficient - 1.0).abs() < 0.0001);
    }

    #[test]
    fn perfectly_inversely_correlated_series_gives_coefficient_near_negative_one() {
        let mut state = CorrelationState::default();
        for i in 0..10 {
            let up = dec!(1.1000) + Decimal::new(i, 4);
            let down = dec!(1.1000) - Decimal::new(i, 4);
            state = record_sample(state, up, down);
        }
        let coefficient = compute_coefficient(&state).unwrap();
        assert!((coefficient - (-1.0)).abs() < 0.0001);
    }

    #[test]
    fn window_drops_the_oldest_sample_once_it_exceeds_the_cap() {
        let mut state = CorrelationState::default();
        for i in 0..(MAX_SAMPLES + 10) {
            state = record_sample(state, Decimal::from(i as i64), Decimal::from(i as i64));
        }
        assert_eq!(state.samples.len(), MAX_SAMPLES);
    }

    #[test]
    fn regime_shift_fires_only_past_the_threshold() {
        let mut state = CorrelationState { baseline_coefficient: Some(0.9), ..Default::default() };
        for i in 0..10 {
            let price = dec!(1.1000) + Decimal::new(i, 4);
            state = record_sample(state, price, price); // current coefficient ~1.0
        }
        // baseline 0.9, current ~1.0: deviation ~0.1, under a 0.2 threshold.
        assert!(detect_regime_shift(&state, 0.20).is_none());
        // but over a tighter 0.05 threshold, it should fire.
        assert!(detect_regime_shift(&state, 0.05).is_some());
    }
}
