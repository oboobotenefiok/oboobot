//! Money is the one place in this whole codebase where a silent type mix-up
//! actually costs something. A percent that gets treated as a coefficient,
//! or a dollar amount that gets treated as a ratio, is exactly the kind of
//! bug that compiles fine, passes a casual read-through, and then does the
//! wrong thing with real capital the first time it runs against live data.
//!
//! So instead of passing `f64` or `Decimal` around directly for anything
//! money-shaped, we wrap each meaning in its own type. It's a few extra
//! lines here so that a mixed-up call site becomes a compiler error instead
//! of a 2 AM incident.
//!
//! Quick note on naming: earlier drafts of this design called these
//! `Percent<T>`, `Usd<T>`, `Coefficient<T>` as if they were generic. In
//! practice each one only ever wraps one concrete type (Percent and Usd
//! wrap `Decimal`, Coefficient wraps `f64`), so making them generic would
//! just be an extra type parameter nobody ever varies. Plain newtypes are
//! simpler and just as safe, so that's what you'll find below.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::errors::DomainError;

/// A ratio between 0.0 and 1.0, representing something like "1.5% of
/// account equity." Internally this is stored as the raw fraction (1.5%
/// is stored as 0.015), not as a "percent out of 100" number, because that
/// keeps every downstream calculation (multiply by equity to get a dollar
/// figure) a plain multiplication instead of a divide-by-100 you have to
/// remember every time.
///
/// If you're coming from a config file that writes risk limits as "5.0"
/// meaning "5 percent," use [`Percent::from_percentage`] rather than
/// [`Percent::from_ratio`], so the /100 conversion happens in exactly one
/// place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Percent(Decimal);

impl Percent {
    /// Build a `Percent` from a raw fraction, where 0.05 means 5%.
    pub fn from_ratio(ratio: Decimal) -> Self {
        Percent(ratio)
    }

    /// Build a `Percent` from a "human config file" number, where 5.0
    /// means 5%. This is the constructor you want when reading
    /// `risk_max_percent = 5.0` out of a TOML file.
    pub fn from_percentage(percentage: Decimal) -> Self {
        Percent(percentage / Decimal::from(100))
    }

    /// The raw 0.0 to 1.0 fraction, for when you need to hand it to
    /// something outside this newtype system (serialization, logging).
    pub fn as_ratio(&self) -> Decimal {
        self.0
    }

    /// The smaller of two percentages. Used by [`crate::newtypes::apply_multiplier`]
    /// to enforce a hard cap without ever needing a raw comparison at the
    /// call site.
    pub fn min(self, other: Percent) -> Percent {
        if self.0 <= other.0 {
            self
        } else {
            other
        }
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Multiply back out to a "5.0%"-style display, purely for logs and
        // error messages. This never feeds back into a calculation, so
        // it's fine that it's a lossy-looking presentation format.
        write!(f, "{}%", self.0 * Decimal::from(100))
    }
}

/// A physical amount of currency (account equity, realized PnL, the dollar
/// size of a risk limit). Always `Decimal`, never `f64`, because we don't
/// want floating point rounding to accumulate across thousands of trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Usd(Decimal);

impl Usd {
    pub fn from_decimal(amount: Decimal) -> Self {
        Usd(amount)
    }

    pub fn as_decimal(&self) -> Decimal {
        self.0
    }

    pub fn zero() -> Self {
        Usd(Decimal::ZERO)
    }

    /// Turn an equity figure and a risk percent into a dollar risk amount.
    /// This is the one and only place a `Percent` is allowed to become a
    /// `Usd`, which is exactly what the newtype was for: you can't
    /// accidentally skip the conversion because there's only one way to
    /// do it.
    pub fn from_percent_of(equity: Usd, percent: Percent) -> Usd {
        Usd(equity.0 * percent.as_ratio())
    }

    pub fn checked_add(self, other: Usd) -> Option<Usd> {
        self.0.checked_add(other.0).map(Usd)
    }

    pub fn checked_sub(self, other: Usd) -> Option<Usd> {
        self.0.checked_sub(other.0).map(Usd)
    }
}

impl fmt::Display for Usd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.0)
    }
}

/// A strict multiplier, like the 2.0x cap on Tuesday-doubling or Double
/// SMT. This one stays `f64` rather than `Decimal` on purpose: it's a
/// small, well-behaved scalar (1.0, 1.5, 2.0) that never itself represents
/// money, only a ratio applied to money elsewhere, so f64's precision is
/// more than enough and we don't need Decimal's overhead for it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coefficient(f64);

impl Coefficient {
    pub fn new(value: f64) -> Self {
        Coefficient(value)
    }

    pub fn identity() -> Self {
        Coefficient(1.0)
    }

    pub fn as_f64(&self) -> f64 {
        self.0
    }

    /// Convert to a `Decimal` so it can be multiplied against a `Percent`.
    /// This can fail for pathological f64 values (NaN, infinity), which is
    /// exactly why `apply_multiplier` below returns a `Result` instead of
    /// doing this conversion with `.unwrap()`.
    fn to_decimal(self) -> Result<Decimal, DomainError> {
        Decimal::try_from(self.0).map_err(|_| DomainError::InvalidCoefficient(self.0))
    }
}

/// The only sanctioned way to combine a `Percent` with a `Coefficient`.
///
/// Deliberately, `Percent` and `Coefficient` do not implement `Mul` against
/// each other. If you want to double a risk percentage, you call this
/// function; you cannot write `percent * coefficient` and have it compile.
/// That's not an accident: the whole point of the multiplier-stacking rule
/// (Tuesday doubling and Double SMT doubling are mutually exclusive and
/// capped at 2.0x) is that the cap gets applied *every single time* a
/// multiplier is used, not just at the call sites someone remembered to
/// guard. Routing every multiplication through one function makes that
/// true by construction instead of by discipline.
pub fn apply_multiplier(
    base: Percent,
    coefficient: Coefficient,
    cap: Percent,
) -> Result<Percent, DomainError> {
    let coefficient_decimal = coefficient.to_decimal()?;
    let scaled = Percent::from_ratio(base.as_ratio() * coefficient_decimal);
    Ok(scaled.min(cap))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    // A handful of ordinary example-based tests live here to check the
    // obvious cases read the way we expect. The proptest suite in
    // `risk/src/sizing.rs` is what actually proves the cap holds for every
    // input, not just these examples; think of these as a readable sanity
    // check for a human skimming the file, not the real proof.

    #[test]
    fn from_percentage_matches_from_ratio() {
        let from_config = Percent::from_percentage(dec!(5.0));
        let from_raw = Percent::from_ratio(dec!(0.05));
        assert_eq!(from_config, from_raw);
    }

    #[test]
    fn apply_multiplier_respects_the_cap() {
        let base = Percent::from_percentage(dec!(2.0)); // 2%
        let doubling = Coefficient::new(2.0);
        let cap = Percent::from_percentage(dec!(3.0)); // cap lower than 2% * 2.0 = 4%

        // This is expected to be well behaved: cap conversion, ordinary
        // decimal, small f64. Panicking here would mean the newtype
        // arithmetic itself is broken, which is exactly the "completely
        // proven inevitable" case where a test unwrap is fine, since a
        // failure here is the test correctly failing, not a swallowed
        // error path in production code.
        let result = apply_multiplier(base, doubling, cap).unwrap();
        assert_eq!(result, cap);
    }

    #[test]
    fn apply_multiplier_passes_through_when_under_cap() {
        let base = Percent::from_percentage(dec!(1.0));
        let no_multiplier = Coefficient::identity();
        let cap = Percent::from_percentage(dec!(5.0));

        let result = apply_multiplier(base, no_multiplier, cap).unwrap();
        assert_eq!(result, base);
    }
}
