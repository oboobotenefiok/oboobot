//! One error enum per crate, each built with `thiserror`, is the pattern
//! this whole workspace follows (it's the same pattern bruh uses). The
//! idea is that every fallible function returns a `Result<T, SomeSpecific
//! Error>` rather than a stringly-typed error or a generic `anyhow::Error`,
//! so that callers who care can match on exactly what went wrong, and
//! callers who don't care can still just propagate it with `?`.
//!
//! `DomainError` covers the bottom layer: bad arithmetic, malformed
//! domain values, that kind of thing. Every crate above this one defines
//! its own error type and typically wraps `DomainError` as a variant via
//! `#[from]`, rather than every crate reaching all the way down and
//! matching on domain internals directly.

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum DomainError {
    #[error("coefficient {0} could not be converted to a Decimal (NaN or infinite?)")]
    InvalidCoefficient(f64),

    #[error("cannot compute a weighted average entry price from zero fill legs")]
    EmptyFillLegs,

    #[error("fill leg size must be strictly positive, got {0}")]
    NonPositiveFillSize(String),

    #[error("percent value {0} is outside the valid 0.0..=1.0 ratio range")]
    PercentOutOfRange(String),
}
