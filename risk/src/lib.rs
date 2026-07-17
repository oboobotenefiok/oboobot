//! Position sizing and the account-wide risk gates. Depends only on
//! `domain`; it doesn't know or care which broker is in use, which is
//! what lets it be property-tested in complete isolation from anything
//! broker- or network-shaped.

pub mod sizing;

pub use sizing::{DefaultRiskEngine, RiskConfig, RiskContext, RiskEngine, RiskError, RiskRejection};
