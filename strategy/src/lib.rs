//! SMT divergence detection plus the True Open gate, wired together into
//! the one pipeline the daemon calls once per macro cycle. Depends on
//! `domain` for shared types and `session_time` for the gate logic and
//! calendar facts; knows nothing about brokers or persistence.

pub mod buffers;
pub mod correlation;
pub mod smt;

pub use buffers::{update_daily_buffer, update_session_buffer, RollingBuffer, SpreadHistory};
pub use correlation::{compute_coefficient, detect_regime_shift, record_sample, CorrelationState, RegimeShift};
pub use smt::{
    detect_divergence, evaluate_smt, generate_signal, BufferLevels, DivergenceInputs,
    SignalOutcome, TradeTarget,
};
