//! SMT divergence detection plus the True Open gate, wired together into
//! the one pipeline the daemon calls once per macro cycle. Depends on
//! `domain` for shared types and `session_time` for the gate logic and
//! calendar facts; knows nothing about brokers or persistence.

pub mod smt;

pub use smt::{
    detect_divergence, evaluate_smt, generate_signal, BufferLevels, DivergenceInputs,
    SignalOutcome,
};
