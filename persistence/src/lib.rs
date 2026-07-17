//! Generic, fsync-before-return, append-only cursor file storage. This
//! crate doesn't know what a Position or an Order is; `daemon::recovery`
//! is where cursor files full of `domain::Event`s get turned into actual
//! reconciled state against a live broker.

pub mod cursor;

pub use cursor::{CursorFile, PersistenceError};
