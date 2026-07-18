//! The `BrokerAdapter` trait plus the mock implementation everything else
//! in this workspace gets tested against. There's no OANDA or MT5 wire
//! protocol implemented here; see the README at the workspace root for
//! why that's a deliberate scope decision rather than an oversight.

pub mod adapter;
pub mod deriv;
pub mod mock;
pub mod stubs;

pub use adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};
pub use deriv::{DerivAdapter, DerivClient};
pub use mock::{mock_health_status, MockBroker, ScriptedResponse};
pub use stubs::BybitAdapter;
