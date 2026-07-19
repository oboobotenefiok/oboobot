//! `BybitAdapter`: honest about not being finished yet.
//!
//! It implements `BrokerAdapter` fully, so the `--broker` CLI flag and
//! the rest of the daemon's wiring already treat it as a first-class
//! option, not an afterthought bolted on later. What it doesn't do is
//! talk to Bybit: every method returns `BrokerError::NotImplemented`
//! with a message pointing at what real work is still needed. Bybit's
//! own API is HMAC-signed REST against `api.bybit.com/v5/...`, verified
//! current and versioned (V5 is the unified, current API generation)
//! while researching this, and is a good match for this daemon's
//! short-lived, one-shot-per-invocation deployment model, arguably a
//! simpler one to build than Deriv's stateful WebSocket flow, since
//! there's no persistent connection to establish and tear down every
//! five minutes. See `deriv.rs` for the real (not stubbed) adapter,
//! prioritized ahead of this one.
//!
//! The constructor still reads its real configuration shape from the
//! environment, so the *intended* config surface is visible and testable
//! even before the wire protocol is. Adding a third broker means writing
//! a new struct like this one and adding one arm to the `--broker` match
//! in `main.rs`, the same "extend by adding, don't modify what's already
//! there" shape as adding a new tracked asset pair.

use async_trait::async_trait;
use domain::{BrokerSnapshot, Order, OrderRequest, Position, Usd};
use uuid::Uuid;

use crate::adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};

fn not_implemented(broker: &str, method: &str) -> BrokerError {
    BrokerError::NotImplemented(format!(
        "{broker}::{method} — wire protocol not yet implemented, use --broker mock for a working run"
    ))
}

/// Configuration shape for a real Bybit connection: an API key/secret
/// pair used to HMAC-SHA256 sign every private REST request (headers
/// `X-BAPI-API-KEY`, `X-BAPI-TIMESTAMP`, `X-BAPI-RECV-WINDOW`,
/// `X-BAPI-SIGN`).
pub struct BybitAdapter {
    api_key: String,
    api_secret: String,
}

impl BybitAdapter {
    /// Reads `BYBIT_API_KEY` and `BYBIT_API_SECRET` from the environment.
    pub fn from_env() -> Result<Self, BrokerError> {
        let api_key = std::env::var("BYBIT_API_KEY").map_err(|_| {
            BrokerError::ConnectionFailed("BYBIT_API_KEY is not set".to_string())
        })?;
        let api_secret = std::env::var("BYBIT_API_SECRET").map_err(|_| {
            BrokerError::ConnectionFailed("BYBIT_API_SECRET is not set".to_string())
        })?;
        Ok(BybitAdapter { api_key, api_secret })
    }
}

#[async_trait]
impl BrokerAdapter for BybitAdapter {
    async fn get_snapshot(&self, _pairs: &[String]) -> Result<BrokerSnapshot, BrokerError> {
        tracing::debug!(
            api_key_present = %(!self.api_key.is_empty()),
            api_secret_present = %(!self.api_secret.is_empty()),
            "BybitAdapter::get_snapshot called on unimplemented stub"
        );
        Err(not_implemented("BybitAdapter", "get_snapshot"))
    }

    async fn submit_order(&self, _request: OrderRequest) -> Result<Order, BrokerError> {
        // A real implementation is a single signed POST to
        // /v5/order/create; no multi-step proposal dance needed here,
        // unlike Deriv.
        Err(not_implemented("BybitAdapter", "submit_order"))
    }

    async fn cancel_order(&self, _order_id: Uuid) -> Result<(), BrokerError> {
        Err(not_implemented("BybitAdapter", "cancel_order"))
    }

    async fn close_position(&self, _position_id: Uuid) -> Result<Order, BrokerError> {
        Err(not_implemented("BybitAdapter", "close_position"))
    }

    async fn get_account_equity(&self) -> Result<Usd, BrokerError> {
        Err(not_implemented("BybitAdapter", "get_account_equity"))
    }

    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError> {
        Err(not_implemented("BybitAdapter", "list_open_positions"))
    }

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError> {
        Err(not_implemented("BybitAdapter", "list_open_orders"))
    }

    fn capabilities(&self) -> BrokerCapabilities {
        BrokerCapabilities {
            market_orders: true,
            limit_orders: true,
            ioc_orders: true,
            fok_orders: true,
            partial_closes: true,
            hedging: false,
            netting: true,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: true,
            supports_oco: false,
            supports_gtc: true,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bybit_stub_returns_not_implemented_not_a_panic() {
        let adapter = BybitAdapter { api_key: "test".to_string(), api_secret: "test".to_string() };
        let result = adapter.get_snapshot(&["BTCUSDT".to_string()]).await;
        assert!(matches!(result, Err(BrokerError::NotImplemented(_))));
    }
}
