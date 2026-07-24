//! `BrokerAdapter` is the one seam in this whole daemon where "config
//! swap, no recompile" meets the reality that OANDA, MT5, IBKR, Binance,
//! and CME don't actually agree on much. The trait below tries to capture
//! the common shape (get prices, submit an order, ask what's open) without
//! pretending every broker's order model is identical.
//!
//! One deliberate change from the original sketch of this trait: instead
//! of a separate method per order type (`submit_market_order`,
//! `submit_limit_order`, ...), there's a single `submit_order` that takes
//! the whole `OrderRequest` and lets the adapter match on
//! `request.order_type` internally. `OrderRequest::order_type` already has
//! four variants (Market, Limit, IOC, FOK); a one-method-per-variant trait
//! would need a new trait method every time a new order type shows up,
//! which is exactly the kind of hardcoded-per-case growth this workspace
//! is trying to avoid everywhere else.

use async_trait::async_trait;
use domain::{BrokerSnapshot, Order, OrderRequest, Position, Usd};
use rust_decimal::Decimal;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum BrokerError {
    #[error("could not reach the broker: {0}")]
    ConnectionFailed(String),

    #[error("broker request timed out after {0}ms")]
    Timeout(u64),

    #[error("broker rate limit hit, retry after {0}ms")]
    RateLimited(u64),

    #[error("broker rejected the request: {0}")]
    Rejected(String),

    /// This is the exact failure class devmind hit with Cognee: the
    /// transport layer says "success" (a 200, a completed TLS handshake)
    /// but the response body is empty, truncated, or otherwise doesn't
    /// parse as the thing we asked for. Treating this as its own error
    /// variant, rather than silently accepting an empty response as "the
    /// order must have worked," is the whole point of having it.
    #[error("broker returned a malformed or empty response: {0}")]
    MalformedResponse(String),

    #[error("broker has no record of order/position {0}")]
    NotFound(Uuid),

    /// For adapters (OANDA, MT5) that exist as a real, honest skeleton in
    /// this workspace but whose actual wire protocol isn't wired up,
    /// because doing that correctly means confirming exact endpoint URLs,
    /// auth flows, and payload shapes against each broker's live docs,
    /// which isn't something to guess at. See `broker::stubs`.
    #[error("{0} is not yet implemented against a live broker")]
    NotImplemented(String),
}

/// What a given broker actually supports. This is a static, compiled-in
/// declaration per adapter, not a live negotiation protocol, since most
/// retail forex brokers don't expose a "what do you support" endpoint.
/// Calling it a capability *declaration* rather than *discovery* is more
/// honest about what's actually happening here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerCapabilities {
    pub market_orders: bool,
    pub limit_orders: bool,
    pub ioc_orders: bool,
    pub fok_orders: bool,
    pub partial_closes: bool,
    pub hedging: bool,
    pub netting: bool,
    pub native_stop_loss: bool,
    pub native_take_profit: bool,
    pub modify_orders: bool,
    pub supports_oco: bool,
    pub supports_gtc: bool,
}

#[async_trait]
pub trait BrokerAdapter: Send + Sync {
    /// One atomic, point-in-time price capture for every requested pair.
    /// Everything in a macro cycle should be evaluated against a single
    /// snapshot from this call, not repeated live-price lookups.
    async fn get_snapshot(&self, pairs: &[String]) -> Result<BrokerSnapshot, BrokerError>;

    /// Submit any kind of order. See the module docs for why this is one
    /// method instead of four.
    async fn submit_order(&self, request: OrderRequest) -> Result<Order, BrokerError>;

    async fn cancel_order(&self, order_id: Uuid) -> Result<(), BrokerError>;

    /// Close an existing open position at market. Distinct from
    /// `cancel_order`, which cancels a pending order that hasn't filled
    /// yet: this closes something that's already open, which is what
    /// exit-condition monitoring (risk-reward, pre-news, SMT
    /// contradiction) actually needs.
    async fn close_position(&self, position_id: Uuid) -> Result<Order, BrokerError>;

    async fn get_account_equity(&self) -> Result<Usd, BrokerError>;

    /// The broker's own account of what's open right now. This is what
    /// reconciliation calls on startup and after every reconnect; it's
    /// meant to be treated as the source of truth, with our local cursor
    /// files as an advisory cache rather than the other way around.
    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError>;

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError>;

    /// Historical daily closes for `pair`, oldest first, going back
    /// `days` days from now. Used to backfill a fresh correlation window
    /// so it doesn't have to learn purely from live observations; see
    /// `strategy::correlation`. Defaults to `NotImplemented` rather than
    /// an empty `Vec`, so "this broker/product combination genuinely has
    /// no historical data to offer" stays distinguishable from "the
    /// request succeeded but returned nothing." Adapters that can offer
    /// this override it; the default means the ones that can't don't
    /// need to change at all.
    async fn fetch_historical_prices(
        &self,
        pair: &str,
        days: u32,
    ) -> Result<Vec<Decimal>, BrokerError> {
        let _ = (pair, days);
        Err(BrokerError::NotImplemented(
            "fetch_historical_prices is not implemented for this broker".to_string(),
        ))
    }

    fn capabilities(&self) -> BrokerCapabilities;

    /// The escape hatch. If a broker needs to expose something no other
    /// broker has an equivalent of (CME contract months, IBKR's specific
    /// pacing rules), downcast through here instead of adding a method to
    /// this trait that only one implementation will ever use.
    fn as_any(&self) -> &dyn std::any::Any;
}
