//! A broker double that can misbehave on purpose. The whole point of
//! `MockBroker` isn't to be a happy-path stub, it's to let tests script
//! exactly the failure modes that matter most: a rejected order, a
//! timeout, a rate limit, a partial fill, and specifically the "the
//! request technically succeeded but the response is empty or garbage"
//! failure devmind actually hit against Cognee. If the daemon's retry and
//! reconciliation logic can't survive this broker being deliberately
//! difficult, it's not ready to survive a real one being difficult by
//! accident.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use domain::{
    BrokerSnapshot, ComponentStatus, FillLeg, Order, OrderRequest, OrderStatus, Position,
    PositionStatus, PriceQuote, Usd,
};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::adapter::{BrokerAdapter, BrokerCapabilities, BrokerError};

/// One scripted outcome for the next call to `submit_order`. Queued
/// responses are consumed in order (FIFO); once the queue is empty,
/// `MockBroker` falls back to filling every order completely at whatever
/// price the request asked for (or a synthetic price for market orders),
/// which is the "everything's fine" baseline the queue lets tests deviate
/// from on purpose.
#[derive(Debug, Clone)]
pub enum ScriptedResponse {
    ConnectionFailed,
    Timeout(u64),
    RateLimited(u64),
    Rejected(String),
    /// The devmind/Cognee failure mode: the call returns `Ok`, not an
    /// error, but the broker's own client code (imagine a real HTTP
    /// client parsing an empty body here) would have nothing usable to
    /// build an `Order` from. We model this directly as a
    /// `BrokerError::MalformedResponse` so it's indistinguishable, from
    /// the daemon's point of view, from what an unlucky real broker call
    /// would actually look like.
    MalformedResponse(String),
    PartialFill(Decimal),
}

struct MockBrokerState {
    equity: Usd,
    positions: std::collections::HashMap<Uuid, Position>,
    orders: std::collections::HashMap<Uuid, Order>,
    submit_order_script: VecDeque<ScriptedResponse>,
    synthetic_price: Decimal,
}

pub struct MockBroker {
    state: parking_lot::Mutex<MockBrokerState>,
    call_count: AtomicU64,
}

impl MockBroker {
    pub fn new(initial_equity: Usd, synthetic_price: Decimal) -> Self {
        MockBroker {
            state: parking_lot::Mutex::new(MockBrokerState {
                equity: initial_equity,
                positions: std::collections::HashMap::new(),
                orders: std::collections::HashMap::new(),
                submit_order_script: VecDeque::new(),
                synthetic_price,
            }),
            call_count: AtomicU64::new(0),
        }
    }

    /// Queue up the next `submit_order` call to behave a specific way.
    /// Calls consume the queue in order; test setup usually looks like
    /// `broker.queue_submit_order_response(...)` once per call it wants
    /// to control, then lets everything after that fall back to normal
    /// behavior.
    pub fn queue_submit_order_response(&self, response: ScriptedResponse) {
        self.state.lock().submit_order_script.push_back(response);
    }

    /// Convenience wrapper naming the exact devmind/Cognee failure mode
    /// directly, so a test reads as "simulate that specific incident"
    /// rather than "queue a MalformedResponse and hope the reader
    /// remembers why."
    pub fn simulate_silent_200_empty_body(&self) {
        self.queue_submit_order_response(ScriptedResponse::MalformedResponse(
            "200 OK with an empty response body".to_string(),
        ));
    }

    /// Directly remove a position from the broker's own state, simulating
    /// a broker that has no record of something our local persistence
    /// still thinks is open (the orphaned-position scenario
    /// reconciliation exists to catch).
    pub fn forget_position(&self, position_id: Uuid) {
        self.state.lock().positions.remove(&position_id);
    }

    pub fn insert_position(&self, position: Position) {
        self.state.lock().positions.insert(position.position_id, position);
    }

    pub fn how_many_calls(&self) -> u64 {
        self.call_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl BrokerAdapter for MockBroker {
    async fn get_snapshot(&self, pairs: &[String]) -> Result<BrokerSnapshot, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        // Lock, read what we need, drop the guard, then build the return
        // value. Nothing here spans an `.await`, so `parking_lot::Mutex`
        // is the right tool; if this method ever needed to `.await` while
        // still holding a lock, it would need `tokio::sync::Mutex`
        // instead. See `daemon::event_bus` for the fuller version of that
        // rule.
        let price = self.state.lock().synthetic_price;

        let mut prices = std::collections::BTreeMap::new();
        let mut spreads = std::collections::BTreeMap::new();
        for pair in pairs {
            prices.insert(
                pair.clone(),
                PriceQuote {
                    bid: price,
                    ask: price + Decimal::new(2, 5), // a small synthetic spread
                },
            );
            spreads.insert(pair.clone(), Decimal::new(2, 5));
        }

        Ok(BrokerSnapshot {
            snapshot_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            prices,
            spreads,
        })
    }

    async fn submit_order(&self, request: OrderRequest) -> Result<Order, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);

        let scripted = {
            let mut guard = self.state.lock();
            guard.submit_order_script.pop_front()
        };

        match scripted {
            Some(ScriptedResponse::ConnectionFailed) => {
                Err(BrokerError::ConnectionFailed("simulated connection failure".to_string()))
            }
            Some(ScriptedResponse::Timeout(ms)) => Err(BrokerError::Timeout(ms)),
            Some(ScriptedResponse::RateLimited(ms)) => Err(BrokerError::RateLimited(ms)),
            Some(ScriptedResponse::Rejected(reason)) => Err(BrokerError::Rejected(reason)),
            Some(ScriptedResponse::MalformedResponse(detail)) => {
                Err(BrokerError::MalformedResponse(detail))
            }
            Some(ScriptedResponse::PartialFill(filled_size)) => {
                let fill_price = request.price.unwrap_or_else(|| self.state.lock().synthetic_price);
                let order = Order {
                    order_id: request.order_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    position_id: None,
                    pair: request.pair.clone(),
                    side: request.side,
                    size: request.size,
                    filled_size,
                    price: fill_price,
                    status: OrderStatus::PartiallyFilled,
                    timestamp: Utc::now(),
                    last_update: Utc::now(),
                };
                self.state.lock().orders.insert(order.order_id, order.clone());
                Ok(order)
            }
            None => {
                // Normal path: fill completely at the requested price (or
                // the synthetic market price for a Market order that
                // didn't specify one).
                let fill_price = request.price.unwrap_or_else(|| self.state.lock().synthetic_price);
                let order = Order {
                    order_id: request.order_id,
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    position_id: None,
                    pair: request.pair.clone(),
                    side: request.side,
                    size: request.size,
                    filled_size: request.size,
                    price: fill_price,
                    status: OrderStatus::Filled,
                    timestamp: Utc::now(),
                    last_update: Utc::now(),
                };
                self.state.lock().orders.insert(order.order_id, order.clone());

                let position = Position {
                    position_id: Uuid::new_v4(),
                    trace_id: request.trace_id,
                    signal_id: request.signal_id,
                    pair: request.pair,
                    direction: request.side,
                    legs: vec![FillLeg {
                        price: fill_price,
                        size: request.size,
                        filled_at: Utc::now(),
                    }],
                    entry_price: fill_price,
                    current_price: fill_price,
                    unrealized_pnl: Decimal::ZERO,
                    realized_pnl: Decimal::ZERO,
                    entry_time: Utc::now(),
                    last_update: Utc::now(),
                    status: PositionStatus::Filled,
                    exit_reason: None,
                };
                self.state.lock().positions.insert(position.position_id, position);

                Ok(order)
            }
        }
    }

    async fn cancel_order(&self, order_id: Uuid) -> Result<(), BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.state.lock();
        if guard.orders.remove(&order_id).is_some() {
            Ok(())
        } else {
            Err(BrokerError::NotFound(order_id))
        }
    }

    async fn get_account_equity(&self) -> Result<Usd, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().equity)
    }

    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().positions.values().cloned().collect())
    }

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.state.lock().orders.values().cloned().collect())
    }

    fn capabilities(&self) -> BrokerCapabilities {
        // The mock claims to support everything, since its job is to let
        // the strategy and risk layers be tested without a capability
        // mismatch getting in the way. A real adapter (see `broker::stubs`)
        // reports its own honest, narrower set.
        BrokerCapabilities {
            market_orders: true,
            limit_orders: true,
            ioc_orders: true,
            fok_orders: true,
            partial_closes: true,
            hedging: true,
            netting: true,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: true,
            supports_oco: true,
            supports_gtc: true,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A basic health probe helper tests can use without pulling in the full
/// `daemon` crate: reports the mock as healthy as long as it's reachable
/// at all, which it always is, being in-process.
pub fn mock_health_status() -> domain::HealthStatus {
    domain::HealthStatus {
        component: "broker".to_string(),
        status: ComponentStatus::Healthy,
        latency_ms: 0.0,
        error: None,
        timestamp: Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{Direction, OrderType};
    use rust_decimal_macros::dec;

    fn sample_request() -> OrderRequest {
        OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: Direction::Buy,
            size: dec!(1.0),
            order_type: OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        }
    }

    #[tokio::test]
    async fn normal_submit_fills_completely_and_opens_a_position() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = broker.submit_order(sample_request()).await.unwrap();
        assert_eq!(order.status, OrderStatus::Filled);
        assert_eq!(order.filled_size, dec!(1.0));

        let positions = broker.list_open_positions().await.unwrap();
        assert_eq!(positions.len(), 1);
    }

    #[tokio::test]
    async fn scripted_malformed_response_surfaces_as_an_error_not_a_success() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        broker.simulate_silent_200_empty_body();

        let result = broker.submit_order(sample_request()).await;
        assert!(matches!(result, Err(BrokerError::MalformedResponse(_))));

        // Crucially: no position should have been opened. This is the
        // whole point of modeling the failure as an error instead of
        // quietly returning something Order-shaped: the daemon's
        // reconciliation logic gets an honest signal to retry against,
        // rather than a phantom fill it has to discover was never real.
        let positions = broker.list_open_positions().await.unwrap();
        assert_eq!(positions.len(), 0);
    }

    #[tokio::test]
    async fn scripted_responses_are_consumed_in_order() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        broker.queue_submit_order_response(ScriptedResponse::ConnectionFailed);
        broker.simulate_silent_200_empty_body();

        let first = broker.submit_order(sample_request()).await;
        assert!(matches!(first, Err(BrokerError::ConnectionFailed(_))));

        let second = broker.submit_order(sample_request()).await;
        assert!(matches!(second, Err(BrokerError::MalformedResponse(_))));

        // Queue is now empty, so this one should succeed normally.
        let third = broker.submit_order(sample_request()).await;
        assert!(third.is_ok());
    }

    #[tokio::test]
    async fn forget_position_simulates_a_broker_side_orphan() {
        let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = broker.submit_order(sample_request()).await.unwrap();
        let positions = broker.list_open_positions().await.unwrap();
        let position_id = positions[0].position_id;

        broker.forget_position(position_id);
        let positions_after = broker.list_open_positions().await.unwrap();
        assert_eq!(positions_after.len(), 0);
        // The order record itself is untouched; only the broker's
        // position bookkeeping was made to "forget," which is the
        // specific inconsistency reconciliation needs to detect.
        let orders = broker.list_open_orders().await.unwrap();
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, order.order_id);
    }
}
