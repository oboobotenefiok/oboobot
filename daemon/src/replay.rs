//! A `BrokerAdapter` that serves historical daily closes instead of a
//! live connection, so the real cycle logic in `main.rs` can run
//! unmodified against history instead of a live account. This is the
//! "first, minimal slice" scoped in `docs/adr/0004-replay-engine-design.md`:
//! one pair-set, no slippage model beyond filling exactly at the
//! historical bar's price, no partial fills. Extending beyond that is
//! future work, not attempted here.
//!
//! The one thing this file depends on from outside itself is timing:
//! nothing here advances its own notion of "now." A `ReplayBroker`'s
//! `current_index` only moves when the harness driving it calls
//! `advance()`, and that same harness is responsible for moving the
//! `session_time::ManualClock` the real cycle logic reads `now` from in
//! lockstep. This module has no opinion about how many simulated bars
//! make up a "cycle" or how the clock should advance between them; see
//! `main.rs`'s replay harness for that.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use broker::{BrokerAdapter, BrokerCapabilities, BrokerError};
use chrono::Utc;
use domain::{
    BrokerSnapshot, Direction, FillLeg, Order, OrderRequest, OrderStatus, Position, PositionStatus,
    PriceQuote, Usd,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A fixed synthetic bid/ask spread applied to every simulated quote,
/// since the historical data this broker replays is daily closes only
/// and carries no real spread history of its own. This is a documented
/// simplification, not a claim of realism: real spread varies by pair,
/// time of day, and market conditions in ways a fixed spread doesn't
/// capture. It's good enough for validating whether the strategy's own
/// logic behaves as intended against history; it is not good enough
/// for a precise P&L estimate, and this module doesn't pretend
/// otherwise.
const SYNTHETIC_SPREAD: Decimal = Decimal::from_parts(5, 0, 0, false, 5);

/// The part of a `ReplayBroker`'s state that needs to survive between
/// repeated constructions of one: every simulated open position, plus
/// running realized P&L. Persisted to a JSON file under the replay run's
/// own state directory, the same way a real broker's account state
/// lives externally to this process rather than inside the adapter
/// struct: `ReplayBroker::new` "reconnects" to this file exactly the
/// way `DerivAdapter::connect_from_env` doesn't need to remember
/// anything between invocations, since Deriv's servers hold the truth
/// there. Here, this file is that truth.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ReplayState {
    positions: Vec<Position>,
    realized_pnl_total: Decimal,
}

pub struct ReplayBroker {
    state_path: PathBuf,
    /// Chronological daily closes per pair. Every pair the replay run
    /// touches needs an entry here; `get_snapshot`/`submit_order` return
    /// `MalformedResponse` for a pair with none, or for an index past
    /// the end of its series, rather than silently fabricating a price.
    bars: BTreeMap<String, Vec<Decimal>>,
    current_index: Mutex<usize>,
    starting_equity: Usd,
}

impl ReplayBroker {
    pub fn new(
        state_path: PathBuf,
        bars: BTreeMap<String, Vec<Decimal>>,
        starting_equity: Usd,
    ) -> Self {
        ReplayBroker {
            state_path,
            bars,
            current_index: Mutex::new(0),
            starting_equity,
        }
    }

    /// The number of bars every configured pair has data for: the
    /// shortest of the individual series, so every symbol the harness
    /// might request always has a price at every index it steps
    /// through, rather than failing partway through a run because one
    /// pair's history happened to be a day shorter than another's.
    pub fn bar_count(&self) -> usize {
        self.bars
            .values()
            .map(|series| series.len())
            .min()
            .unwrap_or(0)
    }

    /// Moves "now" forward one simulated bar. Called by the replay
    /// harness between cycles, never by this broker itself; see the
    /// module doc for why this broker has no opinion of its own about
    /// when to advance.
    pub fn advance(&self) {
        let mut index = self
            .current_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *index += 1;
    }

    fn current_price(&self, pair: &str) -> Option<Decimal> {
        let index = *self
            .current_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.bars.get(pair)?.get(index).copied()
    }

    fn load_state(&self) -> ReplayState {
        std::fs::read_to_string(&self.state_path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
            .unwrap_or_default()
    }

    fn save_state(&self, state: &ReplayState) -> Result<(), BrokerError> {
        let content = serde_json::to_string_pretty(state).map_err(|error| {
            BrokerError::MalformedResponse(format!("failed to serialize replay state: {error}"))
        })?;
        std::fs::write(&self.state_path, content).map_err(|error| {
            BrokerError::ConnectionFailed(format!("failed to persist replay state: {error}"))
        })
    }
}

#[async_trait]
impl BrokerAdapter for ReplayBroker {
    async fn get_snapshot(&self, pairs: &[String]) -> Result<BrokerSnapshot, BrokerError> {
        let mut prices = BTreeMap::new();
        let mut spreads = BTreeMap::new();
        let half_spread = SYNTHETIC_SPREAD / Decimal::TWO;
        for pair in pairs {
            let close = self.current_price(pair).ok_or_else(|| {
                BrokerError::MalformedResponse(format!("no replay data for {pair} at this bar"))
            })?;
            prices.insert(
                pair.clone(),
                PriceQuote {
                    bid: close - half_spread,
                    ask: close + half_spread,
                },
            );
            spreads.insert(pair.clone(), SYNTHETIC_SPREAD);
        }
        Ok(BrokerSnapshot {
            snapshot_id: Uuid::new_v4(),
            timestamp: Utc::now(),
            prices,
            spreads,
        })
    }

    async fn submit_order(&self, request: OrderRequest) -> Result<Order, BrokerError> {
        let fill_price = self.current_price(&request.pair).ok_or_else(|| {
            BrokerError::MalformedResponse(format!(
                "no replay data for {} at this bar",
                request.pair
            ))
        })?;
        let position_id = Uuid::new_v4();
        let now = Utc::now();
        let position = Position {
            position_id,
            trace_id: request.trace_id,
            signal_id: request.signal_id,
            pair: request.pair.clone(),
            direction: request.side,
            legs: vec![FillLeg {
                price: fill_price,
                size: request.size,
                filled_at: now,
            }],
            entry_price: fill_price,
            current_price: fill_price,
            unrealized_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            entry_time: now,
            last_update: now,
            status: PositionStatus::Filled,
            exit_reason: None,
            stop_loss: request.stop_loss,
            take_profit: request.take_profit,
        };

        let mut state = self.load_state();
        state.positions.push(position);
        self.save_state(&state)?;

        Ok(Order {
            order_id: request.order_id,
            trace_id: request.trace_id,
            signal_id: request.signal_id,
            position_id: Some(position_id),
            pair: request.pair,
            side: request.side,
            size: request.size,
            filled_size: request.size,
            price: fill_price,
            status: OrderStatus::Filled,
            timestamp: now,
            last_update: now,
        })
    }

    async fn cancel_order(&self, _order_id: Uuid) -> Result<(), BrokerError> {
        Err(BrokerError::NotImplemented(
            "ReplayBroker has no resting orders to cancel; this daemon only ever submits market orders"
                .to_string(),
        ))
    }

    async fn close_position(&self, position_id: Uuid) -> Result<Order, BrokerError> {
        let mut state = self.load_state();
        let index = state
            .positions
            .iter()
            .position(|position| position.position_id == position_id)
            .ok_or_else(|| {
                BrokerError::Rejected(format!("no open replay position with id {position_id}"))
            })?;
        let position = state.positions.remove(index);

        let close_price = self
            .current_price(&position.pair)
            .unwrap_or(position.current_price);
        let direction_multiplier = match position.direction {
            Direction::Buy => Decimal::ONE,
            Direction::Sell => -Decimal::ONE,
        };
        let size: Decimal = position.legs.iter().map(|leg| leg.size).sum();
        // Matches MockBroker's own realized_pnl convention exactly,
        // rather than modeling Deriv's actual Multiplier payoff math:
        // this codebase's closest existing analog to a broker simulator
        // already treats "size" as scaling linearly with price change,
        // and there's no reason for this one to invent a different,
        // inconsistent simplification.
        let realized_pnl = (close_price - position.entry_price) * size * direction_multiplier;

        state.realized_pnl_total += realized_pnl;
        self.save_state(&state)?;

        let now = Utc::now();
        Ok(Order {
            order_id: Uuid::new_v4(),
            trace_id: position.trace_id,
            signal_id: position.signal_id,
            position_id: Some(position_id),
            pair: position.pair,
            side: match position.direction {
                Direction::Buy => Direction::Sell,
                Direction::Sell => Direction::Buy,
            },
            size,
            filled_size: size,
            price: close_price,
            status: OrderStatus::Filled,
            timestamp: now,
            last_update: now,
        })
    }

    async fn get_account_equity(&self) -> Result<Usd, BrokerError> {
        // Realized P&L only: a known simplification (see the module
        // doc), not a real-time mark-to-market of open positions. Risk
        // sizing during a replay run with a large open drawdown will
        // read a higher equity figure than a real broker would report
        // at that moment.
        let state = self.load_state();
        Ok(Usd::from_decimal(
            self.starting_equity.as_decimal() + state.realized_pnl_total,
        ))
    }

    async fn list_open_positions(&self) -> Result<Vec<Position>, BrokerError> {
        Ok(self.load_state().positions)
    }

    async fn list_open_orders(&self) -> Result<Vec<Order>, BrokerError> {
        Ok(Vec::new())
    }

    fn capabilities(&self) -> BrokerCapabilities {
        BrokerCapabilities {
            market_orders: true,
            limit_orders: false,
            ioc_orders: false,
            fok_orders: false,
            partial_closes: false,
            hedging: true,
            netting: false,
            native_stop_loss: false,
            native_take_profit: false,
            modify_orders: false,
            supports_oco: false,
            supports_gtc: false,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// The result of one replay run: what the strategy would have decided,
/// and how it would have performed, against the historical window it
/// was pointed at.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    pub pair_set: (String, String),
    pub bars_replayed: usize,
    pub starting_equity: Decimal,
    pub ending_equity: Decimal,
    pub trades_opened: usize,
    pub trades_closed: usize,
    pub decision_counts: BTreeMap<String, usize>,
}

impl ReplayReport {
    pub fn realized_pnl(&self) -> Decimal {
        self.ending_equity - self.starting_equity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn broker_with(bars: Vec<Decimal>) -> (tempfile::TempDir, ReplayBroker) {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("replay_state.json");
        let mut series = BTreeMap::new();
        series.insert("EURUSD".to_string(), bars);
        let broker = ReplayBroker::new(state_path, series, Usd::from_decimal(dec!(10000)));
        (dir, broker)
    }

    #[tokio::test]
    async fn get_snapshot_returns_the_current_bars_price_with_a_synthetic_spread() {
        let (_dir, broker) = broker_with(vec![dec!(1.1000), dec!(1.1050)]);
        let snapshot = broker.get_snapshot(&["EURUSD".to_string()]).await.unwrap();
        let quote = &snapshot.prices["EURUSD"];
        assert!(quote.bid < dec!(1.1000));
        assert!(quote.ask > dec!(1.1000));

        broker.advance();
        let snapshot = broker.get_snapshot(&["EURUSD".to_string()]).await.unwrap();
        let quote = &snapshot.prices["EURUSD"];
        assert!(quote.bid < dec!(1.1050) && quote.bid > dec!(1.1000));
    }

    #[tokio::test]
    async fn get_snapshot_past_the_end_of_the_series_is_a_malformed_response_not_a_panic() {
        let (_dir, broker) = broker_with(vec![dec!(1.1000)]);
        broker.advance();
        broker.advance();
        let result = broker.get_snapshot(&["EURUSD".to_string()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn submit_order_fills_at_the_current_bars_price_and_persists_the_position() {
        let (_dir, broker) = broker_with(vec![dec!(1.1000)]);
        let request = OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: Direction::Buy,
            size: dec!(100),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: Some(dec!(1.0950)),
            take_profit: Some(dec!(1.1150)),
            confirming_snapshot_id: Uuid::new_v4(),
        };

        let order = broker.submit_order(request).await.unwrap();
        assert_eq!(order.price, dec!(1.1000));

        let open = broker.list_open_positions().await.unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].entry_price, dec!(1.1000));
    }

    #[tokio::test]
    async fn close_position_realizes_pnl_and_updates_equity() {
        let (_dir, broker) = broker_with(vec![dec!(1.1000), dec!(1.1100)]);
        let request = OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: Direction::Buy,
            size: dec!(100),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        };
        let order = broker.submit_order(request).await.unwrap();
        let position_id = order.position_id.unwrap();

        broker.advance(); // price moves from 1.1000 to 1.1100
        broker.close_position(position_id).await.unwrap();

        let equity = broker.get_account_equity().await.unwrap();
        // (1.1100 - 1.1000) * 100 * 1 (Buy) = 1.0 profit
        assert_eq!(equity.as_decimal(), dec!(10001.0));

        let open = broker.list_open_positions().await.unwrap();
        assert!(open.is_empty());
    }

    #[test]
    fn bar_count_is_the_shortest_series_among_configured_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let mut series = BTreeMap::new();
        series.insert("EURUSD".to_string(), vec![dec!(1.10); 90]);
        series.insert("GBPUSD".to_string(), vec![dec!(1.25); 60]);
        let broker = ReplayBroker::new(
            dir.path().join("state.json"),
            series,
            Usd::from_decimal(dec!(10000)),
        );
        assert_eq!(broker.bar_count(), 60);
    }
}
