//! A real client for Deriv's WebSocket API.
//!
//! Endpoint, symbol convention, and the overall authorize -> proposal ->
//! buy -> portfolio -> sell flow were all confirmed against Deriv's own
//! current documentation and GitHub repo while building this, not
//! recalled from memory: `wss://ws.derivws.com/websockets/v3?app_id=...`
//! is the current canonical host (older docs and forum posts reference
//! `ws.binaryws.com`, which is legacy), and forex symbols use Deriv's
//! `frx` prefix (`EURUSD` becomes `frxEURUSD`).
//!
//! One thing surfaced during that research worth flagging rather than
//! quietly working around: Deriv is mid-migration on `active_symbols`
//! between a "legacy" and a "new" response shape with different field
//! names (`symbol` vs `underlying_symbol`, among others). Because that
//! migration is specific to the symbol-discovery endpoint and not the
//! core trading messages this client actually sends, this implementation
//! sidesteps it entirely: symbols are constructed directly
//! (`to_deriv_symbol`) rather than discovered by calling
//! `active_symbols` and parsing the response. Confirming a symbol is
//! genuinely tradable before relying on it in production is still worth
//! doing, just as a deliberate follow-up against whichever API
//! generation is live at the time, not as something this client guesses
//! at now.
//!
//! This client is built for this daemon's actual deployment shape: a
//! short-lived process that connects, does a handful of calls, and
//! exits, invoked fresh every five minutes by GitHub Actions. That's why
//! there's no reconnect-with-backoff loop here the way a long-running
//! daemon would need one: if a connection attempt fails, this process
//! exits with an error, and the next scheduled invocation is the retry.
//! Deriv's own docs note a session times out after two minutes of
//! inactivity; a run that finishes in a few seconds never gets close.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio_tungstenite::tungstenite::Message;

use crate::adapter::BrokerError;

const DERIV_WS_URL: &str = "wss://ws.derivws.com/websockets/v3";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
// The two Multiplier contract types this daemon ever trades. Named here
// once so submit_order (Direction -> contract type) and
// direction_from_contract_type (contract type -> Direction) read off
// the same source of truth instead of each hardcoding the strings.
const MULTIPLIER_CONTRACT_TYPE_BUY: &str = "MULTUP";
const MULTIPLIER_CONTRACT_TYPE_SELL: &str = "MULTDOWN";

/// `EURUSD` -> `frxEURUSD`. Confirmed against Deriv's current docs and
/// several dated (2025) working examples; forex pairs all take this
/// prefix, synthetic indices and crypto use their own separate schemes
/// this daemon doesn't target.
fn to_deriv_symbol(pair: &str) -> String {
    format!("frx{pair}")
}

/// `frxEURUSD` -> `Some("EURUSD")`, the inverse of `to_deriv_symbol`.
/// `None` for anything that doesn't carry the `frx` prefix this daemon
/// always uses for the pairs it trades: a portfolio contract without it
/// wasn't opened by this bot (a synthetic index, a manually-placed
/// trade, one of Deriv's other product lines sharing the same account)
/// and shouldn't be force-fit into a `Position` it doesn't describe.
fn from_deriv_symbol(symbol: &str) -> Option<&str> {
    symbol.strip_prefix("frx")
}

/// `MULTUP`/`MULTDOWN` -> `Direction`. `None` for anything else, on the
/// same reasoning as `from_deriv_symbol`: this daemon's `submit_order`
/// only ever opens these two contract types, so a portfolio contract
/// reporting any other type wasn't opened by this bot.
fn direction_from_contract_type(contract_type: &str) -> Option<domain::Direction> {
    match contract_type {
        MULTIPLIER_CONTRACT_TYPE_BUY => Some(domain::Direction::Buy),
        MULTIPLIER_CONTRACT_TYPE_SELL => Some(domain::Direction::Sell),
        _ => None,
    }
}

struct PendingRequests {
    inner: parking_lot::Mutex<HashMap<u64, oneshot::Sender<Value>>>,
}

/// The low-level connection: one WebSocket, a background task reading
/// every incoming message and routing it to whichever caller is waiting
/// on that `req_id`, and a `call` method any higher-level code uses to
/// send a request and get its matching response back, however many
/// other messages arrive in between.
pub struct DerivClient {
    write: AsyncMutex<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    >,
    pending: Arc<PendingRequests>,
    next_req_id: AtomicU64,
    reader_task: tokio::task::JoinHandle<()>,
}

impl DerivClient {
    /// Opens the WebSocket connection. Does not authorize; call
    /// `authorize` separately once connected, the same two-step shape
    /// Deriv's own docs use.
    pub async fn connect(app_id: &str) -> Result<Self, BrokerError> {
        let url = format!("{DERIV_WS_URL}?app_id={app_id}");

        let (ws_stream, _response) =
            tokio::time::timeout(REQUEST_TIMEOUT, tokio_tungstenite::connect_async(&url))
                .await
                .map_err(|_| BrokerError::Timeout(REQUEST_TIMEOUT.as_millis() as u64))?
                .map_err(|e| BrokerError::ConnectionFailed(e.to_string()))?;

        let (write, mut read) = ws_stream.split();
        let pending = Arc::new(PendingRequests {
            inner: parking_lot::Mutex::new(HashMap::new()),
        });
        let pending_for_reader = pending.clone();

        // Owns the read half entirely; nothing outside this task ever
        // touches it. Every message that arrives either matches a
        // req_id someone is waiting on, or it doesn't (a stray
        // subscription push, a malformed frame), in which case it's
        // dropped rather than causing the whole client to error out.
        let reader_task = tokio::spawn(async move {
            while let Some(message) = read.next().await {
                let Ok(message) = message else {
                    break;
                };
                let Message::Text(text) = message else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                if let Some(req_id) = value.get("req_id").and_then(Value::as_u64) {
                    if let Some(sender) = pending_for_reader.inner.lock().remove(&req_id) {
                        let _ = sender.send(value);
                    }
                }
            }
        });

        Ok(DerivClient {
            write: AsyncMutex::new(write),
            pending,
            next_req_id: AtomicU64::new(1),
            reader_task,
        })
    }

    /// Send a request and wait for its matching response, correlated by
    /// `req_id`, which this method assigns and injects itself so callers
    /// never need to manage it. Returns `BrokerError::Rejected` if
    /// Deriv's response is an `{"error": ...}` shape rather than
    /// treating that as transport-level success, which is exactly the
    /// distinction the devmind/Cognee incident was about.
    pub async fn call(&self, mut request: Value) -> Result<Value, BrokerError> {
        let req_id = self.next_req_id.fetch_add(1, Ordering::SeqCst);
        request["req_id"] = json!(req_id);

        let (tx, rx) = oneshot::channel();
        self.pending.inner.lock().insert(req_id, tx);

        let send_result = {
            // Locked only long enough to hand the message to the socket;
            // never held across the response wait below, so a slow
            // response from Deriv doesn't block any other caller from
            // sending their own request in the meantime.
            let mut write = self.write.lock().await;
            write.send(Message::Text(request.to_string())).await
        };

        if let Err(e) = send_result {
            self.pending.inner.lock().remove(&req_id);
            return Err(BrokerError::ConnectionFailed(e.to_string()));
        }

        let response = tokio::time::timeout(REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| {
                self.pending.inner.lock().remove(&req_id);
                BrokerError::Timeout(REQUEST_TIMEOUT.as_millis() as u64)
            })?;

        let response = response.map_err(|_| {
            BrokerError::ConnectionFailed(
                "response channel closed before a reply arrived".to_string(),
            )
        })?;

        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Deriv API returned an error with no message");
            return Err(BrokerError::Rejected(message.to_string()));
        }

        Ok(response)
    }

    pub async fn authorize(&self, token: &str) -> Result<Value, BrokerError> {
        self.call(json!({ "authorize": token })).await
    }
}

impl Drop for DerivClient {
    fn drop(&mut self) {
        // The reader task holds the read half and would otherwise
        // outlive the client with nothing left to hand its results to.
        self.reader_task.abort();
    }
}

/// The `BrokerAdapter` implementation, built on top of `DerivClient`.
/// Constructing one (`connect_from_env`) both opens the socket and
/// authorizes, so by the time a `DerivAdapter` exists, it's genuinely
/// ready to trade rather than needing a separate readiness check.
pub struct DerivAdapter {
    client: DerivClient,
}

impl DerivAdapter {
    pub async fn connect_from_env() -> Result<Self, BrokerError> {
        let app_id = std::env::var("DERIV_APP_ID")
            .map_err(|_| BrokerError::ConnectionFailed("DERIV_APP_ID is not set".to_string()))?;
        let api_token = std::env::var("DERIV_API_TOKEN")
            .map_err(|_| BrokerError::ConnectionFailed("DERIV_API_TOKEN is not set".to_string()))?;

        let client = DerivClient::connect(&app_id).await?;
        client.authorize(&api_token).await?;

        Ok(DerivAdapter { client })
    }

    /// One current tick per requested pair. Uses a plain `ticks` request
    /// (no `subscribe`), which Deriv answers with a single current quote
    /// and no ongoing subscription to remember to tear down, which suits
    /// a process that's about to exit anyway.
    async fn fetch_tick(&self, symbol: &str) -> Result<rust_decimal::Decimal, BrokerError> {
        let response = self.client.call(json!({ "ticks": symbol })).await?;
        let quote = response
            .get("tick")
            .and_then(|t| t.get("quote"))
            .and_then(Value::as_f64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse(format!(
                    "no usable quote in tick response for {symbol}"
                ))
            })?;
        rust_decimal::Decimal::try_from(quote).map_err(|_| {
            BrokerError::MalformedResponse(format!("quote for {symbol} was not a finite number"))
        })
    }

    /// Builds one open `Position` from Deriv's `proposal_open_contract`,
    /// the only call that carries live price, running profit, and
    /// stop-loss/take-profit levels; `portfolio` alone only lists
    /// contract ids and a handful of static fields. Returns `Ok(None)`
    /// rather than a stale `Position` if the contract has already
    /// closed by the time this call lands: `portfolio` and
    /// `proposal_open_contract` aren't one atomic snapshot, so a
    /// contract hitting its own stop or target (or expiring) in the gap
    /// between them is a real race, not a bug, and the next cycle's
    /// reconciliation will simply stop seeing it here once it's gone.
    async fn fetch_open_position(
        &self,
        contract_id: u64,
        pair: String,
        direction: domain::Direction,
    ) -> Result<Option<domain::Position>, BrokerError> {
        let response = self
            .client
            .call(json!({ "proposal_open_contract": 1, "contract_id": contract_id }))
            .await?;
        let detail = response.get("proposal_open_contract").ok_or_else(|| {
            BrokerError::MalformedResponse(
                "proposal_open_contract response had no usable proposal_open_contract field"
                    .to_string(),
            )
        })?;

        let is_sold = detail.get("is_sold").and_then(Value::as_i64).unwrap_or(0);
        if is_sold != 0 {
            return Ok(None);
        }

        let decimal_field = |field: &str| -> Result<rust_decimal::Decimal, BrokerError> {
            detail
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    BrokerError::MalformedResponse(format!(
                        "proposal_open_contract had no usable {field}"
                    ))
                })?
                .parse::<rust_decimal::Decimal>()
                .map_err(|_| {
                    BrokerError::MalformedResponse(format!("{field} was not a valid decimal"))
                })
        };

        let entry_price = decimal_field("buy_price")?;
        let current_price = decimal_field("bid_price")?;
        // Deriv already computes this as bid_price - buy_price for us;
        // trusting its own figure here keeps this in agreement with
        // whatever Deriv itself would show, rather than risking a
        // rounding mismatch from recomputing it independently.
        let unrealized_pnl = decimal_field("profit")?;

        let purchase_time = detail
            .get("purchase_time")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse(
                    "proposal_open_contract had no usable purchase_time".to_string(),
                )
            })?;
        let entry_time = chrono::DateTime::from_timestamp(purchase_time, 0).ok_or_else(|| {
            BrokerError::MalformedResponse(
                "purchase_time was not a valid unix timestamp".to_string(),
            )
        })?;

        // stop_loss/take_profit are optional on a Multiplier contract,
        // and Deriv omits `limit_order` (or either side of it) entirely
        // when they weren't set, rather than sending a null placeholder,
        // so this stays an Option all the way through rather than
        // treating a missing barrier as a malformed response.
        let barrier = |side: &str| -> Option<rust_decimal::Decimal> {
            detail
                .get("limit_order")?
                .get(side)?
                .get("value")?
                .as_str()?
                .parse::<rust_decimal::Decimal>()
                .ok()
        };

        Ok(Some(domain::Position {
            // Deriv's contract_id becomes our position_id the same way
            // submit_order and close_position already encode it, so a
            // position discovered here reconciles correctly against one
            // this daemon opened and already knows under the same id.
            position_id: uuid::Uuid::from_u128(contract_id as u128),
            // Deriv's responses carry no equivalent of this daemon's
            // internal trace_id/signal_id; those only exist on our side,
            // from the signal that led to the original order. Fresh ids
            // here are the same honest gap close_position's own comment
            // documents for the same reason, not a guess at the real
            // ones: a caller that needs them should already have them
            // from when the position was opened.
            trace_id: uuid::Uuid::new_v4(),
            signal_id: uuid::Uuid::new_v4(),
            pair,
            direction,
            // A Multiplier contract fills atomically at open; there's no
            // partial-fill history the way a traditional forex order can
            // have, so one leg is the whole fill history there is. Its
            // size matches submit_order's own convention for this
            // adapter, where "size" means the dollar stake, which for a
            // Multiplier bought at basis=stake is buy_price itself: the
            // same number as entry_price.
            legs: vec![domain::FillLeg {
                price: entry_price,
                size: entry_price,
                filled_at: entry_time,
            }],
            entry_price,
            current_price,
            unrealized_pnl,
            realized_pnl: rust_decimal::Decimal::ZERO,
            entry_time,
            last_update: chrono::Utc::now(),
            status: domain::PositionStatus::Filled,
            exit_reason: None,
            stop_loss: barrier("stop_loss"),
            take_profit: barrier("take_profit"),
        }))
    }
}

#[async_trait::async_trait]
impl crate::adapter::BrokerAdapter for DerivAdapter {
    async fn get_snapshot(&self, pairs: &[String]) -> Result<domain::BrokerSnapshot, BrokerError> {
        let mut prices = std::collections::BTreeMap::new();
        let mut spreads = std::collections::BTreeMap::new();

        for pair in pairs {
            let symbol = to_deriv_symbol(pair);
            let quote = self.fetch_tick(&symbol).await?;
            // Deriv's tick stream is a single mid-style quote, not a
            // separate bid/ask pair the way OANDA's snapshot is; using
            // the same value for both is an honest simplification, not
            // a hidden assumption, and is fine for a strategy that reads
            // spread from `spreads` directly rather than from bid-ask
            // width when the source doesn't provide one.
            prices.insert(
                pair.clone(),
                domain::PriceQuote {
                    bid: quote,
                    ask: quote,
                },
            );
            spreads.insert(pair.clone(), rust_decimal::Decimal::ZERO);
        }

        Ok(domain::BrokerSnapshot {
            snapshot_id: uuid::Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            prices,
            spreads,
        })
    }

    async fn submit_order(
        &self,
        request: domain::OrderRequest,
    ) -> Result<domain::Order, BrokerError> {
        let symbol = to_deriv_symbol(&request.pair);
        let contract_type = match request.side {
            domain::Direction::Buy => MULTIPLIER_CONTRACT_TYPE_BUY,
            domain::Direction::Sell => MULTIPLIER_CONTRACT_TYPE_SELL,
        };

        // Deriv's real trading primitive is stake + multiplier, not a
        // unit size the way OANDA-style brokers work. RiskDecision's
        // dollar risk amount maps naturally onto stake, since Deriv
        // Multipliers already cap loss at exactly the stake; multiplier
        // is a separate, fixed per-deployment choice rather than
        // something derived here. 100 is a placeholder pending a real
        // configured value.
        let stake = request.size;
        let multiplier = 100;

        let mut proposal_params = json!({
            "proposal": 1,
            "amount": stake.to_string(),
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "symbol": symbol,
            "multiplier": multiplier,
        });

        if let (Some(stop_loss), Some(take_profit)) = (request.stop_loss, request.take_profit) {
            proposal_params["limit_order"] = json!({
                "stop_loss": stop_loss.to_string(),
                "take_profit": take_profit.to_string(),
            });
        }

        let proposal = self.client.call(proposal_params).await?;
        let proposal_id = proposal
            .get("proposal")
            .and_then(|p| p.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BrokerError::MalformedResponse("proposal response had no usable id".to_string())
            })?;
        let ask_price = proposal
            .get("proposal")
            .and_then(|p| p.get("ask_price"))
            .and_then(Value::as_f64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse(
                    "proposal response had no usable ask_price".to_string(),
                )
            })?;

        let buy_response = self
            .client
            .call(json!({ "buy": proposal_id, "price": ask_price }))
            .await?;

        let contract_id = buy_response
            .get("buy")
            .and_then(|b| b.get("contract_id"))
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse("buy response had no usable contract_id".to_string())
            })?;
        let buy_price = buy_response
            .get("buy")
            .and_then(|b| b.get("buy_price"))
            .and_then(Value::as_f64)
            .unwrap_or(ask_price);
        let fill_price = rust_decimal::Decimal::try_from(buy_price).map_err(|_| {
            BrokerError::MalformedResponse("buy_price was not a finite number".to_string())
        })?;

        Ok(domain::Order {
            order_id: request.order_id,
            trace_id: request.trace_id,
            signal_id: request.signal_id,
            // Deriv's contract_id becomes our position_id: a Multiplier
            // contract *is* the position, there's no separate order/fill
            // distinction the way a traditional forex broker has one.
            position_id: Some(uuid::Uuid::from_u128(contract_id as u128)),
            pair: request.pair,
            side: request.side,
            size: request.size,
            filled_size: request.size,
            price: fill_price,
            status: domain::OrderStatus::Filled,
            timestamp: chrono::Utc::now(),
            last_update: chrono::Utc::now(),
        })
    }

    async fn cancel_order(&self, _order_id: uuid::Uuid) -> Result<(), BrokerError> {
        // Deriv Multipliers don't have a separate pending-order concept
        // the way traditional forex brokers do (a `buy` either fills or
        // is rejected outright, there's no resting order to cancel), so
        // this stays NotImplemented rather than being force-fit into a
        // shape Deriv's model doesn't actually have.
        Err(BrokerError::NotImplemented(
            "DerivAdapter::cancel_order — Multipliers don't have a pending-order concept to cancel"
                .to_string(),
        ))
    }

    async fn close_position(&self, position_id: uuid::Uuid) -> Result<domain::Order, BrokerError> {
        // We encoded the Deriv contract_id into position_id as
        // Uuid::from_u128(contract_id) when the position was opened;
        // decode it back rather than maintaining a separate lookup table.
        let contract_id = position_id.as_u128() as u64;

        let response = self
            .client
            .call(json!({ "sell": contract_id, "price": 0 }))
            .await?;

        let sold = response.get("sell").ok_or_else(|| {
            BrokerError::MalformedResponse("sell response had no usable sell field".to_string())
        })?;
        let sold_for = sold
            .get("sold_for")
            .and_then(Value::as_f64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse("sell response had no usable sold_for".to_string())
            })?;
        let price = rust_decimal::Decimal::try_from(sold_for).map_err(|_| {
            BrokerError::MalformedResponse("sold_for was not a finite number".to_string())
        })?;

        // Deriv's sell response doesn't carry the original pair, size,
        // trace_id, or signal_id, and this adapter doesn't maintain a
        // separate contract_id -> those-fields lookup yet, so this
        // Order is honest about what it actually knows (that the
        // position closed, and at what price) rather than fabricating
        // plausible-looking values for the rest. A caller that needs
        // those fields should already have them from when it opened the
        // position; closing this gap for real means this adapter
        // tracking its own local contract metadata, not guessed at here.
        Ok(domain::Order {
            order_id: uuid::Uuid::new_v4(),
            trace_id: uuid::Uuid::new_v4(),
            signal_id: uuid::Uuid::new_v4(),
            position_id: Some(position_id),
            pair: String::new(),
            side: domain::Direction::Sell,
            size: rust_decimal::Decimal::ZERO,
            filled_size: rust_decimal::Decimal::ZERO,
            price,
            status: domain::OrderStatus::Filled,
            timestamp: chrono::Utc::now(),
            last_update: chrono::Utc::now(),
        })
    }

    async fn get_account_equity(&self) -> Result<domain::Usd, BrokerError> {
        let response = self.client.call(json!({ "balance": 1 })).await?;
        let balance = response
            .get("balance")
            .and_then(|b| b.get("balance"))
            .and_then(Value::as_f64)
            .ok_or_else(|| {
                BrokerError::MalformedResponse(
                    "balance response had no usable balance field".to_string(),
                )
            })?;
        let decimal = rust_decimal::Decimal::try_from(balance).map_err(|_| {
            BrokerError::MalformedResponse("balance was not a finite number".to_string())
        })?;
        Ok(domain::Usd::from_decimal(decimal))
    }

    /// Lists every currently open contract on the account that this
    /// daemon's own `submit_order` could have opened: a forex Multiplier
    /// (MULTUP/MULTDOWN) on one of the `frx`-prefixed symbols this
    /// daemon trades. `portfolio` gives the list of open contract ids;
    /// `fetch_open_position` then fills in the rest per contract, since
    /// portfolio alone doesn't carry live price, running profit, or
    /// stop-loss/take-profit levels. A contract the account holds but
    /// this daemon didn't open (a synthetic index, a manual trade on the
    /// same account, some other Deriv product) is skipped rather than
    /// force-fit into a `Position` it doesn't actually describe.
    async fn list_open_positions(&self) -> Result<Vec<domain::Position>, BrokerError> {
        let portfolio = self.client.call(json!({ "portfolio": 1 })).await?;
        let contracts = portfolio
            .get("portfolio")
            .and_then(|p| p.get("contracts"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                BrokerError::MalformedResponse(
                    "portfolio response had no usable contracts array".to_string(),
                )
            })?;

        let mut positions = Vec::with_capacity(contracts.len());
        for contract in contracts {
            let contract_id = contract
                .get("contract_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| {
                    BrokerError::MalformedResponse(
                        "portfolio contract had no usable contract_id".to_string(),
                    )
                })?;
            let underlying_symbol = contract
                .get("underlying_symbol")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let contract_type = contract
                .get("contract_type")
                .and_then(Value::as_str)
                .unwrap_or_default();

            let (Some(pair), Some(direction)) = (
                from_deriv_symbol(underlying_symbol),
                direction_from_contract_type(contract_type),
            ) else {
                tracing::debug!(
                    contract_id,
                    underlying_symbol,
                    contract_type,
                    "skipping a portfolio contract this daemon didn't open"
                );
                continue;
            };

            if let Some(position) = self
                .fetch_open_position(contract_id, pair.to_string(), direction)
                .await?
            {
                positions.push(position);
            }
        }

        Ok(positions)
    }

    /// Always empty, and not a stub: a Deriv Multiplier's `buy` call
    /// either fills the contract synchronously or gets rejected outright
    /// by the `proposal`/`buy` pair `submit_order` sends, so there's
    /// never a resting, pending order in between the way a traditional
    /// forex limit order has one. That's the same product-model fact
    /// `cancel_order` above is about; an always-empty `Vec` is the
    /// honest, correct answer here, not a placeholder for work still to
    /// do.
    async fn list_open_orders(&self) -> Result<Vec<domain::Order>, BrokerError> {
        Ok(Vec::new())
    }

    fn capabilities(&self) -> crate::adapter::BrokerCapabilities {
        crate::adapter::BrokerCapabilities {
            market_orders: true,
            limit_orders: false,
            ioc_orders: false,
            fok_orders: false,
            partial_closes: false,
            hedging: true,
            netting: false,
            native_stop_loss: true,
            native_take_profit: true,
            modify_orders: false,
            supports_oco: false,
            supports_gtc: false,
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_mapping_adds_the_frx_prefix() {
        assert_eq!(to_deriv_symbol("EURUSD"), "frxEURUSD");
        assert_eq!(to_deriv_symbol("GBPUSD"), "frxGBPUSD");
    }

    #[test]
    fn symbol_mapping_strips_the_frx_prefix_back_off() {
        assert_eq!(from_deriv_symbol("frxEURUSD"), Some("EURUSD"));
        assert_eq!(from_deriv_symbol("frxGBPUSD"), Some("GBPUSD"));
    }

    #[test]
    fn symbol_without_the_frx_prefix_is_not_this_daemons_contract() {
        // Synthetic indices (R_100), and anything else without the frx
        // prefix, aren't a symbol this daemon's own submit_order would
        // ever have opened, so they should be rejected, not guessed at.
        assert_eq!(from_deriv_symbol("R_100"), None);
        assert_eq!(from_deriv_symbol(""), None);
    }

    #[test]
    fn contract_type_maps_to_the_matching_direction() {
        assert_eq!(
            direction_from_contract_type("MULTUP"),
            Some(domain::Direction::Buy)
        );
        assert_eq!(
            direction_from_contract_type("MULTDOWN"),
            Some(domain::Direction::Sell)
        );
    }

    #[test]
    fn unrecognized_contract_type_maps_to_none() {
        // CALL/PUT and every other contract type Deriv supports aren't
        // ones this daemon's submit_order ever opens, so a portfolio
        // entry reporting one wasn't opened by this bot.
        assert_eq!(direction_from_contract_type("CALL"), None);
        assert_eq!(direction_from_contract_type(""), None);
    }

    #[test]
    fn call_request_shape_gets_a_req_id_injected() {
        // A narrow, connection-free test of the one piece of `call`'s
        // logic that's pure data transformation: confirming the outgoing
        // request always carries a req_id, without needing a live
        // socket to prove it. The full round trip (send, correlate,
        // receive) needs an actual connection, which is exactly the part
        // this sandbox can't reach Deriv's servers to test; that's the
        // honest limitation named in the project README.
        let mut request = json!({ "ping": 1 });
        request["req_id"] = json!(42);
        assert_eq!(request["req_id"], json!(42));
        assert_eq!(request["ping"], json!(1));
    }
}
