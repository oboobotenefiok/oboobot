# Broker Layer

## The `BrokerAdapter` trait

Everything the rest of the daemon knows about brokers is expressed through one trait (`broker::adapter::BrokerAdapter`). The daemon's core logic (`daemon::main`) never references Deriv or Bybit by name: it holds a `Box<dyn BrokerAdapter>` and calls its methods. This is what makes the whole cycle testable against `MockBroker` without a network connection.

Required methods:

- `get_snapshot(&[String]) -> BrokerSnapshot`: current bid/ask and spread for a set of pairs, in one round trip.
- `submit_order(OrderRequest) -> Order`: a market order with stop-loss and take-profit attached.
- `cancel_order(Uuid)` / `close_position(Uuid)`: cancel a resting order, or close an open position.
- `get_account_equity() -> Usd`.
- `list_open_positions() -> Vec<Position>`: "the broker's own account of what's open right now," treated as the source of truth for reconciliation.
- `list_open_orders() -> Vec<Order>`.
- `capabilities() -> BrokerCapabilities`: which order types, hedging, netting, and partial closes this broker actually supports.

One method has a default implementation rather than being required:

- `fetch_historical_prices(pair, days) -> Vec<Decimal>`: defaults to `NotImplemented`. Used to backfill a fresh correlation window with historical daily closes instead of only ever learning from live observations. Not every broker/product combination can offer this, so adapters that can't simply don't override it.

## `DerivAdapter`

Talks to Deriv's WebSocket API (`wss://ws.derivws.com/websockets/v3`) directly: no SDK. The endpoint, symbol convention (`EURUSD` → `frxEURUSD`), and the `authorize → proposal → buy → portfolio → sell` flow were all confirmed against Deriv's current documentation and published API schemas while building this, not recalled from memory or assumed.

Trades exclusively Multiplier contracts (`MULTUP`/`MULTDOWN`), a Deriv-specific product with no direct forex-CFD equivalent: no resting limit orders (a `buy` either fills synchronously or is rejected outright), and stop-loss/take-profit are attached at purchase time via `limit_order`, not managed as separate order objects. This is why `cancel_order` and `list_open_orders` both report "there is nothing here to act on" rather than being unimplemented stubs: for this product, that's the honest, correct answer, not a gap.

`list_open_positions` needs two calls per open contract: `portfolio` for the list of contract IDs (which carries little detail), then `proposal_open_contract` per contract for live price, running profit, and the stop-loss/take-profit levels. A contract the account holds that this daemon didn't open (a synthetic index, a manual trade, some other Deriv product sharing the account) is filtered out rather than force-fit into a `Position` it doesn't actually describe.

Read-only requests (`portfolio`, `proposal_open_contract`, `ticks`, `proposal`, `balance`, `authorize`) retry with exponential backoff on `Timeout`, `ConnectionFailed`, or `RateLimited` errors, honoring Deriv's own suggested wait when it's given one. `buy` and `sell` never retry: if either reaches Deriv and executes but the response is what's lost, retrying risks placing or closing the same contract twice. A failure there propagates immediately, the same as before retry logic existed anywhere in this file, and reconciliation is what catches the resulting state if there's a real mismatch.

## `MockBroker`

A fully in-memory, scriptable implementation used across nearly every test in this workspace, including integration tests that exercise the full `strategy → risk → broker → daemon::recovery` pipeline together. Supports scripted responses and adversarial behavior (failures, delays) for testing recovery paths, not just happy-path flows.

## `BybitAdapter`

A documented stub. Every method returns `NotImplemented` with a message explaining what's missing, rather than a silent no-op. Building this out is a separate, focused piece of future work, not attempted as part of this round.
