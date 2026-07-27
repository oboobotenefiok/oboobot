![bruh's Signature Image](docs/images/oboobot.jpg)

**What happens when we isolate invariants rather than memorize patterns?**

This document is the README file for oboobot. It is a Rust program. The program is a trading daemon. It uses traits and events. It implements an SMT (Smart Money Technique) divergence strategy. The strategy uses True Open levels. The internal project name is QuarterlyTheory_SMT_Trader. The public project name and GitHub deployment name are oboobot.

> ## Risk Warning
> Trading foreign exchange, futures, CFDs, cryptocurrencies and other leveraged products carries high risk. You can lose some or all of your invested capital. These products may not suit all investors. Do not invest money that you cannot afford to lose. Consider your financial objectives, experience and risk appetite before trading. Seek independent financial advice if necessary.

> ## Educational Disclaimer
> This content is for information and education only. It does not give financial, investment or legal advice. No representation is made that the information is accurate, complete or current. Trading ideas, setups and strategies are hypothetical. They are not trade recommendations. They are not a solicitation to buy or sell any financial instrument.

> ## Hypothetical Results Disclaimer
> Hypothetical or simulated performance results have limitations. They do not represent real trading. No representation is made that any account will achieve similar profits or losses. Past performance does not indicate future results.

## Project Structure
oboobot is a Cargo workspace. The compiler enforces the rule of no cyclic dependencies. Dependencies flow in one direction only. The crates are as follows:

- domain: shared types, newtypes, events and errors.  
- session_time: NY sessions, DST-correct time, holidays, macro cycles, True Open gate logic and capture timing.  
- broker: BrokerAdapter trait, MockBroker with adversarial behaviours, Deriv WebSocket client and Bybit stub.  
- strategy: SMT divergence detection, True Open gate, rolling buffers, spread history and correlation tracking with regime-shift detection.  
- risk: position sizing, multiplier cap and account-wide gates.  
- persistence: append-only CursorFile and SnapshotFile for state.  
- daemon: health state machine, event bus, config loading, position monitoring, news-driven exits, notifications, kill switch, decisions log, status snapshot, idempotency guard, startup reconciliation, assistant boundary, CLI and binary.

## Changes in This Version
This version closes the remaining structural gaps named in the previous review, on top of the True Open and exit-monitoring work already described below:

- SMT divergence was previously one-directional: it only ever checked primary sweeping while secondary held, and always placed the resulting trade on primary regardless of which asset actually diverged. It now checks both directions and trades whichever asset held, matching the strategy's own symmetric rule (buy the stronger asset, sell the weaker one, regardless of which pair is labeled primary or secondary).  
- The real cycle previously evaluated only the first configured pair. It now iterates every pair-set in `config.pairs`, each with its own buffers, correlation window, spread history, and True Open bias. Account-wide checks (health, kill switch, holidays, the macro-cycle window, the broker snapshot itself) still run once per invocation, not once per pair-set.  
- Stop-loss placement now follows the strategy's actual rule: the previous cycle's high or low that was not taken out, on whichever asset is being traded. Take-profit is set at 3x that distance to preserve the already-documented 1:3 risk-reward ratio now that the stop is a variable distance rather than a fixed pip amount.  
- DerivAdapter's `list_open_positions` and `list_open_orders` are implemented (`portfolio` plus `proposal_open_contract` per contract; `list_open_orders` correctly returns empty always, since Multiplier contracts have no pending-order state).  
- Max exposure and correlation limits are enforced: net exposure per currency, and exposure to any other pair whose live correlation with a candidate signal is above a configurable threshold.  
- Freshness health checks are wired up: correlation staleness, spread-history staleness, snapshot latency, and a news-provider freshness hook.  
- Broker calls now retry with exponential backoff, but only read-only requests. `buy` and `sell` stay fail-fast, since retrying a mutating call whose response was lost risks executing it twice.  
- A fresh correlation window backfills 90 days of historical daily closes from Deriv before live data joins it, instead of only ever learning from live observations.  
- Reconciliation now also runs immediately after every fill (entry or exit), not only at startup.  
- SIGTERM is caught and logged, but never interrupts an in-flight cycle: an earlier attempt cancelled the running cycle outright, which risked abandoning a broker call mid-flight.  
- Assistant recommendations are now persisted to a cursor file, not just logged.  
- Config files are versioned (`version`, defaulting to 1 if absent); a version newer than the running build understands is rejected with a clear error instead of loading anyway.  
- A first-slice replay engine: `--replay-days N` steps the real cycle logic through N days of historical closes with a `ManualClock` and a `ReplayBroker` standing in for wall-clock time and a live connection, producing a `ReplayReport`. One pair-set only, fills exactly at each simulated day's close, no slippage model. See `docs/adr/0004-replay-engine-design.md`.

## Remaining Gaps
- Replay is a first slice: one pair-set, fills exactly at the historical bar's close, no slippage model, no parameter-sweep tooling. See `docs/adr/0004-replay-engine-design.md`.  
- NoNewsProvider is a deliberate stub with no real news source wired in; the pre-news exit exists but never fires.  
- BybitAdapter remains a stub.  
- GitHub Environments configuration (branch protection, required reviewers, environment secrets) is outside the code and needs setting up directly in the repository's GitHub settings.  
- Some wire-protocol coverage is incomplete for Deriv and Bybit beyond what this project actually uses (Deriv Multipliers, primarily).  
- Correlation and exposure tracking is per pair-set (a pair-set's own primary against its own secondary), not a full matrix across every configured pair-set's symbols.

## State Files
The system stores files in the state directory (--state-dir):  

- positions.cursor (append-only)  
- decisions.cursor (append-only)  
- recommendations.cursor (append-only)  
- buffer_daily_<PAIR>.json (one per pair, both primary and secondary of every configured pair-set)  
- buffer_session_<PAIR>.json (one per pair)  
- correlation_<PRIMARY>_<SECONDARY>.json (one per pair-set)  
- spread_history_<PRIMARY>.json (one per pair-set, keyed by that pair-set's primary)  
- true_open_weekly.json  
- true_open_daily.json  
- status.json (overwritten each run)  
- PAUSED (kill switch)  
- replay/ (only created by `--replay-days`; a self-contained copy of the above, isolated from real state, wiped clean at the start of every replay run)

## How to Run
Demo mode:  
`cargo run --bin oboobot -- --demo`

Real cycle with mock broker:  
`cargo run --bin oboobot -- --broker mock --state-dir ./state --force`

Full test suite:  
`cargo test --workspace`

## Deployment
Deployment uses GitHub Actions only. The workflow polls every 5 minutes. It gates execution on the macro cycle window. State persists to a separate repository via git.

## Note on Toolchain
The project builds with Rust 1.75.0. Cargo.lock pins compatible dependency versions.

## Further Documentation
- [Architecture](docs/architecture-spec.md), [Broker layer](docs/broker.md), [Strategy layer](docs/strategy.md), [Persistence layer](docs/persistence.md), [Events](docs/events.md)  
- [Architecture decision records](docs/adr/)  
- [Sequence diagrams](docs/diagrams/)  
- [JSON schemas](docs/schemas/) for config, Position, TradeSignal, and the status snapshot

## Previous Review Fixes
- Partial week calculation now compares consecutive Sunday 18:00 NY opens with a seven-day gap.  
- Broker heartbeat maps to ReadOnly state, consistent with other conditions.

This project uses professional trading concepts. Users without domain knowledge may prefer not to engage with the code.
