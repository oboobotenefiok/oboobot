# oboobot

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
This version closes two structural gaps:

- True Open level was previously hardcoded to Neutral in the real path. It now captures and persists real weekly (Monday 18:00 NY) and daily (midnight NY) levels with SnapshotFile. It re-captures levels when they expire. It calculates bias from the current price against the stored level.  
- The system did not monitor positions after opening. Every invocation now runs an exit sweep. The sweep checks risk-reward, pre-news and SMT-contradiction conditions. It closes qualifying positions with the new BrokerAdapter::close_position method. Entries are gated by the macro-cycle window. Exits are not gated.

Additional changes include:  
- Real rolling buffers replace fixed-offset placeholders. Daily buffers reset at 18:00 NY. Session buffers reset at session boundaries. Buffers persist between runs.  
- Real spread filter with rolling average. It uses risk.spread_multiplier. It passes until sufficient history exists.  
- Real correlation tracker with regime-shift detection. It uses Pearson correlation over a rolling window. It compares against a baseline and sends notifications on drift.  
- Holiday and low-liquidity check is now active. New entries skip on recognised holidays.  
- News provider trait and pre-news exit check. The current implementation (NoNewsProvider) returns no scheduled news. This is a deliberate safe default.  
- Real health checks for broker heartbeat, disk space and memory usage.  
- Single TOML configuration file with validation. It falls back to built-in defaults if the file is missing.  
- Notifications for Slack and Telegram via webhook. A Notifier trait supports demo and test modes.  
- Operational features: kill switch (PAUSED file), decisions log, status snapshot and position-collision guard for idempotency.  
- New close_position method on BrokerAdapter. Implemented for MockBroker and DerivAdapter.  
- Position type now stores stop_loss and take_profit values.

## Remaining Gaps
- The configuration supports multiple pairs, but the real cycle evaluates only the first pair.  
- Some DerivAdapter methods (list_open_positions, list_open_orders) are not implemented.  
- NoNewsProvider prevents the pre-news exit from activating.  
- BybitAdapter remains a stub.  
- SIGTERM handling was removed after testing issues in the sandbox.  
- GitHub Environments configuration is outside the code.  
- Replay and backtesting are not implemented.  
- Some wire-protocol coverage is incomplete for Deriv and Bybit.

## State Files
The system stores files in the state directory (--state-dir):  

- positions.cursor (append-only)  
- decisions.cursor (append-only)  
- buffer_daily_<PAIR>.json  
- buffer_session_<PAIR>.json  
- correlation.json  
- spread_history.json  
- true_open_weekly.json  
- true_open_daily.json  
- status.json (overwritten each run)  
- PAUSED (kill switch)

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

## Previous Review Fixes
- Partial week calculation now compares consecutive Sunday 18:00 NY opens with a seven-day gap.  
- Broker heartbeat maps to ReadOnly state, consistent with other conditions.

This project uses professional trading concepts. Users without domain knowledge may prefer not to engage with the code.
