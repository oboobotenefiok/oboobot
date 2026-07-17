A Rust, trait-based, event-driven trading daemon implementing an SMT
(Smart Money Technique) divergence strategy gated by True Open levels.

This README is honest about scope. Everything described as "implemented"
below actually compiles and is tested. Everything described as "deferred"
is deferred on purpose, with the reason stated.

## Layout

A real Cargo workspace, not one crate with folders, specifically so the
"no cyclic dependencies, only through traits" rule from the spec is
something the compiler enforces rather than something everyone has to
remember. Dependency direction only ever flows one way, top to bottom:

Note that "spec" here refers to the original document I used to create this bot and it CANNOT be found anywhere in this codebase. 

```
domain          <- shared types, newtypes, events, errors. Zero deps on
                    anything else in this workspace.
session_time    <- NY sessions, DST-correct time, holidays, macro cycles,
                    True Open. Depends only on domain.
broker          <- the BrokerAdapter trait, plus MockBroker with
                    injectable adversarial behaviors. Depends only on
                    domain.
strategy        <- SMT divergence detection and the True Open gate,
                    wired together. Depends on domain and session_time.
risk            <- position sizing, the multiplier cap, account-wide
                    gates. Depends only on domain.
persistence     <- append-only, fsync-before-return cursor files.
                    Depends only on domain.
daemon          <- health state machine, the two-channel event bus, the
                    scheduler, startup reconciliation, the assistant
                    boundary, and the binary that wires everything
                    together. Depends on all of the above.
```

## What's fully implemented and tested

- The domain model, with `Percent`, `Usd`, and `Coefficient` as distinct
  newtypes over `Decimal` (never a raw `f64` for anything money-shaped),
  and `apply_multiplier` as the one sanctioned way to combine a percent
  with a coefficient, so the multiplier cap can't be bypassed by a call
  site that forgot to check it.
- DST-correct New York session handling via `chrono-tz`, a holiday
  calendar with a fail-safe low-liquidity check, and the **corrected**
  full-week calculation (the original hardening layer's formula compared
  Monday 18:00 to "the previous Sunday," which is always a one-day gap;
  this version compares consecutive Sunday-18:00 opens to each other).
- The True Open weekly/daily gate, implemented as an explicit decision
  table rather than nested conditionals, resolving the "Daily is only
  consulted when Weekly is neutral" reading.
- SMT divergence detection across daily and session buffers, tiered into
  Tier 1, Tier 2, and Double.
- Position sizing with the Tuesday/Double-SMT multiplier cap, proven with
  a property-based test (`risk::sizing`) that checks the cap holds for
  randomly generated inputs, not just hand-picked examples.
- `MockBroker`, with scriptable failure modes including the exact
  devmind/Cognee failure class (a call that returns success-shaped but
  with nothing usable in it), used to actually exercise the retry and
  reconciliation logic rather than only ever seeing happy-path responses.
- Startup reconciliation that treats the broker's own reported state as
  authoritative and local persistence as an advisory cache, with tests
  covering both an orphaned local position and a broker-side position we
  never locally recorded.
- Append-only cursor persistence that will not return success from
  `append` until the write has been `fsync`'d.
- The health state machine, including the fix applied per review:
  `BrokerHeartbeatFailure` now maps to `ReadOnly` instead of `Degraded`.
- A real two-channel event bus (priority and ordinary) merged with a
  biased `select!`, replacing the original single-queue-with-a-label
  design that had no actual mechanism behind the word "priority."
- The `AssistantEngine` boundary: `Recommendation` is inert data with no
  method or conversion that could feed into `RiskConfig` or `StrategyEngine`
  automatically, and it isn't on the startup or shutdown critical path.
- `cargo run` against `MockBroker`, driving several synthetic macro
  cycles (a clean pass, a no-divergence cycle, a True-Open rejection),
  simulating a health failure blocking new entries, and simulating a full
  process restart that recovers state from disk and reconciles it against
  the broker.

## What's deliberately deferred Friday, July 17th 2026.

- **Real OANDA/MT5/IBKR/Binance/CME wire protocols.** `BrokerAdapter` is
  fully specified and `MockBroker` fully implements it, but no live broker
  adapter is included. Getting endpoint URLs, auth flows, and payload
  shapes right means confirming them against each broker's current live
  documentation, which isn't something to guess. Adding a real adapter means
  writing a new struct that implements `BrokerAdapter`; nothing else in
  the workspace needs to change.
- **Full AssistantEngine pattern detection.** `LoggingAssistant` is the
  only implementation, and it recommends nothing. The point of this stage
  was proving the safety boundary, not building the ML behind it.
- **Full Prometheus/OTLP export.** `tracing` is wired throughout for
  structured logs; wiring `metrics-exporter-prometheus` on top is a small,
  separate addition once there's an actual server to scrape it.
- **Cross-position currency and correlation exposure netting.**
  `risk::sizing` covers per-trade sizing and the account-wide loss/position
  count gates; netting exposure across multiple simultaneous positions
  needs the live correlation matrix and is called out in that module's
  own doc comment rather than being approximated.
- **The full six-file TOML config system.** Config is currently
  constructed directly in code (see `main.rs`); loading it from
  `settings.toml` / `risk.toml` / etc. with the validation rules from the
  original spec is straightforward to add on top of the existing
  `RiskConfig` struct but wasn't the focus of this pass.

## Running it

```
cargo run --bin smt-trader
```

Runs the demonstration harness described above against `MockBroker`. Logs
are structured; set `RUST_LOG=debug` for more detail.

```
cargo test --workspace
```

Runs every unit test, the property-based risk test, the persistence
round-trip tests, and the daemon's black-box integration test.


If you don't understand what's going on in here, you'll live a happier life just closing this repo and never coming back again --trust me.

With Love,

- Obot
