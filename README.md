![bruh's Signature Image](docs/images/oboobot.jpg)
    
A Rust, trait-based, event-driven trading daemon implementing an SMT
(Smart Money Technique) divergence strategy gated by True Open levels —
internally still called `QuarterlyTheory_SMT_Trader` in a few places,
since that's the strategy's own name, but the project and its GitHub
Actions deployment are `oboobot`.

This README is honest about scope. Everything described as "implemented"
below actually compiles and is tested. Everything described as "deferred"
is deferred on purpose, with the reason stated, not silently skipped.

Before I continue:

> [!CAUTION]
> **RISK WARNING**
>
> Trading foreign exchange, futures, CFDs, cryptocurrencies, and other leveraged products carries a high level of risk and may not be suitable for all investors. You could sustain a loss of some or all of your invested capital. You should not invest money you cannot afford to lose. Before trading, carefully consider your financial objectives, level of experience, and risk appetite. Seek independent financial advice if necessary.

> [!WARNING]
> **EDUCATIONAL DISCLAIMER**
>
> The content provided is for informational and educational purposes only and does not constitute financial, investment, or legal advice. No representation is being made that any information provided is accurate, complete, or up to date. Trading ideas, setups, and strategies are hypothetical and should not be considered as trade recommendations or solicitation to buy or sell any financial instrument.

> [!NOTE]
> **HYPOTHETICAL / SIMULATED RESULTS DISCLAIMER**
>
> Hypothetical or simulated performance results have certain inherent limitations. Unlike actual trading records, simulated results do not represent real trading. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed. Past performance is not indicative of future results.



Now that that is said and done:




## Layout

A real Cargo workspace, not one crate with folders, specifically so the
"no cyclic dependencies, only through traits" rule from the spec is
something the compiler enforces rather than something everyone has to
remember. Dependency direction only ever flows one way, top to bottom:

```
domain          <- shared types, newtypes, events, errors. Zero deps on
                    anything else in this workspace.
session_time    <- NY sessions, DST-correct time, holidays, macro cycles,
                    True Open. Depends only on domain.
broker          <- the BrokerAdapter trait, MockBroker with injectable
                    adversarial behaviors, and honest DerivAdapter /
                    BybitAdapter stubs. Depends only on domain.
strategy        <- SMT divergence detection and the True Open gate,
                    wired together. Depends on domain and session_time.
risk            <- position sizing, the multiplier cap, account-wide
                    gates. Depends only on domain.
persistence     <- append-only, fsync-before-return cursor files.
                    Depends only on domain.
daemon          <- health state machine, the two-channel event bus, the
                    scheduler, startup reconciliation, the assistant
                    boundary, the CLI, and the binary itself (`oboobot`).
                    Depends on all of the above.
```

## What's fully implemented and tested

- The domain model, with `Percent`, `Usd`, and `Coefficient` as distinct
  newtypes over `Decimal` (never a raw `f64` for anything money-shaped),
  and `apply_multiplier` as the one sanctioned way to combine a percent
  with a coefficient, so the multiplier cap can't be bypassed by a call
  site that forgot to check it.
- DST-correct New York session handling, a holiday calendar with a
  fail-safe low-liquidity check, and a corrected full-week calculation
  (consecutive Sunday-18:00 opens compared to each other, not the
  original's always-wrong Monday-vs-Sunday math).
- The True Open weekly/daily gate as an explicit decision table.
- SMT divergence detection across daily and session buffers, tiered into
  Tier 1, Tier 2, and Double.
- Position sizing with the Tuesday/Double-SMT multiplier cap, proven with
  a property-based test that checks the cap holds for randomly generated
  inputs, not just hand-picked examples.
- `MockBroker`, with scriptable failure modes including the exact
  devmind/Cognee failure class (a call that returns success-shaped but
  with nothing usable in it), used to actually exercise retry and
  reconciliation logic.
- Startup reconciliation that treats the broker's own reported state as
  authoritative and local persistence as an advisory cache.
- Append-only cursor persistence that will not return success from
  `append` until the write has been `fsync`'d.
- The health state machine, including the fix applied per review:
  `BrokerHeartbeatFailure` maps to `ReadOnly`, not `Degraded`.
- A real two-channel event bus (priority and ordinary) merged with a
  biased `select!`.
- A real `DerivAdapter` speaking Deriv's actual WebSocket protocol:
  connect, authorize, one-shot tick fetches, and the full
  proposal-then-buy flow for Multipliers contracts (`stake` sourced
  directly from `RiskDecision.risk_currency`, since Deriv already caps
  loss at exactly the stake — a close match to what that field already
  means). Endpoint (`wss://ws.derivws.com/websockets/v3`) and the `frx`
  symbol prefix were both confirmed against Deriv's own current docs and
  GitHub repo, not recalled from memory. `list_open_positions`,
  `list_open_orders`, and `cancel_order` are still honest
  `NotImplemented` stubs — see below for why. This sandbox can't reach
  Deriv's servers at all (its network is allowlisted to package
  registries only), so what's actually verified here is that every
  message this client builds is correctly shaped and that the whole
  workspace still compiles and passes 67 tests with it wired in — not
  that a live connection succeeds. That first real connectivity test
  needs to happen either in GitHub Actions or from a machine with real
  network access to Deriv.
- The `AssistantEngine` boundary: `Recommendation` is inert data with no
  method or conversion that could feed into `RiskConfig` or
  `StrategyEngine` automatically.
- A real CLI (`--broker`, `--state-dir`, `--force`, `--demo`). The
  default (non-demo) path checks whether it's inside a macro cycle
  window *before touching the broker at all*, exits immediately if not,
  and otherwise reconciles and runs exactly one cycle. This is what the
  GitHub Actions workflow below actually invokes.
- `.github/workflows/ci.yml` (build, test, fmt, clippy on every push)
  and `.github/workflows/trading.yml` (polls every 5 minutes, lets the
  binary self-gate on the window, persists state to a separate
  `oboobot_report` repo via a git commit + push rather than GitHub
  Actions artifacts — a real cross-repo push, not a same-repo commit,
  which keeps state history out of the code repo's history entirely).

## What's deliberately deferred

- **Real Bybit wire protocol.** `BrokerAdapter` is fully specified and
  `BybitAdapter` implements it, reading its real configuration shape
  from the environment (`BYBIT_API_KEY`/`BYBIT_API_SECRET`), but every
  trading call currently returns a clear `NotImplemented` error. Bybit's
  actual flow is HMAC-signed REST against `api.bybit.com/v5/...`, which
  is arguably simpler to implement than Deriv's WebSocket flow given
  this daemon's short-lived, one-shot-per-invocation deployment shape.
  Deprioritized behind Deriv, which now has a real client (see below).
- **Three DerivAdapter methods.** `list_open_positions`,
  `list_open_orders`, and `cancel_order` are `NotImplemented` stubs.
  `cancel_order` needs an order_id -> Deriv contract_id mapping this
  adapter doesn't maintain yet (our trait passes our own order_id, not
  Deriv's contract_id). `list_open_orders` doesn't map cleanly at all —
  Multipliers contracts don't have a separate "open order" concept the
  way a traditional forex broker does, a contract simply exists or
  doesn't, so this needs its own design rather than a direct port.
  `list_open_positions` needs `portfolio` response parsing, which is
  real but bounded work, not attempted here to keep this pass finished
  rather than sprawling.
- **Real rolling daily/session buffers.** The scheduled (non-demo) path
  currently builds its divergence-detection buffers from a fixed offset
  around the live price rather than genuine highs and lows tracked
  across many invocations. This means it will correctly check the
  window, correctly reconcile, and correctly persist state, but it will
  never find a real divergence and place a trade, on purpose, until
  real buffer persistence (the same `CursorFile` pattern already used
  for positions, applied to a small daily/session high-low record)
  replaces the placeholder. The order-placing path itself is still
  fully proven — by `daemon/tests/integration_test.rs` and by
  `cargo run --bin oboobot -- --demo` — just not by the scheduled path
  yet.
- **Full AssistantEngine pattern detection**, **full Prometheus/OTLP
  export** (skipped by choice, not just deferred — GitHub Actions plus
  `tracing` logs is the whole observability story here), and
  **cross-position currency/correlation exposure netting** — all as
  before, see `risk::sizing`'s own doc comment for the last one.
- **The full six-file TOML config system.** Config is still constructed
  directly in code.

## Running it

```
cargo run --bin oboobot -- --demo
```

The full scripted walkthrough: a clean pass, a no-divergence cycle, a
True-Open rejection, a simulated health failure blocking new entries,
and a simulated restart that recovers state from disk and reconciles it
against the broker.

```
cargo run --bin oboobot -- --broker mock --state-dir ./state
```

The real, deployable path: checks the macro cycle window first, and only
does anything else if it's currently inside one (add `--force` to bypass
that check for manual testing).

```
cargo test --workspace
```

Runs every unit test, the property-based risk test, the persistence
round-trip tests, and the daemon's black-box integration test.

**A note on `Cargo.lock`:** this was built against Rust 1.75.0 (the
newest available via `apt` in the sandbox used to build it — see the
stored preference this project follows of installing Rust via `apt`
rather than `rustup`, since `rustup`'s domain isn't reachable from that
sandbox's network allowlist). Current `idna_adapter` and `native-tls`
both bumped their minimum supported Rust version past 1.75, so
`Cargo.lock` pins `idna_adapter` to `1.0.0` and uses `rustls` instead of
`native-tls` for the WebSocket TLS backend, both compatible with an
older toolchain. Neither pin is needed on a normal current toolchain
(GitHub's hosted runners included, via `dtolnay/rust-toolchain@stable`)
— they're here so this exact lockfile keeps building on an older Rust
too, not because the workspace requires them.

## Deployment: GitHub Actions only, by design

No self-hosted runner, no VPS. Two workflows:

- **CI** (`ci.yml`) — build, test, `cargo fmt --check`, and `cargo clippy
  -D warnings` on every push and PR to `main`.
- **Trading** (`trading.yml`) — runs every 5 minutes via
  `schedule: cron: '*/5 * * * *'`, checks out both this repo and a
  separate `oboobot_report` repo (state only, created and owned outside
  this project), runs one cycle with `--state-dir` pointed at the
  checked-out state repo, and commits + pushes any changed cursor files
  back with a bot identity. A `concurrency` group stops two runs from
  ever overlapping in the first place; a git push naturally fails loud
  (not silently) if a race somehow still happened, which is a much safer
  failure mode than the artifact-based approach this replaced.

**Required secrets** on the `oboobot` repo: `STATE_REPO_TOKEN` (a PAT
with write access to `oboobot_report` — the default `GITHUB_TOKEN` can't
reach a different repository), plus whichever broker's credentials are
actually in use (`DERIV_APP_ID`/`DERIV_API_TOKEN` or
`BYBIT_API_KEY`/`BYBIT_API_SECRET`) once a real adapter replaces the
`--broker mock` currently hardcoded in `trading.yml`.

**Why polling every 5 minutes instead of scheduling the exact cycle
times:** GitHub's own docs say scheduled workflows aren't guaranteed to
fire on time, and workflows scheduled more often than hourly are the
ones most likely to slip — worse right at the top of the hour, which is
exactly when several of this strategy's cycles land. A daily job can
shrug that off; a strategy built on 20-minute windows can't. Polling
every 5 minutes and letting the binary itself decide "am I in a window"
(`session_time::is_within_macro_cycle`, already written and tested)
turns GitHub's imprecision into a non-issue instead of something to
fight. Rough cost check: roughly 288 checks a day at a few seconds each
is well under 100 minutes a month, leaving most of the free 2,000
minutes for cycles that actually trade.

## A note on the two fixes applied from review

1. **Partial week calculation** — `session_time::calendar::is_full_trading_week`
   compares this week's Sunday-18:00-NY open to the previous week's,
   requiring exactly a seven-day gap.
2. **Broker heartbeat severity** — `daemon::health::severity_for` maps
   `BrokerHeartbeatFailure` to `SystemState::ReadOnly`, matching
   `NewsApiDown`, instead of the original `Degraded`.


If you don't understand what's going on in here, you'll live a happier life just closing this repo and never coming back again --trust me.

With Love,

- Obot
