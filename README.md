![bruh's Signature Image](docs/images/oboobot.jpg)

A Rust, trait-based, event-driven trading daemon implementing an SMT
(Smart Money Technique) divergence strategy gated by True Open levels —
internally still called `QuarterlyTheory_SMT_Trader` in a few places,
since that's the strategy's own name, but the project and its GitHub
Actions deployment are `oboobot`.

This README is honest about scope. Everything described as "implemented"
below actually compiles and is tested. Everything described as
"deferred" or "gap" is named on purpose, not silently skipped.




Before I continue:



[!CAUTION]
RISK WARNING

Trading foreign exchange, futures, CFDs, cryptocurrencies, and other leveraged products carries a high level of risk and may not be suitable for all investors. You could sustain a loss of some or all of your invested capital. You should not invest money you cannot afford to lose. Before trading, carefully consider your financial objectives, level of experience, and risk appetite. Seek independent financial advice if necessary.

[!WARNING]
EDUCATIONAL DISCLAIMER

The content provided is for informational and educational purposes only and does not constitute financial, investment, or legal advice. No representation is being made that any information provided is accurate, complete, or up to date. Trading ideas, setups, and strategies are hypothetical and should not be considered as trade recommendations or solicitation to buy or sell any financial instrument.

[!NOTE]
HYPOTHETICAL / SIMULATED RESULTS DISCLAIMER

Hypothetical or simulated performance results have certain inherent limitations. Unlike actual trading records, simulated results do not represent real trading. No representation is being made that any account will or is likely to achieve profits or losses similar to those discussed. Past performance is not indicative of future results.


Now that is said and done:



## Layout

A real Cargo workspace, so the "no cyclic dependencies, only through
traits" rule is something the compiler enforces rather than something
everyone has to remember. Dependency direction only flows one way, top
to bottom:

```
domain          <- shared types, newtypes, events, errors.
session_time    <- NY sessions, DST-correct time, holidays, macro
                    cycles, True Open gate logic and capture timing.
broker          <- BrokerAdapter trait, MockBroker with adversarial
                    behaviors, a real Deriv WebSocket client, an honest
                    Bybit stub.
strategy        <- SMT divergence detection, the True Open gate wired
                    together, rolling daily/session buffers, spread
                    history, and correlation tracking with regime-shift
                    detection.
risk            <- position sizing, the multiplier cap, account-wide
                    gates.
persistence     <- append-only CursorFile (fsync-before-return) and
                    SnapshotFile (atomic write-then-rename) for
                    current-value state.
daemon          <- health state machine with real Linux-target checks,
                    the two-channel event bus, config loading,
                    continuous position monitoring, news-driven exits,
                    notifications, kill switch / decisions log / status
                    snapshot / idempotency guard, startup reconciliation,
                    the assistant boundary, the CLI, and the binary.
```

## What changed in this pass

Two structural gaps from the previous version, closed:

- **True Open was hardcoded to Neutral in the real path.** It now
  captures and persists real weekly (Monday 18:00 NY) and daily
  (midnight NY) levels via `SnapshotFile`, re-capturing when expired,
  and computes a real bias from the current price against whichever
  level is on file.
- **Positions were never watched after opening.** Every single
  invocation — window or not — now runs an exit sweep
  (`daemon::monitor::evaluate_exits`) checking risk-reward, pre-news,
  and SMT-contradiction conditions against every open position, closing
  any that qualify via the newly added `BrokerAdapter::close_position`.
  Entries are gated by the macro-cycle window; exits never are.

Everything else added this pass, grouped:

- **Real rolling buffers** (`strategy::buffers`) replace the fixed-offset
  placeholder that could never actually diverge. Daily buffers reset
  18:00 NY, session buffers at each of the four session boundaries, both
  persisted between invocations.
- **A real spread filter** (`strategy::buffers::SpreadHistory`): rolling
  average against `risk.spread_multiplier`, always passes until enough
  history exists rather than rejecting everything on a cold start.
- **A real (if simple) correlation tracker with regime-shift detection**
  (`strategy::correlation`): genuine Pearson correlation over a rolling
  window, compared against a baseline, triggers a notification when it
  drifts past `risk.regime_shift_threshold`.
- **The holiday/low-liquidity check is wired in** — `StaticHolidayProvider`
  existed and was tested before but nothing called it; new entries now
  skip on a recognized holiday.
- **A news provider trait and pre-news exit check**, with one
  implementation: `NoNewsProvider`, which always returns nothing
  scheduled. That's a deliberate fail-safe, not a placeholder pretending
  to be real — see `daemon::news` for why picking a specific external
  calendar API wasn't done here without the same verification the Deriv
  endpoint got.
- **Real health checks**: `check_broker_heartbeat` times and reports on
  the actual broker call every cycle already makes; `available_disk_mb`
  and `resident_memory_mb` are Linux-specific (matching the actual GitHub
  Actions deployment target), best-effort, and degrade to `None` rather
  than erroring if unavailable.
- **TOML config** (`daemon::config`), one file instead of the original
  spec's six, with the same validation rules (no duplicate pairs, spread
  multiplier must exceed 1.0, bounded percentages). Falls back to a
  built-in default if the file is missing. See `config.toml` at the repo
  root for the shipped example.
- **Notifications** (`daemon::notifications`): Slack and Telegram, both
  a plain webhook POST, both behind a `Notifier` trait so `--demo` and
  tests use a no-op implementation. No metrics stack, as instructed.
- **The operational trio**: a kill switch (drop a `PAUSED` file in the
  state dir, checked before anything else runs), a decisions log
  (`decisions.cursor` — every cycle's outcome, not just the ones that
  traded), and a status snapshot (`status.json`, overwritten each run,
  glanceable from a phone).
- **A position-collision guard that doubles as idempotency protection**
  (`daemon::operations::already_entered_this_cycle`): the original
  spec's "one trade per macro cycle per pair" was never implemented;
  implementing it is the same check that also protects against a
  retried or overlapping workflow run double-entering a signal, since a
  signal's id is freshly generated each evaluation and can't be used as
  a dedup key on its own.
- **`close_position`**, a new `BrokerAdapter` method distinct from
  `cancel_order` (which cancels a pending order, not something already
  open). Implemented for real on `MockBroker` and `DerivAdapter` (via
  `sell`, decoding the contract_id already encoded into `position_id` at
  buy time); a fixed real bug along the way, too — `MockBroker`'s
  returned `Order` never actually linked to the `Position` it created
  (`position_id` was hardcoded `None`).
- **`Position` gained `stop_loss`/`take_profit` fields.** They weren't
  stored anywhere before, which meant nothing could check a position
  against them after the fact.

## What's still a real gap

- **A `--config` file only covers one pair.** `Config.pairs` is a `Vec`,
  but `run_real_cycle` only evaluates `pairs[0]`. Extending to iterate
  every configured pair is a loop, not a redesign, but it wasn't done
  here.
- **DerivAdapter's `list_open_positions` and `list_open_orders`** are
  still `NotImplemented`. `list_open_orders` doesn't map cleanly at all
  onto Multipliers (a contract exists or doesn't, there's no separate
  pending-order concept), which needs its own design rather than a
  direct port.
- **`NoNewsProvider` means the pre-news exit can never actually fire
  yet.** The check itself is real and tested; there's no real calendar
  behind it.
- **BybitAdapter is still fully stubbed.** Deprioritized behind Deriv,
  as instructed.
- **SIGTERM handling was attempted and backed out.** A
  `tokio::select!` racing the real cycle against a signal listener
  caused the real path to silently exit doing nothing in this sandbox —
  every state write is already fsync'd or atomic-rename, so correctness
  never depended on it, but it's worth root-causing (possibly a sandbox-
  specific signal delivery quirk) before attempting it again rather than
  re-adding it blind.
- **GitHub Environments** (separating paper/live secrets with required
  reviewers) is GitHub repo configuration, not code; `trading.yml`
  doesn't reference an `environment:` yet.
- **Replay/backtesting** still doesn't exist.
- **Real Deriv/Bybit wire-protocol coverage gaps**: see `broker/src/deriv.rs`
  and `broker/src/stubs.rs` doc comments for the specifics. This
  sandbox also can't reach either broker's servers at all (network
  allowlisted to package registries only) — everything Deriv-related
  was verified by message-shape unit tests and a full workspace build,
  not a live connection.

## State files

Everything under `--state-dir` (a checkout of `oboobot_report` in the
real deployment):

```
positions.cursor              append-only, every position ever known
decisions.cursor              append-only, every cycle's outcome
buffer_daily_<PAIR>.json       current daily high/low per pair
buffer_session_<PAIR>.json     current session high/low per pair
correlation.json               rolling correlation window + baseline
spread_history.json            rolling spread samples
true_open_weekly.json          current weekly True Open level
true_open_daily.json           current daily True Open level
status.json                    overwritten each run
PAUSED                         kill switch — presence alone matters
```

## Running it

```
cargo run --bin oboobot -- --demo
```

The full scripted walkthrough, unchanged from the first pass.

```
cargo run --bin oboobot -- --broker mock --state-dir ./state --force
```

A real cycle against `MockBroker`. Note `MockBroker` returns the same
synthetic price for every requested pair, so primary and secondary are
always identical against it — real divergence can never fire this way,
which is expected: this path proves the plumbing (reconciliation,
buffers, correlation, spread, True Open, monitoring, persistence, the
window gate) runs correctly end to end, not that the strategy trades,
which `--demo` and the integration tests already cover with realistic
divergent inputs.

```
cargo test --workspace
```

116 tests across every crate.

## Deployment: GitHub Actions only

Unchanged in shape from the previous pass — `ci.yml` for build/test/fmt/
clippy, `trading.yml` polling every 5 minutes with the binary
self-gating on the macro cycle window, state persisted to a separate
`oboobot_report` repo via git commit rather than artifacts. See that
workflow file for the exact steps and required secrets
(`STATE_REPO_TOKEN`, plus whichever broker's credentials once a real
adapter is live).

## A note on `Cargo.lock`

Built against Rust 1.75.0 (the newest available via `apt` in the sandbox
used to build it, per this project's stored preference to install Rust
via `apt` rather than `rustup`). Several dependencies picked up this
pass — `indexmap` (via `toml`/`reqwest`), `idna_adapter` (via the Deriv
client's URL parsing), `native-tls` — all bumped their minimum supported
Rust version past 1.75 in recent releases. `Cargo.lock` pins each to the
newest version still compatible with an older toolchain, and
`tokio-tungstenite` uses `rustls` instead of `native-tls` for the same
reason. None of this is needed on a normal current toolchain, GitHub's
hosted runners included.

## The two fixes from the original review

1. **Partial week calculation** — compares consecutive Sunday-18:00-NY
   opens, requiring exactly a seven-day gap.
2. **Broker heartbeat severity** — maps to `SystemState::ReadOnly`,
   matching `NewsApiDown`, instead of the original `Degraded`.


If you don't understand what's going on in here, you'll live a happier life just closing this repo and never coming back again --trust me.

With Love,

- Obot
