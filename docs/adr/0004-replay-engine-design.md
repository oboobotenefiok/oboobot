# ADR 0004: Replay Engine Design

## Status
Accepted, implemented (first slice, as scoped below). Originally written and delivered as a proposal before implementation started, per the original specification's own note that this is the largest remaining architectural piece and should get a design written down first.

## Context
There is currently no way to validate this strategy's behavior against historical market conditions without either trading live or hand-constructing `DivergenceInputs` fixtures one at a time in a unit test (which is what the existing test suite already does, for individual pieces of logic in isolation: not for a full multi-cycle run). A replay/backtesting engine would run the same entry and exit logic that `run_real_cycle` runs, but against historical data with a controllable clock instead of the live broker and wall-clock time, producing a deterministic result for a given historical window and config.

Two pieces of existing infrastructure make this more tractable than it would otherwise be:

- `session_time::Clock` (with `SystemClock` and `ManualClock` implementations) already exists as an abstraction over "what time is it," even though `run_real_cycle` currently calls `chrono::Utc::now()` directly rather than going through it.
- `BrokerAdapter::fetch_historical_prices` (built for correlation backfill) already provides a way to pull historical daily closes from Deriv. A replay engine needs OHLC at whatever granularity it replays at, which the same `ticks_history` endpoint the correlation backfill uses can provide (it already returns full candles; the daemon just extracts `.close` today).

## Decision (proposed)

**A new `BrokerAdapter` implementation, not a parallel code path.** `ReplayBroker` implements the same trait `MockBroker` and `DerivAdapter` do. Its `get_snapshot` returns whatever historical bar the replay's current simulated time points at; `submit_order`/`close_position` simulate fills against that same historical data deterministically (no slippage model beyond what's explicitly configured); `list_open_positions` returns its own in-memory simulated position book. This is the same reason `MockBroker` exists and is used everywhere in the current test suite: the daemon's actual cycle logic should not need to know or care that it's replaying history instead of trading live.

**`run_real_cycle` takes an injectable clock.** Currently `let now = chrono::Utc::now();` is a direct call. For deterministic replay, this needs to become `let now = clock.now();` against the existing `session_time::Clock` trait, with `SystemClock` used in production (identical behavior to today) and `ManualClock` driven by the replay harness. This is the one change to the *existing* real-cycle code this proposal requires; everything else about replay lives in new code (`ReplayBroker`, the harness itself), not modifications to the live path.

**A separate `replay` binary or CLI subcommand**, not a flag on the existing daemon binary. Replay's job: load a historical window, step through it bar by bar, call the cycle logic once per bar, collect a report: is different enough from "run one real cycle and exit" that folding it into the same `main()` would make both harder to read. It depends on `daemon`, `broker`, `domain`, and `session_time` the same way the daemon binary does.

**A `ReplayReport` output**: per-signal outcomes (generated, rejected and why, filled), realized P&L, max drawdown, and a full decision log: reusing `DecisionRecord`'s existing shape rather than inventing a parallel one, since the meaning ("what did the strategy decide, and why") is identical whether the decision happened live or in replay.

## Explicitly out of scope for a first version
- A slippage or partial-fill model beyond "fills exactly at the historical bar's price." Real fill behavior is a separate, harder problem than getting deterministic replay working at all.
- Multi-pair-set replay in the first pass. Given the real cycle's own multi-pair-set support is new (see ADR 0002), replay should prove itself against a single pair-set before taking on the added complexity of simulating several in lockstep.
- Optimization/parameter-sweep tooling (running many replay windows to tune config values). That's a natural thing to build *on top of* a working replay engine, not part of building the engine itself.

## Implementation notes (added after building the first slice)
Built largely as designed, with one deliberate deviation from the exact wording above: rather than a separate binary, replay is a `--replay-days <N>` flag on the existing daemon binary, branched on in `main()` before the real broker is constructed. The design left this open ("a separate replay binary *or* CLI subcommand"), and a flag on the existing binary turned out to be the lower-risk choice: `run_real_cycle` is a private function inside the binary's own `main.rs`, so a genuinely separate binary would have needed moving it (and everything it calls) into the `daemon` library first, a much larger and riskier change than adding one CLI branch. `ReplayBroker` itself lives in the library (`daemon::replay`), fully unit-tested there, exactly as designed.

`fetch_historical_prices` (built for correlation backfill, see ADR 0002's era of work) already returns closes only, not full OHLC, so the report and fill simulation work from closes; there is no separate historical-data-loading path to build.

One honest limitation: this sandbox has no route to a real Deriv connection, so the full CLI path (`--broker deriv --replay-days N`) could not be exercised end to end here. What was verified: `ReplayBroker`'s own behavior (snapshot pricing, order fill and close simulation, equity tracking, bar-count handling) directly, in isolation, with unit tests; the CLI's argument parsing, config loading, and error propagation, by actually running it against `--broker mock` (which correctly and cleanly fails at the `fetch_historical_prices` call, exactly as designed, since Mock doesn't support historical data); and the full bar-stepping loop, by careful manual review rather than an automated run, since exercising it for real needs the same live connection this workspace's other Deriv-specific gaps already can't reach from here.

## Explicitly out of scope, still, after this first slice
Everything listed above under "Explicitly out of scope for a first version" remains out of scope: no slippage/partial-fill model, no multi-pair-set replay, no parameter-sweep tooling. None of that was attempted in this pass.
