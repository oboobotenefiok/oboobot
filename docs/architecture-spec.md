# Architecture

## Overview

oboobot (internal name QuarterlyTheory_SMT_Trader) is a Cargo workspace of seven crates with a strictly one-directional dependency graph: the compiler enforces no cycles. `domain` sits at the bottom with no internal dependencies; `daemon` sits at the top and depends on everything else. Nothing downstream ever depends on `daemon`, which is what keeps `strategy`, `risk`, and `broker` independently testable without needing the whole system running.

```
domain
├── session_time (NY time, holidays, macro cycles, True Open gate)
├── persistence  (CursorFile, SnapshotFile)
├── broker       (BrokerAdapter trait, MockBroker, DerivAdapter, BybitAdapter stub)
├── risk         (position sizing, account-wide gates)
└── strategy     (SMT divergence, buffers, correlation)
        └── daemon (wires everything together into the CLI binary)
```

## Deployment model

This is not a long-running server. It's a short-lived process invoked on a schedule (every 5 minutes, via a GitHub Actions cron workflow) that runs one cycle to completion and exits. There is no persistent event loop, no reconnect-with-backoff logic for the broker connection itself, and no in-memory state that survives between invocations. Everything that needs to survive a restart is written to disk under `--state-dir` (see `persistence.md`), and in the deployed case that directory is a separate repository state persists to via git commits from the workflow.

This shapes several decisions documented in `docs/adr/`: retries exist for individual broker *requests* within a connection, not for reconnecting a dead connection, because "the next scheduled invocation is the retry" for that case. SIGTERM is logged but never interrupts an in-flight cycle, for the same reason: a half-finished cycle is a worse failure mode than a slightly-late one.

## One cycle, step by step (the real path)

1. **Kill switch check.** A `PAUSED` file in the state directory stops everything before touching the broker at all.
2. **Config load.** `config.toml` (or built-in defaults if absent) is parsed and validated, including a schema version check.
3. **Reconciliation.** What does local state (`positions.cursor`) say is open, and does the broker agree? Mismatches are logged and notified, never silently resolved.
4. **Snapshot fetch.** One `BrokerAdapter::get_snapshot` call covering the union of every configured pair-set's primary and secondary symbols.
5. **Per-pair-set market state.** For each configured pair-set: update daily/session buffers, update the correlation window (backfilling 90 days of history if the window is empty), update spread history, and evaluate SMT divergence.
6. **Exit sweep.** Always runs, for every currently open position regardless of which pair-set it came from, independent of whether this cycle is inside an entry window. Checks risk-reward, pre-news, and SMT-contradiction conditions.
7. **Entry gates.** Three account-wide checks (the macro-cycle window, health state, and the holiday calendar), any of which can end the cycle here without evaluating entries at all.
8. **Per-pair-set entries.** For each pair-set: spread filter, True Open bias load, signal generation, collision check, risk sizing, order submission. A fill triggers reconciliation again immediately, not just at the next startup.
9. **Status write.** `status.json` is overwritten with the current health state, open position count, and a per-pair-set summary of what this cycle decided.

## Crate responsibilities

- **domain**: shared types (`Position`, `Order`, `TradeSignal`, `BrokerSnapshot`, ...), newtypes (`Usd`, `Percent`, `Coefficient`), and the `Event` enum. No I/O, no async.
- **session_time**: NY-time conversion, DST-correct day-of-week and session-boundary logic, the holiday calendar, macro-cycle window detection, and the True Open gate.
- **persistence**: `CursorFile<T>` (append-only, for history that should never be overwritten) and `SnapshotFile<T>` (single current value, for state that should be).
- **broker**: the `BrokerAdapter` trait and its implementations: `MockBroker` for tests, `DerivAdapter` for the real Deriv WebSocket API, `BybitAdapter` as a documented stub.
- **risk**: position sizing, the Tuesday/Double-SMT multiplier, daily/weekly loss limits, max open positions, and the currency/correlation exposure checks.
- **strategy**: SMT divergence detection (symmetric between primary and secondary, see `docs/adr/0001-symmetric-smt-divergence.md`), rolling buffers, correlation tracking with regime-shift detection, and the True Open gate wiring.
- **daemon**: the health state machine, config loading, notifications, the kill switch, decision and recommendation logs, reconciliation, and the CLI binary that ties every other crate together into one cycle.

## Where to look next

- `docs/broker.md`, `docs/strategy.md`, `docs/persistence.md`, `docs/events.md` for per-layer detail.
- `docs/adr/` for the reasoning behind decisions that weren't obvious from the code alone.
- `docs/diagrams/` for the request/response shape of the four flows above that most benefit from a picture instead of prose.
