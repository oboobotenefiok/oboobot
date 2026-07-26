# ADR 0002: Multi-Pair-Set Architecture

## Status
Accepted, implemented.

## Context
`config.toml` has always supported `[[pairs]]` as a list: the schema was ready for multiple correlated pair-sets from the start. The real cycle, however, only ever read `config.pairs.first()`, silently ignoring anything beyond the first entry. Fixing this meant deciding, for every piece of per-cycle state, whether it was genuinely account-wide (evaluated once regardless of how many pair-sets are configured) or genuinely per-pair-set (evaluated independently for each one).

## Decision
Account-wide, evaluated once per invocation regardless of pair-set count:
- Kill switch, config load, health checks (broker heartbeat, disk, memory), reconciliation.
- The broker snapshot itself: one `get_snapshot` call requesting the union of every pair-set's primary and secondary symbols, not one call per pair-set.
- The macro-cycle window and holiday checks (both pure functions of `now`).
- The exit sweep: every open position is checked every cycle, regardless of which pair-set it came from, using a per-pair price lookup and a per-pair divergence-map lookup (see below) rather than one flat price/divergence value.

Per-pair-set, evaluated independently in a loop over `config.pairs`:
- Daily/session buffers, correlation window, spread history: each pair-set gets its own `SnapshotFile`, keyed by that pair-set's own pair names (`buffer_daily_EURUSD.json`, `correlation_EURUSD_GBPUSD.json`, and so on).
- SMT divergence detection and the resulting candidate signal.
- The spread filter, True Open bias, the collision check, risk sizing, and order submission.

This required `monitor::evaluate_exits`'s SMT-contradiction input to generalize from "at most one divergence this cycle" (`Option<(String, Direction, Tier)>`) to "one divergence per pair-set" (`BTreeMap<String, (Direction, Tier)>`), since with more than one pair-set active there can genuinely be more than one live divergence reading at once, each about a different pair.

`status.json`'s single `last_decision` string field was kept as-is rather than restructured into a per-pair-set schema, to avoid touching a serialization format other tooling might already depend on. It's populated with a joined summary (`"EURUSD/GBPUSD: no_divergence; USDJPY/AUDUSD: order_submitted"`) instead.

## Consequences
- Currency and correlation exposure limits (ADR-worthy in their own right, not separately documented here) became meaningfully more important the moment this landed: before, every position was on the same primary pair, so there was no simultaneous-correlated-exposure risk to compute. After, positions can land on either pair within a pair-set, and on any pair across multiple configured pair-sets, in the same account at once.
- Correlation and exposure tracking remain scoped per pair-set (a pair-set's own primary against its own secondary), not a full matrix across every symbol in every configured pair-set. Extending to a full matrix is real additional work: a correlation window per *pair of symbols*, not per pair-set: and is called out as a known limitation in the README rather than attempted here.
- The demo harness (`run_cycle`/`run_demo`) was **not** extended to multi-pair-set: it remains a single scripted pair-set, since it exists to illustrate specific scenarios (a clean signal, no divergence, a True Open conflict) rather than to exercise the full daemon.
