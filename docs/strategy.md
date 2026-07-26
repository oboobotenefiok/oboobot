# Strategy Layer

## SMT divergence

Two correlated assets are checked against their own recent high/low buffers (daily and session timeframes). A signal fires when one of them sweeps past its buffer while the other doesn't confirm that move: that disagreement is smart money divergence.

The rule is symmetric and does not depend on which asset is labeled "primary" versus "secondary" in a given call; those labels only exist because the input needs two named slots. What decides the trade is purely which asset swept and which one held:

- Sweeping the **high** while the other holds means the holder is relatively weak (it should have made a new high too, and didn't): **sell** the holder.
- Sweeping the **low** while the other holds means the holder is relatively strong: **buy** the holder.

`strategy::smt::TradeTarget` (`Primary` or `Secondary`) carries which asset a divergence check identified, separately from `Direction`. `generate_signal` resolves `TradeTarget` down to a concrete pair name using whichever `primary_pair`/`secondary_pair` strings the caller supplied. See `docs/adr/0001-symmetric-smt-divergence.md` for why this needed fixing and what it replaced.

Daily and session are checked independently. If they agree: same target, same direction: that's a `Tier::Double` signal, which is also what triggers the 2x risk multiplier in the `risk` crate. If they disagree, daily wins outright, on the same reasoning the True Open gate uses Weekly as the tie-breaker over Daily: the higher timeframe's read sets the bias.

## Stop-loss placement

The stop for a new position is the previous cycle's high or low that was *not* taken out, on whichever asset is actually being traded. A Buy's stop goes at that buffer's low; a Sell's stop goes at that buffer's high. Tier1 signals use the daily buffer, Tier2 uses session, Double uses daily (the same tie-break `evaluate_smt` itself already applies). This lives in `daemon::main::stop_loss_level` rather than in the `strategy` crate, since it's about where to place an order, not about detecting divergence.

## Rolling buffers

`RollingBuffer` tracks a high/low window that resets on its own schedule: daily buffers reset at 18:00 NY, session buffers reset at session boundaries. Both persist between invocations via `SnapshotFile`.

## Correlation tracking

`CorrelationState` holds up to 500 (primary, secondary) price-pair samples per pair-set and a `last_updated` timestamp (used by the freshness health check, not for staleness logic inside this crate itself: that policy question belongs to the caller). `compute_coefficient` returns the live Pearson coefficient over the current window; `detect_regime_shift` compares it against a stored baseline and flags a deviation past a configurable threshold.

A freshly empty window is backfilled with 90 days of historical daily closes (via `BrokerAdapter::fetch_historical_prices`) before the first live sample joins it, so correlation readings are meaningful from the first cycle instead of needing weeks to fill a window from live data alone. Correlation is tracked per pair-set (a pair-set's own primary against its own secondary): not as a full matrix across every symbol in a multi-pair-set configuration. See "Remaining Gaps" in the README.

## Spread history and the spread filter

`SpreadHistory` is a rolling window of recorded spreads for a pair-set's primary. `passes_filter` rejects a cycle whose current spread exceeds a configurable multiple of the recent average, once there's enough history to compute one.

## True Open gate

Lives in `session_time`, not `strategy`, since it's fundamentally about calendar time (weekly and daily True Open levels, captured at Monday 18:00 NY and midnight NY respectively) rather than about the two-asset divergence check. `generate_signal` calls `session_time::true_open_gate(weekly_bias, daily_bias, direction)` after divergence resolves a candidate direction, and either produces a `TradeSignal` or a `SignalInvalidated` rejection.
