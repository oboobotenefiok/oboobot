# ADR 0001: Symmetric SMT Divergence Targeting

## Status
Accepted, implemented.

## Context
The original `detect_divergence` implementation only ever checked one direction: primary sweeping its buffer while secondary held. There was no check for the mirror case (secondary sweeping while primary held). Separately, `generate_signal` was always called with a single, hardcoded pair name: whatever was configured as "primary": regardless of what `detect_divergence` actually returned.

The practical effect: the daemon could only ever place trades on the primary pair. Half of the strategy's own specified cases (any case where secondary is the one that should be traded) were structurally unreachable, not merely under-tested.

The strategy's own specification is explicit and symmetric: whichever asset holds while its correlated counterpart sweeps a buffer level is the one to trade, regardless of which pair happens to be labeled primary or secondary in a given cycle's inputs. Buy the asset that held a swept low (it's relatively stronger); sell the asset that held a swept high (it's relatively weaker).

## Decision
`detect_divergence` now checks both directions for both buffer levels (four checks total, up from two) and returns which asset held: `TradeTarget::Primary` or `TradeTarget::Secondary`: alongside the direction, instead of assuming it's always whichever pair was passed in as `pair`.

`generate_signal`'s signature changed from a single `pair: String` parameter to `primary_pair: String, secondary_pair: String`; it resolves `TradeTarget` to the correct concrete pair name internally.

Every downstream consumer of the resolved pair: entry price lookup, stop-loss placement, the collision check, notifications, decision logging, and exit-side price/contradiction checks in `monitor.rs`: was updated to key off `signal.pair` (or the position's own `.pair`) rather than a hardcoded `primary` variable. A partial fix that only corrected divergence detection without threading the resolved pair through every consumer would have been worse than the original bug: it would silently compute stop-loss and take-profit off the wrong pair's price for any signal that resolved to secondary.

## Consequences
- The strategy can now trade either pair in a pair-set, as originally specified.
- `evaluate_exits`'s SMT-contradiction check became pair-aware (a `Option<(String, Direction, Tier)>`, later generalized to a per-pair map for multi-pair-set support: see ADR 0002) instead of assuming every open position was on the one pair the daemon ever traded.
- `evaluate_smt`'s Double-tier condition now requires daily and session to agree on *both* target and direction, not just direction; two timeframes agreeing on direction while disagreeing on which asset to trade is a real disagreement, not confirmation, and is no longer silently promoted to Double.
- Tests were added for both previously-unreachable mirror cases, plus the refined Double-tier disagreement case, since a bug fix without a regression test for the specific case it fixes is incomplete.
