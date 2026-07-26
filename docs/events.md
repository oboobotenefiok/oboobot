# Events

`domain::events` defines `Event`, an enum of everything that can happen in one cycle, and `EventEnvelope`, which pairs an `Event` with a timestamp. This is the vocabulary `AssistantEngine::analyze_event` is given to observe the system with: it is explicitly a read-only observation channel, not a control channel. There is no code path anywhere in this workspace that takes an `AssistantEngine`'s output and feeds it back into `risk::RiskConfig` or any `strategy` parameter automatically; every `Recommendation` is either logged, or (now) also persisted to `recommendations.cursor`, for a human to read and decide whether to act on manually.

## Variants

- `MacroCycleStarted`: emitted once per pair-set at the top of the entry evaluation.
- `SignalGenerated(TradeSignal)`: a divergence resolved into a signal that passed the True Open gate.
- `SignalInvalidated(SignalInvalidated)`: a divergence resolved into a candidate, but the True Open gate rejected it. Deliberately does not carry a `pair` field, since it's documented as matching an external "hardening layer" schema this project doesn't control.
- `OrderSubmitted(Order)` / `PositionClosed(Position)`.
- `Recovery(RecoveryState)`: declared for a retry/backoff bookkeeping type that isn't currently constructed anywhere; see `docs/adr/0003-selective-retry-scope.md` for why the backoff that *is* implemented doesn't need it (it's scoped to individual requests, not multi-invocation reconnect state).

## Why a closed enum instead of a trait

Every event this system can produce is known in advance and enumerable: there's no plugin architecture where a third party registers a new event type at runtime. A closed enum makes `AssistantEngine::analyze_event`'s exhaustiveness checkable by the compiler (a new variant forces every match arm across the workspace to be revisited) and makes the event stream trivially serializable, both properties a trait-object-based "any event" design would give up for no corresponding benefit here.
