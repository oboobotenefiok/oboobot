# ADR 0003: Selective Retry Scope for Broker Calls

## Status
Accepted, implemented.

## Context
Every Deriv API call went through one shared method, `DerivClient::call`, and a failure at any point: a dropped connection, a timeout, a rate limit: failed the entire invocation immediately, with no retry at any level. The specification called for exponential backoff to improve resilience against transient failures.

The obvious implementation: wrap `call` itself in a retry loop: is unsafe for this system. `buy` and `sell` are mutating requests. If either reaches Deriv and executes, but the *response* is what's lost to a transient network failure, blindly retrying would risk submitting (or closing) the same contract a second time. That is a strictly worse failure mode than the one retry logic exists to fix.

Separately, this daemon's own header documentation already establishes that a full reconnect-with-backoff loop for the WebSocket connection itself is deliberately out of scope: each invocation is a short-lived process, and "the next scheduled invocation is the retry" for a connection that's actually dead. Retry logic added here needed to respect that existing, deliberate boundary rather than quietly expand past it.

## Decision
A second method, `call_with_retry`, wraps `call` with exponential backoff (1s base, doubling, capped at 30s, 4 attempts) and is used *only* at read-only call sites: `portfolio`, `proposal_open_contract`, `ticks`, `proposal` (the quote request, not the purchase itself), `balance`, and `authorize`. `buy` and `sell` call `call` directly, unchanged, with a comment at each site explaining why.

`RateLimited` errors honor Deriv's own suggested wait time instead of the exponential schedule, since the server's own hint is more accurate than a guess. No jitter: this is a single-instance daemon woken by a cron schedule, not a fleet of clients that could retry in lockstep, so there's no thundering-herd problem for jitter to solve.

The retry decision logic (`is_retryable`, `backoff_delay`) is factored into small, pure functions separate from the actual async retry loop, specifically so it's unit-testable without a live connection: the loop itself needs a real WebSocket and can't be tested in this sandbox, a limitation already named elsewhere in this codebase.

## Consequences
- Read-only requests are now resilient to the kind of transient failure exponential backoff exists for.
- Mutating requests are exactly as fail-fast as they were before this change. A `buy` or `sell` failure still propagates immediately and aborts the cycle; reconciliation (including the post-fill reconciliation added alongside this work) is what catches any resulting local/broker mismatch.
- `domain::RecoveryState` (a `retry_count`/`backoff_seconds` type declared in the event enum but never constructed anywhere) remains unused. It was evaluated as a candidate mechanism for this feature and set aside: the actual retry state this implementation needs is a simple loop counter local to `call_with_retry`, not something that needs to be part of the event stream or persisted across invocations.
