# Sequence: One Pair-Set's Entry Evaluation

Everything account-wide (health, kill switch, the window/holiday gates, the broker snapshot itself) has already happened before this point; this is what runs once per configured pair-set.

```mermaid
sequenceDiagram
    participant Main as daemon::main
    participant Strategy as strategy::smt
    participant SessionTime as session_time
    participant Risk as risk::sizing
    participant Broker as BrokerAdapter

    Main->>Main: spread_history.passes_filter(current_spread)
    alt spread filter rejects
        Main->>Main: log "spread_rejected", continue to next pair-set
    end

    Main->>SessionTime: load_or_capture_bias(Weekly)
    Main->>SessionTime: load_or_capture_bias(Daily)
    Main->>Strategy: generate_signal(inputs, weekly_bias, daily_bias, primary, secondary, ...)
    Strategy->>Strategy: evaluate_smt (checks both directions, both timeframes)
    Strategy->>SessionTime: true_open_gate(weekly_bias, daily_bias, direction)

    alt no divergence this cycle
        Strategy-->>Main: SignalOutcome::NoDivergence
        Main->>Main: log "no_divergence", continue to next pair-set
    else True Open gate rejects
        Strategy-->>Main: SignalOutcome::Rejected(SignalInvalidated)
        Main->>Main: log "gate_rejected", continue to next pair-set
    else signal passes
        Strategy-->>Main: SignalOutcome::Signal(TradeSignal)
        Main->>Main: already_entered_this_cycle(signal.pair)?
        alt collision
            Main->>Main: log "collision_skip", continue to next pair-set
        else clear
            Main->>Main: stop_loss_level(divergence_inputs, signal, secondary)
            Main->>Risk: evaluate(signal, risk_config, risk_context)
            Risk->>Risk: currency exposure check
            Risk->>Risk: correlation exposure check
            alt risk engine rejects
                Risk-->>Main: RiskDecision { approved: false }
                Main->>Main: log "risk_rejected", continue to next pair-set
            else risk engine approves
                Risk-->>Main: RiskDecision { approved: true, position_size, ... }
                Main->>Broker: submit_order(OrderRequest)
                Broker-->>Main: Order
                Main->>Main: reconcile_and_notify (post-fill reconciliation)
                Main->>Main: log "order_submitted"
            end
        end
    end
```
