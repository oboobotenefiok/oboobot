//! This binary is a demonstration harness, not a production entry point.
//! It wires every crate in this workspace together and runs a handful of
//! synthetic macro cycles against `MockBroker`, so that running `cargo
//! run` actually shows the whole pipeline working end to end: SMT
//! divergence detection, the True Open gate, risk sizing, order
//! submission, startup reconciliation, and the health-state gate that
//! blocks new entries when something's wrong.
//!
//! A real production entry point would swap `MockBroker` for a real
//! `BrokerAdapter` implementation, use `session_time::SystemClock`
//! instead of a fixed timestamp, and call `Scheduler::run` (which runs
//! forever, sleeping between real macro cycles) instead of driving a
//! fixed, fast sequence of scenarios by hand. Building that real adapter
//! means confirming exact endpoint URLs and auth flows against a live
//! broker's current docs, which isn't something to guess at, so it's
//! intentionally left as the next step rather than faked here.

use broker::{BrokerAdapter, MockBroker};
use daemon::{
    allows_new_entries, apply_reconciliation, auto_action, reconcile, AssistantEngine,
    HealthCheckFailure, HealthMonitor, LoggingAssistant,
};
use domain::{Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Usd};
use persistence::CursorFile;
use rust_decimal_macros::dec;
use risk::RiskEngine as _;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tracing::info!("starting QuarterlyTheory_SMT_Trader demonstration harness");
    tracing::info!("this run is against MockBroker; see main.rs docs for what a live run would change");

    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));
    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    // A real deployment would keep this under a persistent `state/`
    // directory next to wherever the daemon runs from. For this
    // demonstration harness it goes under the OS temp directory instead,
    // purely so repeated runs of this binary don't accumulate positions
    // from previous runs; the durability behavior (fsync before the
    // append returns, see `persistence::cursor`) is identical either way.
    let state_dir = std::env::temp_dir().join("smt-trader-demo-state");
    tokio::fs::create_dir_all(&state_dir).await?;
    let positions_cursor_path = state_dir.join("positions.cursor");
    // Start every run from a clean cursor file, since this is a
    // from-scratch demonstration each time, not a genuinely persistent
    // deployment.
    let _ = tokio::fs::remove_file(&positions_cursor_path).await;
    let positions_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);

    // Every real daemon startup begins here: what does our own
    // persistence say is open, and does the broker agree? On a genuinely
    // fresh start there's nothing persisted yet, so this should reconcile
    // clean, but running it unconditionally (rather than only when we
    // suspect a problem) is exactly the point: reconciliation isn't a
    // special recovery-mode action, it's just what startup always does.
    let locally_known_positions: Vec<Position> = positions_cursor.read_all().await?;
    let report = reconcile(&broker, &locally_known_positions).await?;
    if report.is_clean() {
        tracing::info!("startup reconciliation: clean, broker and local state agree");
    } else {
        tracing::warn!(
            orphaned = report.orphaned_locally.len(),
            adopted = report.unknown_to_local.len(),
            "startup reconciliation found a mismatch"
        );
    }
    let mut open_positions = apply_reconciliation(&report);

    run_cycle(
        "cycle 1: clean bullish divergence, True Open agrees",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
            session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        },
        Bias::Buy,
        Bias::Sell,
    )
    .await?;

    run_cycle(
        "cycle 2: prices moving together, no divergence at all",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.10050),
            secondary_price: dec!(1.10050),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
        },
        Bias::Buy,
        Bias::Sell,
    )
    .await?;

    run_cycle(
        "cycle 3: real divergence, but Weekly True Open contradicts it",
        &broker,
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            daily_secondary_buffer: BufferLevels { low: dec!(1.10000), high: dec!(1.10500) },
            session_primary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
            session_secondary_buffer: BufferLevels { low: dec!(1.09000), high: dec!(1.11000) },
        },
        Bias::Sell, // Weekly is bearish; the divergence above is bullish. Conflict.
        Bias::Sell,
    )
    .await?;

    tracing::info!("simulating a broker heartbeat failure");
    health.report_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    tracing::warn!(
        state = ?health.current_state(),
        action = auto_action(health.current_state()),
        "health state escalated"
    );

    tracing::info!("cycle 4: same clean setup as cycle 1, but the health gate should now block it");
    if allows_new_entries(health.current_state()) {
        tracing::error!("this should not print: new entries should be blocked right now");
    } else {
        tracing::info!("new entries correctly blocked while system state is not Healthy or Degraded");
    }

    health.clear_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    tracing::info!(state = ?health.current_state(), "broker heartbeat recovered, health restored");

    tracing::info!("open positions before simulated restart: {}", open_positions.len());

    // Simulate the daemon actually restarting: build a brand new
    // CursorFile pointing at the same path, with none of the in-memory
    // state above carried over, the same as if this were a fresh process.
    // If persistence and reconciliation are both doing their job, this
    // should recover exactly the position(s) opened above, straight from
    // disk, confirmed against the broker.
    let post_restart_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);
    let recovered_from_disk = post_restart_cursor.read_all().await?;
    let post_restart_report = reconcile(&broker, &recovered_from_disk).await?;
    let post_restart_positions = apply_reconciliation(&post_restart_report);
    tracing::info!(
        recovered_from_disk = recovered_from_disk.len(),
        confirmed_by_broker = post_restart_positions.len(),
        "simulated restart: recovered position state from disk and reconciled it against the broker"
    );

    tracing::info!("QuarterlyTheory_SMT_Trader demonstration harness finished");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    label: &str,
    broker: &MockBroker,
    health: &HealthMonitor,
    assistant: &dyn AssistantEngine,
    positions_cursor: &CursorFile<Position>,
    open_positions: &mut Vec<Position>,
    inputs: DivergenceInputs,
    weekly_bias: Bias,
    daily_bias: Bias,
) -> anyhow::Result<()> {
    tracing::info!("--- {label} ---");

    if !allows_new_entries(health.current_state()) {
        tracing::info!("skipping: health state does not currently allow new entries");
        return Ok(());
    }

    let snapshot = broker.get_snapshot(&["EURUSD".to_string(), "GBPUSD".to_string()]).await?;
    let macro_cycle_event = EventEnvelope::new(snapshot.timestamp, Event::MacroCycleStarted);
    // The assistant sees every event, but as `daemon::assistant` explains,
    // anything it returns only ever gets logged, never applied.
    for recommendation in assistant.analyze_event(&macro_cycle_event).await {
        daemon::assistant::record_recommendation(&recommendation);
    }

    let outcome = generate_signal(
        &inputs,
        weekly_bias,
        daily_bias,
        "EURUSD".to_string(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        snapshot.timestamp + chrono::Duration::minutes(20),
    );

    match outcome {
        SignalOutcome::NoDivergence => {
            tracing::info!("no SMT divergence this cycle, nothing to evaluate");
        }
        SignalOutcome::Rejected(invalidated) => {
            tracing::info!(
                reason = ?invalidated.rejection_reason,
                weekly_bias = ?invalidated.weekly_bias,
                daily_bias = ?invalidated.daily_bias,
                smt_direction = ?invalidated.smt_direction,
                "signal generated but rejected by the True Open gate"
            );
        }
        SignalOutcome::Signal(signal) => {
            tracing::info!(tier = ?signal.tier, direction = ?signal.direction, "signal passed the True Open gate");

            let config = risk::RiskConfig {
                base_risk_percent: domain::Percent::from_percentage(dec!(1.0)),
                max_risk_percent: domain::Percent::from_percentage(dec!(5.0)),
                max_open_positions: 5,
                daily_loss_limit_percent: domain::Percent::from_percentage(dec!(5.0)),
                weekly_loss_limit_percent: domain::Percent::from_percentage(dec!(10.0)),
            };

            let equity = broker.get_account_equity().await?;
            let entry_price = match signal.direction {
                Direction::Buy => snapshot
                    .prices
                    .get("EURUSD")
                    .map(|q| q.ask)
                    .unwrap_or(dec!(1.10000)),
                Direction::Sell => snapshot
                    .prices
                    .get("EURUSD")
                    .map(|q| q.bid)
                    .unwrap_or(dec!(1.10000)),
            };
            let stop_loss_price = match signal.direction {
                Direction::Buy => entry_price - dec!(0.0050),
                Direction::Sell => entry_price + dec!(0.0050),
            };
            let take_profit_price = match signal.direction {
                Direction::Buy => entry_price + dec!(0.0150),
                Direction::Sell => entry_price - dec!(0.0150),
            };

            let context = risk::RiskContext {
                equity,
                open_position_count: open_positions.len(),
                is_tuesday: false,
                is_double_smt: signal.tier == domain::Tier::Double,
                entry_price,
                stop_loss_price,
                take_profit_price,
                realized_pnl_today: Usd::zero(),
                realized_pnl_this_week: Usd::zero(),
            };

            let risk_engine = risk::DefaultRiskEngine;
            let decision = risk_engine.evaluate(&signal, &config, &context)?;

            if !decision.approved {
                tracing::info!(reason = ?decision.rejection_reason, "risk engine rejected the signal");
                return Ok(());
            }

            tracing::info!(
                size = %decision.position_size,
                risk_percent = %decision.risk_percent,
                risk_currency = %decision.risk_currency,
                "risk engine approved sizing"
            );

            let request = OrderRequest {
                order_id: Uuid::new_v4(),
                trace_id: signal.trace_id,
                signal_id: signal.signal_id,
                pair: signal.pair.clone(),
                side: signal.direction,
                size: decision.position_size,
                order_type: OrderType::Market,
                price: None,
                stop_loss: Some(decision.stop_loss),
                take_profit: Some(decision.take_profit),
                confirming_snapshot_id: snapshot.snapshot_id,
            };

            let order = broker.submit_order(request).await?;
            tracing::info!(order_id = %order.order_id, status = ?order.status, "order submitted to broker");

            let previously_known_ids: std::collections::HashSet<Uuid> =
                open_positions.iter().map(|p| p.position_id).collect();

            open_positions.clear();
            open_positions.extend(broker.list_open_positions().await?);

            // Persist only what's actually new since the last cycle. This
            // mirrors "state persisted before ack" in spirit: we don't
            // treat a fill as durably recorded until it's been through
            // `CursorFile::append`, which doesn't return until its
            // `fsync` has completed.
            for position in open_positions.iter() {
                if !previously_known_ids.contains(&position.position_id) {
                    positions_cursor.append(position).await?;
                }
            }
        }
    }

    Ok(())
}
