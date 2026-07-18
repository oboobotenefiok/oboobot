//! `oboobot` — real entry point for the QuarterlyTheory_SMT_Trader daemon.
//!
//! Two distinct modes live in this file:
//!
//! - The default, real mode: parse CLI flags, check whether we're inside
//!   a macro cycle window *before touching the broker at all*, and if
//!   so, reconcile and run exactly one cycle, then exit. This is the
//!   shape a GitHub Actions workflow invokes every five minutes: cheap
//!   to run, cheap to skip, no assumption that the process stays alive
//!   between invocations.
//! - `--demo`: the original scripted walkthrough (a clean pass, a
//!   no-divergence cycle, a True-Open rejection, a health-triggered
//!   lockout, a simulated restart), unchanged from the first pass,
//!   useful for anyone exploring this repo who wants to see the whole
//!   pipeline narrated in one run rather than deployed for real.
//!
//! One honest gap, named rather than hidden: the real-mode path below
//! builds its divergence-detection buffers from a fixed offset around
//! the current price rather than genuine rolling daily/session highs
//! and lows tracked across many invocations. That means it will
//! correctly reconcile, correctly persist state, and correctly decide
//! "not in a window, skip" — but it will never actually find a
//! divergence and place a trade, on purpose, until real buffer
//! persistence replaces the placeholder. Faking a buffer that could
//! produce a real trade from synthetic data would be a much worse kind
//! of dishonesty than a buffer that only ever proves the pipeline runs.
//! The order-placing path itself is still fully proven, just by
//! `daemon/tests/integration_test.rs` and `--demo`, not by this path yet.

use std::path::PathBuf;

use broker::{BrokerAdapter, BybitAdapter, DerivAdapter, MockBroker};
use clap::{Parser, ValueEnum};
use daemon::{
    allows_new_entries, apply_reconciliation, auto_action, reconcile, AssistantEngine,
    HealthCheckFailure, HealthMonitor, LoggingAssistant,
};
use domain::{Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Usd};
use persistence::CursorFile;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use risk::RiskEngine as _;
use session_time::Clock;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "oboobot", about = "QuarterlyTheory_SMT_Trader: an SMT-divergence trading daemon")]
struct Cli {
    /// Which broker to trade through. `deriv` and `bybit` are wired into
    /// the trait and read their config from the environment, but their
    /// wire protocols aren't implemented yet, so choosing either fails
    /// clearly rather than pretending to work. `mock` runs end to end.
    #[arg(long, value_enum, default_value_t = BrokerKind::Mock)]
    broker: BrokerKind,

    /// Where cursor files (positions, etc.) are read from and written
    /// to. In the GitHub Actions deployment this points at a checkout of
    /// the dedicated state repo, not a path inside the code repo.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Skip the macro-cycle window check and run a cycle regardless.
    /// Meant for a manual workflow_dispatch debugging run, not the
    /// scheduled path.
    #[arg(long)]
    force: bool,

    /// Run the original scripted walkthrough instead of a single real
    /// cycle. Ignores --broker, --state-dir, and --force.
    #[arg(long)]
    demo: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum BrokerKind {
    Mock,
    Deriv,
    Bybit,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if cli.demo {
        return run_demo().await;
    }

    run_real_cycle(cli).await
}

/// The real, deployable path: check the window first, act only if we're
/// in one (or told to force it), and exit either way without lingering.
async fn run_real_cycle(cli: Cli) -> anyhow::Result<()> {
    let now = session_time::SystemClock.now();

    if !cli.force && !session_time::is_within_macro_cycle(now) {
        tracing::info!(now = %now, "not within a macro cycle window, exiting without contacting the broker");
        return Ok(());
    }

    tracing::info!(broker = ?cli.broker, forced = cli.force, "within a macro cycle window, proceeding");

    tokio::fs::create_dir_all(&cli.state_dir).await?;
    let positions_cursor: CursorFile<Position> =
        CursorFile::new(cli.state_dir.join("positions.cursor"));

    let broker: Box<dyn BrokerAdapter> = match cli.broker {
        BrokerKind::Mock => Box::new(MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000))),
        BrokerKind::Deriv => Box::new(DerivAdapter::connect_from_env().await?),
        BrokerKind::Bybit => Box::new(BybitAdapter::from_env()?),
    };

    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    let locally_known_positions = positions_cursor.read_all().await?;
    let report = reconcile(broker.as_ref(), &locally_known_positions).await?;
    if report.is_clean() {
        tracing::info!("reconciliation: clean, broker and local state agree");
    } else {
        tracing::warn!(
            orphaned = report.orphaned_locally.len(),
            adopted = report.unknown_to_local.len(),
            "reconciliation found a mismatch"
        );
    }
    let mut open_positions = apply_reconciliation(&report);

    if !allows_new_entries(health.current_state()) {
        tracing::info!(
            state = ?health.current_state(),
            action = auto_action(health.current_state()),
            "health state does not currently allow new entries, exiting after reconciliation"
        );
        return Ok(());
    }

    // See the module docs: these buffers are a fixed, always-neutral
    // offset around the live price, not real rolling highs and lows.
    // That makes this call correctly, honestly report "no divergence"
    // every time, until real buffer persistence replaces this.
    let snapshot = broker
        .get_snapshot(&["EURUSD".to_string(), "GBPUSD".to_string()])
        .await?;
    let half_width = dec!(0.00500);
    let primary_price = snapshot
        .prices
        .get("EURUSD")
        .map(|q| q.bid)
        .unwrap_or(dec!(1.10000));
    let secondary_price = snapshot
        .prices
        .get("GBPUSD")
        .map(|q| q.bid)
        .unwrap_or(dec!(1.10000));
    let inputs = DivergenceInputs {
        primary_price,
        secondary_price,
        daily_primary_buffer: placeholder_buffers(primary_price, half_width),
        daily_secondary_buffer: placeholder_buffers(secondary_price, half_width),
        session_primary_buffer: placeholder_buffers(primary_price, half_width),
        session_secondary_buffer: placeholder_buffers(secondary_price, half_width),
    };

    run_cycle(
        "scheduled cycle",
        broker.as_ref(),
        &health,
        &assistant,
        &positions_cursor,
        &mut open_positions,
        inputs,
        Bias::Neutral,
        Bias::Neutral,
    )
    .await?;

    tracing::info!(open_positions = open_positions.len(), "cycle complete");
    Ok(())
}

fn placeholder_buffers(price: Decimal, half_width: Decimal) -> BufferLevels {
    BufferLevels { low: price - half_width, high: price + half_width }
}

/// The original scripted walkthrough against MockBroker: a clean pass, a
/// no-divergence cycle, a True-Open rejection, a health-triggered
/// lockout, and a simulated restart. Unchanged from the first pass.
async fn run_demo() -> anyhow::Result<()> {
    tracing::info!("starting oboobot (QuarterlyTheory_SMT_Trader) demonstration harness");
    tracing::info!("this run is against MockBroker; see main.rs docs for what a live run would change");

    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));
    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    let state_dir = std::env::temp_dir().join("oboobot-demo-state");
    tokio::fs::create_dir_all(&state_dir).await?;
    let positions_cursor_path = state_dir.join("positions.cursor");
    let _ = tokio::fs::remove_file(&positions_cursor_path).await;
    let positions_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);

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
        Bias::Sell,
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

    drop(open_positions);
    let cursor_after_restart: CursorFile<Position> = CursorFile::new(&positions_cursor_path);
    let recovered_positions = cursor_after_restart.read_all().await?;
    let restart_report = reconcile(&broker, &recovered_positions).await?;
    let restart_reconciled = apply_reconciliation(&restart_report);
    tracing::info!(
        recovered_from_disk = recovered_positions.len(),
        reconciled_after_restart = restart_reconciled.len(),
        clean = restart_report.is_clean(),
        "simulated restart: recovered local state and reconciled against the broker"
    );

    tracing::info!("oboobot demonstration harness finished");

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    label: &str,
    broker: &dyn BrokerAdapter,
    health: &HealthMonitor,
    assistant: &dyn AssistantEngine,
    cursor: &CursorFile<Position>,
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

            open_positions.clear();
            open_positions.extend(broker.list_open_positions().await?);

            for position in open_positions.iter() {
                cursor.append(position).await?;
            }
        }
    }

    Ok(())
}
