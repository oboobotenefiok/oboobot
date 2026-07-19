//! `oboobot` — real entry point for the QuarterlyTheory_SMT_Trader daemon.
//!
//! Two distinct modes live in this file:
//!
//! - The default, real mode: parse CLI flags, check the kill switch,
//!   reconcile, run the (always-on, window-independent) exit-condition
//!   sweep, and only then check whether this invocation is inside a
//!   macro cycle window before considering any *new* entry. This is the
//!   shape a GitHub Actions workflow invokes every five minutes.
//! - `--demo`: the original scripted walkthrough, unchanged, useful for
//!   anyone exploring this repo who wants the whole pipeline narrated
//!   in one run rather than deployed for real.
//!
//! What changed in this pass, versus the previous version of this file:
//! real rolling daily/session buffers replace the always-neutral
//! placeholder (so divergence detection can actually fire in the real
//! path now), True Open is captured and persisted rather than hardcoded
//! to Neutral, the spread filter and holiday check are wired in, and
//! open positions are watched every single invocation for risk-reward,
//! pre-news, and SMT-contradiction exits, independent of whether this
//! invocation is inside an entry window at all.

use std::path::PathBuf;

use broker::{BrokerAdapter, BybitAdapter, DerivAdapter, MockBroker};
use clap::{Parser, ValueEnum};
use daemon::{
    already_entered_this_cycle, allows_new_entries, apply_reconciliation, auto_action,
    available_disk_mb, check_broker_heartbeat, evaluate_exits, kill_switch_engaged,
    notifier_from_config, reconcile, resident_memory_mb, AssistantEngine, Config,
    DecisionRecord, HealthCheckFailure, HealthMonitor, LoggingAssistant, NewsProvider,
    NoNewsProvider, StatusSnapshot,
};
use domain::{Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Usd};
use persistence::{CursorFile, SnapshotFile};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use risk::RiskEngine as _;
use session_time::HolidayProvider;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "oboobot", about = "QuarterlyTheory_SMT_Trader: an SMT-divergence trading daemon")]
struct Cli {
    /// Which broker to trade through. `deriv` and `bybit` read their
    /// config from the environment; `deriv` has a real WebSocket client
    /// wired in (some methods still stubbed), `bybit` is fully stubbed.
    /// `mock` runs end to end.
    #[arg(long, value_enum, default_value_t = BrokerKind::Mock)]
    broker: BrokerKind,

    /// Where cursor and snapshot files are read from and written to. In
    /// the GitHub Actions deployment this points at a checkout of the
    /// dedicated state repo.
    #[arg(long, default_value = "./state")]
    state_dir: PathBuf,

    /// Path to the TOML config file. Missing is fine — falls back to
    /// Config::default_config.
    #[arg(long, default_value = "./config.toml")]
    config: PathBuf,

    /// Skip the macro-cycle window check and consider a new entry
    /// regardless. Exit-condition monitoring always runs either way.
    #[arg(long)]
    force: bool,

    /// Run the original scripted walkthrough instead of a real cycle.
    /// Ignores every other flag.
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

/// Writes the status snapshot. Called from every exit path in
/// `run_real_cycle` so `state/status.json` always reflects the most
/// recent invocation, whatever it decided to do.
async fn write_status(
    status_snap: &SnapshotFile<StatusSnapshot>,
    open_positions: &[Position],
    health: &HealthMonitor,
    last_decision: Option<&str>,
    paused: bool,
) {
    let snapshot = StatusSnapshot {
        last_run: Some(chrono::Utc::now()),
        open_position_count: open_positions.len(),
        health_state: format!("{:?}", health.current_state()),
        last_decision: last_decision.map(|s| s.to_string()),
        paused,
    };
    // A failure to write the status file is logged, not propagated:
    // it's a convenience artifact for a human to glance at, not
    // something the cycle's actual correctness depends on.
    if let Err(error) = status_snap.write(&snapshot).await {
        tracing::warn!(%error, "failed to write status snapshot");
    }
}

/// The real, deployable path.
async fn run_real_cycle(cli: Cli) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&cli.state_dir).await?;
    let status_snap: SnapshotFile<StatusSnapshot> = SnapshotFile::new(cli.state_dir.join("status.json"));

    if kill_switch_engaged(&cli.state_dir).await {
        tracing::warn!("kill switch (PAUSED file) engaged, exiting without evaluating anything new");
        let health = HealthMonitor::new();
        write_status(&status_snap, &[], &health, Some("paused"), true).await;
        return Ok(());
    }

    let config = Config::load(&cli.config).await?;
    let Some(pair_config) = config.pairs.first().cloned() else {
        anyhow::bail!("no pairs configured");
    };
    let primary = pair_config.primary;
    let secondary = pair_config.secondary;

    let health = HealthMonitor::new();
    let notifier = notifier_from_config(&config.notifications);
    let news_provider = NoNewsProvider;
    let holidays = session_time::StaticHolidayProvider;

    let positions_cursor: CursorFile<Position> = CursorFile::new(cli.state_dir.join("positions.cursor"));
    let decisions_cursor: CursorFile<DecisionRecord> = CursorFile::new(cli.state_dir.join("decisions.cursor"));
    let daily_primary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{primary}.json")));
    let daily_secondary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{secondary}.json")));
    let session_primary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_session_{primary}.json")));
    let session_secondary_snap: SnapshotFile<strategy::RollingBuffer> =
        SnapshotFile::new(cli.state_dir.join(format!("buffer_session_{secondary}.json")));
    let correlation_snap: SnapshotFile<strategy::CorrelationState> =
        SnapshotFile::new(cli.state_dir.join("correlation.json"));
    let spread_snap: SnapshotFile<strategy::SpreadHistory> =
        SnapshotFile::new(cli.state_dir.join("spread_history.json"));
    let weekly_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_weekly.json"));
    let daily_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_daily.json"));

    let broker: Box<dyn BrokerAdapter> = match cli.broker {
        BrokerKind::Mock => Box::new(MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000))),
        BrokerKind::Deriv => Box::new(DerivAdapter::connect_from_env().await?),
        BrokerKind::Bybit => Box::new(BybitAdapter::from_env()?),
    };

    // Reconciliation always runs first: what does local state say is
    // open, and does the broker agree?
    let locally_known_positions = positions_cursor.read_all().await?;
    let report = reconcile(broker.as_ref(), &locally_known_positions).await?;
    if !report.is_clean() {
        tracing::warn!(
            orphaned = report.orphaned_locally.len(),
            adopted = report.unknown_to_local.len(),
            "reconciliation found a mismatch"
        );
        notifier
            .notify(&format!(
                "oboobot: reconciliation mismatch (orphaned={}, adopted={})",
                report.orphaned_locally.len(),
                report.unknown_to_local.len()
            ))
            .await;
    } else {
        tracing::info!(known_positions = locally_known_positions.len(), "reconciliation clean");
    }
    let mut open_positions = apply_reconciliation(&report);

    // The heartbeat-wrapped snapshot call: this is both the broker
    // health check and the actual market data for everything below it.
    let heartbeat_timeout = std::time::Duration::from_secs(15);
    let snapshot = match check_broker_heartbeat(
        &health,
        heartbeat_timeout,
        broker.get_snapshot(&[primary.clone(), secondary.clone()]),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::error!(%error, "broker heartbeat failed");
            notifier.notify(&format!("oboobot: broker heartbeat failed: {error}")).await;
            write_status(&status_snap, &open_positions, &health, Some("heartbeat_failed"), false).await;
            return Ok(());
        }
    };

    let now = chrono::Utc::now();
    let primary_price = snapshot.prices.get(&primary).map(|q| q.bid).unwrap_or(Decimal::ZERO);
    let secondary_price = snapshot.prices.get(&secondary).map(|q| q.bid).unwrap_or(Decimal::ZERO);
    tracing::info!(
        %primary, primary_price = %primary_price, %secondary, secondary_price = %secondary_price,
        "broker heartbeat ok, snapshot fetched"
    );

    if let Some(mb) = available_disk_mb(&cli.state_dir).await {
        tracing::debug!(available_disk_mb = mb, "disk check");
        if mb < 500 {
            health.report_failure(HealthCheckFailure::DiskUsageCritical);
        } else {
            health.clear_failure(HealthCheckFailure::DiskUsageCritical);
        }
    }
    if let Some(mb) = resident_memory_mb().await {
        tracing::debug!(resident_memory_mb = mb, "memory check");
        if mb > 512 {
            health.report_failure(HealthCheckFailure::MemoryUsageCritical);
        } else {
            health.clear_failure(HealthCheckFailure::MemoryUsageCritical);
        }
    }

    // Update the rolling market-state files. Always, regardless of
    // window: even a cycle that ends up not trading still contributes
    // an observation to the buffers, correlation window, and spread
    // history, which is exactly what lets those be real by the time a
    // window does roll around.
    let daily_primary = strategy::update_daily_buffer(daily_primary_snap.read().await?, primary_price, now);
    daily_primary_snap.write(&daily_primary).await?;
    let daily_secondary = strategy::update_daily_buffer(daily_secondary_snap.read().await?, secondary_price, now);
    daily_secondary_snap.write(&daily_secondary).await?;
    let session_primary = strategy::update_session_buffer(session_primary_snap.read().await?, primary_price, now);
    session_primary_snap.write(&session_primary).await?;
    let session_secondary =
        strategy::update_session_buffer(session_secondary_snap.read().await?, secondary_price, now);
    session_secondary_snap.write(&session_secondary).await?;

    let mut correlation_state = correlation_snap.read().await?.unwrap_or_default();
    correlation_state = strategy::record_sample(correlation_state, primary_price, secondary_price);
    correlation_snap.write(&correlation_state).await?;
    if let Some(shift) = strategy::detect_regime_shift(&correlation_state, config.risk.regime_shift_threshold) {
        tracing::warn!(baseline = shift.baseline, current = shift.current, "correlation regime shift detected");
        notifier
            .notify(&format!(
                "oboobot: correlation regime shift (baseline {:.2} -> current {:.2})",
                shift.baseline, shift.current
            ))
            .await;
    }

    let mut spread_history = spread_snap.read().await?.unwrap_or_default();
    let current_spread = snapshot.spreads.get(&primary).copied().unwrap_or(Decimal::ZERO);
    spread_history.record(current_spread);
    spread_snap.write(&spread_history).await?;

    let divergence_inputs = DivergenceInputs {
        primary_price,
        secondary_price,
        daily_primary_buffer: daily_primary.as_buffer_levels(),
        daily_secondary_buffer: daily_secondary.as_buffer_levels(),
        session_primary_buffer: session_primary.as_buffer_levels(),
        session_secondary_buffer: session_secondary.as_buffer_levels(),
    };
    let current_divergence = strategy::evaluate_smt(&divergence_inputs);
    tracing::debug!(
        daily_primary_high = %daily_primary.high, daily_primary_low = %daily_primary.low,
        session_primary_high = %session_primary.high, session_primary_low = %session_primary.low,
        divergence = ?current_divergence,
        "market state updated"
    );

    // Exit-condition monitoring: always runs, independent of the entry
    // window below. This is the fix for the bigger of the two gaps
    // named in review: a position no longer sits unwatched between the
    // cycle that opened it and whenever the next window happens to be.
    let news_events = news_provider.upcoming_events(now, chrono::Duration::minutes(15)).await;
    let exits = evaluate_exits(
        &open_positions,
        primary_price,
        &news_events,
        now,
        chrono::Duration::minutes(15),
        current_divergence,
    );
    for exit in &exits {
        match broker.close_position(exit.position_id).await {
            Ok(order) => {
                tracing::info!(position_id = %exit.position_id, reason = ?exit.reason, order_id = %order.order_id, "position closed");
                notifier
                    .notify(&format!("oboobot: closed position {} ({:?})", exit.position_id, exit.reason))
                    .await;
                decisions_cursor
                    .append(&DecisionRecord::new(primary.clone(), "position_closed").with_detail(format!("{:?}", exit.reason)))
                    .await?;
            }
            Err(error) => {
                tracing::error!(%error, position_id = %exit.position_id, "failed to close a position flagged for exit");
            }
        }
    }
    if !exits.is_empty() {
        open_positions = broker.list_open_positions().await?;
        for position in &open_positions {
            positions_cursor.append(position).await?;
        }
    } else {
        tracing::debug!(open_positions = open_positions.len(), "exit sweep: nothing to close");
    }

    // Everything from here on is about *new* entries, which the window
    // gates and exits never were.
    if !cli.force && !session_time::is_within_macro_cycle(now) {
        tracing::info!("not within a macro cycle window; exits were already checked above, no new entry considered");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "outside_window")).await?;
        write_status(&status_snap, &open_positions, &health, Some("outside_window"), false).await;
        return Ok(());
    }
    tracing::info!(forced = cli.force, "within a macro cycle window, considering a new entry");

    if !allows_new_entries(health.current_state()) {
        tracing::info!(state = ?health.current_state(), action = auto_action(health.current_state()), "health state does not allow new entries");
        decisions_cursor
            .append(&DecisionRecord::new(primary.clone(), "health_blocked").with_detail(format!("{:?}", health.current_state())))
            .await?;
        write_status(&status_snap, &open_positions, &health, Some("health_blocked"), false).await;
        return Ok(());
    }

    if holidays.is_low_liquidity(now.date_naive()) {
        tracing::info!("today is a recognized low-liquidity period, skipping new entries");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "holiday_skip")).await?;
        write_status(&status_snap, &open_positions, &health, Some("holiday_skip"), false).await;
        return Ok(());
    }

    let spread_multiplier = Decimal::try_from(config.risk.spread_multiplier).unwrap_or(dec!(1.5));
    if !spread_history.passes_filter(current_spread, spread_multiplier) {
        tracing::info!(current_spread = %current_spread, "spread filter rejected this cycle");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "spread_rejected")).await?;
        write_status(&status_snap, &open_positions, &health, Some("spread_rejected"), false).await;
        return Ok(());
    }

    if already_entered_this_cycle(&primary, &open_positions, now) {
        tracing::info!("already entered this pair within the current cycle window, skipping");
        decisions_cursor.append(&DecisionRecord::new(primary.clone(), "collision_skip")).await?;
        write_status(&status_snap, &open_positions, &health, Some("collision_skip"), false).await;
        return Ok(());
    }

    let weekly_bias = load_or_capture_bias(
        &weekly_true_open_snap,
        session_time::Timeframe::Weekly,
        &primary,
        primary_price,
        now,
        &holidays,
    )
    .await?;
    let daily_bias = load_or_capture_bias(
        &daily_true_open_snap,
        session_time::Timeframe::Daily,
        &primary,
        primary_price,
        now,
        &holidays,
    )
    .await?;

    let outcome = generate_signal(
        &divergence_inputs,
        weekly_bias,
        daily_bias,
        primary.clone(),
        snapshot.snapshot_id,
        dec!(0.8),
        dec!(0.8),
        now + chrono::Duration::minutes(20),
    );

    let last_decision = match outcome {
        SignalOutcome::NoDivergence => {
            tracing::info!("no SMT divergence this cycle, nothing to evaluate");
            decisions_cursor.append(&DecisionRecord::new(primary.clone(), "no_divergence")).await?;
            "no_divergence".to_string()
        }
        SignalOutcome::Rejected(invalidated) => {
            tracing::info!(
                reason = ?invalidated.rejection_reason,
                weekly_bias = ?invalidated.weekly_bias,
                daily_bias = ?invalidated.daily_bias,
                smt_direction = ?invalidated.smt_direction,
                "signal generated but rejected by the True Open gate"
            );
            decisions_cursor
                .append(&DecisionRecord::new(primary.clone(), "gate_rejected").with_detail(format!("{:?}", invalidated.rejection_reason)))
                .await?;
            "gate_rejected".to_string()
        }
        SignalOutcome::Signal(signal) => {
            tracing::info!(tier = ?signal.tier, direction = ?signal.direction, "signal passed the True Open gate");
            let risk_config = risk::RiskConfig {
                base_risk_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.base_risk_percent).unwrap_or(dec!(1.0))),
                max_risk_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.max_risk_percent).unwrap_or(dec!(5.0))),
                max_open_positions: config.risk.max_open_positions,
                daily_loss_limit_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.daily_loss_limit_percent).unwrap_or(dec!(5.0))),
                weekly_loss_limit_percent: domain::Percent::from_percentage(Decimal::try_from(config.risk.weekly_loss_limit_percent).unwrap_or(dec!(10.0))),
            };

            let equity = broker.get_account_equity().await?;
            let entry_price = match signal.direction {
                Direction::Buy => snapshot.prices.get(&primary).map(|q| q.ask).unwrap_or(primary_price),
                Direction::Sell => primary_price,
            };
            let stop_loss_price = match signal.direction {
                Direction::Buy => entry_price - dec!(0.0050),
                Direction::Sell => entry_price + dec!(0.0050),
            };
            let take_profit_price = match signal.direction {
                Direction::Buy => entry_price + dec!(0.0150),
                Direction::Sell => entry_price - dec!(0.0150),
            };

            let risk_context = risk::RiskContext {
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
            let decision = risk_engine.evaluate(&signal, &risk_config, &risk_context)?;

            if !decision.approved {
                tracing::info!(reason = ?decision.rejection_reason, "risk engine rejected the signal");
                decisions_cursor
                    .append(&DecisionRecord::new(primary.clone(), "risk_rejected").with_detail(decision.rejection_reason.clone().unwrap_or_default()))
                    .await?;
                "risk_rejected".to_string()
            } else {
                tracing::info!(
                    size = %decision.position_size, risk_percent = %decision.risk_percent, risk_currency = %decision.risk_currency,
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
                tracing::info!(order_id = %order.order_id, status = ?order.status, "order submitted");
                notifier
                    .notify(&format!("oboobot: opened {:?} {} (size {})", signal.direction, primary, decision.position_size))
                    .await;

                open_positions = broker.list_open_positions().await?;
                for position in &open_positions {
                    positions_cursor.append(position).await?;
                }
                decisions_cursor.append(&DecisionRecord::new(primary.clone(), "order_submitted")).await?;
                "order_submitted".to_string()
            }
        }
    };

    write_status(&status_snap, &open_positions, &health, Some(&last_decision), false).await;
    Ok(())
}

/// Load the persisted True Open level for `timeframe`, capturing a fresh
/// one if it's missing or expired, and return the bias that level (or
/// its absence, for a partial week) implies against the current price.
async fn load_or_capture_bias(
    snap: &SnapshotFile<session_time::TrueOpenLevel>,
    timeframe: session_time::Timeframe,
    symbol: &str,
    price: Decimal,
    now: chrono::DateTime<chrono::Utc>,
    holidays: &dyn HolidayProvider,
) -> anyhow::Result<Bias> {
    let mut level = snap.read().await?;
    if session_time::needs_capture(now, level.as_ref()) {
        level = session_time::capture_level(timeframe, symbol, price, now, holidays);
        if let Some(level) = &level {
            snap.write(level).await?;
        }
    }
    Ok(level.map(|l| session_time::bias_from_price(price, l.level)).unwrap_or(Bias::Neutral))
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
