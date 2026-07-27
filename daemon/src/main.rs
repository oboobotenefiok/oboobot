//! `oboobot`: real entry point for the QuarterlyTheory_SMT_Trader daemon.
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

use std::{collections::BTreeMap, path::PathBuf};

use broker::{BrokerAdapter, BybitAdapter, DerivAdapter, MockBroker};
use clap::{Parser, ValueEnum};
use daemon::{
    allows_new_entries, already_entered_this_cycle, apply_reconciliation, auto_action,
    available_disk_mb, check_broker_heartbeat, evaluate_exits, kill_switch_engaged,
    notifier_from_config, reconcile, resident_memory_mb, AssistantEngine, Config, DecisionRecord,
    HealthCheckFailure, HealthMonitor, LoggingAssistant, NewsProvider, NoNewsProvider, PairConfig,
    StatusSnapshot,
};
use domain::{
    Bias, Direction, Event, EventEnvelope, OrderRequest, OrderType, Position, Tier, TradeSignal,
    Usd,
};
use persistence::{CursorFile, SnapshotFile};
use risk::RiskEngine as _;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use session_time::HolidayProvider;
use strategy::{generate_signal, BufferLevels, DivergenceInputs, SignalOutcome, TradeTarget};
use uuid::Uuid;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "oboobot",
    about = "QuarterlyTheory_SMT_Trader: an SMT-divergence trading daemon"
)]
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

    /// Path to the TOML config file. Missing is fine, falls back to
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

    /// Replay this many days of history through the real cycle logic
    /// instead of trading live. Fetches historical daily closes for the
    /// first configured pair-set via --broker (which must be able to
    /// offer them; MockBroker cannot), then steps through them one
    /// simulated day at a time. Ignores --force; a replay run always
    /// evaluates every simulated day as if it were inside the window.
    #[arg(long)]
    replay_days: Option<u32>,
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

    spawn_shutdown_logger();

    let cli = Cli::parse();

    if cli.demo {
        return run_demo().await;
    }

    if let Some(days) = cli.replay_days {
        return run_replay(cli, days).await;
    }

    let broker: Box<dyn BrokerAdapter> = match cli.broker {
        BrokerKind::Mock => Box::new(MockBroker::new(
            Usd::from_decimal(dec!(10000)),
            dec!(1.10000),
        )),
        BrokerKind::Deriv => Box::new(DerivAdapter::connect_from_env().await?),
        BrokerKind::Bybit => Box::new(BybitAdapter::from_env()?),
    };
    run_real_cycle(cli, &session_time::SystemClock, broker.as_ref()).await
}

/// Logs a SIGTERM if one arrives, but never interrupts the in-flight
/// cycle to act on it. An earlier attempt at SIGTERM handling here
/// cancelled the running cycle outright (via `tokio::select!` racing
/// the signal against the cycle's own future), which risked abandoning
/// a broker call mid-flight: an order already sent to Deriv but whose
/// response never got processed, leaving local and broker state out of
/// sync in exactly the way reconciliation exists to catch, not cause.
/// GitHub Actions gives a grace period after SIGTERM before SIGKILL
/// follows, and a single invocation's cycle is short, so simply letting
/// it run to completion while logging that a shutdown was requested is
/// both safer and just as useful for a batch job like this one. Unix
/// only (`tokio::signal::unix`), which matches every other assumption
/// this codebase already makes about where it runs (GitHub Actions'
/// Ubuntu runners); if the signal listener itself fails to install,
/// that's not worth failing the whole process over, so it's just
/// silently skipped.
fn spawn_shutdown_logger() {
    tokio::spawn(async {
        let Ok(mut term) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            return;
        };
        term.recv().await;
        tracing::warn!(
            "received SIGTERM; letting the in-flight cycle run to completion rather than \
             interrupting it, since cancelling mid-cycle risks abandoning a broker call in an \
             ambiguous state"
        );
    });
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

/// One configured pair-set's market state for a single cycle: its own
/// prices, buffers-derived divergence inputs, resolved divergence (if
/// any), and spread history. Collected once per pair-set before the
/// exit sweep (which needs all of them at once) and then walked again
/// for entries (which only needs one at a time).
struct PairCycleState {
    pair_config: PairConfig,
    primary_price: Decimal,
    secondary_price: Decimal,
    divergence_inputs: DivergenceInputs,
    resolved_divergence: Option<(String, Direction, Tier)>,
    spread_history: strategy::SpreadHistory,
    current_spread: Decimal,
    /// The live Pearson coefficient between this pair-set's primary and
    /// secondary, `None` if there aren't enough samples yet. Reused for
    /// the correlated-exposure risk check below: it only ever covers
    /// this pair-set's own primary/secondary relationship, not a full
    /// matrix against every other configured pair-set's pairs too, so a
    /// position on some other pair-set never counts as correlated
    /// exposure here even if it happens to be, in reality. Covering
    /// that would need a correlation window per pair of symbols, not
    /// per pair-set, which is real work of its own.
    correlation_coefficient: Option<f64>,
}

/// The real, deployable path.
async fn run_real_cycle(
    cli: Cli,
    clock: &dyn session_time::Clock,
    broker: &dyn BrokerAdapter,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&cli.state_dir).await?;
    let status_snap: SnapshotFile<StatusSnapshot> =
        SnapshotFile::new(cli.state_dir.join("status.json"));

    if kill_switch_engaged(&cli.state_dir).await {
        tracing::warn!(
            "kill switch (PAUSED file) engaged, exiting without evaluating anything new"
        );
        let health = HealthMonitor::new();
        write_status(&status_snap, &[], &health, Some("paused"), true).await;
        return Ok(());
    }

    let config = Config::load(&cli.config).await?;
    if config.pairs.is_empty() {
        anyhow::bail!("no pairs configured");
    }

    let health = HealthMonitor::new();
    let notifier = notifier_from_config(&config.notifications);
    let news_provider = NoNewsProvider;
    let holidays = session_time::StaticHolidayProvider;

    let positions_cursor: CursorFile<Position> =
        CursorFile::new(cli.state_dir.join("positions.cursor"));
    let decisions_cursor: CursorFile<DecisionRecord> =
        CursorFile::new(cli.state_dir.join("decisions.cursor"));
    let weekly_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_weekly.json"));
    let daily_true_open_snap: SnapshotFile<session_time::TrueOpenLevel> =
        SnapshotFile::new(cli.state_dir.join("true_open_daily.json"));

    // Reconciliation always runs first: what does local state say is
    // open, and does the broker agree?
    let locally_known_positions = positions_cursor.read_all().await?;
    let mut open_positions =
        reconcile_and_notify(broker, &locally_known_positions, notifier.as_ref()).await?;

    // One snapshot covers every pair-set configured this cycle: the
    // union of each pair-set's primary and secondary, deduplicated, so a
    // symbol shared across two pair-sets is only ever fetched once.
    let mut all_symbols: Vec<String> = Vec::new();
    for pair_config in &config.pairs {
        if !all_symbols.contains(&pair_config.primary) {
            all_symbols.push(pair_config.primary.clone());
        }
        if !all_symbols.contains(&pair_config.secondary) {
            all_symbols.push(pair_config.secondary.clone());
        }
    }

    // The heartbeat-wrapped snapshot call: this is both the broker
    // health check and the actual market data for everything below it.
    let heartbeat_timeout = std::time::Duration::from_secs(15);
    let snapshot = match check_broker_heartbeat(
        &health,
        heartbeat_timeout,
        broker.get_snapshot(&all_symbols),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::error!(%error, "broker heartbeat failed");
            notifier
                .notify(&format!("oboobot: broker heartbeat failed: {error}"))
                .await;
            write_status(
                &status_snap,
                &open_positions,
                &health,
                Some("heartbeat_failed"),
                false,
            )
            .await;
            return Ok(());
        }
    };

    let now = clock.now();
    // Quarterly Theory's Tuesday risk-doubling is an NY-calendar-day
    // concept, like every other session boundary this strategy cares
    // about, so it's computed in NY local time rather than UTC: the two
    // disagree for several hours around each NY midnight, which matters
    // right at the day boundary this flag cares about.
    let is_tuesday = chrono::Datelike::weekday(&session_time::to_ny(now)) == chrono::Weekday::Tue;
    tracing::debug!(%is_tuesday, "day-of-week check (NY time)");

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

    // How far behind wall-clock time the broker's own reported snapshot
    // timestamp is. A broker feed can respond successfully (so the
    // heartbeat check above stays green) while still handing back
    // increasingly stale prices, which this catches and the heartbeat
    // check can't.
    let snapshot_latency = now.signed_duration_since(snapshot.timestamp);
    tracing::debug!(
        snapshot_latency_seconds = snapshot_latency.num_seconds(),
        "snapshot latency check"
    );
    if snapshot_latency.num_seconds() > config.risk.snapshot_latency_threshold_seconds {
        health.report_failure(HealthCheckFailure::SnapshotLatencyExceeded);
    } else {
        health.clear_failure(HealthCheckFailure::SnapshotLatencyExceeded);
    }

    if news_provider.is_fresh(now).await {
        health.clear_failure(HealthCheckFailure::NewsApiDown);
    } else {
        health.report_failure(HealthCheckFailure::NewsApiDown);
    }

    // Per-pair-set market state. Each pair-set gets its own buffers,
    // correlation window, and spread history, since a GBPUSD/EURUSD
    // divergence reading has nothing to do with a USDJPY/AUDUSD one, and
    // averaging them together would corrupt both.
    let mut pair_states = Vec::with_capacity(config.pairs.len());
    let mut any_correlation_stale = false;
    let mut any_spread_stale = false;
    let correlation_staleness_limit =
        chrono::Duration::minutes(config.risk.correlation_staleness_minutes);
    let spread_staleness_limit = chrono::Duration::minutes(config.risk.spread_staleness_minutes);
    for pair_config in &config.pairs {
        let primary = &pair_config.primary;
        let secondary = &pair_config.secondary;

        let primary_price = snapshot
            .prices
            .get(primary)
            .map(|q| q.bid)
            .unwrap_or(Decimal::ZERO);
        let secondary_price = snapshot
            .prices
            .get(secondary)
            .map(|q| q.bid)
            .unwrap_or(Decimal::ZERO);

        let daily_primary_snap: SnapshotFile<strategy::RollingBuffer> =
            SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{primary}.json")));
        let daily_secondary_snap: SnapshotFile<strategy::RollingBuffer> =
            SnapshotFile::new(cli.state_dir.join(format!("buffer_daily_{secondary}.json")));
        let session_primary_snap: SnapshotFile<strategy::RollingBuffer> =
            SnapshotFile::new(cli.state_dir.join(format!("buffer_session_{primary}.json")));
        let session_secondary_snap: SnapshotFile<strategy::RollingBuffer> = SnapshotFile::new(
            cli.state_dir
                .join(format!("buffer_session_{secondary}.json")),
        );
        let correlation_snap: SnapshotFile<strategy::CorrelationState> = SnapshotFile::new(
            cli.state_dir
                .join(format!("correlation_{primary}_{secondary}.json")),
        );
        let spread_snap: SnapshotFile<strategy::SpreadHistory> =
            SnapshotFile::new(cli.state_dir.join(format!("spread_history_{primary}.json")));

        let daily_primary =
            strategy::update_daily_buffer(daily_primary_snap.read().await?, primary_price, now);
        daily_primary_snap.write(&daily_primary).await?;
        let daily_secondary =
            strategy::update_daily_buffer(daily_secondary_snap.read().await?, secondary_price, now);
        daily_secondary_snap.write(&daily_secondary).await?;
        let session_primary =
            strategy::update_session_buffer(session_primary_snap.read().await?, primary_price, now);
        session_primary_snap.write(&session_primary).await?;
        let session_secondary = strategy::update_session_buffer(
            session_secondary_snap.read().await?,
            secondary_price,
            now,
        );
        session_secondary_snap.write(&session_secondary).await?;

        let mut correlation_state = correlation_snap.read().await?.unwrap_or_default();
        // A fresh window (no samples yet, whether this is the very
        // first cycle for this pair-set or the state file was somehow
        // cleared) gets a one-time historical warmup before today's
        // live sample joins it, so correlation readings are meaningful
        // from the start instead of needing weeks of live cycles to
        // fill a 90-day window. Not every broker can offer this
        // (fetch_historical_prices defaults to NotImplemented), in
        // which case this is skipped rather than failing the cycle
        // over it: live observations populate the window either way,
        // just more slowly.
        if correlation_state.samples.is_empty() {
            match backfill_correlation(broker, primary, secondary).await {
                Ok(samples) => {
                    let backfilled = samples.len();
                    for (primary_close, secondary_close) in samples {
                        correlation_state = strategy::record_sample(
                            correlation_state,
                            primary_close,
                            secondary_close,
                            now,
                        );
                    }
                    tracing::info!(%primary, %secondary, backfilled, "seeded correlation window from historical prices");
                }
                Err(error) => {
                    tracing::debug!(%primary, %secondary, %error, "historical correlation backfill unavailable, learning from live data only");
                }
            }
        }
        // Captured before record_sample below stamps this cycle's own
        // now onto it: staleness has to mean "the last invocation that
        // actually got this far was too long ago," not "the update this
        // line is about to perform is itself late," which would never
        // be true.
        let correlation_last_updated_before_this_cycle = correlation_state.last_updated;
        correlation_state =
            strategy::record_sample(correlation_state, primary_price, secondary_price, now);
        correlation_snap.write(&correlation_state).await?;
        if let Some(shift) =
            strategy::detect_regime_shift(&correlation_state, config.risk.regime_shift_threshold)
        {
            tracing::warn!(
                %primary, %secondary, baseline = shift.baseline, current = shift.current,
                "correlation regime shift detected"
            );
            notifier
                .notify(&format!(
                    "oboobot: correlation regime shift on {primary}/{secondary} (baseline {:.2} -> current {:.2})",
                    shift.baseline, shift.current
                ))
                .await;
        }

        let mut spread_history = spread_snap.read().await?.unwrap_or_default();
        // Same reasoning as correlation_last_updated_before_this_cycle
        // above: captured before this cycle's own record() call, so the
        // staleness check below reflects the previous invocation, not
        // this one.
        let spread_last_updated_before_this_cycle = spread_history.last_updated;
        let current_spread = snapshot
            .spreads
            .get(primary)
            .copied()
            .unwrap_or(Decimal::ZERO);
        spread_history.record(current_spread, now);
        spread_snap.write(&spread_history).await?;

        let divergence_inputs = DivergenceInputs {
            primary_price,
            secondary_price,
            daily_primary_buffer: daily_primary.as_buffer_levels(),
            daily_secondary_buffer: daily_secondary.as_buffer_levels(),
            session_primary_buffer: session_primary.as_buffer_levels(),
            session_secondary_buffer: session_secondary.as_buffer_levels(),
        };
        let resolved_divergence =
            strategy::evaluate_smt(&divergence_inputs).map(|(target, direction, tier)| {
                let pair = match target {
                    TradeTarget::Primary => primary.clone(),
                    TradeTarget::Secondary => secondary.clone(),
                };
                (pair, direction, tier)
            });
        tracing::debug!(
            %primary, %secondary,
            daily_primary_high = %daily_primary.high, daily_primary_low = %daily_primary.low,
            session_primary_high = %session_primary.high, session_primary_low = %session_primary.low,
            divergence = ?resolved_divergence,
            "market state updated"
        );

        // A brand new state (last_updated still None) isn't "stale" in
        // the sense this check cares about, it just hasn't had a first
        // sample yet; that's an expected, temporary condition for a
        // newly-added pair-set, not a data source that's stopped
        // updating. Only an actual age past the threshold counts.
        if correlation_last_updated_before_this_cycle
            .is_some_and(|t| now - t > correlation_staleness_limit)
        {
            any_correlation_stale = true;
        }
        if spread_last_updated_before_this_cycle.is_some_and(|t| now - t > spread_staleness_limit) {
            any_spread_stale = true;
        }

        pair_states.push(PairCycleState {
            pair_config: pair_config.clone(),
            primary_price,
            secondary_price,
            divergence_inputs,
            resolved_divergence,
            spread_history,
            current_spread,
            correlation_coefficient: strategy::compute_coefficient(&correlation_state),
        });
    }

    if any_correlation_stale {
        health.report_failure(HealthCheckFailure::CorrelationStale);
    } else {
        health.clear_failure(HealthCheckFailure::CorrelationStale);
    }
    if any_spread_stale {
        health.report_failure(HealthCheckFailure::SpreadHistoryStale);
    } else {
        health.clear_failure(HealthCheckFailure::SpreadHistoryStale);
    }

    // Exit-condition monitoring: always runs, independent of the entry
    // window below, and covers every open position regardless of which
    // configured pair-set it came from. This is the fix for the bigger
    // of the two gaps named in review: a position no longer sits
    // unwatched between the cycle that opened it and whenever the next
    // window happens to be.
    let news_events = news_provider
        .upcoming_events(now, chrono::Duration::minutes(15))
        .await;
    let current_prices: BTreeMap<String, Decimal> = snapshot
        .prices
        .iter()
        .map(|(pair, quote)| (pair.clone(), quote.bid))
        .collect();
    let current_divergences: BTreeMap<String, (Direction, Tier)> = pair_states
        .iter()
        .filter_map(|state| {
            state
                .resolved_divergence
                .as_ref()
                .map(|(pair, direction, tier)| (pair.clone(), (*direction, *tier)))
        })
        .collect();
    let exits = evaluate_exits(
        &open_positions,
        &current_prices,
        &news_events,
        now,
        chrono::Duration::minutes(15),
        &current_divergences,
    );
    for exit in &exits {
        match broker.close_position(exit.position_id).await {
            Ok(order) => {
                tracing::info!(position_id = %exit.position_id, reason = ?exit.reason, order_id = %order.order_id, "position closed");
                notifier
                    .notify(&format!(
                        "oboobot: closed position {} ({:?})",
                        exit.position_id, exit.reason
                    ))
                    .await;
                // Look up which pair the closed position actually was:
                // exits aren't only ever primary anymore, so the decision
                // log should say which one this was, not default to
                // whichever pair-set happens to be configured first.
                let closed_pair = open_positions
                    .iter()
                    .find(|p| p.position_id == exit.position_id)
                    .map(|p| p.pair.clone())
                    .unwrap_or_else(|| config.pairs[0].primary.clone());
                decisions_cursor
                    .append(
                        &DecisionRecord::new(closed_pair, "position_closed")
                            .with_detail(format!("{:?}", exit.reason)),
                    )
                    .await?;
            }
            Err(error) => {
                tracing::error!(%error, position_id = %exit.position_id, "failed to close a position flagged for exit");
            }
        }
    }
    if !exits.is_empty() {
        // Reconciling here (not just re-listing) is the "after fills"
        // half of the original spec's requirement; the startup
        // reconciliation is the other half. The in-memory open_positions
        // from just before this closed, not another read of the cursor
        // file, is the locally-known baseline: the cursor log is an
        // append-only history that never marks an old entry closed, so
        // reading it back here would permanently misflag every position
        // that ever closed as still "locally known" long after it
        // stopped being true.
        open_positions = reconcile_and_notify(broker, &open_positions, notifier.as_ref()).await?;
        for position in &open_positions {
            positions_cursor.append(position).await?;
        }
    } else {
        tracing::debug!(
            open_positions = open_positions.len(),
            "exit sweep: nothing to close"
        );
    }

    // Everything from here on is about *new* entries, which the window
    // gates and exits never were. These three gates are global rather
    // than per-pair-set: the window and holiday check are pure
    // functions of `now`, and health state is account-wide, so none of
    // them depend on which pair-set a signal might end up naming.
    if !cli.force && !session_time::is_within_macro_cycle(now) {
        tracing::info!(
            "not within a macro cycle window; exits were already checked above, no new entry considered"
        );
        write_status(
            &status_snap,
            &open_positions,
            &health,
            Some("outside_window"),
            false,
        )
        .await;
        return Ok(());
    }
    tracing::info!(
        forced = cli.force,
        "within a macro cycle window, considering new entries"
    );

    if !allows_new_entries(health.current_state()) {
        tracing::info!(state = ?health.current_state(), action = auto_action(health.current_state()), "health state does not allow new entries");
        write_status(
            &status_snap,
            &open_positions,
            &health,
            Some("health_blocked"),
            false,
        )
        .await;
        return Ok(());
    }

    if holidays.is_low_liquidity(now.date_naive()) {
        tracing::info!("today is a recognized low-liquidity period, skipping new entries");
        write_status(
            &status_snap,
            &open_positions,
            &health,
            Some("holiday_skip"),
            false,
        )
        .await;
        return Ok(());
    }

    // Net exposure per currency across every currently open position,
    // regardless of which pair-set it came from: this is what the
    // currency-exposure risk check compares each candidate signal
    // against. Computed fresh inside the loop below, per pair-set, not
    // once here: an earlier pair-set in this same cycle can open a new
    // position, and a later pair-set's own exposure check needs to see
    // it, the same reason correlated_exposure is already computed
    // fresh per pair-set rather than once up front.
    fn currency_exposure_snapshot(positions: &[Position]) -> BTreeMap<String, Decimal> {
        let mut exposure = BTreeMap::new();
        for position in positions {
            if let Some((base, quote)) = risk::currency_pair(&position.pair) {
                let size: Decimal = position.legs.iter().map(|leg| leg.size).sum();
                let (base_direction, quote_direction) = match position.direction {
                    Direction::Buy => (Decimal::ONE, -Decimal::ONE),
                    Direction::Sell => (-Decimal::ONE, Decimal::ONE),
                };
                *exposure.entry(base.to_string()).or_insert(Decimal::ZERO) += base_direction * size;
                *exposure.entry(quote.to_string()).or_insert(Decimal::ZERO) +=
                    quote_direction * size;
            }
        }
        exposure
    }

    // One pass per configured pair-set: each gets its own spread filter,
    // True Open bias, signal, collision check, and risk decision. A
    // decision label is collected per pair-set so status.json's single
    // summary field still says something useful about every pair-set
    // this cycle touched, not just the last one evaluated.
    let mut decision_summaries = Vec::with_capacity(pair_states.len());
    for state in &pair_states {
        let primary = &state.pair_config.primary;
        let secondary = &state.pair_config.secondary;
        let label = format!("{primary}/{secondary}");

        let spread_multiplier =
            Decimal::try_from(config.risk.spread_multiplier).unwrap_or(dec!(1.5));
        if !state
            .spread_history
            .passes_filter(state.current_spread, spread_multiplier)
        {
            tracing::info!(%primary, current_spread = %state.current_spread, "spread filter rejected this cycle");
            decisions_cursor
                .append(&DecisionRecord::new(primary.clone(), "spread_rejected"))
                .await?;
            decision_summaries.push(format!("{label}: spread_rejected"));
            continue;
        }

        let weekly_bias = load_or_capture_bias(
            &weekly_true_open_snap,
            session_time::Timeframe::Weekly,
            primary,
            state.primary_price,
            now,
            &holidays,
        )
        .await?;
        let daily_bias = load_or_capture_bias(
            &daily_true_open_snap,
            session_time::Timeframe::Daily,
            primary,
            state.primary_price,
            now,
            &holidays,
        )
        .await?;

        let outcome = generate_signal(
            &state.divergence_inputs,
            weekly_bias,
            daily_bias,
            primary.clone(),
            secondary.clone(),
            snapshot.snapshot_id,
            dec!(0.8),
            dec!(0.8),
            now + chrono::Duration::minutes(20),
        );

        // The collision check needs to know which pair the signal
        // actually names, which isn't known until generate_signal
        // returns: a divergence can point at either primary or
        // secondary, so this can't be checked against a hardcoded pair
        // up front.
        if let SignalOutcome::Signal(ref signal) = outcome {
            if already_entered_this_cycle(&signal.pair, &open_positions, now) {
                tracing::info!(pair = %signal.pair, "already entered this pair within the current cycle window, skipping");
                decisions_cursor
                    .append(&DecisionRecord::new(signal.pair.clone(), "collision_skip"))
                    .await?;
                decision_summaries.push(format!("{label}: collision_skip"));
                continue;
            }
        }

        let decision_label = match outcome {
            SignalOutcome::NoDivergence => {
                tracing::info!(%primary, %secondary, "no SMT divergence this cycle, nothing to evaluate");
                decisions_cursor
                    .append(&DecisionRecord::new(primary.clone(), "no_divergence"))
                    .await?;
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
                    .append(
                        &DecisionRecord::new(primary.clone(), "gate_rejected")
                            .with_detail(format!("{:?}", invalidated.rejection_reason)),
                    )
                    .await?;
                "gate_rejected".to_string()
            }
            SignalOutcome::Signal(signal) => {
                tracing::info!(tier = ?signal.tier, direction = ?signal.direction, pair = %signal.pair, "signal passed the True Open gate");
                let risk_config = risk::RiskConfig {
                    base_risk_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.base_risk_percent).unwrap_or(dec!(1.0)),
                    ),
                    max_risk_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.max_risk_percent).unwrap_or(dec!(5.0)),
                    ),
                    max_open_positions: config.risk.max_open_positions,
                    daily_loss_limit_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.daily_loss_limit_percent)
                            .unwrap_or(dec!(5.0)),
                    ),
                    weekly_loss_limit_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.weekly_loss_limit_percent)
                            .unwrap_or(dec!(10.0)),
                    ),
                    max_exposure_per_currency_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.max_exposure_per_currency_percent)
                            .unwrap_or(dec!(15.0)),
                    ),
                    max_correlation_exposure_percent: domain::Percent::from_percentage(
                        Decimal::try_from(config.risk.max_correlation_exposure_percent)
                            .unwrap_or(dec!(10.0)),
                    ),
                    correlation_exposure_threshold: config.risk.correlation_exposure_threshold,
                };

                let equity = broker.get_account_equity().await?;
                // Fall back to whichever of primary_price/secondary_price
                // actually matches signal.pair, not always primary_price: a
                // missing snapshot entry for secondary shouldn't silently
                // price a secondary-pair entry off primary's number.
                let fallback_price = if signal.pair == *secondary {
                    state.secondary_price
                } else {
                    state.primary_price
                };
                let entry_price = match signal.direction {
                    Direction::Buy => snapshot
                        .prices
                        .get(&signal.pair)
                        .map(|q| q.ask)
                        .unwrap_or(fallback_price),
                    Direction::Sell => snapshot
                        .prices
                        .get(&signal.pair)
                        .map(|q| q.bid)
                        .unwrap_or(fallback_price),
                };
                let stop_loss_price = stop_loss_level(&state.divergence_inputs, &signal, secondary);
                let stop_distance = (entry_price - stop_loss_price).abs();
                // The strategy only specifies where the stop goes; the
                // target isn't part of that rule. Keeping it at this
                // codebase's own already-documented 1:3 risk-reward (see
                // monitor.rs's module doc) means the target scales with
                // whatever the buffer-based stop distance turns out to
                // be this cycle, rather than staying a fixed pip amount
                // that would silently drift away from 1:3 now that the
                // stop itself is no longer fixed.
                let take_profit_price = match signal.direction {
                    Direction::Buy => entry_price + stop_distance * dec!(3),
                    Direction::Sell => entry_price - stop_distance * dec!(3),
                };

                // Exposure already open on this pair-set's *other* pair,
                // counted only if the live correlation between them is
                // strong enough to matter. If signal.pair is primary,
                // that's an open position on secondary, and vice versa;
                // a pair-set only ever has the one counterpart to check.
                let other_pair = if signal.pair == *primary {
                    secondary
                } else {
                    primary
                };
                let correlated_exposure = match state.correlation_coefficient {
                    Some(coefficient)
                        if coefficient.abs() >= config.risk.correlation_exposure_threshold =>
                    {
                        open_positions
                            .iter()
                            .filter(|position| position.pair == *other_pair)
                            .map(|position| {
                                position.legs.iter().map(|leg| leg.size).sum::<Decimal>()
                            })
                            .sum()
                    }
                    _ => Decimal::ZERO,
                };

                let risk_context = risk::RiskContext {
                    equity,
                    open_position_count: open_positions.len(),
                    is_tuesday,
                    is_double_smt: signal.tier == domain::Tier::Double,
                    entry_price,
                    stop_loss_price,
                    take_profit_price,
                    realized_pnl_today: Usd::zero(),
                    realized_pnl_this_week: Usd::zero(),
                    currency_exposure: currency_exposure_snapshot(&open_positions),
                    correlated_exposure,
                };

                let risk_engine = risk::DefaultRiskEngine;
                let decision = risk_engine.evaluate(&signal, &risk_config, &risk_context)?;

                if !decision.approved {
                    tracing::info!(reason = ?decision.rejection_reason, "risk engine rejected the signal");
                    decisions_cursor
                        .append(
                            &DecisionRecord::new(signal.pair.clone(), "risk_rejected")
                                .with_detail(decision.rejection_reason.clone().unwrap_or_default()),
                        )
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
                        .notify(&format!(
                            "oboobot: opened {:?} {} (size {})",
                            signal.direction, signal.pair, decision.position_size
                        ))
                        .await;

                    // Same reasoning as the exits-loop reconciliation
                    // above: the in-memory open_positions, not the raw
                    // cursor log, is the correct locally-known baseline
                    // here.
                    open_positions =
                        reconcile_and_notify(broker, &open_positions, notifier.as_ref()).await?;
                    for position in &open_positions {
                        positions_cursor.append(position).await?;
                    }
                    decisions_cursor
                        .append(&DecisionRecord::new(signal.pair.clone(), "order_submitted"))
                        .await?;
                    "order_submitted".to_string()
                }
            }
        };
        decision_summaries.push(format!("{label}: {decision_label}"));
    }

    write_status(
        &status_snap,
        &open_positions,
        &health,
        Some(&decision_summaries.join("; ")),
        false,
    )
    .await;
    Ok(())
}

/// Loads `days` of historical daily closes for the first configured
/// pair-set, then steps through them one simulated day at a time,
/// calling the exact same `run_real_cycle` logic each step with a
/// `ManualClock` and a `ReplayBroker` standing in for wall-clock time
/// and a live connection. Scoped to one pair-set for this first
/// version, with no slippage model beyond filling exactly at each
/// simulated day's closing price; see
/// `docs/adr/0004-replay-engine-design.md` for the full design and what
/// else is deliberately left out of it.
///
/// Runs against an entirely separate `<state_dir>/replay` directory,
/// never the real state directory: replay's decisions, positions, and
/// buffers must never mix with (or corrupt) real trading state, and a
/// fresh replay run always starts clean rather than resuming a
/// previous one's.
async fn run_replay(cli: Cli, days: u32) -> anyhow::Result<()> {
    let config = Config::load(&cli.config).await?;
    let Some(pair_config) = config.pairs.first().cloned() else {
        anyhow::bail!("no pairs configured to replay");
    };
    if config.pairs.len() > 1 {
        tracing::warn!(
            pairs_configured = config.pairs.len(),
            replaying = %format!("{}/{}", pair_config.primary, pair_config.secondary),
            "replay only evaluates the first configured pair-set in this first version"
        );
    }

    // Historical data comes from whichever broker --broker names.
    // MockBroker and BybitAdapter both fall back to
    // BrokerAdapter::fetch_historical_prices's NotImplemented default,
    // so a real replay run needs --broker deriv even though nothing is
    // traded live.
    let source_broker: Box<dyn BrokerAdapter> = match cli.broker {
        BrokerKind::Mock => Box::new(MockBroker::new(
            Usd::from_decimal(dec!(10000)),
            dec!(1.10000),
        )),
        BrokerKind::Deriv => Box::new(DerivAdapter::connect_from_env().await?),
        BrokerKind::Bybit => Box::new(BybitAdapter::from_env()?),
    };
    let primary_history = source_broker
        .fetch_historical_prices(&pair_config.primary, days)
        .await?;
    let secondary_history = source_broker
        .fetch_historical_prices(&pair_config.secondary, days)
        .await?;

    let mut bars = BTreeMap::new();
    bars.insert(pair_config.primary.clone(), primary_history);
    bars.insert(pair_config.secondary.clone(), secondary_history);

    let replay_state_dir = cli.state_dir.join("replay");
    // A fresh run every time, not a resumed one: replaying the same
    // window twice should give the same answer both times, not one
    // that depends on leftover state from the last run.
    let _ = tokio::fs::remove_dir_all(&replay_state_dir).await;
    tokio::fs::create_dir_all(&replay_state_dir).await?;

    let starting_equity = Usd::from_decimal(dec!(10000));
    let replay_broker = daemon::ReplayBroker::new(
        replay_state_dir.join("replay_broker_state.json"),
        bars,
        starting_equity,
    );
    let bar_count = replay_broker.bar_count();
    if bar_count == 0 {
        anyhow::bail!(
            "no historical data available to replay for {}/{}",
            pair_config.primary,
            pair_config.secondary
        );
    }

    let clock = session_time::ManualClock::new(
        chrono::Utc::now() - chrono::Duration::days(i64::from(days)),
    );

    for bar in 0..bar_count {
        let mut cycle_cli = cli.clone();
        cycle_cli.state_dir.clone_from(&replay_state_dir);
        // Replay always evaluates every simulated day: there is no
        // real macro-cycle window to be inside or outside of when the
        // clock isn't real wall-clock time.
        cycle_cli.force = true;

        run_real_cycle(cycle_cli, &clock, &replay_broker).await?;

        tracing::info!(bar = bar + 1, of = bar_count, "replay day complete");
        if bar + 1 < bar_count {
            replay_broker.advance();
            clock.advance(chrono::Duration::days(1));
        }
    }

    let ending_equity = replay_broker.get_account_equity().await?.as_decimal();
    let decisions_cursor: CursorFile<DecisionRecord> =
        CursorFile::new(replay_state_dir.join("decisions.cursor"));
    let decisions = decisions_cursor.read_all().await?;
    let mut decision_counts: BTreeMap<String, usize> = BTreeMap::new();
    for decision in &decisions {
        *decision_counts.entry(decision.outcome.clone()).or_insert(0) += 1;
    }

    let report = daemon::ReplayReport {
        pair_set: (pair_config.primary.clone(), pair_config.secondary.clone()),
        bars_replayed: bar_count,
        starting_equity: starting_equity.as_decimal(),
        ending_equity,
        trades_opened: decision_counts.get("order_submitted").copied().unwrap_or(0),
        trades_closed: decision_counts.get("position_closed").copied().unwrap_or(0),
        decision_counts,
    };

    let report_json = serde_json::to_string_pretty(&report)?;
    tracing::info!("replay complete");
    println!("{report_json}");
    tokio::fs::write(replay_state_dir.join("replay_report.json"), &report_json).await?;

    Ok(())
}

/// now, logging and notifying on any mismatch, and returns the
/// reconciled position list. Used at startup, and per the original
/// spec's "reconcile after fills" requirement, again immediately after
/// every fill (an entry or an exit) rather than only ever catching a
/// problem at the next invocation's startup reconciliation. A fill is
/// exactly the kind of event that could introduce a local/broker
/// mismatch (a submit_order response lost after the order actually
/// went through, say), so it's the moment reconciling again is worth
/// the extra broker round trip.
async fn reconcile_and_notify(
    broker: &dyn BrokerAdapter,
    locally_known_positions: &[Position],
    notifier: &dyn daemon::Notifier,
) -> anyhow::Result<Vec<Position>> {
    let report = reconcile(broker, locally_known_positions).await?;
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
        tracing::debug!(
            known_positions = locally_known_positions.len(),
            "reconciliation clean"
        );
    }
    Ok(apply_reconciliation(&report))
}

/// The stop-loss level for a new position, per the strategy's own rule:
/// always the previous cycle's high or low that was *not* taken out, on
/// the asset actually being traded (the one that held, not the one
/// that swept). A Buy's stop goes at that buffer's low; a Sell's stop
/// goes at that buffer's high. Tier1 signals come from the daily
/// buffer, Tier2 from the session buffer; a Double signal agrees on
/// both, but their numeric levels aren't necessarily identical, so
/// daily is used for Double too, the same tie-break `evaluate_smt`
/// itself already uses when daily and session disagree.
fn stop_loss_level(
    divergence_inputs: &DivergenceInputs,
    signal: &TradeSignal,
    secondary_pair: &str,
) -> Decimal {
    let is_secondary = signal.pair == secondary_pair;
    let buffer = match (signal.tier, is_secondary) {
        (Tier::Tier2, false) => divergence_inputs.session_primary_buffer,
        (Tier::Tier2, true) => divergence_inputs.session_secondary_buffer,
        (_, false) => divergence_inputs.daily_primary_buffer,
        (_, true) => divergence_inputs.daily_secondary_buffer,
    };
    match signal.direction {
        Direction::Buy => buffer.low,
        Direction::Sell => buffer.high,
    }
}

/// Fetches 90 days of historical daily closes for both `primary` and
/// `secondary` and pairs them up by index (oldest first, matching what
/// `fetch_historical_prices` promises) into the (primary, secondary)
/// tuples `strategy::record_sample` expects. If the two series come
/// back different lengths, only pairs up to the shorter one: a same-day
/// mismatch is possible if the two symbols don't share identical
/// trading calendars, and there's no way to know which days actually
/// lined up, so it's more honest to under-seed than to risk pairing a
/// primary close against the wrong day's secondary close.
async fn backfill_correlation(
    broker: &dyn BrokerAdapter,
    primary: &str,
    secondary: &str,
) -> anyhow::Result<Vec<(Decimal, Decimal)>> {
    const BACKFILL_DAYS: u32 = 90;
    let primary_history = broker
        .fetch_historical_prices(primary, BACKFILL_DAYS)
        .await?;
    let secondary_history = broker
        .fetch_historical_prices(secondary, BACKFILL_DAYS)
        .await?;
    if primary_history.len() != secondary_history.len() {
        tracing::debug!(
            %primary, %secondary,
            primary_days = primary_history.len(), secondary_days = secondary_history.len(),
            "historical price series lengths didn't match, pairing only up to the shorter one"
        );
    }
    Ok(primary_history.into_iter().zip(secondary_history).collect())
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
    Ok(level
        .map(|l| session_time::bias_from_price(price, l.level))
        .unwrap_or(Bias::Neutral))
}

/// The original scripted walkthrough against MockBroker: a clean pass, a
/// no-divergence cycle, a True-Open rejection, a health-triggered
/// lockout, and a simulated restart. Unchanged from the first pass.
async fn run_demo() -> anyhow::Result<()> {
    tracing::info!("starting oboobot (QuarterlyTheory_SMT_Trader) demonstration harness");
    tracing::info!(
        "this run is against MockBroker; see main.rs docs for what a live run would change"
    );

    let broker = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.10000));
    let health = HealthMonitor::new();
    let assistant = LoggingAssistant;

    let state_dir = std::env::temp_dir().join("oboobot-demo-state");
    tokio::fs::create_dir_all(&state_dir).await?;
    let positions_cursor_path = state_dir.join("positions.cursor");
    let _ = tokio::fs::remove_file(&positions_cursor_path).await;
    let positions_cursor: CursorFile<Position> = CursorFile::new(&positions_cursor_path);
    let recommendations_cursor: CursorFile<daemon::assistant::RecommendationRecord> =
        CursorFile::new(state_dir.join("recommendations.cursor"));

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
        &recommendations_cursor,
        &mut open_positions,
        "GBPUSD",
        "EURUSD",
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            daily_secondary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            session_primary_buffer: BufferLevels {
                low: dec!(1.09000),
                high: dec!(1.11000),
            },
            session_secondary_buffer: BufferLevels {
                low: dec!(1.09000),
                high: dec!(1.11000),
            },
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
        &recommendations_cursor,
        &mut open_positions,
        "GBPUSD",
        "EURUSD",
        DivergenceInputs {
            primary_price: dec!(1.10050),
            secondary_price: dec!(1.10050),
            daily_primary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            daily_secondary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            session_primary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            session_secondary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
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
        &recommendations_cursor,
        &mut open_positions,
        "GBPUSD",
        "EURUSD",
        DivergenceInputs {
            primary_price: dec!(1.09900),
            secondary_price: dec!(1.10100),
            daily_primary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            daily_secondary_buffer: BufferLevels {
                low: dec!(1.10000),
                high: dec!(1.10500),
            },
            session_primary_buffer: BufferLevels {
                low: dec!(1.09000),
                high: dec!(1.11000),
            },
            session_secondary_buffer: BufferLevels {
                low: dec!(1.09000),
                high: dec!(1.11000),
            },
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
        tracing::info!(
            "new entries correctly blocked while system state is not Healthy or Degraded"
        );
    }

    health.clear_failure(HealthCheckFailure::BrokerHeartbeatFailure);
    tracing::info!(state = ?health.current_state(), "broker heartbeat recovered, health restored");

    tracing::info!(
        "open positions before simulated restart: {}",
        open_positions.len()
    );

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
    recommendations_cursor: &CursorFile<daemon::assistant::RecommendationRecord>,
    open_positions: &mut Vec<Position>,
    primary_pair: &str,
    secondary_pair: &str,
    inputs: DivergenceInputs,
    weekly_bias: Bias,
    daily_bias: Bias,
) -> anyhow::Result<()> {
    tracing::info!("--- {label} ---");

    if !allows_new_entries(health.current_state()) {
        tracing::info!("skipping: health state does not currently allow new entries");
        return Ok(());
    }

    let snapshot = broker
        .get_snapshot(&[primary_pair.to_string(), secondary_pair.to_string()])
        .await?;
    let macro_cycle_event = EventEnvelope::new(snapshot.timestamp, Event::MacroCycleStarted);
    for recommendation in assistant.analyze_event(&macro_cycle_event).await {
        daemon::assistant::record_recommendation(&recommendation, recommendations_cursor).await?;
    }

    let outcome = generate_signal(
        &inputs,
        weekly_bias,
        daily_bias,
        primary_pair.to_string(),
        secondary_pair.to_string(),
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
                max_exposure_per_currency_percent: domain::Percent::from_percentage(dec!(15.0)),
                max_correlation_exposure_percent: domain::Percent::from_percentage(dec!(10.0)),
                correlation_exposure_threshold: 0.7,
            };

            let equity = broker.get_account_equity().await?;
            let entry_price = match signal.direction {
                Direction::Buy => snapshot
                    .prices
                    .get(&signal.pair)
                    .map(|q| q.ask)
                    .unwrap_or(dec!(1.10000)),
                Direction::Sell => snapshot
                    .prices
                    .get(&signal.pair)
                    .map(|q| q.bid)
                    .unwrap_or(dec!(1.10000)),
            };
            let stop_loss_price = stop_loss_level(&inputs, &signal, secondary_pair);
            let stop_distance = (entry_price - stop_loss_price).abs();
            let take_profit_price = match signal.direction {
                Direction::Buy => entry_price + stop_distance * dec!(3),
                Direction::Sell => entry_price - stop_distance * dec!(3),
            };

            let context = risk::RiskContext {
                equity,
                open_position_count: open_positions.len(),
                is_tuesday: chrono::Datelike::weekday(&session_time::to_ny(snapshot.timestamp))
                    == chrono::Weekday::Tue,
                is_double_smt: signal.tier == domain::Tier::Double,
                entry_price,
                stop_loss_price,
                take_profit_price,
                realized_pnl_today: Usd::zero(),
                realized_pnl_this_week: Usd::zero(),
                // The demo harness only ever runs one scripted pair, so
                // there's no meaningful cross-pair exposure to compute.
                currency_exposure: std::collections::BTreeMap::new(),
                correlated_exposure: Decimal::ZERO,
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

#[cfg(test)]
mod stop_loss_level_tests {
    use super::*;
    use rust_decimal_macros::dec;
    use strategy::BufferLevels;
    use uuid::Uuid;

    fn buffer(low: rust_decimal::Decimal, high: rust_decimal::Decimal) -> BufferLevels {
        BufferLevels { low, high }
    }

    fn sample_inputs() -> DivergenceInputs {
        DivergenceInputs {
            primary_price: dec!(1.2000),   // GBPUSD
            secondary_price: dec!(1.1000), // EURUSD
            daily_primary_buffer: buffer(dec!(1.1950), dec!(1.2050)),
            daily_secondary_buffer: buffer(dec!(1.0950), dec!(1.1050)),
            session_primary_buffer: buffer(dec!(1.1980), dec!(1.2020)),
            session_secondary_buffer: buffer(dec!(1.0980), dec!(1.1020)),
        }
    }

    fn signal(pair: &str, direction: Direction, tier: Tier) -> TradeSignal {
        TradeSignal {
            signal_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            pair: pair.to_string(),
            direction,
            tier,
            strength: dec!(0.8),
            confidence: dec!(0.8),
            valid_until: chrono::Utc::now(),
            originating_snapshot_id: Uuid::new_v4(),
        }
    }

    // The four cases from the strategy spec, given GBPUSD as primary and
    // EURUSD as secondary: whichever asset held (the one actually
    // traded) gets its stop at the previous cycle's un-swept level.

    #[test]
    fn case_1_buy_eurusd_stops_at_eurusds_own_daily_low() {
        // EURUSD (secondary) does not take out its low, GBPUSD (primary)
        // does: buy EURUSD, stop at EURUSD's low that wasn't taken out.
        let inputs = sample_inputs();
        let sig = signal("EURUSD", Direction::Buy, Tier::Tier1);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.daily_secondary_buffer.low
        );
    }

    #[test]
    fn case_2_buy_gbpusd_stops_at_gbpusds_own_daily_low() {
        // GBPUSD (primary) does not take out its low, EURUSD (secondary)
        // does: buy GBPUSD, stop at GBPUSD's low that wasn't taken out.
        let inputs = sample_inputs();
        let sig = signal("GBPUSD", Direction::Buy, Tier::Tier1);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.daily_primary_buffer.low
        );
    }

    #[test]
    fn case_3_sell_gbpusd_stops_at_gbpusds_own_daily_high() {
        // The sell-side mirror: GBPUSD held (didn't take out the high),
        // EURUSD swept it. Sell GBPUSD, stop at GBPUSD's high.
        let inputs = sample_inputs();
        let sig = signal("GBPUSD", Direction::Sell, Tier::Tier1);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.daily_primary_buffer.high
        );
    }

    #[test]
    fn case_4_sell_eurusd_stops_at_eurusds_own_daily_high() {
        let inputs = sample_inputs();
        let sig = signal("EURUSD", Direction::Sell, Tier::Tier1);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.daily_secondary_buffer.high
        );
    }

    #[test]
    fn tier2_signals_use_the_session_buffer_not_daily() {
        let inputs = sample_inputs();
        let sig = signal("GBPUSD", Direction::Buy, Tier::Tier2);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.session_primary_buffer.low
        );
    }

    #[test]
    fn double_tier_uses_the_daily_buffer_same_as_tier1() {
        let inputs = sample_inputs();
        let sig = signal("EURUSD", Direction::Sell, Tier::Double);
        assert_eq!(
            stop_loss_level(&inputs, &sig, "EURUSD"),
            inputs.daily_secondary_buffer.high
        );
    }
}
