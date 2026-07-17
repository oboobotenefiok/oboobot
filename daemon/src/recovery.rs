//! This module exists because of one incident: devmind's offline buffer
//! silently failed to drain to Cognee because the remote API returned a
//! success-shaped response with nothing usable in it. The lesson that
//! carries over here is broader than "check the response body." It's
//! that whatever this daemon's own cursor files say about what's open
//! should never be treated as fact on its own. The broker is the only
//! party that can't lie about what's actually open, so every restart (and
//! ideally every reconnect) reconciles against it directly, and the
//! broker's answer wins.

use std::collections::HashSet;

use broker::{BrokerAdapter, BrokerError};
use domain::Position;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    /// Positions our local persistence thinks are open, that the broker
    /// has no record of. These get closed out locally with
    /// `ExitReason::ReconciliationOrphan`; we don't get to keep believing
    /// in a position the broker doesn't recognize.
    pub orphaned_locally: Vec<Position>,
    /// Positions the broker reports as open that our local state didn't
    /// know about at all (the exact shape of bug a "silent success"
    /// broker response could cause: an order that really went through,
    /// but that we never durably recorded on our side).
    pub unknown_to_local: Vec<Position>,
    /// Positions both sides agree on.
    pub confirmed: Vec<Position>,
}

impl ReconciliationReport {
    pub fn is_clean(&self) -> bool {
        self.orphaned_locally.is_empty() && self.unknown_to_local.is_empty()
    }
}

/// Ask the broker what's actually open, and diff it against whatever our
/// own persistence layer believes. This should run before the daemon
/// accepts any new signals, every time it starts up, and again after any
/// broker reconnect following an outage.
pub async fn reconcile(
    broker: &dyn BrokerAdapter,
    locally_known_positions: &[Position],
) -> Result<ReconciliationReport, BrokerError> {
    let broker_positions = broker.list_open_positions().await?;

    let broker_ids: HashSet<Uuid> = broker_positions.iter().map(|p| p.position_id).collect();
    let local_ids: HashSet<Uuid> = locally_known_positions.iter().map(|p| p.position_id).collect();

    let orphaned_locally = locally_known_positions
        .iter()
        .filter(|p| !broker_ids.contains(&p.position_id))
        .cloned()
        .collect();

    let unknown_to_local = broker_positions
        .iter()
        .filter(|p| !local_ids.contains(&p.position_id))
        .cloned()
        .collect();

    let confirmed = locally_known_positions
        .iter()
        .filter(|p| broker_ids.contains(&p.position_id))
        .cloned()
        .collect();

    Ok(ReconciliationReport {
        orphaned_locally,
        unknown_to_local,
        confirmed,
    })
}

/// Turn a reconciliation report into the position list the daemon should
/// actually trust going forward. The broker's view always wins: confirmed
/// and previously-unknown-but-broker-reported positions are kept,
/// locally-orphaned ones are dropped from the active set entirely (a
/// caller that wants to log or persist their closure with
/// `ExitReason::ReconciliationOrphan` does that separately, using the
/// `orphaned_locally` list on the report before this function discards
/// them from the active set).
pub fn apply_reconciliation(report: &ReconciliationReport) -> Vec<Position> {
    let mut reconciled = report.confirmed.clone();
    reconciled.extend(report.unknown_to_local.iter().cloned());
    reconciled
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker::MockBroker;
    use domain::Usd;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn matching_state_reconciles_clean() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let order = mock
            .submit_order(domain::OrderRequest {
                order_id: Uuid::new_v4(),
                trace_id: Uuid::new_v4(),
                signal_id: Uuid::new_v4(),
                pair: "EURUSD".to_string(),
                side: domain::Direction::Buy,
                size: dec!(1.0),
                order_type: domain::OrderType::Market,
                price: None,
                stop_loss: None,
                take_profit: None,
                confirming_snapshot_id: Uuid::new_v4(),
            })
            .await
            .unwrap();
        let _ = order;

        let broker_positions = mock.list_open_positions().await.unwrap();
        let report = reconcile(&mock, &broker_positions).await.unwrap();

        assert!(report.is_clean());
        assert_eq!(report.confirmed.len(), 1);
    }

    #[tokio::test]
    async fn a_position_the_broker_forgot_is_reported_as_orphaned_locally() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        let broker_dyn: &dyn BrokerAdapter = &mock;

        mock.submit_order(domain::OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: domain::Direction::Buy,
            size: dec!(1.0),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        })
        .await
        .unwrap();

        let locally_known = mock.list_open_positions().await.unwrap();
        let position_id = locally_known[0].position_id;

        // Simulate the broker no longer recognizing this position, the
        // way it would if an earlier "successful" submission was actually
        // the silent-failure case and never really went through.
        mock.forget_position(position_id);

        let report = reconcile(broker_dyn, &locally_known).await.unwrap();
        assert_eq!(report.orphaned_locally.len(), 1);
        assert_eq!(report.orphaned_locally[0].position_id, position_id);
        assert!(report.confirmed.is_empty());

        let reconciled = apply_reconciliation(&report);
        assert!(reconciled.is_empty(), "an orphaned position should not survive reconciliation");
    }

    #[tokio::test]
    async fn a_position_the_broker_has_but_we_never_recorded_is_adopted() {
        let mock = MockBroker::new(Usd::from_decimal(dec!(10000)), dec!(1.1000));
        mock.submit_order(domain::OrderRequest {
            order_id: Uuid::new_v4(),
            trace_id: Uuid::new_v4(),
            signal_id: Uuid::new_v4(),
            pair: "EURUSD".to_string(),
            side: domain::Direction::Buy,
            size: dec!(1.0),
            order_type: domain::OrderType::Market,
            price: None,
            stop_loss: None,
            take_profit: None,
            confirming_snapshot_id: Uuid::new_v4(),
        })
        .await
        .unwrap();

        // Our local view is empty, as if we crashed before persisting the
        // fill confirmation, exactly the scenario reconciliation exists
        // to catch.
        let locally_known: Vec<Position> = Vec::new();

        let report = reconcile(&mock, &locally_known).await.unwrap();
        assert_eq!(report.unknown_to_local.len(), 1);
        assert!(!report.is_clean());

        let reconciled = apply_reconciliation(&report);
        assert_eq!(reconciled.len(), 1, "a broker-confirmed position we didn't know about should be adopted, not discarded");
    }
}
