//! The original design called certain events "priority" (orders, health,
//! shutdown) inside what was going to be a single bounded queue, but a
//! plain `tokio::sync::mpsc` channel has no concept of priority at all,
//! it's strictly first-in-first-out. Labeling some messages "priority"
//! without a mechanism to actually enforce that doesn't do anything.
//!
//! This is the fix: two separate channels, and a consumer loop that uses
//! `tokio::select!` with the `biased` keyword, which checks branches in
//! the order they're written instead of tokio's normal random selection
//! among ready branches. Listing the priority channel first means it
//! always wins whenever both channels have something waiting at the same
//! time.

use domain::EventEnvelope;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum EventBusError {
    #[error("event bus is closed, no receiver is listening")]
    Closed,
}

#[derive(Clone)]
pub struct EventBusHandle {
    priority_tx: mpsc::Sender<EventEnvelope>,
    ordinary_tx: mpsc::Sender<EventEnvelope>,
}

impl EventBusHandle {
    /// Publish an event. Which channel it goes down is decided by
    /// `Event::is_priority`, not by the caller, so producers can't
    /// accidentally mis-route something by forgetting which channel a
    /// given event kind belongs on.
    pub async fn publish(&self, envelope: EventEnvelope) -> Result<(), EventBusError> {
        let is_priority = envelope.payload.is_priority();
        let tx = if is_priority { &self.priority_tx } else { &self.ordinary_tx };
        tx.send(envelope).await.map_err(|_| EventBusError::Closed)
    }
}

pub struct EventBusReceiver {
    priority_rx: mpsc::Receiver<EventEnvelope>,
    ordinary_rx: mpsc::Receiver<EventEnvelope>,
}

impl EventBusReceiver {
    /// The next event, always preferring anything already waiting on the
    /// priority channel. Returns `None` only once both channels are
    /// closed and drained, which is how the daemon's main loop knows it's
    /// time to stop rather than keep waiting forever.
    pub async fn recv(&mut self) -> Option<EventEnvelope> {
        tokio::select! {
            biased;
            Some(event) = self.priority_rx.recv() => Some(event),
            Some(event) = self.ordinary_rx.recv() => Some(event),
            else => None,
        }
    }
}

/// Build a new event bus with the given per-channel capacity. Returns a
/// cloneable handle for producers and the single receiver for the
/// consumer loop; there's deliberately no way to clone the receiver,
/// since a bus with more than one consumer racing to pull events isn't a
/// scenario this daemon's architecture calls for.
pub fn event_bus(capacity: usize) -> (EventBusHandle, EventBusReceiver) {
    let (priority_tx, priority_rx) = mpsc::channel(capacity);
    let (ordinary_tx, ordinary_rx) = mpsc::channel(capacity);
    (
        EventBusHandle { priority_tx, ordinary_tx },
        EventBusReceiver { priority_rx, ordinary_rx },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Event;

    fn envelope(payload: Event) -> EventEnvelope {
        EventEnvelope::new(chrono::Utc::now(), payload)
    }

    #[tokio::test]
    async fn priority_event_is_received_before_an_already_queued_ordinary_one() {
        let (handle, mut receiver) = event_bus(8);

        // Queue an ordinary event first...
        handle
            .publish(envelope(Event::MacroCycleStarted))
            .await
            .unwrap();
        // ...then a priority one.
        handle
            .publish(envelope(Event::Shutdown { reason: "test".to_string() }))
            .await
            .unwrap();

        // Despite arriving second, the priority event should come out
        // first, because the biased select always checks that channel
        // before the ordinary one.
        let first = receiver.recv().await.unwrap();
        assert_eq!(first.payload.kind(), "shutdown");

        let second = receiver.recv().await.unwrap();
        assert_eq!(second.payload.kind(), "macro_cycle_started");
    }

    #[tokio::test]
    async fn receiver_returns_none_once_every_sender_is_dropped() {
        let (handle, mut receiver) = event_bus(8);
        drop(handle);
        assert!(receiver.recv().await.is_none());
    }
}
