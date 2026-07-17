//! The scheduler's only job is deciding when the next macro cycle is and
//! sleeping until then. It doesn't evaluate strategy, doesn't touch the
//! broker, doesn't know what SMT divergence is; it just emits
//! `MacroCycleStarted` on schedule so everything downstream can react to
//! it. That separation is what makes the strategy and risk logic testable
//! without needing a real clock: they only ever see events, never read
//! the wall clock themselves.

use std::sync::Arc;

use domain::{Event, EventEnvelope};
use session_time::Clock;

use crate::event_bus::EventBusHandle;

pub struct Scheduler<C: Clock> {
    clock: Arc<C>,
}

impl<C: Clock> Scheduler<C> {
    pub fn new(clock: Arc<C>) -> Self {
        Scheduler { clock }
    }

    /// Run forever, publishing a `MacroCycleStarted` event at every macro
    /// cycle boundary. This is what `main.rs` calls in production.
    pub async fn run(&self, bus: EventBusHandle) -> ! {
        loop {
            self.sleep_until_next_cycle().await;
            let envelope = EventEnvelope::new(self.clock.now(), Event::MacroCycleStarted);
            // A publish failure here means the receiver side has been
            // dropped, i.e. the daemon is shutting down. There's nothing
            // useful to do about that inside the scheduler itself, so we
            // just let the next loop iteration's publish attempt fail the
            // same way rather than treating it as fatal to this function.
            let _ = bus.publish(envelope).await;
        }
    }

    /// The same loop, but stopping after `cycles` iterations. This exists
    /// specifically so tests (and the replay engine) can drive a bounded,
    /// deterministic number of cycles instead of running forever.
    pub async fn run_n_cycles(&self, bus: EventBusHandle, cycles: u32) {
        for _ in 0..cycles {
            self.sleep_until_next_cycle().await;
            let envelope = EventEnvelope::new(self.clock.now(), Event::MacroCycleStarted);
            let _ = bus.publish(envelope).await;
        }
    }

    async fn sleep_until_next_cycle(&self) {
        let now = self.clock.now();
        let next = session_time::next_macro_cycle_after(now);
        let wait = (next - now)
            .to_std()
            .unwrap_or(std::time::Duration::ZERO);
        tokio::time::sleep(wait).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_bus::event_bus;
    use chrono::TimeZone;
    use session_time::ManualClock;

    #[tokio::test(start_paused = true)]
    async fn scheduler_publishes_a_macro_cycle_started_event() {
        // Pausing tokio's virtual clock means the `sleep` inside the
        // scheduler resolves immediately in real wall-clock time instead
        // of actually waiting up to three hours of simulated market time.
        // Note this is a different clock than `ManualClock`: tokio's
        // time-pausing only affects tokio's own timer APIs, not a
        // hand-rolled clock like this one, so this test checks the
        // scheduler fires once and wires up correctly, and leaves
        // verifying the exact three-hour spacing between cycles to the
        // pure, clock-independent unit tests in `session_time::macro_cycle`.
        let start = chrono::Utc.with_ymd_and_hms(2026, 3, 10, 9, 30, 0).unwrap();
        let clock = Arc::new(ManualClock::new(start));
        let scheduler = Scheduler::new(clock);
        let (handle, mut receiver) = event_bus(8);

        scheduler.run_n_cycles(handle, 1).await;

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.payload.kind(), "macro_cycle_started");
    }
}
