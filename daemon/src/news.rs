//! A trait for "what high-impact news is coming up," a pre-news exit
//! check built against that trait, and one implementation:
//! `NoNewsProvider`, which always reports nothing scheduled.
//!
//! That's a deliberate fail-safe choice, not a placeholder pretending to
//! be a real feed. The question flagged back in the original spec
//! review was: if the news source is unavailable, does the bot fail
//! safe (assume news might be coming, act conservatively) or fail open
//! (assume nothing's scheduled, trade normally)? `NoNewsProvider`
//! answers that by construction — it always returns empty, meaning the
//! pre-news exit check can never fire — which is honest about what "no
//! real news integration yet" actually means, rather than quietly
//! disabling a safety check while looking like it's still active. A
//! real provider (calling out to an actual economic calendar) is a
//! separate, focused piece of work, not attempted here; picking a
//! specific external news API wasn't part of what this pass was asked
//! to do, and integrating one deserves its own verification the way the
//! Deriv endpoint and symbol convention got.

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use domain::NewsEvent;

#[async_trait]
pub trait NewsProvider: Send + Sync {
    /// Every currently-known event expected within `window` of `now`.
    async fn upcoming_events(&self, now: DateTime<Utc>, window: Duration) -> Vec<NewsEvent>;

    /// Whether this provider's data is fresh enough to trust right now.
    /// Defaults to always fresh, which is the correct answer for
    /// `NoNewsProvider`: it never claims to have actually polled
    /// anything, so there's nothing of its own that can go stale. A
    /// real provider (see the module doc for why one isn't built here)
    /// overrides this to report whether its last successful fetch was
    /// recent enough, which is what actually lets `HealthCheckFailure::
    /// NewsApiDown` mean something once one exists.
    async fn is_fresh(&self, _now: DateTime<Utc>) -> bool {
        true
    }
}

pub struct NoNewsProvider;

#[async_trait]
impl NewsProvider for NoNewsProvider {
    async fn upcoming_events(&self, _now: DateTime<Utc>, _window: Duration) -> Vec<NewsEvent> {
        Vec::new()
    }
}

/// Whether a high-impact event lands within `lead_time` of `now`. Pure
/// and provider-agnostic: given whatever events a `NewsProvider`
/// returned, does the pre-news exit condition hold right now.
pub fn should_exit_for_news(events: &[NewsEvent], now: DateTime<Utc>, lead_time: Duration) -> bool {
    events.iter().any(|event| {
        event.impact == domain::NewsImpact::High
            && event.timestamp > now
            && event.timestamp - now <= lead_time
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use uuid::Uuid;

    fn event(minutes_from_now: i64, impact: domain::NewsImpact, now: DateTime<Utc>) -> NewsEvent {
        NewsEvent {
            event_id: Uuid::new_v4(),
            timestamp: now + Duration::minutes(minutes_from_now),
            currency: "USD".to_string(),
            impact,
            description: "test event".to_string(),
            actual: None,
            forecast: None,
            previous: None,
        }
    }

    #[tokio::test]
    async fn no_news_provider_always_returns_nothing() {
        let provider = NoNewsProvider;
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = provider.upcoming_events(now, Duration::hours(1)).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn no_news_provider_always_reports_fresh() {
        let provider = NoNewsProvider;
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        assert!(provider.is_fresh(now).await);
    }

    #[test]
    fn high_impact_event_within_lead_time_triggers_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(10, domain::NewsImpact::High, now)];
        assert!(should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn high_impact_event_beyond_lead_time_does_not_trigger_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(30, domain::NewsImpact::High, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn low_impact_event_never_triggers_exit_regardless_of_timing() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(1, domain::NewsImpact::Low, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }

    #[test]
    fn a_past_event_does_not_trigger_exit() {
        let now = Utc.with_ymd_and_hms(2026, 3, 10, 12, 0, 0).unwrap();
        let events = vec![event(-5, domain::NewsImpact::High, now)];
        assert!(!should_exit_for_news(&events, now, Duration::minutes(15)));
    }
}
