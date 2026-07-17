//! This module exists because of the tension flagged in review: the spec
//! states "the risk engine is the sole authority for position sizing" in
//! one place, and describes an AssistantEngine that scores signals and
//! assesses risk in another, without ever saying which one wins if they'd
//! ever disagree.
//!
//! The resolution implemented here is structural, not just a comment
//! promising good behavior: a `Recommendation` is inert data. It has a
//! severity, a message, and an optional *suggestion* of a config change
//! that's just a field name, a proposed value, and a rationale, all
//! strings. There is no method on `Recommendation`, no `From` impl, no
//! callback, nothing that turns it into an actual mutation of a
//! `RiskConfig` or a `StrategyEngine` parameter. The only thing this
//! crate ever does with one is log it. If a future version of this
//! daemon wants to let an operator manually apply a suggested change,
//! that has to be a new, separate, explicitly human-invoked function, not
//! an extension of anything in this module. `AssistantEngine` itself is
//! also explicitly not on the daemon's startup or shutdown critical path,
//! since a component whose own health checks include things like "model
//! corruption" has no business being a hard dependency for whether the
//! core trading loop can run at all.

use async_trait::async_trait;
use domain::EventEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// A proposed change, described only as data: which field, what value,
/// why. Nothing here can execute; it's exactly as inert as a comment in a
/// log file, which is deliberate.
#[derive(Debug, Clone)]
pub struct ConfigChangeSuggestion {
    pub field: String,
    pub suggested_value: String,
    pub rationale: String,
}

#[derive(Debug, Clone)]
pub struct Recommendation {
    pub severity: Severity,
    pub message: String,
    pub suggested_change: Option<ConfigChangeSuggestion>,
}

#[async_trait]
pub trait AssistantEngine: Send + Sync {
    async fn analyze_event(&self, event: &EventEnvelope) -> Vec<Recommendation>;
}

/// The reference implementation: analyzes nothing, recommends nothing.
/// A real pattern-detection engine would replace this, but it would still
/// only ever be able to produce `Recommendation`s, which, as above,
/// cannot do anything on their own. That's what makes this safe to add
/// real intelligence to later without revisiting this boundary.
pub struct LoggingAssistant;

#[async_trait]
impl AssistantEngine for LoggingAssistant {
    async fn analyze_event(&self, _event: &EventEnvelope) -> Vec<Recommendation> {
        Vec::new()
    }
}

/// The one and only thing this daemon does with a `Recommendation`: write
/// it to the log for a human to read. There is no function anywhere in
/// this codebase that takes a `Recommendation` and feeds it into
/// `risk::RiskConfig` or any `strategy` parameter automatically. If you're
/// looking for where that wiring is, it doesn't exist, on purpose.
pub fn record_recommendation(recommendation: &Recommendation) {
    match recommendation.severity {
        Severity::Info => tracing::info!(message = %recommendation.message, "assistant recommendation (advisory only)"),
        Severity::Warning => tracing::warn!(message = %recommendation.message, "assistant recommendation (advisory only)"),
        Severity::Critical => tracing::error!(message = %recommendation.message, "assistant recommendation (advisory only)"),
    }

    if let Some(change) = &recommendation.suggested_change {
        tracing::info!(
            field = %change.field,
            suggested_value = %change.suggested_value,
            rationale = %change.rationale,
            "suggested config change requires manual operator review, it will not be applied automatically"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::Event;

    #[tokio::test]
    async fn logging_assistant_never_recommends_anything() {
        let assistant = LoggingAssistant;
        let envelope = EventEnvelope::new(chrono::Utc::now(), Event::MacroCycleStarted);
        let recommendations = assistant.analyze_event(&envelope).await;
        assert!(recommendations.is_empty());
    }
}
