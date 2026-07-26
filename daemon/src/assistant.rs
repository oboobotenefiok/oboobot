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
use chrono::{DateTime, Utc};
use domain::EventEnvelope;
use serde::{Deserialize, Serialize};

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

/// The persisted form of a `Recommendation`, one line per record in a
/// `CursorFile<RecommendationRecord>`, mirroring exactly how
/// `DecisionRecord` already persists decisions: a timestamp plus plain
/// strings, not the typed `Severity`/`ConfigChangeSuggestion` those
/// strings came from. That's a deliberate, existing convention in this
/// codebase for its lightweight audit logs, not a shortcut specific to
/// this one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendationRecord {
    pub timestamp: DateTime<Utc>,
    pub severity: String,
    pub message: String,
    pub suggested_field: Option<String>,
    pub suggested_value: Option<String>,
    pub suggested_rationale: Option<String>,
}

impl RecommendationRecord {
    fn from_recommendation(recommendation: &Recommendation) -> Self {
        let severity = match recommendation.severity {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Critical => "critical",
        };
        RecommendationRecord {
            timestamp: Utc::now(),
            severity: severity.to_string(),
            message: recommendation.message.clone(),
            suggested_field: recommendation
                .suggested_change
                .as_ref()
                .map(|s| s.field.clone()),
            suggested_value: recommendation
                .suggested_change
                .as_ref()
                .map(|s| s.suggested_value.clone()),
            suggested_rationale: recommendation
                .suggested_change
                .as_ref()
                .map(|s| s.rationale.clone()),
        }
    }
}

/// The one and only thing this daemon does with a `Recommendation`:
/// write it to the log for a human to read, and (now) also append it to
/// a durable cursor file, since a log line from a five-minute-lived
/// process is otherwise gone the moment that invocation's job log
/// rotates out of GitHub Actions' retention window. There is no
/// function anywhere in this codebase that takes a `Recommendation` and
/// feeds it into `risk::RiskConfig` or any `strategy` parameter
/// automatically. If you're looking for where that wiring is, it
/// doesn't exist, on purpose.
pub async fn record_recommendation(
    recommendation: &Recommendation,
    cursor: &persistence::CursorFile<RecommendationRecord>,
) -> Result<(), persistence::PersistenceError> {
    match recommendation.severity {
        Severity::Info => {
            tracing::info!(message = %recommendation.message, "assistant recommendation (advisory only)")
        }
        Severity::Warning => {
            tracing::warn!(message = %recommendation.message, "assistant recommendation (advisory only)")
        }
        Severity::Critical => {
            tracing::error!(message = %recommendation.message, "assistant recommendation (advisory only)")
        }
    }
    if let Some(change) = &recommendation.suggested_change {
        tracing::info!(
            field = %change.field,
            suggested_value = %change.suggested_value,
            rationale = %change.rationale,
            "suggested config change requires manual operator review, it will not be applied automatically"
        );
    }

    cursor
        .append(&RecommendationRecord::from_recommendation(recommendation))
        .await?;
    Ok(())
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

    #[tokio::test]
    async fn record_recommendation_persists_to_the_cursor_file() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = persistence::CursorFile::new(dir.path().join("recommendations.cursor"));

        let recommendation = Recommendation {
            severity: Severity::Warning,
            message: "spread has been elevated for six consecutive cycles".to_string(),
            suggested_change: Some(ConfigChangeSuggestion {
                field: "spread_multiplier".to_string(),
                suggested_value: "2.0".to_string(),
                rationale: "current 1.5x threshold is rejecting most cycles right now".to_string(),
            }),
        };

        record_recommendation(&recommendation, &cursor)
            .await
            .unwrap();

        let persisted = cursor.read_all().await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].severity, "warning");
        assert_eq!(persisted[0].message, recommendation.message);
        assert_eq!(
            persisted[0].suggested_field.as_deref(),
            Some("spread_multiplier")
        );
    }
}
