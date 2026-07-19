//! Slack and Telegram notifications, both just an HTTP POST to a
//! webhook. This is the entire "alerting" story for this project, on
//! purpose: no metrics registry, no exporter, nothing Prometheus-shaped.
//! A `Notifier` is a trait so `--demo` and tests can use a no-op
//! implementation without needing real webhook URLs configured.

use async_trait::async_trait;

#[async_trait]
pub trait Notifier: Send + Sync {
    async fn notify(&self, message: &str);
}

pub struct NoopNotifier;

#[async_trait]
impl Notifier for NoopNotifier {
    async fn notify(&self, message: &str) {
        tracing::debug!(message, "NoopNotifier: notification suppressed (no webhook configured)");
    }
}

pub struct SlackNotifier {
    webhook_url: String,
    client: reqwest::Client,
}

impl SlackNotifier {
    pub fn new(webhook_url: String) -> Self {
        SlackNotifier { webhook_url, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl Notifier for SlackNotifier {
    async fn notify(&self, message: &str) {
        let body = serde_json::json!({ "text": message });
        // Deliberately swallowing the error here rather than propagating
        // it: a failed notification should never be the reason a
        // trading cycle itself fails. It's still logged, so it isn't
        // silent, just non-fatal.
        if let Err(error) = self.client.post(&self.webhook_url).json(&body).send().await {
            tracing::warn!(%error, "failed to deliver Slack notification");
        }
    }
}

pub struct TelegramNotifier {
    bot_token: String,
    chat_id: String,
    client: reqwest::Client,
}

impl TelegramNotifier {
    pub fn new(bot_token: String, chat_id: String) -> Self {
        TelegramNotifier { bot_token, chat_id, client: reqwest::Client::new() }
    }
}

#[async_trait]
impl Notifier for TelegramNotifier {
    async fn notify(&self, message: &str) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        let body = serde_json::json!({ "chat_id": self.chat_id, "text": message });
        if let Err(error) = self.client.post(&url).json(&body).send().await {
            tracing::warn!(%error, "failed to deliver Telegram notification");
        }
    }
}

/// Build whichever notifier the config and environment actually support,
/// falling back to `NoopNotifier` if nothing is configured. Slack is
/// preferred if both are set; there's no strong reason to fan out to
/// both by default.
pub fn notifier_from_config(config: &crate::config::NotificationSection) -> Box<dyn Notifier> {
    if let Some(env_var) = &config.slack_webhook_env {
        if let Ok(webhook_url) = std::env::var(env_var) {
            return Box::new(SlackNotifier::new(webhook_url));
        }
    }

    if let (Some(token_env), Some(chat_env)) =
        (&config.telegram_bot_token_env, &config.telegram_chat_id_env)
    {
        if let (Ok(bot_token), Ok(chat_id)) = (std::env::var(token_env), std::env::var(chat_env)) {
            return Box::new(TelegramNotifier::new(bot_token, chat_id));
        }
    }

    Box::new(NoopNotifier)
}
