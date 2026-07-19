//! One TOML file instead of the original spec's six, matching the
//! project's own "keep it simple" instruction. Everything the daemon
//! actually reads at runtime lives here: risk limits, which pairs to
//! trade, and where to send notifications. Validation happens once, at
//! load time, so a bad config fails loudly before any broker call
//! rather than surfacing as a confusing error three steps into a cycle.

use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("could not parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },

    #[error("config is invalid: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskSection {
    pub base_risk_percent: f64,
    pub max_risk_percent: f64,
    pub max_open_positions: usize,
    pub daily_loss_limit_percent: f64,
    pub weekly_loss_limit_percent: f64,
    #[serde(default = "default_spread_multiplier")]
    pub spread_multiplier: f64,
    #[serde(default = "default_regime_shift_threshold")]
    pub regime_shift_threshold: f64,
}

fn default_spread_multiplier() -> f64 {
    1.5
}

fn default_regime_shift_threshold() -> f64 {
    0.20
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PairConfig {
    pub primary: String,
    pub secondary: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct NotificationSection {
    /// Name of the environment variable holding the webhook URL, not the
    /// URL itself: config files in this project are meant to be
    /// checked into an open-source repo, so the secret stays in an
    /// environment variable (a GitHub Actions secret in the deployed
    /// case), and config only ever names which one to read.
    pub slack_webhook_env: Option<String>,
    pub telegram_bot_token_env: Option<String>,
    pub telegram_chat_id_env: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub risk: RiskSection,
    pub pairs: Vec<PairConfig>,
    #[serde(default)]
    pub notifications: NotificationSection,
}

impl Config {
    /// A sensible built-in default so the daemon still runs without a
    /// config file present — useful for `--demo` and for a first-ever
    /// run before anyone's authored a config.toml.
    pub fn default_config() -> Self {
        Config {
            risk: RiskSection {
                base_risk_percent: 1.0,
                max_risk_percent: 5.0,
                max_open_positions: 5,
                daily_loss_limit_percent: 5.0,
                weekly_loss_limit_percent: 10.0,
                spread_multiplier: default_spread_multiplier(),
                regime_shift_threshold: default_regime_shift_threshold(),
            },
            pairs: vec![PairConfig { primary: "EURUSD".to_string(), secondary: "GBPUSD".to_string() }],
            notifications: NotificationSection::default(),
        }
    }

    pub async fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default_config());
            }
            Err(source) => {
                return Err(ConfigError::Io { path: path.display().to_string(), source });
            }
        };

        let config: Config = toml::from_str(&contents)
            .map_err(|source| ConfigError::Parse { path: path.display().to_string(), source })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..=100.0).contains(&self.risk.max_risk_percent) {
            return Err(ConfigError::Invalid(format!(
                "risk.max_risk_percent must be between 0 and 100, got {}",
                self.risk.max_risk_percent
            )));
        }
        if self.risk.base_risk_percent <= 0.0 {
            return Err(ConfigError::Invalid("risk.base_risk_percent must be positive".to_string()));
        }
        if self.risk.spread_multiplier <= 1.0 {
            return Err(ConfigError::Invalid(format!(
                "risk.spread_multiplier must be greater than 1.0, got {} (a multiplier at or below 1.0 would make the spread filter reject almost every ordinary tick)",
                self.risk.spread_multiplier
            )));
        }
        if !(0.0..=1.0).contains(&self.risk.regime_shift_threshold) {
            return Err(ConfigError::Invalid(format!(
                "risk.regime_shift_threshold must be between 0.0 and 1.0, got {}",
                self.risk.regime_shift_threshold
            )));
        }
        if self.risk.max_open_positions == 0 {
            return Err(ConfigError::Invalid("risk.max_open_positions must be at least 1".to_string()));
        }
        if self.pairs.is_empty() {
            return Err(ConfigError::Invalid("at least one pair must be configured".to_string()));
        }

        let mut seen = std::collections::HashSet::new();
        for pair in &self.pairs {
            let key = (pair.primary.clone(), pair.secondary.clone());
            if !seen.insert(key) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate pair configured: {}/{}",
                    pair.primary, pair.secondary
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn missing_config_file_falls_back_to_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config::load(&dir.path().join("does_not_exist.toml")).await.unwrap();
        assert_eq!(config.pairs.len(), 1);
    }

    #[tokio::test]
    async fn a_valid_config_file_loads_and_validates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let config = Config::load(&path).await.unwrap();
        assert_eq!(config.risk.spread_multiplier, 1.5); // default applied
    }

    #[tokio::test]
    async fn duplicate_pairs_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let result = Config::load(&path).await;
        assert!(matches!(result, Err(ConfigError::Invalid(_))));
    }

    #[tokio::test]
    async fn spread_multiplier_at_or_below_one_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"
            [risk]
            base_risk_percent = 1.0
            max_risk_percent = 5.0
            max_open_positions = 5
            daily_loss_limit_percent = 5.0
            weekly_loss_limit_percent = 10.0
            spread_multiplier = 0.9

            [[pairs]]
            primary = "EURUSD"
            secondary = "GBPUSD"
            "#,
        )
        .await
        .unwrap();

        let result = Config::load(&path).await;
        assert!(matches!(result, Err(ConfigError::Invalid(_))));
    }
}
