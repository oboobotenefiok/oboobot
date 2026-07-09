use crate::error::{BotError, Result};
use dotenvy::dotenv;
use std::env;

pub struct Config {
    pub api_key: Option<String>,
    pub interval_seconds: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenv().ok(); // Silently ignore missing .env file

        let api_key = env::var("COINGECKO_API_KEY").ok();

        // If key is empty string or "your-api-key-here", treat as None
        let api_key = api_key.and_then(|key| {
            if key.is_empty() || key == "your-api-key-here" {
                None
            } else {
                Some(key)
            }
        });

        Ok(Config {
            api_key,
            interval_seconds: 300, // 5 minutes
        })
    }

    pub fn coingecko_url(&self) -> String {
        let base_url = "https://api.coingecko.com/api/v3/simple/price";
        let params = "?ids=bitcoin&vs_currencies=usd";

        match &self.api_key {
            Some(key) => format!("{}&x_cg_demo_api_key={}", params, key),
            None => params.to_string(),
        }
    }
}
