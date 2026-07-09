use crate::config::Config;
use crate::error::{BotError, Result};
use serde_json::Value;
use std::process::Command;

pub fn get_btc_price(config: &Config) -> Result<f64> {
    let url = config.coingecko_url();

    let output = Command::new("curl")
        .args(["-s", &url])
        .output()
        .map_err(|e| BotError::Command(format!("curl failed: {}", e)))?;

    let response = String::from_utf8_lossy(&output.stdout);

    parse_price_from_json(&response)
}

fn parse_price_from_json(response: &str) -> Result<f64> {
    let json: Value = serde_json::from_str(response)
        .map_err(|e| BotError::Parse(format!("JSON parse error: {}", e)))?;

    // Extract price from nested structure: {"bitcoin":{"usd":86500.0}}
    json.get("bitcoin")
        .and_then(|btc| btc.get("usd"))
        .and_then(|usd| usd.as_f64())
        .ok_or_else(|| BotError::Parse("Failed to extract price from response".to_string()))
}
