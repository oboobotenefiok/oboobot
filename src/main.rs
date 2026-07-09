mod config;
mod error;
mod notification;
mod price;
mod time;

use crate::config::Config;
use crate::notification::send_notification;
use crate::price::get_btc_price;
use crate::time::get_current_time;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;

    println!(
        "Starting price bot... Checking every {} seconds",
        config.interval_seconds
    );
    println!(
        "Using API key: {}",
        if config.api_key.is_some() {
            "Yes (authenticated)"
        } else {
            "No (keyless)"
        }
    );

    let mut last_check = Instant::now();

    loop {
        if last_check.elapsed() >= Duration::from_secs(config.interval_seconds) {
            match get_btc_price(&config) {
                Ok(price) => {
                    let now = get_current_time().unwrap_or_else(|_| "Unknown time".to_string());
                    let message = format!("[{}] BTC/USD: ${:.2}", now, price);
                    println!("{}", message);
                    let _ = send_notification(&format!("BTC: ${:.2}", price));
                }
                Err(e) => println!("Error: {}", e),
            }
            last_check = Instant::now();
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
