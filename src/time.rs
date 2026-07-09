use crate::error::{BotError, Result};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_current_time() -> Result<String> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;

    let seconds = now.as_secs();
    let hours = (seconds / 3600 % 24) + 1; // UTC+1 adjustment
    let minutes = (seconds / 60) % 60;
    let secs = seconds % 60;

    Ok(format!("{:02}:{:02}:{:02}", hours, minutes, secs))
}

pub fn parse_duration_to_seconds(hours: u64, minutes: u64) -> u64 {
    (hours * 3600) + (minutes * 60)
}
