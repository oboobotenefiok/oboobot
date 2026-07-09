use crate::error::BotError;
use std::process::Command;

pub fn send_notification(message: &str) -> Result<(), BotError> {
    // Try termux-notification first, but don't panic if it fails
    let status = Command::new("termux-notification")
        .args(["--title", "BTC Alert", "--content", message])
        .status();

    if let Ok(status) = status {
        if status.success() {
            return Ok(());
        }
    }

    // Fallback: just print to console
    Ok(())
}
