use std::fmt;

#[derive(Debug)]
pub enum BotError {
    Time(std::time::SystemTimeError),
    Command(String),
    Parse(String),
    Config(String),
}

impl fmt::Display for BotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BotError::Time(e) => write!(f, "Time error: {}", e),
            BotError::Command(e) => write!(f, "Command error: {}", e),
            BotError::Parse(e) => write!(f, "Parse error: {}", e),
            BotError::Config(e) => write!(f, "Config error: {}", e),
        }
    }
}

impl std::error::Error for BotError {}

impl From<std::time::SystemTimeError> for BotError {
    fn from(e: std::time::SystemTimeError) -> Self {
        BotError::Time(e)
    }
}

pub type Result<T> = std::result::Result<T, BotError>;
